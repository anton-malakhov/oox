// SPDX-License-Identifier: Apache-2.0
#include <oox/eigen/small_object_pool.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {
using namespace oox::detail::eigen_pool;

constexpr std::uint64_t seed = 0x716d4c39b2508a61ULL;
std::uint64_t Value(std::uint64_t index) {
  index ^= seed;
  index ^= index >> 29;
  return index * 0x9e3779b97f4a7c15ULL;
}

struct Small {
  Small(size_t index, std::atomic<unsigned> *destroyed)
      : index(index), value(Value(index)), destroyed(destroyed) {}
  ~Small() { destroyed[index].fetch_add(1, std::memory_order_relaxed); }
  size_t index;
  std::uint64_t value;
  std::atomic<unsigned> *destroyed;
};
struct Large { std::array<unsigned char, 513> data{}; };
struct alignas(128) Aligned { std::uint64_t value = 17; };
static_assert(uses_small_object_pool<Small>);
static_assert(!uses_small_object_pool<Large>);
static_assert(!uses_small_object_pool<Aligned>);

void ExpectReclaimed() {
  EXPECT_EQ(internal::small_object_blocks.load(), 0u);
  EXPECT_EQ(internal::small_object_owners.load(), 0u);
}

void RunCase(size_t count, unsigned pattern, size_t case_index) {
  SCOPED_TRACE(testing::Message() << "seed=" << seed << " case=" << case_index
               << " count=" << count << " pattern=" << pattern);
  std::vector<Small *> objects(count);
  std::vector<std::atomic<unsigned>> destroyed(count);
  std::vector<size_t> order(count);
  std::uint64_t random = seed + case_index;
  for (size_t i = 0; i < count; ++i)
    order[i] = i;
  for (size_t i = count; i > 1; --i) {
    random ^= random << 13;
    random ^= random >> 7;
    random ^= random << 17;
    std::swap(order[i - 1], order[random % i]);
  }
  std::atomic<bool> ready{false}, start{false}, finished{false};
  std::atomic<bool> values_ok{true};
  const auto consume = [&](size_t begin, size_t stride) {
    for (size_t j = begin; j < count; j += stride) {
      const auto i = order[j];
      auto *object = objects[i];
      if (object->index != i || object->value != Value(i))
        values_ok.store(false, std::memory_order_relaxed);
      DeleteSmallObject(object);
    }
  };
  std::thread owner([&] {
    for (size_t i = 0; i < count; ++i)
      objects[i] = NewSmallObject<Small>(i, destroyed.data());
    ready.store(true, std::memory_order_release);
    ready.notify_one();
    start.wait(false, std::memory_order_acquire);
    if (pattern == 0)
      consume(0, 1);
    if (pattern == 1)
      finished.wait(false, std::memory_order_acquire);
    // Patterns 2 and 3 close the owner with live objects.
  });
  ready.wait(false, std::memory_order_acquire);
  auto addresses = objects;
  std::sort(addresses.begin(), addresses.end(), std::less<Small *>{});
  EXPECT_EQ(std::adjacent_find(addresses.begin(), addresses.end()),
            addresses.end());
  if (pattern == 2) {
    start.store(true, std::memory_order_release);
    start.notify_all();
    owner.join();
  }
  std::vector<std::thread> consumers;
  if (pattern != 0) {
    for (size_t worker = 0; worker < 4; ++worker)
      consumers.emplace_back([&, worker] {
        start.wait(false, std::memory_order_acquire);
        consume(worker, 4);
      });
  }
  start.store(true, std::memory_order_release);
  start.notify_all();
  for (auto &consumer : consumers)
    consumer.join();
  finished.store(true, std::memory_order_release);
  finished.notify_one();
  if (owner.joinable())
    owner.join();
  EXPECT_TRUE(values_ok.load());
  // Independent flat oracle: each allocation has its original value and
  // exactly one destructor call, regardless of the generated release order.
  for (size_t i = 0; i < count; ++i)
    EXPECT_EQ(destroyed[i].load(), 1u) << "item=" << i;
  ExpectReclaimed();
}

TEST(EigenSmallObjectPool, GeneratedLifetimeOracle) {
  size_t case_index = 0;
  for (size_t count = 0; count < 48; ++count)
    for (unsigned pattern = 0; pattern < 4; ++pattern)
      RunCase(count, pattern, case_index++);
  for (size_t count : {257u, 4097u, 65537u, 262147u})
    for (unsigned pattern = 0; pattern < 4; ++pattern)
      RunCase(count, pattern, case_index++);
}

TEST(EigenSmallObjectPool, ReusesAndBoundsLocalCache) {
  std::thread owner([] {
    std::array<std::atomic<unsigned>, 4097> destroyed{};
    auto *first = NewSmallObject<Small>(0, destroyed.data());
    const auto address = reinterpret_cast<uintptr_t>(first);
    DeleteSmallObject(first);
    auto *second = NewSmallObject<Small>(0, destroyed.data());
    EXPECT_EQ(reinterpret_cast<uintptr_t>(second), address);
    DeleteSmallObject(second);
    std::vector<Small *> objects;
    for (size_t i = 0; i < destroyed.size(); ++i)
      objects.push_back(NewSmallObject<Small>(i, destroyed.data()));
    for (auto *object : objects)
      DeleteSmallObject(object);
    EXPECT_LE(internal::small_object_blocks.load(), 256u);
    EXPECT_EQ(destroyed[0].load(), 3u);
    for (size_t i = 1; i < destroyed.size(); ++i)
      EXPECT_EQ(destroyed[i].load(), 1u);
  });
  owner.join();
  ExpectReclaimed();
}

TEST(EigenSmallObjectPool, BoundsRemoteCacheAndReusesReturnedBlocks) {
  std::array<std::atomic<unsigned>, 4097> destroyed{};
  std::vector<Small *> objects(destroyed.size());
  std::atomic<bool> ready{false}, returned{false};
  std::thread owner([&] {
    for (size_t i = 0; i < objects.size(); ++i)
      objects[i] = NewSmallObject<Small>(i, destroyed.data());
    ready.store(true, std::memory_order_release);
    ready.notify_one();
    returned.wait(false, std::memory_order_acquire);
    EXPECT_LE(internal::small_object_blocks.load(), 256u);
    auto *reused = NewSmallObject<Small>(0, destroyed.data());
    DeleteSmallObject(reused);
  });
  ready.wait(false, std::memory_order_acquire);
  for (auto *object : objects)
    DeleteSmallObject(object); // Freeing does not initialize a local owner.
  EXPECT_EQ(internal::small_object_owners.load(), 1u);
  returned.store(true, std::memory_order_release);
  returned.notify_one();
  owner.join();
  EXPECT_EQ(destroyed[0].load(), 2u);
  for (size_t i = 1; i < destroyed.size(); ++i)
    EXPECT_EQ(destroyed[i].load(), 1u);
  ExpectReclaimed();
}

TEST(EigenSmallObjectPool, ConstructionFailureReturnsStorage) {
  struct Throwing {
    Throwing() { throw std::runtime_error("construction"); }
  };
  std::thread owner([] {
    for (unsigned i = 0; i < 512; ++i)
      EXPECT_THROW(NewSmallObject<Throwing>(), std::runtime_error);
    EXPECT_EQ(internal::small_object_blocks.load(), 1u);
  });
  owner.join();
  ExpectReclaimed();
}

TEST(EigenSmallObjectPool, OversizedAndOveralignedObjectsUseFallback) {
  auto *large = NewSmallObject<Large>();
  auto *aligned = NewSmallObject<Aligned>();
  EXPECT_EQ(reinterpret_cast<uintptr_t>(aligned) % alignof(Aligned), 0u);
  large->data.back() = 91;
  EXPECT_EQ(large->data.back(), 91);
  EXPECT_EQ(aligned->value, 17u);
  DeleteSmallObject(large);
  DeleteSmallObject(aligned);
  ExpectReclaimed();
}


TEST(EigenSmallObjectPool, CapacityBoundaryAndFundamentalAlignment) {
  struct alignas(std::max_align_t) Boundary {
    std::array<unsigned char, 256> bytes{};
  };
  struct Overflow { std::array<unsigned char, 257> bytes{}; };
  static_assert(sizeof(Boundary) == 256 && uses_small_object_pool<Boundary>);
  static_assert(!uses_small_object_pool<Overflow>);
  std::thread owner([] {
    auto *small = NewSmallObject<Boundary>();
    auto *large = NewSmallObject<Overflow>();
    EXPECT_EQ(reinterpret_cast<uintptr_t>(small) % alignof(Boundary), 0u);
    for (size_t i = 0; i < small->bytes.size(); ++i)
      small->bytes[i] = static_cast<unsigned char>(i);
    large->bytes.back() = 71;
    for (size_t i = 0; i < small->bytes.size(); ++i)
      EXPECT_EQ(small->bytes[i], static_cast<unsigned char>(i));
    EXPECT_EQ(large->bytes.back(), 71);
    DeleteSmallObject(small);
    DeleteSmallObject(large);
  });
  owner.join();
  ExpectReclaimed();
}

TEST(EigenSmallObjectPool, AllocationAfterThreadLocalOwnerShutdown) {
  struct LateAllocation {
    std::atomic<bool> *completed = nullptr;
    ~LateAllocation() {
      struct Value { unsigned value = 29; };
      auto *value = NewSmallObject<Value>();
      completed->store(value->value == 29, std::memory_order_relaxed);
      DeleteSmallObject(value);
    }
  };
  std::atomic<bool> completed{false};
  std::thread owner([&] {
    // This object is initialized before the allocator owner, so its destructor
    // runs after the allocator's thread-local teardown.
    static thread_local LateAllocation late;
    late.completed = &completed;
    struct Value { unsigned value = 11; };
    DeleteSmallObject(NewSmallObject<Value>());
  });
  owner.join();
  EXPECT_TRUE(completed.load());
  ExpectReclaimed();
}
} // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
