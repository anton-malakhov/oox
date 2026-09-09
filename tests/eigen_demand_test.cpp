// SPDX-License-Identifier: Apache-2.0

#include <oox/eigen/nonblocking_thread_pool.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <deque>
#include <future>
#include <numeric>
#include <random>
#include <thread>
#include <vector>

namespace {

using namespace oox::detail::eigen_pool;
using partitioning::AutoPartition;
using partitioning::IndexRange;
using partitioning::RangePool;
using partitioning::Split;
using namespace std::chrono_literals;

TEST(TbbPartition, PinnedRangeTrace) {
  RangePool<IndexRange> ranges(IndexRange{0, 17});
  ranges.Fill(3);
  // oneTBB 2021.5.0 split_to_fill: old/right ranges at the front,
  // new/left range at the back. Depth is attached to each range.
  const std::array<std::array<std::size_t, 3>, 4> expected{{
      {8, 17, 1}, {4, 8, 2}, {2, 4, 3}, {0, 2, 3}}};
  for (const auto &value : expected) {
    ASSERT_FALSE(ranges.Empty());
    EXPECT_EQ(ranges.Front().begin, value[0]);
    EXPECT_EQ(ranges.Front().end, value[1]);
    EXPECT_EQ(ranges.FrontDepth(), value[2]);
    ranges.PopFront();
  }
  EXPECT_TRUE(ranges.Empty());
}

TEST(TbbPartition, PinnedAutomaticTransitions) {
  AutoPartition root(4);
  EXPECT_EQ(root.Divisor(), 8u);
  EXPECT_EQ(root.MaxDepth(), 5u);
  EXPECT_FALSE(root.CheckBeingStolen(true, true)); // Initial distribution.
  AutoPartition child(root, Split{});
  EXPECT_EQ(root.Divisor(), 4u);
  EXPECT_EQ(child.Divisor(), 4u);
  AutoPartition grandchild(child, Split{});
  AutoPartition leaf(grandchild, Split{});
  EXPECT_EQ(leaf.Divisor(), 1u);
  ASSERT_TRUE(leaf.IsDivisible()); // Balancing task.
  EXPECT_EQ(leaf.Divisor(), 0u);
  EXPECT_EQ(leaf.MaxDepth(), 4u);
  AutoPartition offered(leaf, Split{});
  offered.AlignDepth(2);
  EXPECT_EQ(offered.MaxDepth(), 2u);
  EXPECT_TRUE(offered.CheckBeingStolen(true, true));
  EXPECT_EQ(offered.Divisor(), 1u);
  EXPECT_EQ(offered.MaxDepth(), 3u);
  EXPECT_TRUE(leaf.CheckForDemand(true));
  EXPECT_EQ(leaf.MaxDepth(), 5u);
  EXPECT_FALSE(leaf.CheckForDemand(false)); // New sibling group.
  EXPECT_EQ(leaf.MaxDepth(), 5u);
}

TEST(TbbPartition, LocalAndFinishedPeersDoNotSignal) {
  for (const auto flags : {std::pair{false, true}, std::pair{true, false}}) {
    AutoPartition part(1, 0);
    ASSERT_TRUE(part.IsDivisible());
    AutoPartition child(part, Split{});
    EXPECT_EQ(child.Divisor(), 1u);
    // Obtain the zero-divisor state used by a balancing child.
    AutoPartition zero(child, Split{});
    EXPECT_FALSE(zero.CheckBeingStolen(flags.first, flags.second));
    EXPECT_EQ(zero.MaxDepth(), 0u);
  }
  AutoPartition bounded(1, AutoPartition::max_depth);
  for (unsigned i = 0; i < 1000; ++i)
    bounded.CheckForDemand(true);
  EXPECT_EQ(bounded.MaxDepth(), AutoPartition::max_depth);
}

// Independent oracle: ordinary deque of intervals; no circular indices,
// optional storage, production splitting constructor, or depth policy.
TEST(TbbPartition, GeneratedRangePoolAgainstDequeOracle) {
  constexpr std::uint64_t seed = 0x74524242504f4f4cULL;
  std::mt19937_64 random(seed);
  struct Span { std::size_t first, last; unsigned depth; };
  for (std::size_t n = 1; n <= 257; ++n) {
    for (int case_index = 0; case_index < 24; ++case_index) {
      SCOPED_TRACE(::testing::Message() << "seed=" << seed << " n=" << n
                                       << " case=" << case_index);
      const auto first = case_index % 2 ? std::size_t{100}
          : std::numeric_limits<std::size_t>::max() - n;
      RangePool<IndexRange> actual(IndexRange{first, first + n});
      std::deque<Span> expected{{first, first + n, 0}};
      std::vector<unsigned> visits(n);
      while (!expected.empty()) {
        const unsigned depth = random() % 10;
        while (expected.size() < 8 && expected.back().depth < depth &&
               expected.back().last - expected.back().first > 1) {
          const Span span = expected.back();
          expected.pop_back();
          const auto mid = span.first + (span.last - span.first) / 2;
          expected.push_back({mid, span.last, span.depth + 1});
          expected.push_back({span.first, mid, span.depth + 1});
        }
        actual.Fill(depth);
        ASSERT_EQ(actual.Size(), expected.size());
        const bool front = random() & 1;
        const auto span = front ? expected.front() : expected.back();
        const auto range = front ? actual.Front() : actual.Back();
        EXPECT_EQ(range.begin, span.first);
        EXPECT_EQ(range.end, span.last);
        EXPECT_EQ(front ? actual.FrontDepth() : actual.BackDepth(), span.depth);
        for (std::size_t i = span.first; i < span.last; ++i)
          ++visits[i - first];
        if (front) { expected.pop_front(); actual.PopFront(); }
        else { expected.pop_back(); actual.PopBack(); }
      }
      EXPECT_TRUE(actual.Empty());
      EXPECT_TRUE(std::all_of(visits.begin(), visits.end(),
                              [](unsigned count) { return count == 1; }));
    }
  }
}

TEST(EigenDemand, SiblingStealFeedsBackAndRenewsGroup) {
  using Registry = DemandRegistry<std::size_t, DemandPolicy>;
  Registry owner, first_thief, second_thief;
  std::deque<std::size_t> input;
  for (std::size_t i = 1; i <= 64; ++i)
    input.push_back(i);
  auto pop = [&] {
    if (input.empty()) return std::size_t{0};
    auto value = input.front(); input.pop_front(); return value;
  };
  std::vector<unsigned> visits(65);
  {
    auto left = owner.AcquireLocal(0, pop);
    ASSERT_TRUE(left);
    ++visits[left.task];
    auto initial = first_thief.AcquireRemote(owner, 1);
    ASSERT_TRUE(initial);
    ++visits[initial.task];
    auto balancing = second_thief.AcquireRemote(owner, 2);
    ASSERT_TRUE(balancing);
    ++visits[balancing.task];
    EXPECT_EQ(second_thief.Statistics().feedback, 1u);
    const auto before = owner.Statistics().offers;
    auto next = owner.AcquireLocal(0, pop);
    ASSERT_TRUE(next);
    ++visits[next.task];
    EXPECT_GT(owner.Statistics().offers, before);
  }
  auto record = [&](std::size_t task) { ++visits[task]; };
  owner.Drain(record);
  first_thief.Drain(record);
  second_thief.Drain(record);
  for (std::size_t i = 1; i <= 64; ++i)
    EXPECT_EQ(visits[i], 1u) << i;
}

TEST(EigenDemand, GeneratedGroupOwnershipAgainstSerialIDs) {
  constexpr std::uint64_t seed = 0x44454d414e445442ULL;
  std::mt19937_64 random(seed);
  using Registry = DemandRegistry<std::size_t, DemandPolicy>;
  for (std::size_t count = 0; count <= 257; ++count) {
    SCOPED_TRACE(::testing::Message() << "seed=" << seed << " count=" << count);
    std::array<Registry, 4> registries;
    std::array<std::deque<std::size_t>, 4> input;
    std::vector<unsigned> visits(count + 1);
    for (std::size_t i = 1; i <= count; ++i)
      input[random() % 4].push_back(i);
    std::size_t completed = 0;
    for (std::size_t step = 0; step < 10000 && completed < count; ++step) {
      const auto worker = random() % 4;
      auto pop = [&] {
        if (input[worker].empty()) return std::size_t{0};
        auto value = input[worker].back(); input[worker].pop_back(); return value;
      };
      auto task = registries[worker].AcquireLocal(worker, pop);
      if (!task)
        task = registries[worker].AcquireRemote(registries[random() % 4], worker);
      if (task) {
        ASSERT_LE(task.task, count);
        ++visits[task.task];
        ++completed;
      }
    }
    EXPECT_EQ(completed, count);
    for (auto &registry : registries)
      registry.Drain([&](std::size_t task) { ++visits[task]; });
    for (std::size_t i = 1; i <= count; ++i)
      EXPECT_EQ(visits[i], 1u) << "task=" << i;
  }
}

TEST(EigenDemand, PendingGroupCanBeHelpedWhileCallbackIsActive) {
  DemandRegistry<std::size_t, DemandPolicy> registry;
  std::size_t next = 0;
  auto pop = [&] { return next < 64 ? ++next : std::size_t{0}; };
  auto parent = registry.AcquireLocal(0, pop);
  ASSERT_TRUE(parent);
  std::vector<unsigned> visits(65);
  ++visits[parent.task];
  // Keep the parent token live while helping the rest of its collected peers.
  for (std::size_t i = 1; i < 64; ++i) {
    auto child = registry.AcquireLocal(0, pop);
    ASSERT_TRUE(child);
    ++visits[child.task];
  }
  EXPECT_FALSE(registry.AcquireLocal(0, pop));
  for (std::size_t i = 1; i <= 64; ++i)
    EXPECT_EQ(visits[i], 1u);
}

TEST(EigenDemand, ConcurrentThievesKeepEveryTaskExactlyOnce) {
  constexpr std::size_t count = 200000;
  DemandThreadPool pool(4, false, false);
  auto visits = std::make_unique<std::atomic<unsigned>[]>(count);
  std::atomic<std::size_t> finished{0};
  std::promise<void> done;
  auto future = done.get_future();
  pool.Schedule(MakeTask([&] {
    for (std::size_t i = 0; i < count; ++i) {
      pool.Schedule(MakeTask([&, i] {
        visits[i].fetch_add(1, std::memory_order_relaxed);
        if (finished.fetch_add(1) + 1 == count)
          done.set_value();
      }));
    }
  }));
  if (future.wait_for(20s) != std::future_status::ready) {
    pool.Cancel();
    FAIL() << "case=large-flat count=" << count;
  }
  for (std::size_t i = 0; i < count; ++i)
    ASSERT_EQ(visits[i].load(), 1u) << "case=large-flat task=" << i;
}

TEST(EigenDemand, ExceptionsReleaseCallableAndKeepWorkerAlive) {
  DemandThreadPool pool(1, false, false);
  auto object = std::make_shared<int>(7);
  std::weak_ptr<int> weak = object;
  std::promise<void> done;
  auto future = done.get_future();
  pool.Schedule(MakeTask([object = std::move(object)] {
    throw std::runtime_error("task failed");
  }));
  pool.Schedule(MakeTask([&] { done.set_value(); }));
  ASSERT_EQ(future.wait_for(5s), std::future_status::ready);
  pool.Wait([&] { return weak.expired(); }); // Already ready after preceding task.
  EXPECT_TRUE(weak.expired());
}

TEST(EigenDemand, CancellationDiscardsGroupedAndQueuedWork) {
  std::atomic<unsigned> disposed{0};
  struct Item : Task {
    std::atomic<unsigned> &disposed;
    explicit Item(std::atomic<unsigned> &value) : disposed(value) {}
    ~Item() override { ++disposed; }
    void operator()() override { delete this; }
  };
  constexpr unsigned count = 10000;
  {
    DemandThreadPool pool(2, false, false);
    for (unsigned i = 0; i < count; ++i)
      pool.Schedule(new Item(disposed));
    pool.Cancel();
  }
  EXPECT_EQ(disposed.load(), count);
}

} // namespace
