// SPDX-License-Identifier: Apache-2.0
#include <functional>
#include <optional>
namespace lazy_wait_observer {
inline std::function<void()> prepared;
inline std::function<void()> acquired;
}
#define OOX_EIGEN_TEST_WAIT_PREPARED() \
  do { if (lazy_wait_observer::prepared) lazy_wait_observer::prepared(); } while (false)
#define OOX_EIGEN_TEST_LAZY_TASK_ACQUIRED() \
  do { if (lazy_wait_observer::acquired) lazy_wait_observer::acquired(); } while (false)
#include <oox/eigen/rapid_auto.h>
#include "eigen_test_wait.h"
#include "test_support/eigen_resident.h"
#include <gtest/gtest.h>

namespace {
using namespace oox::detail::eigen_pool;
using namespace oox::detail::eigen_pool::rapid;
using namespace std::chrono_literals;

TEST(EigenRapidLazy, PublishedStackRangeCanBeSafelyReused) {
  std::atomic<uintptr_t> slot{0};
  std::atomic<bool> stop{false}, corrupted{false};
  std::atomic<unsigned> visits{0};
  struct Range final : StealableRange {
    Range(std::atomic<bool> &bad, std::atomic<unsigned> &seen) : bad(bad), seen(seen) {}
    Task *Steal() noexcept final {
      if (magic != 0x300a01) bad = true;
      ++seen;
      if (magic != 0x300a01) bad = true;
      return nullptr;
    }
    ~Range() { magic = 0; }
    unsigned magic = 0x300a01;
    std::atomic<bool> &bad;
    std::atomic<unsigned> &seen;
  };
  std::thread thief([&] {
    while (!stop.load(std::memory_order_acquire)) StealableRange::TrySteal(slot);
  });
  for (unsigned generation = 0; generation < 10000; ++generation) {
    Range range(corrupted, visits);
    range.Publish(slot);
    if (!generation)
      eigen_test_support::WaitUntil([&] { return visits != 0; }, "first stack range theft");
    range.Unpublish();
  }
  stop.store(true, std::memory_order_release);
  thief.join();
  EXPECT_FALSE(corrupted.load());
  EXPECT_EQ(slot.load(), 0u);
}

TEST(EigenRapidLazy, PublishedOwnerRetainsLeadingHalfBeforeRun) {
  constexpr uint64_t seed = 0x101026f1;
  ThreadPool pool(1, false, false);
  size_t test = 0;
  for (size_t size : {2u, 3u, 7u, 17u, 65u}) for (size_t grain : {1u, 2u, 7u}) {
    if (size < 2 * grain) continue;
    SCOPED_TRACE(::testing::Message() << "seed=" << seed << " case=" << test++
        << " size=" << size << " grain=" << grain);
    const size_t begin = test % 2 ? size_t(-1) - size : 17;
    std::vector<std::atomic<unsigned>> actual(size);
    std::vector<unsigned> expected(size);
    for (size_t i = 0; i < size; ++i) expected[i] = unsigned((begin + i) % 23 + 1);
    std::atomic<bool> outside{false};
    auto body = [&](size_t i) {
      if (i < begin || i - begin >= size) { outside = true; return; }
      actual[i - begin].fetch_add(unsigned(i % 23 + 1));
    };
    using State = partitioning::LazyAutoPartitionState;
    using Work = partitioner_detail::Work<decltype(body), State>;
    auto *region = NewSmallObject<partitioner_detail::Region>(pool, nullptr, true);
    std::atomic<uintptr_t> slot{0};
    {
      partitioner_detail::Branch<Work, true> branch(nullptr);
      partitioner_detail::ActiveRange<decltype(body), State> owner(
          *region, &body, {begin, begin + size, grain}, State(pool.NumThreads()), branch);
      owner.Publish(slot);
      std::thread receiver([&] {
        if (Task *task = StealableRange::TrySteal(slot)) (*task)();
      });
      receiver.join();
      // Tail theft must retain the leading half even before owner Run begins.
      // Retaining only the first grain is insufficient for donation progress.
      for (size_t i = 0; i < size / 2; ++i)
        EXPECT_EQ(actual[i].load(), 0u) << "leading owner half migrated before Run; index=" << i;
      size_t chunks = 0;
      owner.Run(chunks);
      owner.Close();
    }
    // The unregistered receiver may have published further ordinary children.
    // Join them before the callback or its output storage leaves this frame.
    pool.Wait([&] { return region->IsComplete(); });
    region->CloseAndWait();
    region->TaskComplete();
    EXPECT_EQ(slot.load(), 0u);
    EXPECT_FALSE(outside.load());
    for (size_t i = 0; i < size; ++i) EXPECT_EQ(actual[i].load(), expected[i]) << "index=" << i;
  }
}

TEST(EigenRapidLazy, NestedPublicationRestoresOuterRange) {
  std::atomic<uintptr_t> slot{0};
  struct Range final : StealableRange {
    Task *Steal() noexcept final { ++visits; return nullptr; }
    unsigned visits = 0;
  } outer, inner;
  outer.Publish(slot);
  StealableRange::TrySteal(slot);
  inner.Publish(slot);
  StealableRange::TrySteal(slot);
  inner.Unpublish();
  StealableRange::TrySteal(slot);
  outer.Unpublish();
  EXPECT_EQ(outer.visits, 2u);
  EXPECT_EQ(inner.visits, 1u);
  EXPECT_EQ(slot.load(), 0u);
}

TEST(EigenRapidLazy, AcquiredWorkDoesNotAdvertiseItsCallerAsIdle) {
  std::atomic<bool> started{false}, release{false};
  ThreadPool pool(2, false, true);
  struct ReleaseWorker {
    std::atomic<bool> &release;
    ~ReleaseWorker() { release = true; release.notify_one(); }
  } release_worker{release};
  pool.RunOnThread(MakeTask([&] {
    started = true;
    release.wait(false);
  }), 1);
  eigen_test_support::WaitUntil([&] { return started.load(); }, "hold the background worker");
  bool ready = false, injected = false, advertised = true;
  struct Reset {
    ~Reset() { lazy_wait_observer::prepared = {}; }
  } reset;
  lazy_wait_observer::prepared = [&] {
    if (std::exchange(injected, true)) return;
    pool.Schedule(MakeTask([&] {
      advertised = pool.HasWaitingHelper();
      ready = true;
    }));
  };
  pool.Wait([&] { return ready; });
  EXPECT_TRUE(injected);
  EXPECT_FALSE(advertised);
  EXPECT_FALSE(pool.HasWaitingHelper());
}

TEST(EigenRapidLazy, BusyOwnersDoNotPublishSpeculativeTasks) {
  constexpr uint64_t seed = 0x300930d;
  uint64_t random = seed;
  for (unsigned workers : {2u, 3u, 8u}) {
    ThreadPool pool(workers, true, true, WorkerIdleMode::ResidentBusy);
    RapidDomainState domain(pool);
    RapidStartGroup group{&domain, {0, workers}};
    for (unsigned test = 0; test < 40; ++test) {
      random = random * 6364136223846793005ULL + 1;
      const size_t size = test < 32 ? workers + test : test < 37
          ? 64 + random % 2048 : (size_t{1} << (test - 19)) + random % 31;
      const size_t begin = test % 2 ? size_t(-1) - size : random % 1024;
      SCOPED_TRACE(::testing::Message() << "seed=" << seed << " case=" << test
          << " workers=" << workers << " begin=" << begin << " size=" << size);
      // Independent serial equal-slice model: distribute extras, then
      // accumulate endpoints without Launch's product/min formula.
      std::vector<size_t> lengths(workers, size / workers);
      for (size_t extra = 0; extra < size % workers; ++extra) ++lengths[extra];
      std::vector<std::pair<size_t, size_t>> ranges;
      size_t cursor = 0;
      for (size_t length : lengths) {
        ranges.emplace_back(cursor, cursor + length);
        cursor += length;
      }
      ASSERT_EQ(ranges.size(), workers);
      std::vector<unsigned char> first(size), last(size);
      for (const auto &[a, b] : ranges) { first[a] = 1; last[b - 1] = 1; }
      std::vector<std::atomic<unsigned>> actual(size);
      std::vector<unsigned> expected(size);
      for (size_t i = 0; i < size; ++i) expected[i] = unsigned((begin + i) % 31 + 1);
      std::atomic<unsigned> entered{0}, finished{0};
      std::atomic<bool> outside{false};
      partitioner_detail::Metrics metrics;
      size_t activated = 0;
      eigen_test_support::WaitForResidentWorkers(group, 5s);
      ParallelForAuto(group, begin, begin + size, [&](size_t i) {
        if (i < begin || i - begin >= size) { outside = true; return; }
        const size_t offset = i - begin;
        if (first[offset]) {
          ++entered;
          eigen_test_support::WaitUntil([&] { return entered == workers; }, "all lazy owners entered");
        }
        actual[offset].fetch_add(unsigned(i % 31 + 1), std::memory_order_relaxed);
        if (last[offset]) {
          ++finished;
          eigen_test_support::WaitUntil([&] { return finished == workers; }, "all lazy owners finished");
        }
      }, 1, &metrics, &activated);
      EXPECT_EQ(activated, workers - 1);
      EXPECT_EQ(metrics.range_tasks.load(), 0u);
      EXPECT_EQ(metrics.initial_tasks.load(), 0u);
      EXPECT_EQ(metrics.donated_ranges.load(), 0u);
      EXPECT_FALSE(outside.load());
      for (size_t i = 0; i < size; ++i)
        ASSERT_EQ(actual[i].load(), expected[i]) << "index=" << i;
    }
  }
}

TEST(EigenRapidLazy, FinishedResidentReceivesWorkFromAnActiveOwner) {
  ThreadPool pool(2, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  RapidStartGroup group{&domain, {0, 2}};
  constexpr size_t size = 4096;
  std::vector<std::atomic<unsigned>> actual(size);
  std::atomic<bool> helper_received{false};
  partitioner_detail::Metrics metrics;
  eigen_test_support::WaitForResidentWorkers(group, 5s);
  ParallelForAuto(group, 0, size, [&](size_t i) {
    if (i == 0)
      eigen_test_support::WaitUntil([&] { return pool.ResidentAvailableWorkers(group.domain) == 1; },
                                    "helper has finished its initial range");
    if (i > 0 && i < size / 2 && pool.CurrentThreadId() == 1) helper_received = true;
    if (i == 1)
      eigen_test_support::WaitUntil([&] { return helper_received.load(); }, "resident receives donated range");
    actual[i].fetch_add(unsigned(i % 23 + 1));
  }, 1, &metrics);
  EXPECT_TRUE(helper_received.load());
  EXPECT_GT(metrics.donated_ranges.load(), 0u);
  for (size_t i = 0; i < size; ++i) EXPECT_EQ(actual[i].load(), unsigned(i % 23 + 1));
}

TEST(EigenRapidLazy, FinishedCallerReceivesWorkBeforeResidentCommandCompletes) {
  ThreadPool pool(2, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  RapidStartGroup group{&domain, {0, 2}};
  constexpr size_t size = 4096;
  std::vector<std::atomic<unsigned>> actual(size);
  std::atomic<bool> caller_received{false};
  partitioner_detail::Metrics metrics;
  eigen_test_support::WaitForResidentWorkers(group, 5s);
  ParallelForAuto(group, 0, size, [&](size_t i) {
    if (i == size / 2)
      eigen_test_support::WaitUntil([&] { return caller_received.load(); }, "caller steals while owner is in callback");
    if (i > size / 2 && pool.CurrentThreadId() == 0) caller_received = true;
    actual[i].fetch_add(unsigned(i % 29 + 1));
  }, 1, &metrics);
  EXPECT_TRUE(caller_received.load());
  EXPECT_GT(metrics.donated_ranges.load(), 0u);
  for (size_t i = 0; i < size; ++i) EXPECT_EQ(actual[i].load(), unsigned(i % 29 + 1));
}

TEST(EigenRapidLazy, AnotherIdleHelperDoesNotWaitForTheFirstDonationToFinish) {
  ThreadPool pool(4, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  RapidStartGroup group{&domain, {0, 4}};
  constexpr size_t size = 4096;
  std::vector<std::atomic<unsigned>> actual(size);
  std::atomic<bool> first_started{false}, second_received{false};
  std::atomic<unsigned> acquisitions{0};
  partitioner_detail::Metrics metrics;
  eigen_test_support::WaitForResidentWorkers(group, 5s);
  struct Reset {
    ~Reset() { lazy_wait_observer::acquired = {}; }
  } reset;
  // Stop the first receiver before it can subdivide its own range. The
  // original owner is the only producer able to serve the other idle helpers.
  lazy_wait_observer::acquired = [&] {
    if (acquisitions.fetch_add(1) == 0) {
      first_started = true;
      eigen_test_support::WaitUntil([&] { return second_received.load(); }, "second donation while first is active");
    } else second_received = true;
  };
  auto body = [&](size_t i) {
    if (i == 0)
      eigen_test_support::WaitUntil([&] { return first_started.load(); }, "first donation has started");
    if (i == 1)
      eigen_test_support::WaitUntil([&] { return second_received.load(); }, "another idle helper receives work");
    actual[i].fetch_add(unsigned(i % 19 + 1));
  };
  using Launch = batch_detail::Launch<decltype(body), 8>;
  auto *region = NewSmallObject<partitioner_detail::Region>(pool, &metrics);
  std::optional<Launch> launch(std::in_place, group, *region, &body);
  launch->Prepare({0, size, 1}, 0);
  launch->Run(0);
  pool.Wait([&] { return region->IsComplete(); });
  region->CloseAndWait();
  launch.reset();
  region->TaskComplete();
  EXPECT_TRUE(first_started.load());
  EXPECT_TRUE(second_received.load());
  EXPECT_GE(metrics.donated_ranges.load(), 2u);
  for (size_t i = 0; i < size; ++i) EXPECT_EQ(actual[i].load(), unsigned(i % 19 + 1));
}
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
