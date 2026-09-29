// SPDX-License-Identifier: Apache-2.0
#include <atomic>
#include <vector>
namespace rapid_dispatch_observers {
inline std::atomic<unsigned> batch_publications{0}, shared_admissions{0};
inline thread_local std::vector<size_t> *direct_indices = nullptr;
}
#define OOX_EIGEN_TEST_RAPID_BATCH_PUBLICATION(count) \
  rapid_dispatch_observers::batch_publications.fetch_add(1, std::memory_order_relaxed)
#define OOX_EIGEN_TEST_RAPID_SHARED_ADMISSION(count) \
  rapid_dispatch_observers::shared_admissions.fetch_add(1, std::memory_order_relaxed)
#define OOX_EIGEN_TEST_RAPID_DIRECT_SEED(slot, index) \
  do { if (rapid_dispatch_observers::direct_indices) \
    rapid_dispatch_observers::direct_indices->push_back(index); } while (false)
#include <oox/eigen/rapid_auto.h>
#include <gtest/gtest.h>

namespace {
using namespace oox::detail::eigen_pool;
using namespace oox::detail::eigen_pool::rapid;

using PlainWork = partitioner_detail::Work<void (*)(size_t), partitioning::AutoPartitionState>;
static_assert(alignof(batch_detail::Launch<void (*)(size_t), 8>) <=
              internal::SmallObjectPool::alignment);
#if OOX_EIGEN_SMALL_OBJECT_POOL
static_assert(uses_small_object_pool<PlainWork>);
#endif

TEST(EigenRapidDispatch, ExpandedSeedsMatchIndependentOracle) {
  constexpr uint64_t seed = 0x280928;
  uint64_t random = seed;
  ThreadPool pool(8, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  struct Expected {
    size_t begin, end, budget;
    unsigned depth = 5, owner = 0;
    bool queued = false;
  };
  for (size_t test = 0; test < 100; ++test) {
    random = random * 6364136223846793005ULL + 1;
    const size_t size = test < 64 ? test + 1 : test >= 97
        ? size_t{1} << (18 + test - 97) : 65 + random % 8192;
    const size_t grain = test % 7 + 1;
    const size_t helpers = test >= 97 ? 7 : random % 8;
    const size_t begin = test % 2 ? size_t(-1) - size : random % 4096;
    SCOPED_TRACE(::testing::Message() << "seed=" << seed << " case=" << test);
    std::vector<std::atomic<unsigned>> actual(size);
    std::vector<unsigned> expected_values(size);
    for (size_t i = 0; i < size; ++i) expected_values[i] = (begin + i) % 29 + 1;
    std::atomic<bool> outside{false};
    auto body = [&](size_t i) {
      if (i < begin || i - begin >= size) { outside = true; return; }
      actual[i - begin].fetch_add(i % 29 + 1, std::memory_order_relaxed);
    };
    using Launch = batch_detail::Launch<decltype(body), 8>;
    auto *region = NewSmallObject<partitioner_detail::Region>(pool, nullptr);
    auto *launch = NewSmallObject<Launch>(RapidStartGroup{&domain, {0, 8}}, *region, &body);
    launch->Prepare({begin, begin + size, grain}, helpers, true);

    // Deliberately plain serial model: integers and a FIFO, not partitioner
    // state or range classes. In particular budget==1 consumes one depth.
    std::vector<Expected> expected{{begin, begin + size, 16}};
    std::vector<size_t> pending{0};
    for (size_t head = 0; expected.size() < helpers + 1 && head < pending.size();) {
      const auto index = pending[head++];
      const auto old = expected[index];
      if (old.end - old.begin <= grain || old.budget <= 1) continue;
      const size_t middle = old.begin + (old.end - old.begin) / 2;
      expected[index] = {old.begin, middle, old.budget / 2};
      expected.push_back({middle, old.end, old.budget / 2});
      pending.push_back(index);
      pending.push_back(expected.size() - 1);
    }
    for (size_t i = 0; i < expected.size(); ++i) expected[i].owner = unsigned(i);
    for (size_t i = 0; i < expected.size(); ++i) {
      if (expected[i].budget > 2) continue;
      while (expected[i].end - expected[i].begin > grain) {
        auto old = expected[i];
        if (old.budget <= 1) {
          if (!old.budget || !old.depth) break;
          --old.depth;
          old.budget = 0;
        }
        const size_t middle = old.begin + (old.end - old.begin) / 2;
        expected[i] = {old.begin, middle, old.budget / 2, old.depth, old.owner, false};
        expected.push_back({middle, old.end, old.budget / 2, old.depth, old.owner, true});
      }
    }
    EXPECT_EQ(launch->SeedCount(), expected.size());
    std::array<unsigned, 8> direct{}, queued{};
    std::vector<uintptr_t> nodes;
    for (size_t i = 0; i < std::min(launch->SeedCount(), expected.size()); ++i) {
      const auto &value = launch->GetSeed(i);
      const auto &oracle = expected[i];
      EXPECT_EQ(value.range.begin, oracle.begin);
      EXPECT_EQ(value.range.end, oracle.end);
      EXPECT_EQ(value.state.divisor, oracle.budget);
      EXPECT_EQ(value.state.max_depth, oracle.depth);
      EXPECT_EQ(value.owner, oracle.owner);
      EXPECT_EQ(value.queued, oracle.queued);
      ++(value.queued ? queued[value.owner] : direct[value.owner]);
      for (auto *node = value.parent; !Launch::Join::IsRoot(node); node = node->parent)
        nodes.push_back(reinterpret_cast<uintptr_t>(node));
    }
    std::sort(nodes.begin(), nodes.end());
    nodes.erase(std::unique(nodes.begin(), nodes.end()), nodes.end());
    for (size_t i = 1; i < nodes.size(); ++i)
      EXPECT_GE(nodes[i] - nodes[i - 1], sizeof(typename Launch::Join));
    for (size_t owner = 0; owner <= helpers; ++owner) {
      EXPECT_LE(direct[owner], 2u);
      EXPECT_LE(queued[owner], 2u);
      if (test >= 97) {
        EXPECT_EQ(direct[owner], 2u);
        EXPECT_EQ(queued[owner], 2u);
      }
      launch->Run(owner);
    }
    pool.Wait([&] { return region->IsComplete(); });
    region->CloseAndWait();
    launch->Release();
    region->TaskComplete();
    ASSERT_FALSE(outside.load());
    for (size_t i = 0; i < size; ++i)
      ASSERT_EQ(actual[i].load(), expected_values[i]) << "index=" << i;
  }
}

template <size_t Participants> void CheckDirectOrderAndRangeOracle() {
  constexpr uint64_t seed = 0x290929d1;
  uint64_t random = seed;
  // Prefix/dispatch can be tested directly on a parking pool: no actual
  // resident capture is involved, and unused test workers need not busy-spin.
  ThreadPool pool(Participants, false, true);
  RapidDomainState domain(pool);
  for (bool expand : {false, true}) {
    for (size_t trace = 0; trace < 128; ++trace) {
      random = random * 6364136223846793005ULL + 1;
      const size_t size = trace < 64 ? trace + 1 : trace >= 124
          ? size_t{1} << (trace - 108) : 1 + random % 521;
      const size_t begin = trace % 2 ? size_t(-1) - size : random % 65536;
      const size_t grain = 1 + trace % 11;
      const size_t helpers = trace % Participants;
      SCOPED_TRACE(::testing::Message() << "seed=" << seed << " case=" << trace
          << " participants=" << Participants << " expand=" << expand
          << " helpers=" << helpers << " size=" << size << " grain=" << grain);
      std::vector<std::atomic<unsigned>> actual(size);
      std::vector<unsigned> expected(size);
      for (size_t index = 0; index < size; ++index)
        expected[index] = static_cast<unsigned>((begin + index) % 29 + 1);
      std::atomic<bool> outside{false};
      auto body = [&](size_t index) {
        if (index < begin || index - begin >= size) { outside = true; return; }
        actual[index - begin].fetch_add(index % 29 + 1, std::memory_order_relaxed);
      };
      using Launch = batch_detail::Launch<decltype(body), Participants>;
      auto *region = NewSmallObject<partitioner_detail::Region>(pool, nullptr);
      auto *launch = NewSmallObject<Launch>(RapidStartGroup{&domain, {0, Participants}}, *region, &body);
      launch->Prepare({begin, begin + size, grain}, helpers, expand);
      for (size_t owner = 0; owner <= helpers; ++owner) {
        std::vector<size_t> expected_direct, original_chain, observed;
        int head = -1;
        for (size_t index = 0; index < launch->SeedCount(); ++index) {
          const auto &value = launch->GetSeed(index);
          if (value.owner != owner) continue;
          if (head == -1) head = static_cast<int>(index);
          if (!value.queued) expected_direct.push_back(index);
        }
        size_t traversed = 0;
        for (int index = head; index != -1 && traversed++ < launch->SeedCount();
             index = launch->GetSeed(index).next) {
          const auto &value = launch->GetSeed(index);
          EXPECT_EQ(value.owner, owner);
          if (!value.queued) original_chain.push_back(index);
        }
        EXPECT_LE(expected_direct.size(), 2u);
        EXPECT_LE(traversed, launch->SeedCount());
        EXPECT_EQ(original_chain, expected_direct);
        rapid_dispatch_observers::direct_indices = &observed;
        launch->Run(owner);
        rapid_dispatch_observers::direct_indices = nullptr;
        EXPECT_EQ(observed, original_chain);
      }
      pool.Wait([&] { return region->IsComplete(); });
      region->CloseAndWait();
      launch->Release();
      region->TaskComplete();
      ASSERT_FALSE(outside.load());
      for (size_t index = 0; index < size; ++index)
        ASSERT_EQ(actual[index].load(), expected[index]) << "index=" << index;
    }
  }
}

TEST(EigenRapidDispatch, DirectDispatchPreservesOriginalChainAndSerialOracle) {
  CheckDirectOrderAndRangeOracle<2>();
  CheckDirectOrderAndRangeOracle<8>();
  CheckDirectOrderAndRangeOracle<16>();
  CheckDirectOrderAndRangeOracle<64>();
}

TEST(EigenRapidDispatch, BorrowedLeaseKeepsClosingRegionAlive) {
  ThreadPool pool(2, true, true);
  auto *region = NewSmallObject<partitioner_detail::Region>(pool, nullptr);
  std::atomic<bool> closed{false};
  std::thread closer;
  size_t calls = 0;
  auto body = [&](size_t) { ++calls; };
  using Work = partitioner_detail::Work<decltype(body), partitioning::AutoPartitionState>;
  {
    partitioner_detail::WorkLease admission(*region);
    EXPECT_TRUE(admission);
    closer = std::thread([&] { region->CloseAndWait(); closed = true; });
    // Observe closure through public admission, not timing assumptions. The
    // original lease pins callback lifetime while CloseAndWait is blocked.
    while (region->BeginWork()) {
      region->EndWork();
      std::this_thread::yield();
    }
    EXPECT_FALSE(closed.load());
    for (size_t direct = 0; direct < 2; ++direct)
      partitioner_detail::Process(*region, &body, {0, 17, 17},
          partitioning::AutoPartitionState(2),
          static_cast<partitioner_detail::PeerJoin<Work> *>(nullptr),
          partitioner_detail::TaskContext<partitioning::AutoPartitionState>{}, &admission);
    EXPECT_EQ(calls, 34u);
    EXPECT_FALSE(closed.load());
    region->Fail(std::make_exception_ptr(std::runtime_error("cancel direct siblings")));
    partitioner_detail::WorkLease cancelled(*region, &admission);
    EXPECT_FALSE(cancelled);
    partitioner_detail::Process(*region, &body, {0, 17, 17},
        partitioning::AutoPartitionState(2),
        static_cast<partitioner_detail::PeerJoin<Work> *>(nullptr),
        partitioner_detail::TaskContext<partitioning::AutoPartitionState>{}, &admission);
    EXPECT_EQ(calls, 34u);
  }
  closer.join();
  EXPECT_TRUE(closed.load());
  region->TaskComplete();
}

TEST(EigenRapidDispatch, GrainSizedSeedsSkipEmptyPublicationAndSharedAdmission) {
  ThreadPool pool(8, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  std::array<std::atomic<unsigned>, 8> visits{};
  auto body = [&](size_t i) { ++visits[i]; };
  using Launch = batch_detail::Launch<decltype(body), 8>;
  auto *region = NewSmallObject<partitioner_detail::Region>(pool, nullptr);
  auto *launch = NewSmallObject<Launch>(RapidStartGroup{&domain, {0, 8}}, *region, &body);
  launch->Prepare({0, 8, 1}, 7, true);
  EXPECT_EQ(launch->SeedCount(), 8u);
  for (size_t i = 0; i < launch->SeedCount(); ++i) {
    EXPECT_FALSE(launch->GetSeed(i).queued);
    EXPECT_EQ(launch->GetSeed(i).range.end - launch->GetSeed(i).range.begin, 1u);
  }
  rapid_dispatch_observers::batch_publications = 0;
  rapid_dispatch_observers::shared_admissions = 0;
  for (size_t slot = 0; slot < 8; ++slot) launch->Run(slot);
  pool.Wait([&] { return region->IsComplete(); });
  region->CloseAndWait();
  EXPECT_EQ(rapid_dispatch_observers::batch_publications.load(), 0u);
  EXPECT_EQ(rapid_dispatch_observers::shared_admissions.load(), 0u);
  for (const auto &value : visits) EXPECT_EQ(value.load(), 1u);
  launch->Release();
  region->TaskComplete();
}
TEST(EigenRapidDispatch, WaitingCallerCanReceiveWorkAfterPeerHasCompleted) {
  ThreadPool pool(2, true, true, WorkerIdleMode::ResidentBusy);
  constexpr size_t size = 8192;
  std::vector<std::atomic<unsigned>> actual(size);
  std::atomic<bool> caller_ran{false}, release_command{false}, timed_out{false};
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  auto body = [&](size_t i) {
    if (i == 0) {
      while (!pool.HasWaitingHelper() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
      if (!pool.HasWaitingHelper()) timed_out = true;
    }
    if (pool.CurrentThreadId() == 0) caller_ran.store(true, std::memory_order_release);
    actual[i].fetch_add(static_cast<unsigned>(i % 31 + 1));
  };
  partitioner_detail::Metrics metrics;
  auto *region = NewSmallObject<partitioner_detail::Region>(pool, &metrics);
  region->AddTask();
  struct Root : partitioner_detail::PinnedRoot {
    explicit Root(partitioner_detail::Region &r) : PinnedRoot{[](PinnedRoot *p, bool notify) noexcept {
      static_cast<Root *>(p)->region.TaskComplete(notify);
    }}, region(r) {}
    partitioner_detail::Region &region;
  } root(*region);
  using State = partitioning::AutoPartitionState;
  using Work = partitioner_detail::Work<decltype(body), State>;
  using Join = partitioner_detail::PeerJoin<Work>;
  Join join(Join::Root(root));
  join.references.store(Join::executing | 1); // The other branch has finished.
  auto execute = [&] {
    State state(1);
    state.divisor = 0;
    state.max_depth = 4;
    pool.RunInlineWork([&] {
      partitioner_detail::Process<decltype(body), State, false>(*region, &body,
          {0, size, 1}, state, &join, partitioner_detail::TaskContext<State>{});
    });
    // Keep this worker from taking its own donation before the caller can help.
    while (!caller_ran.load(std::memory_order_acquire) && !release_command.load())
      std::this_thread::yield();
  };
  struct Command final : ResidentTask {
    explicit Command(decltype(execute) &f) : function(f) {}
    void Run(size_t) noexcept final { function(); }
    decltype(execute) &function;
  } command(execute);
  unsigned worker = 99;
  while (!pool.ClaimResidentWorkers({1, 2}, &worker, 1) &&
         std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
  if (worker == 99) {
    region->TaskComplete();
    region->TaskComplete();
    FAIL() << "helper did not become ready";
  }
  std::atomic<size_t> remaining{1};
  pool.PublishResident(command, remaining, worker, 0);
  pool.Wait([&] { return region->IsComplete(); });
  release_command = true;
  while (remaining.load(std::memory_order_acquire)) std::this_thread::yield();
  region->CloseAndWait();
  EXPECT_FALSE(timed_out.load());
  EXPECT_TRUE(caller_ran.load());
  EXPECT_GT(metrics.donated_ranges.load(), 0u);
  EXPECT_EQ(join.references.load(), Join::executing);
  for (size_t i = 0; i < size; ++i) EXPECT_EQ(actual[i].load(), i % 31 + 1) << i;
  region->TaskComplete();
}

TEST(EigenRapidDispatch, ClosedAndCancelledBatchesReleaseEveryPin) {
  for (bool registered : {false, true}) {
    for (bool cancel : {false, true}) {
      ThreadPool pool(8, true, registered, WorkerIdleMode::ResidentBusy);
      RapidDomainState domain(pool);
      std::atomic<bool> called{false};
      auto body = [&](size_t) { called = true; };
      auto callback = std::make_unique<decltype(body)>(body);
      auto metrics = std::make_unique<partitioner_detail::Metrics>();
      using Launch = batch_detail::Launch<decltype(body), 8>;
      auto *region = NewSmallObject<partitioner_detail::Region>(pool, metrics.get());
      auto *launch = NewSmallObject<Launch>(RapidStartGroup{&domain, {0, 8}},
                                          *region, callback.get());
      launch->Prepare({0, 65536, 1}, 7, true);
      region->CloseAndWait();
      callback.reset();
      metrics.reset();
      if (cancel) pool.Cancel();
      for (size_t slot = 0; slot < 8; ++slot) launch->Run(slot);
      pool.Wait([&] { return region->IsComplete(); });
      EXPECT_TRUE(region->IsComplete());
      EXPECT_FALSE(called.load());
      // Launch destruction asserts every pinned node has returned both of
      // its branch references; surviving queued pins would prevent deletion.
      launch->Release();
      region->TaskComplete();
    }
  }
}
} // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
