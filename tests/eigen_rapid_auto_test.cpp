// SPDX-License-Identifier: Apache-2.0
#include <oox/eigen/rapid_auto.h>
#include <oox/eigen/rapid_auto_calibration.h>
#include "test_support/eigen_resident.h"
#include "eigen_test_wait.h"
#include <gtest/gtest.h>
#include <future>

namespace {
using namespace oox::detail::eigen_pool;
using namespace oox::detail::eigen_pool::rapid;
using namespace std::chrono_literals;

static_assert(sizeof(batch_detail::Launch<void (*)(size_t), 2>) <
              sizeof(batch_detail::Launch<void (*)(size_t), 8>));
static_assert(sizeof(batch_detail::Launch<void (*)(size_t), 8>) <
              sizeof(batch_detail::Launch<void (*)(size_t), 16>));
static_assert(sizeof(batch_detail::Launch<void (*)(size_t), 16>) <
              sizeof(batch_detail::Launch<void (*)(size_t), 64>));

TEST(EigenRapidAuto, FrontierInheritsOneRootBudget) {
  ThreadPool pool(2, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  using State = partitioning::AutoPartitionState;
  for (size_t workers : {2u, 3u, 4u, 8u, 16u, 31u, 64u}) {
    std::vector<State> states{State(workers)};
    std::vector<size_t> expected{2 * workers};
    while (states.size() < workers) {
      SCOPED_TRACE(::testing::Message() << "workers=" << workers
          << " split=" << states.size());
      const size_t index = std::max_element(expected.begin(), expected.end()) - expected.begin();
      ASSERT_GT(states[index].divisor, 1u);
      const auto child = State(states[index], partitioning::Split{});
      states.push_back(child);
      const size_t half = expected[index] / 2;
      expected[index] = half;
      expected.push_back(half);
      for (size_t i = 0; i < states.size(); ++i) {
        EXPECT_EQ(states[i].divisor, expected[i]) << "branch=" << i;
        EXPECT_EQ(states[i].max_depth, 5u) << "branch=" << i;
      }
    }
  }
}

TEST(EigenRapidAuto, PinnedPrefixJoinsMatchBooleanOracle) {
  using Work = partitioner_detail::Work<void (*)(size_t), partitioning::AutoPartitionState>;
  using Join = partitioner_detail::PeerJoin<Work>;
  struct Root : partitioner_detail::PinnedRoot {
    Root() : PinnedRoot{[](PinnedRoot *root, bool) noexcept {
      ++static_cast<Root *>(root)->completions;
    }} {}
    unsigned completions = 0;
  };
  std::array<unsigned, 4> order{0, 1, 2, 3};
  size_t test = 0;
  do {
    Root root;
    Join parent(Join::Root(root)), left(&parent), right(&parent);
    std::array<bool, 4> done{};
    for (auto event : order) {
      done[event] = true;
      Join::Release(event < 2 ? &left : &right);
      const bool left_done = done[0] && done[1], right_done = done[2] && done[3];
      EXPECT_EQ(root.completions, unsigned(left_done && right_done)) << "case=" << test;
      EXPECT_EQ(parent.references.load(), Join::executing + 2u - left_done - right_done);
      EXPECT_EQ(left.references.load(), Join::executing + 2u - done[0] - done[1]);
      EXPECT_EQ(right.references.load(), Join::executing + 2u - done[2] - done[3]);
    }
    ++test;
  } while (std::next_permutation(order.begin(), order.end()));
  EXPECT_EQ(test, 24u);
}

TEST(EigenRapidAuto, BatchFrontierMatchesSerialSplitOracle) {
  ThreadPool pool(8, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  size_t test = 0;
  for (size_t size : {2u, 3u, 7u, 8u, 17u, 129u, 4097u}) {
    for (size_t grain : {1u, 2u, 7u}) {
      for (size_t helpers = 0; helpers < 8; ++helpers) {
        SCOPED_TRACE(::testing::Message() << "case=" << test++);
        std::vector<std::atomic<unsigned>> visits(size);
        auto body = [&](size_t i) { ++visits[i]; };
        using Launch = batch_detail::Launch<decltype(body)>;
        auto *region = NewSmallObject<partitioner_detail::Region>(pool, nullptr, true);
        auto *launch = NewSmallObject<Launch>(RapidStartGroup{&domain, {0, 8}}, *region, &body);
        launch->Prepare({0, size, grain}, helpers);
        struct Expected { size_t begin, end, budget; };
        std::vector<Expected> expected{{0, size, 16}};
        std::vector<size_t> pending{0};
        size_t head = 0;
        while (expected.size() < helpers + 1 && head < pending.size()) {
          const size_t index = pending[head++];
          const auto old = expected[index];
          if (old.end - old.begin <= grain || old.budget <= 1) continue;
          const size_t middle = old.begin + (old.end - old.begin) / 2;
          expected[index] = {old.begin, middle, old.budget / 2};
          expected.push_back({middle, old.end, old.budget / 2});
          pending.push_back(index);
          pending.push_back(expected.size() - 1);
        }
        EXPECT_EQ(launch->SeedCount(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
          const auto &seed = launch->GetSeed(i);
          EXPECT_EQ(seed.range.begin, expected[i].begin);
          EXPECT_EQ(seed.range.end, expected[i].end);
          EXPECT_EQ(seed.state.divisor, expected[i].budget);
          EXPECT_EQ(seed.state.max_depth, 5u);
          launch->Run(i);
        }
        pool.Wait([&] { return region->IsComplete(); });
        region->CloseAndWait();
        launch->Release();
        region->TaskComplete();
        for (const auto &value : visits) EXPECT_EQ(value.load(), 1u);
      }
    }
  }
}

TEST(EigenRapidAuto, GrainSizedOwnersNeedNoOrdinaryTaskAllocations) {
  ThreadPool pool(8, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  RapidStartGroup group{&domain, {0, 8}};
  eigen_test_support::WaitForResidentWorkers(group, 2s);
  partitioner_detail::Metrics metrics;
  size_t activated = 0;
  std::vector<std::atomic<unsigned>> visits(8);
  ParallelForAuto(group, 0, 8, [&](size_t i) { ++visits[i]; }, 1, &metrics, &activated);
  ASSERT_EQ(activated, 7u);
  EXPECT_EQ(metrics.range_tasks.load(), 0u);
  for (const auto &value : visits) EXPECT_EQ(value.load(), 1u);
}

TEST(EigenRapidAuto, ExpansionRespectsRemainingWorkerBudget) {
  ThreadPool pool(8, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  for (size_t helpers : {1u, 7u}) {
    std::vector<std::atomic<unsigned>> visits(4096);
    auto body = [&](size_t i) { ++visits[i]; };
    using Launch = batch_detail::Launch<decltype(body)>;
    auto *region = NewSmallObject<partitioner_detail::Region>(pool, nullptr, true);
    auto *launch = NewSmallObject<Launch>(RapidStartGroup{&domain, {0, 8}}, *region, &body);
    launch->Prepare({0, visits.size(), 1}, helpers, true);
    EXPECT_EQ(launch->SeedCount(), helpers == 1 ? 2u : 32u);
    for (size_t i = 0; i < launch->SeedCount(); ++i) {
      const auto &seed = launch->GetSeed(i);
      EXPECT_EQ(seed.state.divisor, helpers == 1 ? 8u : 0u);
    }
    for (size_t i = 0; i <= helpers; ++i) launch->Run(i);
    pool.Wait([&] { return region->IsComplete(); });
    region->CloseAndWait();
    launch->Release();
    region->TaskComplete();
    for (const auto &value : visits) EXPECT_EQ(value.load(), 1u);
  }
}

TEST(EigenRapidAuto, GeneratedRangesMatchSerialOracle) {
  constexpr uint64_t seed = 0x240924;
  for (unsigned workers : {1u, 3u, 8u}) {
    for (bool caller_registered : {false, true}) {
      ThreadPool pool(workers, true, caller_registered, WorkerIdleMode::ResidentBusy);
      RapidDomainState domain(pool);
      uint64_t random = seed;
      for (size_t test = 0; test < 100; ++test) {
        random = random * 6364136223846793005ULL + 1;
        const size_t size = test < 64 ? test : test >= 97 && workers == 8
            ? size_t{1} << (18 + test - 97) : random % 8192;
        const size_t begin = test % 2 ? size_t(-1) - size : random % 4096;
        const size_t grain = test % 7;
        pool.SetResidentLimit(test % 4 ? workers : random % (workers + 1));
        SCOPED_TRACE(::testing::Message() << "seed=" << seed << " case=" << test
            << " workers=" << workers << " registered=" << caller_registered);
        std::vector<std::atomic<unsigned>> actual(size);
        std::vector<unsigned> expected(size);
        for (size_t i = 0; i < size; ++i) expected[i] = (begin + i) % 23 + 1;
        std::atomic<bool> outside{false};
        const unsigned start = workers > 1 && test % 3 == 0 ? 1 : 0;
        ParallelForAuto({&domain, {start, workers}}, begin, begin + size, [&](size_t i) {
          if (i < begin || i - begin >= size) { outside = true; return; }
          actual[i - begin].fetch_add(i % 23 + 1, std::memory_order_relaxed);
        }, grain);
        ASSERT_FALSE(outside.load());
        for (size_t i = 0; i < size; ++i)
          ASSERT_EQ(actual[i].load(), expected[i]) << "index=" << i;
      }
    }
  }
}

TEST(EigenRapidAuto, ActivatesExistingAutoTasks) {
  ThreadPool pool(4, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  RapidStartGroup group{&domain, {0, 4}};
  eigen_test_support::WaitForResidentWorkers(group, 2s);
  partitioner_detail::Metrics metrics;
  size_t activated = 0;
  std::vector<std::atomic<unsigned>> visits(8192);
  ParallelForAuto(group, 0, visits.size(), [&](size_t i) { ++visits[i]; }, 1,
                     &metrics, &activated);
  EXPECT_GT(activated, 0u);
  EXPECT_LE(activated, 4u);
  EXPECT_GE(metrics.owner_ranges.load(), activated + 1);
  EXPECT_GT(metrics.private_chunks.load(), 0u);
  for (const auto &count : visits) EXPECT_EQ(count.load(), 1u);
}

TEST(EigenRapidAuto, ExpandedFrontierMatchesSerialOracle) {
  constexpr uint64_t seed = 0x240926;
  for (unsigned workers : {2u, 3u, 4u, 8u}) {
    ThreadPool pool(workers, true, true, WorkerIdleMode::ResidentBusy);
    RapidDomainState domain(pool);
    RapidStartGroup group{&domain, {0, workers}};
    uint64_t random = seed;
    for (size_t test = 0; test < 100; ++test) {
      random = random * 6364136223846793005ULL + 1;
      const size_t size = test < 64 ? test : test >= 97
          ? size_t{1} << (18 + test - 97) : 64 + random % 8192;
      SCOPED_TRACE(::testing::Message() << "seed=" << seed << " case=" << test << " workers=" << workers);
      std::vector<std::atomic<unsigned>> actual(size);
      std::vector<unsigned> expected(size);
      for (size_t i = 0; i < size; ++i) expected[i] = i % 29 + 1;
      eigen_test_support::WaitForResidentWorkers(group, 2s);
      ParallelForAuto(group, 0, size, [&](size_t i) { actual[i].fetch_add(i % 29 + 1); },
                         test % 5 + 1, nullptr, nullptr);
      for (size_t i = 0; i < size; ++i)
        ASSERT_EQ(actual[i].load(), expected[i]) << "index=" << i;
    }
  }
}

TEST(EigenRapidAuto, CompletedCommandStaysReadyWithoutStealing) {
  ThreadPool pool(2, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  RapidStartGroup group{&domain, {0, 2}};
  eigen_test_support::WaitForResidentWorkers(group, 2s);
  const auto before = pool.GetStatistics();
  std::atomic<size_t> visits{0};
  for (size_t launch = 0; launch < 100; ++launch) {
    auto body = [&](size_t, size_t) { ++visits; };
    ResidentRegion<decltype(body)> region(domain, body, 0, 2, 2, 1);
    unsigned worker = 0;
    ASSERT_EQ(pool.ClaimResidentWorkers(group.domain, &worker, 1), 1u);
    pool.PublishResident(region, region.CompletionCounter(), worker, 1);
    region.RunCaller();
    // Isolate worker return: caller-side helping has its own steal attempts.
    eigen_test_support::WaitUntil([&] { return region.IsComplete(); },
                                 "resident availability after completion");
    region.RethrowAfterJoin();
    EXPECT_EQ(pool.ResidentAvailableWorkers(group.domain), 1u);
  }
  EXPECT_EQ(visits.load(), 200u);
  EXPECT_EQ(pool.GetStatistics().failed_steal_rounds, before.failed_steal_rounds);
}

TEST(EigenRapidAuto, NestedAndConcurrentRootsMatchSerialOracle) {
  ThreadPool pool(8, true, false, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  RapidStartGroup group{&domain, {0, 8}};
  std::vector<std::atomic<unsigned>> visits(2048);
  const auto run = [&] {
    ParallelForAuto(group, 0, 32, [&](size_t row) {
      size_t nested_activations = 99;
      ParallelForAuto(group, 0, 64, [&](size_t column) {
        ++visits[row * 64 + column];
      }, 1, nullptr, &nested_activations);
      EXPECT_EQ(nested_activations, 0u);
    });
  };
  eigen_test_support::WaitForResidentWorkers(group, 2s);
  run();
  auto other = std::async(std::launch::async, run);
  run();
  eigen_test_support::GetReady(other, "nested concurrent Auto root");
  for (const auto &count : visits) EXPECT_EQ(count.load(), 3u);
}

TEST(EigenRapidAuto, OrdinaryFallbackKeepsNestedActivationDisabled) {
  ThreadPool pool(2, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  RapidStartGroup group{&domain, {0, 2}};
  const auto run = [&](auto &&body, size_t *activated) {
    ParallelForAuto(group, 0, 2,
        std::forward<decltype(body)>(body), 1, nullptr, activated);
  };
  std::array<std::atomic<unsigned>, 2> visits{};
  pool.SetResidentLimit(0);
  EXPECT_FALSE(pool.IsExecutingTask());
  size_t outer_activated = 99;
  run([&, ownership = std::make_unique<unsigned>(7)](size_t row) {
    EXPECT_EQ(*ownership, 7u); // The adapter must accept a move-only callback.
    EXPECT_TRUE(pool.IsExecutingTask());
    if (row != 0) return;
    // Ordinary Auto keeps the left leaf on the synchronous caller. Make a
    // helper ready only after that fallback has entered its root callback.
    pool.SetResidentLimit(2);
    eigen_test_support::WaitForResidentWorkers(group, 2s);
    size_t nested_activated = 99;
    run([&](size_t column) { ++visits[column]; }, &nested_activated);
    EXPECT_EQ(nested_activated, 0u);
  }, &outer_activated);
  EXPECT_EQ(outer_activated, 0u);
  EXPECT_FALSE(pool.IsExecutingTask());
  for (const auto &value : visits) EXPECT_EQ(value.load(), 1u);
}

TEST(EigenRapidAuto, OrdinaryFallbackRestoresAncestryAfterException) {
  ThreadPool pool(2, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  RapidStartGroup group{&domain, {0, 2}};
  auto body = [&](size_t i) {
    EXPECT_TRUE(pool.IsExecutingTask());
    if (i == 0) throw std::runtime_error("fallback root callback");
  };
  pool.SetResidentLimit(0);
  EXPECT_THROW(ParallelForAuto(group, 0, 2, body), std::runtime_error);
  EXPECT_FALSE(pool.IsExecutingTask());
  pool.SetResidentLimit(2);
  eigen_test_support::WaitForResidentWorkers(group, 2s);
  size_t activated = 0;
  ParallelForAuto(group, 0, 2, [](size_t) {}, 1, nullptr, &activated);
  EXPECT_EQ(activated, 1u);
}

TEST(EigenRapidAuto, ExceptionsJoinCallbacksAndAllowReuse) {
  ThreadPool pool(8, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  RapidStartGroup group{&domain, {0, 8}};
  eigen_test_support::WaitForResidentWorkers(group, 2s);
  std::atomic<unsigned> active{0};
  EXPECT_THROW(ParallelForAuto(group, 0, 65536, [&](size_t i) {
    struct Guard {
      std::atomic<unsigned> &active;
      explicit Guard(std::atomic<unsigned> &a) : active(a) { ++active; }
      ~Guard() { --active; }
    } guard(active);
    if (i % 257 == 0) throw std::runtime_error("rapid auto callback");
  }, 1, nullptr, nullptr), std::runtime_error);
  EXPECT_EQ(active.load(), 0u);
  std::vector<std::atomic<unsigned>> visits(8192);
  ParallelForAuto(group, 0, visits.size(), [&](size_t i) { ++visits[i]; });
  for (const auto &count : visits) EXPECT_EQ(count.load(), 1u);
}

TEST(EigenRapidAuto, CancellationJoinsResidentCommands) {
  for (size_t cancel_at : {0u, 31u, 4096u}) {
    ThreadPool pool(8, true, false, WorkerIdleMode::ResidentBusy);
    RapidDomainState domain(pool);
    RapidStartGroup group{&domain, {0, 8}};
    eigen_test_support::WaitForResidentWorkers(group, 2s);
    ParallelForAuto(group, 0, 65536, [&](size_t i) {
      if (i == cancel_at) pool.Cancel();
    }, 1, nullptr, nullptr);
    EXPECT_TRUE(pool.IsCancelled());
  }
}

TEST(EigenRapidAuto, OrdinaryTasksProgressBetweenResidentCommands) {
  ThreadPool pool(4, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  std::atomic<size_t> ordinary{0};
  for (size_t launch = 0; launch < 100; ++launch) {
    pool.Schedule(MakeTask([&] { ++ordinary; }));
    ParallelForAuto({&domain, {0, 4}}, 0, 1024, [](size_t) {}, 1, nullptr, nullptr);
  }
  pool.Wait([&] { return ordinary.load() == 100; });
  EXPECT_EQ(ordinary.load(), 100u);
}

TEST(EigenRapidAuto, ClosedAutoTaskDoesNotAccessExpiredCallback) {
  ThreadPool pool(2, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  bool called = false;
  auto body = [&](size_t) { called = true; };
  auto callback = std::make_unique<decltype(body)>(body);
  using State = partitioning::AutoPartitionState;
  using Work = partitioner_detail::Work<decltype(body), State>;
  using Join = partitioner_detail::PeerJoin<Work>;
  auto *region = NewSmallObject<partitioner_detail::Region>(pool, nullptr);
  region->AddTask();
  auto *task = NewSmallObject<Work>(*region, callback.get(),
      partitioner_detail::LoopRange(0, 1024, 1),
      State(2), 0, Join::Root(*region));
  region->CloseAndWait();
  callback.reset();
  (*task)();
  Join::Release(task);
  EXPECT_FALSE(called);
  EXPECT_TRUE(region->IsComplete());
  region->TaskComplete();
}

TEST(EigenRapidAuto, ReconfigurationDuringWorkMatchesSerialOracle) {
  ThreadPool pool(8, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  std::atomic<bool> stop{false};
  struct Join {
    std::atomic<bool> &stop;
    std::thread thread;
    ~Join() { stop.store(true, std::memory_order_release); thread.join(); }
  } changer{stop, std::thread([&] {
    size_t step = 0;
    while (!stop.load(std::memory_order_acquire)) {
      pool.SetResidentLimit(step++ % 9);
      std::this_thread::yield();
    }
  })};
  for (size_t test = 0; test < 100; ++test) {
    std::vector<std::atomic<unsigned>> actual(257 + test);
    std::vector<unsigned> expected(actual.size());
    for (size_t i = 0; i < expected.size(); ++i) expected[i] = i % 19 + 1;
    ParallelForAuto({&domain, {0, 8}}, 0, actual.size(),
        [&](size_t i) { actual[i].fetch_add(i % 19 + 1); }, 1, nullptr, nullptr);
    for (size_t i = 0; i < actual.size(); ++i)
      ASSERT_EQ(actual[i].load(), expected[i]) << "case=" << test << " index=" << i;
  }
}

TEST(EigenRapidAuto, LargePoolsUseBoundedActivationSlots) {
  ThreadPool pool(65, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  std::vector<std::atomic<unsigned>> visits(4097);
  size_t activated = 0;
  ParallelForAuto({&domain, {0, 65}}, 0, visits.size(), [&](size_t i) { ++visits[i]; },
                     1, nullptr, &activated);
  EXPECT_LE(activated, 63u);
  for (const auto &count : visits) EXPECT_EQ(count.load(), 1u);
}

TEST(EigenRapidAuto, CalibrationSelectionUsesToleranceAndSmallerCohort) {
  std::vector<AutoCalibrationTrial> trials{{0, {100, 100, 100}},
      {2, {10, 10, 10}}, {4, {9, 10, 10}}, {1, {10, 10, 10}}};
  EXPECT_EQ(auto_calibration_detail::Select(trials, 0), 2u);
  EXPECT_EQ(auto_calibration_detail::Select(trials, 0.05), 3u);
  EXPECT_EQ(auto_calibration_detail::Select({}, 0.05), 0u);
}

TEST(EigenRapidAuto, CalibrationProbesMatchSerialOracle) {
  ThreadPool pool(3, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  for (size_t limit : {0u, 1u, 3u}) {
    pool.SetResidentLimit(limit);
    for (unsigned kind = 0; kind < 3; ++kind) {
      std::vector<std::uint64_t> actual(129), expected(129);
      for (size_t i = 0; i < expected.size(); ++i) {
        auto value = static_cast<std::uint64_t>(i + 1);
        const unsigned count = kind == 0 ? 0 : kind == 1 ? 16 : (i < 4 || i >= 125) ? 256 : 4;
        for (unsigned step = 0; step < count; ++step)
          value = (value ^ (value >> 27)) * 0x3c79ac492ba7b653ULL + step;
        expected[i] = value;
      }
      EXPECT_GT(auto_calibration_detail::Probe({&domain, {0, 3}}, actual, kind), 0u);
      EXPECT_EQ(actual, expected) << "limit=" << limit << " kind=" << kind;
    }
  }
}

TEST(EigenRapidAuto, CalibrationFallbackCancellationAndReuse) {
  ThreadPool pool(4, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  RapidStartGroup group{&domain, {0, 4}};
  const auto disabled = CalibrateAutoGroup(group, {0ns});
  EXPECT_TRUE(disabled.trials.empty());
  EXPECT_EQ(pool.ResidentLimit(), 0u);
  const auto calibrated = CalibrateAutoGroup(group, {100ms});
  EXPECT_LE(calibrated.resident_limit, 4u);
  EXPECT_EQ(pool.ResidentLimit(), calibrated.resident_limit);
  std::vector<std::atomic<unsigned>> visits(4096);
  ParallelForAuto(group, 0, visits.size(), [&](size_t i) { ++visits[i]; });
  for (const auto &value : visits) EXPECT_EQ(value.load(), 1u);
  EXPECT_THROW(CalibrateAutoGroup({&domain, {1, 4}}), std::invalid_argument);
  EXPECT_THROW(CalibrateAutoGroup(group, {100ms, -1}), std::invalid_argument);
  EXPECT_THROW(CalibrateAutoGroup(group,
      {100ms, std::numeric_limits<double>::quiet_NaN()}), std::invalid_argument);
  pool.Cancel();
  const auto cancelled = CalibrateAutoGroup(group);
  EXPECT_TRUE(cancelled.cancelled);
  EXPECT_TRUE(cancelled.trials.empty());
  EXPECT_EQ(pool.ResidentLimit(), 0u);
}

TEST(EigenRapidAuto, CalibrationRejectsTaskContext) {
  ThreadPool pool(2, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  auto done = std::make_shared<std::promise<void>>();
  auto completed = done->get_future();
  pool.Schedule(MakeTask([&, done] {
    EXPECT_THROW(CalibrateAutoGroup({&domain, {0, 2}}), std::logic_error);
    done->set_value();
  }));
  eigen_test_support::GetReady(completed, "calibration task-context rejection");
  EXPECT_EQ(pool.ResidentLimit(), 2u);
}

TEST(EigenRapidAuto, CalibrationHandlesEmptyAndSingleWorker) {
  EXPECT_TRUE(CalibrateAutoGroup({}).trials.empty());
  ThreadPool pool(1, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  const auto result = CalibrateAutoGroup({&domain, {0, 1}});
  EXPECT_TRUE(result.trials.empty());
  EXPECT_EQ(pool.ResidentLimit(), 0u);
}

TEST(EigenRapidAuto, TerminalPairsMatchSerialOracleAndAutoMidpoint) {
  constexpr uint64_t seed = 0x240930;
  ThreadPool pool(4, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  RapidStartGroup group{&domain, {0, 4}};
  size_t test = 0;
  for (size_t grain : {1u, 2u, 3u, 7u, 31u}) {
    for (size_t size = grain + 1; size <= 2 * grain; ++size) {
      const size_t begin = test % 2 ? size_t(-1) - size : 17;
      SCOPED_TRACE(::testing::Message() << "seed=" << seed << " case=" << test++);
      std::vector<std::atomic<unsigned>> visits(size);
      std::vector<size_t> workers(size);
      size_t activated = 0;
      partitioner_detail::Metrics metrics;
      eigen_test_support::WaitForResidentWorkers(group, 2s);
      ParallelForAuto(group, begin, begin + size, [&](size_t i) {
        ASSERT_GE(i, begin);
        ASSERT_LT(i - begin, size);
        ++visits[i - begin];
        workers[i - begin] = pool.CurrentThreadId();
        EXPECT_TRUE(pool.IsExecutingTask());
      }, grain, &metrics, &activated);
      ASSERT_EQ(activated, 1u);
      EXPECT_EQ(metrics.range_tasks.load(), 0u);
      EXPECT_EQ(metrics.owner_ranges.load(), 2u);
      for (size_t i = 0; i < size; ++i) {
        EXPECT_EQ(visits[i].load(), 1u) << "index=" << i;
        EXPECT_EQ(workers[i] == 0, i < size / 2) << "index=" << i;
      }
    }
  }
}

TEST(EigenRapidAuto, TerminalPairsJoinExceptionsCancellationAndNestedWork) {
  for (unsigned scenario = 0; scenario < 3; ++scenario) {
    ThreadPool pool(2, true, true, WorkerIdleMode::ResidentBusy);
    RapidDomainState domain(pool);
    RapidStartGroup group{&domain, {0, 2}};
    eigen_test_support::WaitForResidentWorkers(group, 2s);
    std::atomic<unsigned> active{0};
    std::vector<std::atomic<unsigned>> visits(128);
    auto body = [&](size_t i) {
      struct Guard {
        std::atomic<unsigned> &n;
        explicit Guard(std::atomic<unsigned> &value) : n(value) { ++n; }
        ~Guard() { --n; }
      } guard(active);
      if (scenario == 0) throw std::runtime_error("terminal pair");
      if (scenario == 1) { pool.Cancel(); return; }
      ParallelForAuto(group, 0, 64, [&](size_t j) { ++visits[i * 64 + j]; });
    };
    if (scenario == 0) EXPECT_THROW(ParallelForAuto(group, 0, 2, body), std::runtime_error);
    else ParallelForAuto(group, 0, 2, body);
    EXPECT_EQ(active.load(), 0u);
    if (scenario == 1) EXPECT_TRUE(pool.IsCancelled());
    if (scenario == 2) for (const auto &value : visits) EXPECT_EQ(value.load(), 1u);
    if (!pool.IsCancelled()) {
      std::atomic<unsigned> count{0};
      ParallelForAuto(group, 0, 2, [&](size_t) { ++count; });
      EXPECT_EQ(count.load(), 2u);
    }
  }
}

TEST(EigenRapidAuto, TerminalPairsPreferReadyRecipientAndDoNotWaitForBusyOne) {
  ThreadPool pool(8, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  RapidStartGroup group{&domain, {0, 8}};
  std::vector<std::atomic<unsigned>> actual(512);
  for (size_t test = 0; test < 256; ++test) {
    SCOPED_TRACE(test);
    eigen_test_support::WaitForResidentWorkers(group, 2s);
    size_t helper = 99;
    ParallelForAuto(group, 0, 2, [&](size_t i) {
      actual[test * 2 + i].fetch_add(static_cast<unsigned>((test + i) % 29 + 1));
      if (i) helper = pool.CurrentThreadId();
    });
    EXPECT_EQ(helper, 1u);
  }
  for (size_t i = 0; i < actual.size(); ++i)
    EXPECT_EQ(actual[i].load(), (i / 2 + i % 2) % 29 + 1) << i;

  eigen_test_support::WaitForResidentWorkers(group, 2s);
  struct Command final : ResidentTask { void Run(size_t) noexcept final {} } held;
  std::atomic<size_t> remaining{1};
  unsigned reserved = 99;
  const auto claimed = pool.ClaimResidentWorkers({1, 2}, &reserved, 1);
  ASSERT_EQ(claimed, 1u);
  size_t helper = 99;
  std::atomic<unsigned> visits{0};
  // The normal pair must use another ready helper, not spin on worker 1.
  ParallelForAuto(group, 0, 2, [&](size_t i) {
    ++visits;
    if (i) helper = pool.CurrentThreadId();
  });
  pool.PublishResident(held, remaining, reserved, 0);
  eigen_test_support::WaitUntil([&] { return remaining.load(std::memory_order_acquire) == 0; },
                               "reserved terminal-pair helper completion");
  EXPECT_EQ(visits.load(), 2u);
  EXPECT_EQ(helper, 2u);
}


TEST(EigenRapidAuto, EmptyInvalidAndDisabledGroups) {
  EXPECT_NO_THROW(ParallelForAuto({}, 0, 10, [](size_t) {}));
  ThreadPool pool(2, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  EXPECT_THROW(ParallelForAuto({&domain, {0, 3}}, 0, 10, [](size_t) {}), std::invalid_argument);
  pool.SetResidentLimit(0);
  size_t activated = 99;
  std::atomic<unsigned> visits{0};
  ParallelForAuto({&domain, {0, 2}}, 0, 1024, [&](size_t) { ++visits; }, 1, nullptr, &activated);
  EXPECT_EQ(activated, 0u);
  EXPECT_EQ(visits.load(), 1024u);
  ThreadPool parked(2, true, false);
  RapidDomainState parked_domain(parked);
  EXPECT_THROW(ParallelForAuto({&parked_domain, {0, 2}}, 0, 10, [](size_t) {}), std::invalid_argument);
}
} // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
