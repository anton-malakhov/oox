// SPDX-License-Identifier: Apache-2.0
#include "benchmarks/eigen/resident_test_support.h"
#include <oox/eigen/rapid_calibration.h>
#include <gtest/gtest.h>
#include <future>
#include <vector>

namespace {
using namespace oox::detail::eigen_pool;
using namespace oox::detail::eigen_pool::rapid;
using namespace std::chrono_literals;
constexpr MailboxHandoff policies[] = {
    MailboxHandoff::Immediate, MailboxHandoff::LocalFirst};

TEST(EigenRapidMailbox, GeneratedRangesMatchSerialOracle) {
  constexpr uint64_t seed = 0x230921;
  for (unsigned workers : {1u, 3u, 8u}) {
    ThreadPool pool(workers, true, false, WorkerIdleMode::ResidentBusy);
    RapidDomainState domain(pool);
    for (auto policy : policies) {
      for (bool prefer : {false, true}) {
        uint64_t random = seed;
        for (size_t test = 0; test < 195; ++test) {
          random = random * 6364136223846793005ULL + 1;
          const size_t size = test < 128 ? test : test >= 192
              ? size_t{1} << (18 + test - 192) : random % 4096;
          const size_t begin = test % 2 ? size_t(-1) - size : random % 4096;
          SCOPED_TRACE(::testing::Message() << "seed=" << seed << " case=" << test
              << " workers=" << workers << " policy=" << int(policy)
              << " prefer=" << prefer);
          std::vector<std::atomic<unsigned>> actual(size);
          std::vector<unsigned> expected(size);
          for (size_t i = 0; i < size; ++i)
            expected[i] = (begin + i) % 17 + 1;
          std::atomic<bool> outside{false};
          RapidStartGroup group{&domain, {0, test % 3 ? workers : 1u}};
          ParallelForMailbox(group, begin, begin + size, [&](size_t i) {
            if (i < begin || i - begin >= size) {
              outside.store(true);
              return;
            }
            actual[i - begin].fetch_add(i % 17 + 1, std::memory_order_relaxed);
          }, policy, prefer, test % 5 + 1);
          ASSERT_FALSE(outside.load());
          for (size_t i = 0; i < size; ++i)
            ASSERT_EQ(actual[i].load(), expected[i]) << "index=" << i;
        }
      }
    }
  }
}

TEST(EigenRapidMailbox, LocalWorkPrecedesDeregistrationAndRemoteSteal) {
  for (auto policy : policies) {
    ThreadPool pool(2, true, true, WorkerIdleMode::ResidentBusy);
    std::promise<void> entered, release;
    auto unblocked = release.get_future().share();
    pool.RunInMailbox(MakeTask([&, unblocked] {
      entered.set_value();
      unblocked.wait();
    }), 1);
    ASSERT_EQ(entered.get_future().wait_for(2s), std::future_status::ready);
    struct Probe : ResidentTask {
      Probe(ThreadPool &p, MailboxHandoff m) : pool(p), state(p), policy(m) {}
      void Deregister(size_t) noexcept override { events.push_back(2); }
      void Run(size_t) noexcept override {
        pool.RunInMailbox(MakeTask([&] {
          ParallelForMailbox({&state, {0, 2}}, 0, 1,
              [&](size_t) { events.push_back(1); }, policy);
        }), 0);
        pool.RunInMailbox(MakeTask([&] { events.push_back(3); }), 1);
        if (policy == MailboxHandoff::Immediate)
          pool.DeregisterResident();
        EXPECT_TRUE(pool.TryExecuteLocalTask());
        EXPECT_FALSE(pool.TryExecuteLocalTask());
        EXPECT_TRUE(pool.TryExecuteSomething());
      }
      ThreadPool &pool;
      RapidDomainState state;
      MailboxHandoff policy;
      std::vector<unsigned> events;
    } probe(pool, policy);
    probe.events.reserve(3);
    pool.RunResidentProducer(probe, 0);
    release.set_value();
    const std::vector<unsigned> expected = policy == MailboxHandoff::Immediate
        ? std::vector<unsigned>{2, 1, 3} : std::vector<unsigned>{1, 2, 3};
    EXPECT_EQ(probe.events, expected);
  }
}

TEST(EigenRapidMailbox, NonmembersReceiveAdditionalSeeds) {
  ThreadPool pool(4, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState state(pool);
  for (auto policy : policies) {
    for (bool prefer : {false, true}) {
      const auto before = pool.GetStatistics();
      std::atomic<unsigned> visits{0};
      ParallelForMailbox({&state, {0, 1}}, 0, 100, [&](size_t) { ++visits; },
                         policy, prefer, 25);
      const auto after = pool.GetStatistics();
      EXPECT_EQ(visits.load(), 100u);
      EXPECT_EQ(after.scheduled - before.scheduled, prefer ? 6u : 3u);
      EXPECT_EQ(after.executed - before.executed, prefer ? 6u : 3u);
    }
  }
}

TEST(EigenRapidMailbox, GrainSizedRangesRunInlineWithoutTasks) {
  ThreadPool pool(4, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState state(pool);
  for (auto policy : policies) {
    for (bool prefer : {false, true}) {
      for (size_t size : {1u, 7u}) {
        const auto before = pool.GetStatistics();
        size_t visits = 0;
        ParallelForMailbox({&state, {0, 4}}, 11, 11 + size, [&](size_t i) {
          EXPECT_EQ(pool.CurrentThreadId(), 0u);
          EXPECT_FALSE(pool.IsExecutingTask());
          EXPECT_EQ(i, 11 + visits++);
        }, policy, prefer, size == 1 ? 0 : size);
        EXPECT_EQ(visits, size);
        EXPECT_EQ(pool.GetStatistics().scheduled, before.scheduled);
      }
    }
  }
}

TEST(EigenRapidMailbox, SaturatedMailboxLeavesBeforeInlineExecution) {
  ThreadPool pool(2, true, true, WorkerIdleMode::ResidentBusy);
  std::promise<void> entered, release;
  auto unblocked = release.get_future().share();
  pool.RunInMailbox(MakeTask([&, unblocked] {
    entered.set_value(); unblocked.wait();
  }), 1);
  ASSERT_EQ(entered.get_future().wait_for(2s), std::future_status::ready);
  struct Probe : ResidentTask {
    explicit Probe(ThreadPool &p) : pool(p) {}
    void Deregister(size_t) noexcept override { left = true; }
    void Run(size_t) noexcept override {
      for (size_t i = 0; i < 1024; ++i)
        pool.RunInMailbox(MakeTask([] {}), 1);
      pool.RunInMailbox(MakeTask([&] { observed = left; }), 1);
    }
    ThreadPool &pool;
    bool left = false, observed = false;
  } probe(pool);
  pool.RunResidentProducer(probe, 0);
  release.set_value();
  EXPECT_TRUE(probe.observed);
}

TEST(EigenRapidMailbox, ExceptionsJoinCallbacksAndAllowReuse) {
  ThreadPool pool(8, true, false, WorkerIdleMode::ResidentBusy);
  RapidDomainState state(pool);
  RapidStartGroup group{&state, {0, 8}};
  for (auto policy : policies) {
    std::atomic<unsigned> active{0};
    EXPECT_THROW(ParallelForMailbox(group, 0, 8192, [&](size_t i) {
      struct Guard {
        explicit Guard(std::atomic<unsigned> &n) : n(n) { ++n; }
        ~Guard() { --n; }
        std::atomic<unsigned> &n;
      } guard(active);
      if (i == 0) throw std::runtime_error("mailbox callback");
    }, policy), std::runtime_error);
    EXPECT_EQ(active.load(), 0u);
    std::atomic<size_t> count{0};
    ParallelForMailbox(group, 0, 8192, [&](size_t) { ++count; }, policy);
    EXPECT_EQ(count.load(), 8192u);
  }
}

TEST(EigenRapidMailbox, NestedAndConcurrentRootsMakeProgress) {
  ThreadPool pool(8, true, false, WorkerIdleMode::ResidentBusy);
  RapidDomainState state(pool);
  RapidStartGroup group{&state, {0, 4}};
  for (auto policy : policies) {
    std::vector<std::atomic<unsigned>> visits(2048);
    const auto run = [&] {
      ParallelForMailbox(group, 0, 32, [&](size_t row) {
        ParallelForMailbox(group, 0, 64, [&](size_t column) {
          ++visits[row * 64 + column];
        }, policy, true);
      }, policy, true);
    };
    auto other = std::async(std::launch::async, run);
    run();
    const auto status = other.wait_for(5s);
    if (status != std::future_status::ready) pool.Cancel();
    other.get();
    ASSERT_EQ(status, std::future_status::ready);
    for (const auto &value : visits) EXPECT_EQ(value.load(), 2u);
  }
}

TEST(EigenRapidMailbox, CancellationJoinsCapturedProducers) {
  for (auto policy : policies) {
    ThreadPool pool(8, true, false, WorkerIdleMode::ResidentBusy);
    RapidDomainState state(pool);
    RapidStartGroup group{&state, {0, 8}};
    eigen_test_support::WaitForResidentWorkers(group, std::chrono::seconds(5));
    ParallelForMailbox(group, 0, 65536, [&](size_t) { pool.Cancel(); }, policy);
    EXPECT_TRUE(pool.IsCancelled());
  }
}

TEST(EigenRapidMailbox, LargePoolAndUnavailableWorkers) {
  for (unsigned workers : {3u, 65u}) {
    ThreadPool pool(workers, true, false, WorkerIdleMode::ResidentBusy);
    RapidDomainState state(pool);
    std::promise<void> entered, release;
    auto unblocked = release.get_future().share();
    pool.RunInMailbox(MakeTask([&, unblocked] {
      entered.set_value(); unblocked.wait();
    }), workers - 1);
    ASSERT_EQ(entered.get_future().wait_for(2s), std::future_status::ready);
    for (auto policy : policies) {
      std::vector<std::atomic<unsigned>> visits(4096);
      ParallelForMailbox({&state, {0, workers}}, 0, visits.size(),
          [&](size_t i) { ++visits[i]; }, policy, true);
      for (const auto &value : visits) EXPECT_EQ(value.load(), 1u);
    }
    release.set_value();
  }
}

TEST(EigenRapidMailbox, DirectCallbacksRetainTaskAncestry) {
  ThreadPool pool(8, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState state(pool);
  RapidStartGroup group{&state, {0, 8}};
  for (auto policy : policies) {
    std::vector<std::atomic<unsigned>> visits(1024);
    ParallelForMailbox(group, 0, 32, [&](size_t row) {
      EXPECT_TRUE(pool.IsExecutingTask());
      pool.DeregisterResident();
      EXPECT_TRUE(pool.IsExecutingTask());
      ParallelForMailbox(group, 0, 32,
          [&](size_t column) { ++visits[row * 32 + column]; }, policy);
    }, policy);
    for (const auto &count : visits) EXPECT_EQ(count.load(), 1u);
  }
}

TEST(EigenRapidMailbox, GeneratedSubgroupsMatchSerialOracle) {
  constexpr uint64_t seed = 0x230924;
  ThreadPool pool(8, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState state(pool);
  size_t test = 0;
  for (unsigned start = 0; start < 8; ++start) {
    for (unsigned limit = start + 1; limit <= 8; ++limit) {
      for (size_t size : {2u, 3u, 7u, 8u, 17u, 129u}) {
        for (auto policy : policies) {
          for (bool prefer : {false, true}) {
            SCOPED_TRACE(::testing::Message() << "seed=" << seed << " case=" << test++
                << " domain=" << start << ':' << limit << " size=" << size);
            std::vector<std::atomic<unsigned>> visits(size);
            ParallelForMailbox({&state, {start, limit}}, 0, size,
                [&](size_t i) { ++visits[i]; }, policy, prefer);
            for (const auto &count : visits) EXPECT_EQ(count.load(), 1u);
          }
        }
      }
    }
  }
}

TEST(EigenRapidMailbox, AdaptiveChunkMatchesIntegerOracle) {
  using rapid::mailbox_detail::AdaptiveChunk;
  size_t test = 0;
  for (size_t previous : {1u, 2u, 3u, 7u, 16u, 64u})
    for (size_t grain : {size_t{1}, previous})
      for (size_t remaining : {1u, 2u, 3u, 7u, 63u, 64u, 257u})
        for (size_t executed : {size_t{1}, previous})
          for (std::uint64_t elapsed : {0u, 1u, 7u, 31u, 1000u})
            for (std::uint64_t target : {1u, 17u, 500u}) {
              SCOPED_TRACE(::testing::Message() << "adaptive case=" << test++);
              const size_t ideal = executed * target / std::max<std::uint64_t>(1, elapsed);
              const size_t expected = std::min(remaining,
                  std::max(grain, std::min(previous * 4, ideal)));
              EXPECT_EQ(AdaptiveChunk(previous, executed, elapsed, target,
                                      grain, remaining), expected);
            }
  const auto maximum = std::numeric_limits<size_t>::max();
  EXPECT_EQ(AdaptiveChunk(maximum, maximum, 1, maximum, 1, maximum), maximum);
  EXPECT_EQ(AdaptiveChunk(maximum, maximum / 2, maximum, 1, 1, maximum), 1u);
  EXPECT_EQ(AdaptiveChunk(1, 1, 0, maximum, 1, maximum), 4u);
  EXPECT_EQ(AdaptiveChunk(1, 1, 0, maximum, 1, 0), 0u);
}

TEST(EigenRapidMailbox, ClosedSeedDoesNotBindDestroyedCallback) {
  ThreadPool pool(2, true, true, WorkerIdleMode::ResidentBusy);
  bool called = false;
  auto body = [&](size_t) { called = true; };
  using Body = decltype(body);
  static_assert(std::is_same_v<decltype(&mailbox_detail::ProcessSeed<Body>),
      void (*)(partitioner_detail::Region &, Body *, partitioner_detail::LoopRange) noexcept>);
  auto callback = std::make_unique<Body>(body);
  auto *region = NewSmallObject<partitioner_detail::Region>(pool, nullptr, true);
  auto *seed = NewSmallObject<mailbox_detail::Seed<Body>>(*region, *callback, 0, 17, 1);
  region->CloseAndWait();
  callback.reset();
  (*seed)();
  EXPECT_FALSE(called);
  EXPECT_TRUE(region->IsComplete());
  region->TaskComplete();
}

TEST(EigenRapidMailbox, ResidentLimitTransitionsMatchSerialOracle) {
  constexpr uint64_t seed = 0x230923;
  ThreadPool pool(8, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState state(pool);
  RapidStartGroup group{&state, {0, 8}};
  uint64_t random = seed;
  for (size_t test = 0; test < 100; ++test) {
    random = random * 6364136223846793005ULL + 1;
    const size_t limit = test % 5 ? random % 9 : 0;
    const size_t size = test < 64 ? test : 1024 + random % 4096;
    SCOPED_TRACE(::testing::Message() << "seed=" << seed << " case=" << test << " limit=" << limit);
    pool.SetResidentLimit(limit);
    eigen_test_support::WaitForResidentWorkers(group, 2s);
    std::vector<std::atomic<unsigned>> actual(size);
    std::vector<unsigned> expected(size);
    for (size_t i = 0; i < size; ++i) expected[i] = i % 23 + 1;
    ParallelForMailbox(group, 0, size, [&](size_t i) {
      actual[i].fetch_add(i % 23 + 1, std::memory_order_relaxed);
    }, policies[test % 2], test % 3 == 0);
    for (size_t i = 0; i < size; ++i) EXPECT_EQ(actual[i].load(), expected[i]);
  }
}

TEST(EigenRapidMailbox, ClaimedCommandSurvivesResidencyWithdrawal) {
  for (bool handoff : {false, true}) {
    ThreadPool pool(2, true, true, WorkerIdleMode::ResidentBusy);
    RapidDomainState state(pool);
    eigen_test_support::WaitForResidentWorkers({&state, {0, 2}}, 2s);
    unsigned worker = 0;
    ASSERT_EQ(pool.ClaimResidentWorkers({1, 2}, &worker, 1), 1u);
    pool.SetResidentLimit(0);
    struct Probe : ResidentTask {
      void Run(size_t) noexcept override { ++visits; }
      bool IsComplete() const noexcept { return remaining.load(std::memory_order_acquire) == 0; }
      std::atomic<size_t> remaining{1};
      std::atomic<unsigned> visits{0};
    } probe;
    pool.PublishResident(probe, probe.remaining, worker, 0, handoff);
    pool.HelpResidentUntil(probe);
    EXPECT_EQ(probe.visits.load(), 1u);
  }
}

TEST(EigenRapidMailbox, AutomaticCalibrationAndFallbackRemainCorrect) {
  ThreadPool pool(4, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState state(pool);
  RapidStartGroup group{&state, {0, 4}};
  const auto disabled = CalibrateMailbox(group, policies[1], {0ns});
  EXPECT_EQ(pool.ResidentLimit(), 0u);
  EXPECT_TRUE(disabled.trials.empty());
  const auto calibrated = CalibrateMailbox(group, policies[1], {100ms});
  EXPECT_LE(calibrated.resident_limit, 4u);
  EXPECT_GT(calibrated.multiplier, 0u);
  EXPECT_EQ(pool.ResidentLimit(), calibrated.resident_limit);
  EXPECT_EQ(pool.CalibrationMultiplier(), calibrated.multiplier);
  std::vector<std::atomic<unsigned>> actual(4096);
  ParallelForMailbox(group, 0, actual.size(), [&](size_t i) { ++actual[i]; }, policies[1]);
  for (const auto &count : actual) EXPECT_EQ(count.load(), 1u);
  EXPECT_THROW(CalibrateMailbox({&state, {1, 4}}), std::invalid_argument);
  EXPECT_THROW(pool.SetCalibrationMultiplier(0), std::invalid_argument);
  EXPECT_THROW(CalibrateMailbox(group, policies[1], {100ms, -1}), std::invalid_argument);
  pool.Cancel();
  const auto cancelled = CalibrateMailbox(group);
  EXPECT_TRUE(cancelled.cancelled);
  EXPECT_TRUE(cancelled.trials.empty());
  EXPECT_EQ(pool.ResidentLimit(), 0u);
}

TEST(EigenRapidMailbox, CalibrationSelectionPrefersCheaperCohortWithinBand) {
  std::vector<CalibrationTrial> trials{{0, 50, {}, 8}, {2, 20, {}, 1},
      {4, 50, {}, 0.99}, {2, 100, {}, 1.02}};
  EXPECT_EQ(calibration_detail::Select(trials, 0), 2u);
  EXPECT_EQ(calibration_detail::Select(trials, 0.05), 3u);
  EXPECT_EQ(calibration_detail::Select({}, 0.05), 0u);
}

TEST(EigenRapidMailbox, ReconfigurationDuringWorkMatchesSerialOracle) {
  ThreadPool pool(8, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState state(pool);
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
  for (size_t test = 0; test < 50; ++test) {
    SCOPED_TRACE(::testing::Message() << "reconfiguration case=" << test);
    const size_t size = 127 + test * 17;
    std::vector<std::atomic<unsigned>> actual(size);
    std::vector<unsigned> expected(size);
    for (size_t i = 0; i < size; ++i) expected[i] = i % 13 + 1;
    ParallelForMailbox({&state, {0, 8}}, 0, size,
        [&](size_t i) { actual[i].fetch_add(i % 13 + 1); }, policies[test % 2]);
    for (size_t i = 0; i < size; ++i) EXPECT_EQ(actual[i].load(), expected[i]);
  }
}

TEST(EigenRapidMailbox, CalibrationProbesMatchSerialOracle) {
  ThreadPool pool(3, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState state(pool);
  for (size_t limit : {0u, 1u, 3u}) {
    pool.SetResidentLimit(limit);
    for (unsigned kind = 0; kind < 3; ++kind) {
      std::vector<std::uint64_t> actual(129), expected(129);
      for (size_t i = 0; i < expected.size(); ++i) {
        auto value = static_cast<std::uint64_t>(i + 1);
        unsigned count = kind == 0 ? 0 : kind == 1 ? 16 : (i < 8 ? 128 : 4);
        while (count) {
          const unsigned step = (kind == 1 ? 16 : i < 8 ? 128 : 4) - count;
          value = (value ^ (value >> 27)) * 0x3c79ac492ba7b653ULL + step;
          --count;
        }
        expected[i] = value;
      }
      EXPECT_GT(calibration_detail::Probe({&state, {0, 3}}, policies[1], actual, kind), 0u);
      EXPECT_EQ(actual, expected) << "limit=" << limit << " kind=" << kind;
    }
  }
}

TEST(EigenRapidMailbox, CalibrationRejectsTaskContextWithoutChangingPolicy) {
  ThreadPool pool(2, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState state(pool);
  auto done = std::make_shared<std::promise<void>>();
  auto completed = done->get_future();
  pool.RunOnThread(MakeTask([&, done] {
    EXPECT_THROW(CalibrateMailbox({&state, {0, 2}}), std::logic_error);
    done->set_value();
  }), 1);
  completed.wait();
  EXPECT_EQ(pool.ResidentLimit(), 2u);
}

TEST(EigenRapidMailbox, EmptyAndInvalidDomains) {
  EXPECT_NO_THROW(ParallelForMailbox({}, 0, 10, [](size_t) {}, policies[0]));
  ThreadPool pool(2, true, true);
  RapidDomainState state(pool);
  EXPECT_THROW(ParallelForMailbox({&state, {0, 2}}, 0, 10, [](size_t) {},
      policies[0]), std::invalid_argument);
  EXPECT_THROW(ParallelForMailbox({&state, {0, 3}}, 0, 10, [](size_t) {},
      policies[1]), std::invalid_argument);
}
} // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
