// SPDX-License-Identifier: Apache-2.0

#include <oox/eigen/rapid_start.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

using oox::detail::eigen_pool::MakeTask;
using oox::detail::eigen_pool::ThreadPool;
using oox::detail::eigen_pool::rapid::ParallelFor;
using oox::detail::eigen_pool::rapid::RapidDomainState;
using oox::detail::eigen_pool::rapid::RapidStartGroup;
using namespace std::chrono_literals;

struct RapidHarness {
  explicit RapidHarness(unsigned workers, bool spinning = false,
                        size_t slots_per_worker = 128)
      : pool(static_cast<int>(workers), spinning, false),
        state(pool, slots_per_worker),
        group{&state, {0, workers}} {}

  void AwaitRegistrations() {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (pool.WorkerRegistrationCount() != pool.NumThreads()) {
      ASSERT_LT(std::chrono::steady_clock::now(), deadline);
      std::this_thread::yield();
    }
  }

  ThreadPool pool;
  RapidDomainState state;
  RapidStartGroup group;
};

TEST(EigenRapidStart, RegistrationsStayConstantAcrossLoopsAndNesting) {
  RapidHarness harness(8);
  harness.AwaitRegistrations();
  const size_t registrations = harness.pool.WorkerRegistrationCount();
  for (int iteration = 0; iteration < 200; ++iteration) {
    ParallelFor(harness.group, 0, 4, [&](size_t) {
      ParallelFor(harness.group, 0, 8, [](size_t) {});
    });
  }
  EXPECT_EQ(harness.pool.WorkerRegistrationCount(), registrations);
  EXPECT_EQ(registrations, harness.pool.NumThreads());
}

TEST(EigenRapidStart, InheritsProportionalDomains) {
  RapidHarness harness(16);
  for (const auto [outer, expected_budget] :
       std::vector<std::pair<size_t, size_t>>{
           {1, 16}, {4, 4}, {16, 1}, {64, 1}}) {
    std::atomic<bool> correct{true};
    ParallelFor(harness.group, 0, outer, [&](size_t) {
      const auto *context = harness.pool.CurrentRegionContext();
      if (!context || context->domain.Size() != expected_budget) {
        correct.store(false, std::memory_order_relaxed);
      }
      ParallelFor(harness.group, 0, 1, [&](size_t) {
        const auto *inner = harness.pool.CurrentRegionContext();
        if (!inner || inner->domain.Size() != expected_budget) {
          correct.store(false, std::memory_order_relaxed);
        }
      });
    });
    EXPECT_TRUE(correct.load()) << "outer size " << outer;
  }
}

TEST(EigenRapidStart, ConcurrentRootsAreIndependent) {
  RapidHarness harness(8);
  constexpr int callers = 8;
  constexpr int work = 1000;
  std::atomic<int> completed{0};
  std::vector<std::thread> threads;
  for (int caller = 0; caller < callers; ++caller) {
    threads.emplace_back([&] {
      ParallelFor(harness.group, 0, work,
                  [&](size_t) { completed.fetch_add(1); });
    });
  }
  for (auto &thread : threads) {
    thread.join();
  }
  EXPECT_EQ(completed.load(), callers * work);
}

TEST(EigenRapidStart, NestedDepthFourMakesProgress) {
  RapidHarness harness(8);
  std::atomic<size_t> leaves{0};
  std::function<void(int)> recurse = [&](int depth) {
    if (depth == 0) {
      leaves.fetch_add(1);
      return;
    }
    ParallelFor(harness.group, 0, 2, [&](size_t) { recurse(depth - 1); });
  };
  recurse(4);
  EXPECT_EQ(leaves.load(), 16u);
}

TEST(EigenRapidStart, QueueSaturationAndParkedWorkersMakeProgress) {
  RapidHarness harness(4, false);
  constexpr int ordinary_tasks = 5000;
  std::atomic<int> ordinary_done{0};
  for (int i = 0; i < ordinary_tasks; ++i) {
    harness.pool.Schedule(MakeTask([&] {
      if (ordinary_done.fetch_add(1, std::memory_order_acq_rel) + 1 ==
          ordinary_tasks) {
        harness.pool.NotifyTaskCompletion();
      }
    }));
  }
  std::atomic<int> rapid_done{0};
  ParallelFor(harness.group, 0, 512, [&](size_t) { rapid_done.fetch_add(1); });
  harness.pool.Wait([&] { return ordinary_done.load() == ordinary_tasks; });
  EXPECT_EQ(rapid_done.load(), 512);
}

TEST(EigenRapidStart, PropagatesExceptionsAndReusesDescriptors) {
  RapidHarness harness(8);
  EXPECT_THROW(ParallelFor(harness.group, 0, 128,
                           [](size_t index) {
                             if (index == 37) {
                               throw std::runtime_error("rapid failure");
                             }
                           }),
               std::runtime_error);
  for (int iteration = 0; iteration < 2000; ++iteration) {
    std::atomic<int> count{0};
    ParallelFor(harness.group, 0, 8, [&](size_t) { count.fetch_add(1); });
    ASSERT_EQ(count.load(), 8);
  }
}

TEST(EigenRapidStart, DirectActivationsStayAliveUntilTryRunReturns) {
  RapidHarness harness(8, false, 2);
  for (int iteration = 0; iteration < 5000; ++iteration) {
    std::atomic<int> count{0};
    ParallelFor(harness.group, 0, 64, [&](size_t) { count.fetch_add(1); });
    ASSERT_EQ(count.load(), 64) << "iteration " << iteration;
  }
}

TEST(EigenRapidStart, DescriptorScarcityFallsBackWithoutDeadlock) {
  RapidHarness harness(2, false, 1);
  std::atomic<int> count{0};
  ParallelFor(harness.group, 0, 256, [&](size_t) { count.fetch_add(1); });
  EXPECT_EQ(count.load(), 256);
}

TEST(EigenRapidStart, SerialSubdomainPropagatesExceptions) {
  RapidHarness harness(8);
  EXPECT_THROW(
      ParallelFor(harness.group, 0, 8, [&](size_t outer) {
        ParallelFor(harness.group, 0, 4, [&](size_t inner) {
          if (outer == 3 && inner == 2) {
            throw std::runtime_error("nested rapid failure");
          }
        });
      }),
      std::runtime_error);
}

TEST(EigenRapidStart, PoolCancellationCompletesPublishedRegions) {
  RapidHarness harness(8);
  std::atomic<bool> entered{false};
  auto result = std::async(std::launch::async, [&] {
    ParallelFor(harness.group, 0, 10000, [&](size_t) {
      entered.store(true, std::memory_order_release);
      std::this_thread::sleep_for(100us);
    });
  });
  while (!entered.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  harness.pool.Cancel();
  EXPECT_EQ(result.wait_for(5s), std::future_status::ready);
}

TEST(EigenRapidStart, SupportsMoreThanSixtyFourWorkers) {
  RapidHarness harness(65);
  harness.AwaitRegistrations();
  std::atomic<int> completed{0};
  ParallelFor(harness.group, 0, 260, [&](size_t) { completed.fetch_add(1); });
  EXPECT_EQ(completed.load(), 260);
  EXPECT_EQ(harness.pool.WorkerRegistrationCount(), 65u);
}

TEST(EigenRapidStart, ElasticLendingLeasesWholeTopologySubtrees) {
  RapidHarness harness(16);
  auto left = harness.state.TryLeaseSubtree({0, 8});
  ASSERT_TRUE(left);
  EXPECT_EQ(left.Domain().Size(), 8u);
  EXPECT_GT(left.Generation(), 0u);
  EXPECT_FALSE(harness.state.TryLeaseSubtree({0, 8}));
  auto right = harness.state.TryLeaseSubtree({8, 16});
  EXPECT_TRUE(right);
  left.Reset();
  EXPECT_TRUE(harness.state.TryLeaseSubtree({0, 8}));
  EXPECT_FALSE(harness.state.TryLeaseSubtree({1, 7}));
}

} // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
