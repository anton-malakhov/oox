// SPDX-License-Identifier: Apache-2.0

#include <oox/eigen/rapid_start.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace {

using oox::detail::eigen_pool::MakeTask;
using oox::detail::eigen_pool::ThreadPool;
using oox::detail::eigen_pool::rapid::CalibratedTimespanSchedulingOverheadNs;
using oox::detail::eigen_pool::rapid::ParallelFor;
using oox::detail::eigen_pool::rapid::ParallelForLazyStealing;
using oox::detail::eigen_pool::rapid::ParallelForMailbox;
using oox::detail::eigen_pool::rapid::ParallelForTimespanLazyStealing;
using oox::detail::eigen_pool::rapid::RapidDomainState;
using oox::detail::eigen_pool::rapid::RapidStartGroup;
using oox::detail::eigen_pool::rapid::TimespanBlockSize;
using oox::detail::eigen_pool::rapid::TimespanDomainSchedulingOverheadNs;
using oox::detail::eigen_pool::rapid::TimespanTargetNanoseconds;
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
  threads.reserve(callers);
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

TEST(EigenRapidStart, RejectsDomainsOutsideThePool) {
  RapidHarness harness(4);
  EXPECT_THROW(ParallelFor({&harness.state, {0, 5}}, 0, 1, [](size_t) {}),
               std::invalid_argument);
  EXPECT_THROW(ParallelFor({&harness.state, {3, 2}}, 0, 1, [](size_t) {}),
               std::invalid_argument);
}

TEST(EigenRapidStart, RejectsActivationCapacityOverflow) {
  ThreadPool pool(1, false, false);
  EXPECT_THROW(
      { RapidDomainState state(pool, std::numeric_limits<size_t>::max()); },
      std::length_error);
}

TEST(EigenRapidStart, CancelledSingleWorkerDoesNotStartNewWork) {
  RapidHarness harness(1);
  harness.pool.Cancel();
  std::atomic<unsigned> completed{0};
  ParallelFor(harness.group, 0, 128,
              [&](size_t) { completed.fetch_add(1, std::memory_order_relaxed); });
  EXPECT_EQ(completed.load(), 0u);
}

TEST(EigenRapidStart, CancellationReleasesDescriptorWaiters) {
  RapidHarness harness(2, false, 1);
  auto *first = harness.state.Acquire();
  auto *second = harness.state.Acquire();
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);

  std::promise<void> started;
  auto started_future = started.get_future();
  auto result = std::async(std::launch::async, [&] {
    started.set_value();
    ParallelFor(harness.group, 0, 128, [](size_t) {});
  });
  ASSERT_EQ(started_future.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(result.wait_for(20ms), std::future_status::timeout);
  harness.pool.Cancel();
  EXPECT_EQ(result.wait_for(5s), std::future_status::ready);

  harness.state.Release(*second);
  harness.state.Release(*first);
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

TEST(EigenRapidStart, CancellationCompletesConcurrentRoots) {
  RapidHarness harness(8);
  constexpr int callers = 8;
  std::atomic<int> entered{0};
  std::vector<std::future<void>> results;
  results.reserve(callers);
  for (int caller = 0; caller < callers; ++caller) {
    results.push_back(std::async(std::launch::async, [&] {
      ParallelFor(harness.group, 0, 10000, [&](size_t index) {
        if (index == 0) {
          entered.fetch_add(1, std::memory_order_release);
        }
        std::this_thread::sleep_for(100us);
      });
    }));
  }
  while (entered.load(std::memory_order_acquire) != callers) {
    std::this_thread::yield();
  }
  harness.pool.Cancel();
  for (auto &result : results) {
    EXPECT_EQ(result.wait_for(5s), std::future_status::ready);
  }
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

enum class HybridPolicy : std::uint8_t {
  Mailbox,
  LazyStealing,
  TimespanLazyStealing
};

template <typename F>
void RunHybrid(HybridPolicy policy, RapidStartGroup group, size_t begin,
               size_t end, F &&function, size_t grain = 1) {
  if (policy == HybridPolicy::Mailbox) {
    ParallelForMailbox(group, begin, end, std::forward<F>(function), grain);
  } else if (policy == HybridPolicy::LazyStealing) {
    ParallelForLazyStealing(group, begin, end, std::forward<F>(function),
                            grain);
  } else {
    ParallelForTimespanLazyStealing(group, begin, end,
                                    std::forward<F>(function), grain);
  }
}

class EigenRapidHybridTest : public testing::TestWithParam<HybridPolicy> {};

TEST_P(EigenRapidHybridTest, ExecutesEveryIterationExactlyOnceAndNests) {
  RapidHarness harness(8);
  std::vector<std::atomic<unsigned>> visits(4096);
  RunHybrid(GetParam(), harness.group, 0, visits.size(), [&](size_t index) {
    visits[index].fetch_add(1, std::memory_order_relaxed);
    if (index < 8) {
      std::atomic<unsigned> nested{0};
      RunHybrid(GetParam(), harness.group, 0, 16,
                [&](size_t) { nested.fetch_add(1, std::memory_order_relaxed); });
      EXPECT_EQ(nested.load(), 16u);
    }
  });
  for (const auto &visit : visits) {
    EXPECT_EQ(visit.load(), 1u);
  }
}

TEST_P(EigenRapidHybridTest, MatchesSerialOracleAcrossSmallRangesAndGrains) {
  RapidHarness harness(8);
  for (size_t size = 0; size <= 65; ++size) {
    for (const size_t grain : {size_t{1}, size_t{2}, size_t{7}, size_t{64},
                               size_t{1024}}) {
      constexpr size_t begin = 3;
      std::vector<std::atomic<unsigned>> visits(begin + size + 3);
      RunHybrid(GetParam(), harness.group, begin, begin + size,
                [&](size_t index) {
                  visits[index].fetch_add(1, std::memory_order_relaxed);
                },
                grain);
      for (size_t index = 0; index < visits.size(); ++index) {
        const unsigned expected =
            index >= begin && index < begin + size ? 1u : 0u;
        ASSERT_EQ(visits[index].load(), expected)
            << "size " << size << ", grain " << grain << ", index " << index;
      }
    }
  }
}

TEST_P(EigenRapidHybridTest, PropagatesExceptionsAndCanBeReused) {
  RapidHarness harness(8);
  EXPECT_THROW(RunHybrid(GetParam(), harness.group, 0, 1024,
                         [](size_t index) {
                           if (index == 127) {
                             throw std::runtime_error("hybrid failure");
                           }
                         }),
               std::runtime_error);
  std::atomic<unsigned> completed{0};
  RunHybrid(GetParam(), harness.group, 0, 1024,
            [&](size_t) { completed.fetch_add(1, std::memory_order_relaxed); });
  EXPECT_EQ(completed.load(), 1024u);
}

TEST_P(EigenRapidHybridTest, CancellationCompletesPublishedWork) {
  RapidHarness harness(8);
  std::atomic<bool> entered{false};
  auto result = std::async(std::launch::async, [&] {
    RunHybrid(GetParam(), harness.group, 0, 10000, [&](size_t) {
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

TEST_P(EigenRapidHybridTest, OneWorkerHonorsCancellation) {
  RapidHarness harness(1);
  harness.pool.Cancel();
  std::atomic<unsigned> completed{0};
  RunHybrid(GetParam(), harness.group, 0, 1024,
            [&](size_t) { completed.fetch_add(1, std::memory_order_relaxed); });
  EXPECT_EQ(completed.load(), 0u);
}

TEST(EigenRapidMailbox, RunsOrdinaryTasksOutsideRapidDomains) {
  RapidHarness harness(8);
  std::atomic<bool> outside{true};
  ParallelForMailbox(harness.group, 0, 1024, [&](size_t) {
    if (harness.pool.CurrentRegionContext() != nullptr) {
      outside.store(false, std::memory_order_relaxed);
    }
  });
  EXPECT_TRUE(outside.load());
}

TEST(EigenRapidMailbox, CallableCanCancelItsPool) {
  RapidHarness harness(8);
  auto result = std::async(std::launch::async, [&] {
    ParallelForMailbox(harness.group, 0, 4096, [&](size_t index) {
      if (index == 0) {
        harness.pool.Cancel();
      }
    });
  });
  EXPECT_EQ(result.wait_for(5s), std::future_status::ready);
}

TEST(EigenRapidMailbox, KeepsNestedWorkInsideItsLogicalOwner) {
  RapidHarness harness(8);
  std::atomic<bool> nested_stayed_local{true};
  ParallelForMailbox(harness.group, 0, 1024, [&](size_t index) {
    if (index != 0) {
      return;
    }
    const size_t outer_worker = harness.pool.CurrentThreadId();
    ParallelForMailbox(harness.group, 0, 128, [&](size_t) {
      if (harness.pool.CurrentThreadId() != outer_worker) {
        nested_stayed_local.store(false, std::memory_order_relaxed);
      }
    });
  });
  EXPECT_TRUE(nested_stayed_local.load());
}

TEST(EigenRapidMailbox, NestedOneWorkerMakesProgress) {
  RapidHarness harness(1);
  std::atomic<unsigned> completed{0};
  ParallelForMailbox(harness.group, 0, 16, [&](size_t) {
    ParallelForMailbox(harness.group, 0, 16, [&](size_t) {
      completed.fetch_add(1, std::memory_order_relaxed);
    });
  });
  EXPECT_EQ(completed.load(), 256u);
}

TEST(EigenRapidMailbox, DifferentPoolDoesNotInheritLogicalDomain) {
  RapidHarness outer(8);
  RapidHarness inner(2);
  std::atomic<bool> ran_on_inner_pool{true};
  ParallelForMailbox(outer.group, 0, 1024, [&](size_t index) {
    if (index != 0) {
      return;
    }
    ParallelForMailbox(inner.group, 0, 128, [&](size_t) {
      if (inner.pool.CurrentThreadId() == -1) {
        ran_on_inner_pool.store(false, std::memory_order_relaxed);
      }
    });
  });
  EXPECT_TRUE(ran_on_inner_pool.load());
}

TEST(EigenRapidLazyStealing, DoesNotDeregisterWithoutAStall) {
  RapidHarness harness(8);
  const size_t before = harness.pool.RapidDeregistrationCount();
  ParallelForLazyStealing(harness.group, 0, 8, [](size_t) {}, 1024);
  EXPECT_EQ(harness.pool.RapidDeregistrationCount(), before);
}

TEST(EigenRapidTimespanLazyStealing, DoesNotDeregisterWithoutAStall) {
  RapidHarness harness(8);
  const size_t before = harness.pool.RapidDeregistrationCount();
  ParallelForTimespanLazyStealing(harness.group, 0, 8, [](size_t) {}, 1024);
  EXPECT_EQ(harness.pool.RapidDeregistrationCount(), before);
}

TEST(EigenRapidTimespanLazyStealing, BoundsAdaptiveBlockChanges) {
  EXPECT_EQ(TimespanBlockSize(16, 16, 10'000, 1024, 1, 80'000),
            128u);
  EXPECT_EQ(TimespanBlockSize(16, 16, 320'000, 1024, 1, 80'000),
            4u);
  EXPECT_EQ(TimespanBlockSize(64, 64, 1, 10, 2, 80'000), 3u);
}

TEST(EigenRapidTimespanLazyStealing, DerivesTargetFromRuntimeInputs) {
  EXPECT_EQ(TimespanDomainSchedulingOverheadNs(100, 8), 800u);
  EXPECT_EQ(TimespanDomainSchedulingOverheadNs(
                std::numeric_limits<size_t>::max(), 2),
            std::numeric_limits<size_t>::max());
  EXPECT_EQ(TimespanTargetNanoseconds(100, 10, 1'000, 10'000, 0, 4),
            10'000u);
  EXPECT_EQ(TimespanTargetNanoseconds(100, 10, 1'000, 10'000, 4, 4),
            7'071u);
  EXPECT_EQ(TimespanTargetNanoseconds(100, 10, 1'000, 0, 1, 4), 100u);
  EXPECT_GT(CalibratedTimespanSchedulingOverheadNs(), 0u);
}

TEST(EigenRapidLazyStealing, NestedOneWorkerMakesProgress) {
  RapidHarness harness(1);
  std::atomic<unsigned> completed{0};
  ParallelForLazyStealing(harness.group, 0, 16, [&](size_t) {
    ParallelForLazyStealing(harness.group, 0, 16, [&](size_t) {
      completed.fetch_add(1, std::memory_order_relaxed);
    });
  });
  EXPECT_EQ(completed.load(), 256u);
}

TEST(EigenRapidLazyStealing, DescriptorScarcityGroupsRangesWithoutDeadlock) {
  RapidHarness harness(8, false, 0);
  std::vector<std::atomic<unsigned>> visits(1024);
  ParallelForLazyStealing(harness.group, 0, visits.size(), [&](size_t index) {
    visits[index].fetch_add(1, std::memory_order_relaxed);
  });
  for (const auto &visit : visits) {
    EXPECT_EQ(visit.load(), 1u);
  }
}

TEST(EigenRapidLazyStealing, DifferentPoolDoesNotInheritRapidDomain) {
  RapidHarness outer(8);
  RapidHarness inner(2);
  std::atomic<bool> used_inner_context{true};
  ParallelForLazyStealing(outer.group, 0, 1024, [&](size_t index) {
    if (index != 0) {
      return;
    }
    ParallelForLazyStealing(inner.group, 0, 128, [&](size_t) {
      const auto *context = inner.pool.CurrentRegionContext();
      if (!context || context->rapid_state != &inner.state) {
        used_inner_context.store(false, std::memory_order_relaxed);
      }
    });
  });
  EXPECT_TRUE(used_inner_context.load());
}

TEST(EigenRapidLazyStealing, ProtectsOwnerBlockWhilePeerStealsLaterBlocks) {
  RapidHarness harness(2);
  harness.AwaitRegistrations();
  std::atomic<bool> owner_entered{false};
  std::atomic<unsigned> stolen{0};
  std::promise<void> release_owner;
  auto release = release_owner.get_future().share();
  auto watchdog = std::async(std::launch::async, [&] {
    auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!owner_entered.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::yield();
    }
    if (!owner_entered.load(std::memory_order_acquire)) {
      release_owner.set_value();
      return false;
    }
    deadline = std::chrono::steady_clock::now() + 5s;
    while (stolen.load(std::memory_order_acquire) == 0 &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::yield();
    }
    release_owner.set_value();
    return stolen.load(std::memory_order_acquire) != 0;
  });
  const size_t before = harness.pool.RapidDeregistrationCount();
  ParallelForLazyStealing(harness.group, 0, 128, [&](size_t index) {
    if (index == 0) {
      owner_entered.store(true, std::memory_order_release);
      release.wait();
    } else if (index < 64) {
      stolen.fetch_add(1, std::memory_order_release);
    }
  });
  EXPECT_TRUE(watchdog.get());
  EXPECT_GT(harness.pool.RapidDeregistrationCount(), before);
}

TEST(EigenRapidLazyStealing, DeregistersAtFirstGlobalSteal) {
  auto pool = std::make_unique<ThreadPool>(2, false, true);
  std::promise<void> blocker_entered;
  std::promise<void> release_blocker;
  auto release = release_blocker.get_future().share();
  pool->RunOnThread(MakeTask([&blocker_entered, release] {
                              blocker_entered.set_value();
                              release.wait();
                            }),
                            1);
  if (blocker_entered.get_future().wait_for(5s) !=
      std::future_status::ready) {
    release_blocker.set_value();
    FAIL() << "blocking worker did not start";
    return;
  }

  std::atomic<unsigned> stolen{0};
  pool->RunOnThread(
      MakeTask([&] { stolen.fetch_add(1, std::memory_order_relaxed); }), 1);
  oox::detail::eigen_pool::RegionContext context{{0, 1}, nullptr, true};
  const size_t before = pool->RapidDeregistrationCount();
  EXPECT_TRUE(pool->ExecuteInRegion(
      &context, [&] { return pool->TryExecuteSomething(); }));
  EXPECT_EQ(stolen.load(), 1u);
  EXPECT_EQ(pool->RapidDeregistrationCount(), before + 1);
  release_blocker.set_value();
}

INSTANTIATE_TEST_SUITE_P(AllPolicies, EigenRapidHybridTest,
                         testing::Values(HybridPolicy::Mailbox,
                                         HybridPolicy::LazyStealing,
                                         HybridPolicy::TimespanLazyStealing));

} // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
