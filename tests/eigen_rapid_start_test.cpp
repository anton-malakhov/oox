// SPDX-License-Identifier: Apache-2.0

#include <oox/eigen/rapid_start.h>
#include <oox/eigen/parallel_for.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

using oox::detail::eigen_pool::MakeTask;
using oox::detail::eigen_pool::ThreadPool;
using oox::detail::eigen_pool::WorkerIdleMode;
using oox::detail::eigen_pool::rapid::PrepareResidentGroup;
using oox::detail::eigen_pool::rapid::RapidDomainState;
using oox::detail::eigen_pool::rapid::RapidStartGroup;
using namespace std::chrono_literals;

template <typename F>
void ParallelForResident(RapidStartGroup group, size_t begin, size_t end, F body) {
  oox::detail::eigen_pool::rapid::ParallelForResidentRanges(
      group, begin, end, [&](size_t first, size_t last) {
        for (size_t i = first; i < last; ++i)
          body(i);
      });
}

struct RapidHarness {
  explicit RapidHarness(unsigned workers, bool spinning = false,
                        WorkerIdleMode idle_mode = WorkerIdleMode::Park)
      : pool(static_cast<int>(workers), spinning, false, idle_mode),
        state(pool),
        group{&state, {0, workers}} {}

  ThreadPool pool;
  RapidDomainState state;
  RapidStartGroup group;
};

TEST(EigenRapidResident, MatchesExactOnceOracleAcrossSmallRanges) {
  RapidHarness harness(8, true, WorkerIdleMode::ResidentBusy);
  PrepareResidentGroup(harness.group);
  for (size_t size = 0; size <= 257; ++size) {
    std::vector<std::atomic<unsigned>> visits(size);
    ParallelForResident(harness.group, 0, size, [&](size_t index) {
      visits[index].fetch_add(1, std::memory_order_relaxed);
    });
    for (size_t index = 0; index < size; ++index) {
      ASSERT_EQ(visits[index].load(std::memory_order_relaxed), 1u)
          << "size " << size << ", index " << index;
    }
  }
}

TEST(EigenRapidResident, ClaimMaskMatchesScanningOracle) {
  const auto oracle = [](uint64_t candidates, unsigned first, size_t count) {
    uint64_t result = 0;
    for (unsigned offset = 0; offset < 64 && count; ++offset) {
      const uint64_t bit = uint64_t{1} << ((first + offset) % 64);
      if (candidates & bit) {
        result |= bit;
        --count;
      }
    }
    return result;
  };
  uint64_t random = 0x21aab731;
  for (size_t case_index = 0; case_index < 1256; ++case_index) {
    random = random * 6364136223846793005ULL + 1;
    const uint64_t mask = case_index < 256 ? case_index : random;
    for (unsigned first = 0; first < 64; ++first) {
      for (size_t count : {0u, 1u, 2u, 7u, 16u, 63u, 64u}) {
        EXPECT_EQ(oox::detail::eigen_pool::ResidentClaimMask(mask, first, count),
                  oracle(mask, first, count))
            << "seed=0x21aab731 case=" << case_index
            << " first=" << first << " count=" << count;
      }
    }
  }
}

TEST(EigenRapidResident, OrdinaryTasksAndNestedFallbackMakeProgress) {
  RapidHarness harness(8, true, WorkerIdleMode::ResidentBusy);
  PrepareResidentGroup(harness.group);
  constexpr size_t tasks = 1000;
  std::atomic<size_t> ordinary{0};
  for (size_t task = 0; task < tasks; ++task) {
    harness.pool.Schedule(MakeTask([&] {
      if (ordinary.fetch_add(1, std::memory_order_acq_rel) + 1 == tasks) {
        harness.pool.NotifyTaskCompletion();
      }
    }));
  }
  std::atomic<size_t> nested{0};
  ParallelForResident(harness.group, 0, 128, [&](size_t outer) {
    ParallelForResident(harness.group, 0, 3, [&](size_t inner) {
      nested.fetch_add(outer + inner + 1, std::memory_order_relaxed);
    });
  });
  harness.pool.Wait([&] { return ordinary.load() == tasks; });
  EXPECT_EQ(ordinary.load(), tasks);
  EXPECT_EQ(nested.load(), 25152u);
}

TEST(EigenRapidResident, PropagatesExceptionsAndSupportsLargePools) {
  RapidHarness harness(65, true, WorkerIdleMode::ResidentBusy);
  PrepareResidentGroup(harness.group);
  EXPECT_THROW(ParallelForResident(harness.group, 0, 1024,
                                   [](size_t index) {
                                     if (index == 517) {
                                       throw std::runtime_error("resident");
                                     }
                                   }),
               std::runtime_error);
  std::atomic<size_t> completed{0};
  ParallelForResident(harness.group, 0, 4096,
                      [&](size_t) { completed.fetch_add(1); });
  EXPECT_EQ(completed.load(), 4096u);
}

TEST(EigenRapidResident, OrdinaryPublicationReleasesIdleResidents) {
  RapidHarness harness(8, true, WorkerIdleMode::ResidentBusy);
  PrepareResidentGroup(harness.group);
  std::vector<std::atomic<unsigned>> visits(257);
  auto execution = std::async(std::launch::async, [&] {
    oox::detail::eigen_pool::ParallelFor(harness.pool, 0, visits.size(), [&](size_t i) {
      visits[i].fetch_add(1, std::memory_order_relaxed);
    });
  });
  const auto status = execution.wait_for(2s);
  if (status != std::future_status::ready)
    harness.pool.Cancel();
  execution.get();
  ASSERT_EQ(status, std::future_status::ready);
  for (size_t i = 0; i < visits.size(); ++i)
    EXPECT_EQ(visits[i].load(), 1u) << "index=" << i;
}

TEST(EigenRapidResident, PartialAvailabilityMatchesSerialOracle) {
  constexpr uint64_t seed = 0x7e572021;
  for (unsigned workers : {1u, 3u, 8u, 65u}) {
    for (unsigned busy : {0u, workers / 2, workers}) {
      RapidHarness harness(workers, true, WorkerIdleMode::ResidentBusy);
      PrepareResidentGroup(harness.group);
      std::promise<void> release, entered;
      auto unblocked = release.get_future().share();
      std::atomic<unsigned> arrivals{0};
      for (unsigned i = 0; i < busy; ++i) {
        harness.pool.Schedule(MakeTask([&, unblocked] {
          if (arrivals.fetch_add(1) + 1 == busy)
            entered.set_value();
          unblocked.wait();
        }));
      }
      if (busy && entered.get_future().wait_for(2s) != std::future_status::ready) {
        release.set_value();
        FAIL() << "workers=" << workers << ", busy=" << busy;
      }
      uint64_t random = seed;
      for (size_t case_index = 0; case_index < 98; ++case_index) {
        random = random * 6364136223846793005ULL + 1;
        const size_t size = case_index < 64 ? case_index :
                            case_index < 96 ? (random >> 32) % 1024 :
                            65537 + (random >> 32) % 65536;
        const size_t first = random % 97;
        std::vector<std::atomic<unsigned>> visits(size);
        std::atomic<bool> outside{false};
        auto body = [&](size_t i) {
          if (i < first || i - first >= size)
            outside.store(true);
          else
            visits[i - first].fetch_add(1);
        };
        // When every worker is blocked, resident execution must use the
        // caller. The other APIs require at least one scheduler worker.
        if (busy == workers || case_index % 4 == 0) {
          oox::detail::eigen_pool::rapid::ParallelForResidentRanges(
              harness.group, first, first + size, [&](size_t begin, size_t end) {
                for (size_t i = begin; i < end; ++i)
                  body(i);
              });
        } else if (case_index % 4 == 1) {
          ParallelForResident(harness.group, first, first + size, body);
        } else if (case_index % 4 == 2) {
          oox::detail::eigen_pool::ParallelFor(harness.pool, first, first + size, body);
        } else {
          oox::detail::eigen_pool::ParallelFor(
              harness.pool, first, first + size, body,
              oox::detail::eigen_pool::StaticPartitioner{});
        }
        EXPECT_FALSE(outside.load()) << "seed=" << seed << ", case=" << case_index;
        for (size_t i = 0; i < size; ++i) {
          if (visits[i].load() != 1) {
            release.set_value();
            FAIL() << "seed=" << seed << ", case=" << case_index
                   << ", workers=" << workers << ", busy=" << busy
                   << ", index=" << i;
          }
        }
      }
      release.set_value();
    }
  }
}

TEST(EigenRapidResident, ConcurrentRangeRootsComposeWithDemandPartitioning) {
  RapidHarness harness(8, true, WorkerIdleMode::ResidentBusy);
  PrepareResidentGroup(harness.group);
  std::vector<std::atomic<unsigned>> visits(4 * 257);
  std::vector<std::future<void>> roots;
  for (size_t root = 0; root < 4; ++root) {
    roots.push_back(std::async(std::launch::async, [&, root] {
      for (unsigned round = 0; round < 16; ++round) {
        oox::detail::eigen_pool::rapid::ParallelForResidentRanges(
            harness.group, 0, 257, [&](size_t first, size_t last) {
              oox::detail::eigen_pool::ParallelFor(
                  harness.pool, first, last, [&](size_t i) {
                    visits[root * 257 + i].fetch_add(1);
                  });
            });
      }
    }));
  }
  for (auto &root : roots) {
    const auto status = root.wait_for(5s);
    if (status != std::future_status::ready)
      harness.pool.Cancel();
    EXPECT_EQ(status, std::future_status::ready);
    root.get();
  }
  for (size_t i = 0; i < visits.size(); ++i)
    EXPECT_EQ(visits[i].load(), 16u) << "index=" << i;
}

TEST(EigenRapidResident, RangeExceptionsReleaseHelpersBeforeReuse) {
  RapidHarness harness(8, true, WorkerIdleMode::ResidentBusy);
  PrepareResidentGroup(harness.group);
  using oox::detail::eigen_pool::rapid::ParallelForResidentRanges;
  EXPECT_THROW(ParallelForResidentRanges(harness.group, 0, 257,
      [](size_t first, size_t last) {
        if (first <= 129 && 129 < last)
          throw std::runtime_error("range callback");
      }), std::runtime_error);
  std::vector<std::atomic<unsigned>> visits(257);
  ParallelForResidentRanges(harness.group, 0, visits.size(),
      [&](size_t first, size_t last) {
        ParallelForResidentRanges(harness.group, first, last,
            [&](size_t begin, size_t end) {
              for (size_t i = begin; i < end; ++i)
                visits[i].fetch_add(1);
            });
      });
  for (size_t i = 0; i < visits.size(); ++i)
    EXPECT_EQ(visits[i].load(), 1u) << "index=" << i;
}

TEST(EigenRapidResident, EmptyGroupsAndInvalidDomains) {
  using oox::detail::eigen_pool::rapid::ParallelForResidentRanges;
  bool called = false;
  PrepareResidentGroup({});
  ParallelForResidentRanges({}, 0, 8, [&](size_t, size_t) { called = true; });
  EXPECT_FALSE(called);
  RapidHarness harness(2, true, WorkerIdleMode::ResidentBusy);
  EXPECT_THROW(harness.group.Subgroup({0, 3}), std::invalid_argument);
  const RapidStartGroup invalid{&harness.state, {1, 3}};
  EXPECT_THROW(ParallelForResidentRanges(invalid, 0, 8,
      [](size_t, size_t) {}), std::invalid_argument);
  RapidHarness parked(2);
  EXPECT_THROW(PrepareResidentGroup(parked.group), std::invalid_argument);
  EXPECT_THROW(ParallelForResidentRanges(parked.group, 0, 8,
      [](size_t, size_t) {}), std::invalid_argument);
}

TEST(EigenRapidResident, CancellationJoinsCapturedHelpers) {
  RapidHarness harness(8, true, WorkerIdleMode::ResidentBusy);
  PrepareResidentGroup(harness.group);
  oox::detail::eigen_pool::rapid::ParallelForResidentRanges(
      harness.group, 0, 257, [&](size_t first, size_t) {
        if (first == 0)
          harness.pool.Cancel();
      });
  EXPECT_TRUE(harness.pool.IsCancelled());
  bool called = false;
  ParallelForResident(harness.group, 0, 8, [&](size_t) { called = true; });
  EXPECT_FALSE(called);
}

} // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
