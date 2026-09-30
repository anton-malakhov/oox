// SPDX-License-Identifier: Apache-2.0
namespace cancellation_test { void Claimed(unsigned) noexcept; }
#define OOX_EIGEN_TEST_RAPID_CLAIMED(count) cancellation_test::Claimed(count)
#include <oox/eigen/rapid_auto.h>
#undef OOX_EIGEN_TEST_RAPID_CLAIMED
#include "test_support/eigen_resident.h"
#include "eigen_test_wait.h"
#include <gtest/gtest.h>
#include <string>

namespace cancellation_test {
struct Gate {
  const char *context;
  std::atomic<unsigned> publishers{0}, helpers{0};
  std::atomic<bool> publish{false};
};
std::atomic<Gate *> active_gate{nullptr};

void Claimed(unsigned helpers) noexcept {
  if (auto *gate = active_gate.load()) {
    gate->helpers.fetch_add(helpers);
    gate->publishers.fetch_add(1);
    eigen_test_support::WaitUntil([&] { return gate->publish.load(); }, gate->context);
  }
}
} // namespace cancellation_test

TEST(EigenRapidCancellationDeathTest, WaitTimeoutReportsFailure) {
  EXPECT_EXIT({
    eigen_test_support::WaitUntil([] { return false; }, "predicate deadline",
                                 std::chrono::seconds(0));
  }, testing::ExitedWithCode(EXIT_FAILURE), "predicate deadline");
}

TEST(EigenRapidCancellationDeathTest, FutureTimeoutDoesNotUnwindIntoBlockedDestructor) {
  EXPECT_EXIT({
    std::promise<void> release;
    auto ready = release.get_future().share();
    auto future = std::async(std::launch::async, [ready] { ready.wait(); });
    eigen_test_support::GetReady(future, "blocked future deadline", std::chrono::seconds(0));
  }, testing::ExitedWithCode(EXIT_FAILURE), "blocked future deadline");
}

TEST(EigenRapidCancellation, ConcurrentRootsMatchSerialOracleBeforeAndAfterPublication) {
  using namespace oox::detail::eigen_pool;
  using namespace oox::detail::eigen_pool::rapid;
  constexpr uint64_t seed = 0x300926;
  uint64_t random = seed;
  // Dense small ranges, odd splits, and a few large generated cases.
  for (unsigned test = 0; test < 36; ++test) {
    random = random * 6364136223846793005ULL + 1;
    const size_t size = test < 32 ? test + 2 :
        (size_t{1} << (test - 14)) + random % 31;
    const size_t begin = test % 2 ? size_t(-1) - size : random % 4096;
    const unsigned callers = 2 + test % 3;
    const bool terminal = test % 2 == 0;
    const size_t grain = terminal ? (size + 1) / 2 : 1;
    // 0: full execution; 1: cancel with claims held; 2: cancel in callbacks.
    for (unsigned phase = 0; phase < 3; ++phase) {
      const std::string context = "seed=" + std::to_string(seed) + " case=" +
          std::to_string(test) + " phase=" + std::to_string(phase);
      SCOPED_TRACE(context);
      ThreadPool pool(callers + 1, true, true, WorkerIdleMode::ResidentBusy);
      RapidDomainState domain(pool);
      eigen_test_support::WaitForResidentWorkers({&domain, {0, callers + 1}},
                                                 std::chrono::seconds(10));
      std::vector<std::atomic<unsigned>> actual(callers * size);
      std::vector<std::atomic<size_t>> callback_entries(callers);
      std::vector<size_t> activated(callers);
      std::atomic<unsigned> arrivals{0}, active_callbacks{0};
      std::atomic<bool> release_callbacks{false}, outside{false};
      cancellation_test::Gate gate{context.c_str()};
      std::vector<std::future<void>> roots;
      roots.reserve(callers);
      // Release both gates before futures are destroyed on an exception.
      struct ReleaseGates {
        cancellation_test::Gate &gate;
        std::atomic<bool> &callbacks;
        ~ReleaseGates() {
          gate.publish = true;
          callbacks = true;
          cancellation_test::active_gate = nullptr;
        }
      } release{gate, release_callbacks};
      cancellation_test::active_gate = &gate;
      for (unsigned root = 0; root < callers; ++root) {
        roots.push_back(std::async(std::launch::async, [&, root] {
          // Terminal pairs compete in one shared domain, capturing one helper
          // each. Batched roots use disjoint domains to guarantee capture
          // before any root may publish ordinary work.
          const DomainId workers = terminal ? DomainId{0, callers + 1} :
                                               DomainId{root + 1, root + 2};
          ParallelForAuto({&domain, workers}, begin, begin + size,
              [&](size_t i) {
                ++callback_entries[root]; // Count even incorrectly admitted calls.
                if (pool.IsCancelled()) return; // Cooperative callback policy.
                if (i < begin || i - begin >= size) { outside = true; return; }
                const size_t index = i - begin;
                ++active_callbacks;
                actual[root * size + index].fetch_add(unsigned(index % 23 + root + 1));
                if (index == 0 || index == size / 2) {
                  ++arrivals;
                  eigen_test_support::WaitUntil([&] { return release_callbacks.load(); },
                                                context.c_str());
                }
                --active_callbacks;
              }, grain, nullptr, &activated[root]);
        }));
      }
      eigen_test_support::WaitUntil([&] { return gate.publishers == callers; }, context.c_str());
      EXPECT_EQ(gate.helpers.load(), callers);
      if (phase == 1) pool.Cancel();
      gate.publish = true;
      if (phase != 1) {
        eigen_test_support::WaitUntil([&] { return arrivals == 2 * callers; }, context.c_str());
        EXPECT_EQ(active_callbacks.load(), 2 * callers);
        if (phase == 2) pool.Cancel();
      }
      release_callbacks = true;
      for (auto &root : roots) eigen_test_support::GetReady(root, context.c_str());
      cancellation_test::active_gate = nullptr;
      EXPECT_EQ(active_callbacks.load(), 0u);
      EXPECT_FALSE(outside.load());
      for (unsigned root = 0; root < callers; ++root) {
        EXPECT_EQ(activated[root], 1u);
        if (phase == 1) EXPECT_EQ(callback_entries[root].load(), 0u);
        if (phase == 0) EXPECT_EQ(callback_entries[root].load(), size);
        // Independent serial model: all indices without cancellation, none
        // before publication, only the two blocked entries for phase 2.
        for (size_t index = 0; index < size; ++index) {
          const bool executed = phase == 0 ||
              (phase == 2 && (index == 0 || index == size / 2));
          const unsigned expected = executed ? unsigned(index % 23 + root + 1) : 0;
          ASSERT_EQ(actual[root * size + index].load(), expected)
              << "root=" << root << " index=" << index;
        }
      }
    }
  }
}

namespace {
using namespace oox::detail::eigen_pool;

// Hold two ordinary workers while the first root captures the other two.
// Release before returning or unwinding, including partially scheduled setup.
struct HeldWorkers {
  ThreadPool &pool;
  const char *context;
  std::atomic<bool> released{false};
  std::atomic<unsigned> started{0}, finished{0};
  unsigned submitted = 0;

  ~HeldWorkers() { Release(); }
  void Start() {
    for (unsigned worker : {1u, 2u}) {
      pool.RunOnThread(MakeTask([this] {
        ++started;
        eigen_test_support::WaitUntil([&] { return released.load(); }, context);
        ++finished;
      }), worker);
      ++submitted;
    }
    eigen_test_support::WaitUntil([&] { return started == submitted; }, context);
  }
  void Release() {
    released = true;
    eigen_test_support::WaitUntil([&] { return finished == submitted; }, context);
  }
};

std::vector<bool> SerialPrefixStarts(size_t size, size_t participants) {
  // A plain FIFO of integer intervals, independent of Auto state/Launch.
  std::vector<std::pair<size_t, size_t>> ranges{{0, size}};
  std::vector<size_t> pending{0};
  for (size_t head = 0; ranges.size() < participants && head < pending.size();) {
    const size_t index = pending[head++];
    const auto [first, last] = ranges[index];
    if (last - first <= 1) continue;
    const size_t middle = first + (last - first) / 2;
    ranges[index].second = middle;
    ranges.emplace_back(middle, last);
    pending.push_back(index);
    pending.push_back(ranges.size() - 1);
  }
  std::vector<bool> starts(size);
  for (const auto &[first, last] : ranges) starts[first] = true;
  return starts;
}
} // namespace

TEST(EigenRapidCancellation, SharedDomainBatchesAndOrdinaryFallbackMatchSerialOracle) {
  using namespace oox::detail::eigen_pool;
  using namespace oox::detail::eigen_pool::rapid;
  constexpr uint64_t seed = 0x300927;
  uint64_t random = seed;
  for (unsigned test = 0; test < 28; ++test) {
    random = random * 6364136223846793005ULL + 1;
    const size_t size = test < 24 ? test + 3 :
        (size_t{1} << (test - 6)) + random % 31;
    const auto starts = SerialPrefixStarts(size, 3); // Caller and two helpers.
    for (unsigned phase = 0; phase < 3; ++phase) {
      const std::string context = "shared domain seed=" + std::to_string(seed) +
          " case=" + std::to_string(test) + " phase=" + std::to_string(phase);
      SCOPED_TRACE(context);
      ThreadPool pool(5, true, true, WorkerIdleMode::ResidentBusy);
      RapidDomainState domain(pool);
      RapidStartGroup group{&domain, {0, 5}};
      eigen_test_support::WaitForResidentWorkers(group, std::chrono::seconds(10));
      HeldWorkers held{pool, context.c_str()};
      held.Start();
      eigen_test_support::WaitUntil([&] {
        return pool.ResidentAvailableWorkers(group.domain) == 2;
      }, context.c_str());

      std::vector<std::atomic<unsigned>> actual(3 * size);
      std::array<std::atomic<size_t>, 3> callback_entries{};
      std::array<size_t, 3> activated{};
      std::atomic<unsigned> leaders_started{0}, fallback_started{0}, active{0};
      std::atomic<bool> release_callbacks{false}, outside{false};
      cancellation_test::Gate gate{context.c_str()};
      std::vector<std::future<void>> roots;
      roots.reserve(3);
      struct ReleaseGates {
        cancellation_test::Gate &gate;
        std::atomic<bool> &callbacks;
        ~ReleaseGates() {
          gate.publish = true;
          callbacks = true;
          cancellation_test::active_gate = nullptr;
        }
      } release{gate, release_callbacks};
      cancellation_test::active_gate = &gate;
      const auto launch = [&](unsigned root) {
        roots.push_back(std::async(std::launch::async, [&, root] {
          ParallelForAuto(group, 0, size, [&](size_t i) {
            ++callback_entries[root];
            if (pool.IsCancelled()) return;
            if (i >= size) { outside = true; return; }
            ++active;
            actual[root * size + i].fetch_add(unsigned(i % 29 + root + 1));
            if (root < 2 ? starts[i] : i == 0) {
              if (root < 2) ++leaders_started; else ++fallback_started;
              eigen_test_support::WaitUntil([&] { return release_callbacks.load(); },
                                            context.c_str());
            }
            --active;
          }, 1, nullptr, &activated[root]);
        }));
      };
      launch(0);
      eigen_test_support::WaitUntil([&] { return gate.publishers == 1; }, context.c_str());
      EXPECT_EQ(gate.helpers.load(), 2u);
      held.Release();
      eigen_test_support::WaitUntil([&] {
        return pool.ResidentAvailableWorkers(group.domain) == 2;
      }, context.c_str());
      launch(1);
      eigen_test_support::WaitUntil([&] { return gate.publishers == 2; }, context.c_str());
      EXPECT_EQ(gate.helpers.load(), 4u);
      // Both batched roots use the same full domain. All helpers are captured,
      // so the third root must enter ordinary Auto and publish fallback work.
      launch(2);
      eigen_test_support::WaitUntil([&] { return fallback_started == 1; }, context.c_str());
      if (phase == 1) pool.Cancel();
      gate.publish = true;
      if (phase != 1) {
        eigen_test_support::WaitUntil([&] { return leaders_started == 6; }, context.c_str());
        EXPECT_EQ(active.load(), 7u);
        if (phase == 2) pool.Cancel();
      }
      release_callbacks = true;
      for (auto &root : roots) eigen_test_support::GetReady(root, context.c_str());
      cancellation_test::active_gate = nullptr;
      EXPECT_EQ(active.load(), 0u);
      EXPECT_FALSE(outside.load());
      EXPECT_EQ(activated, (std::array<size_t, 3>{2, 2, 0}));
      for (unsigned root = 0; root < 3; ++root) {
        if (phase == 1 && root < 2) EXPECT_EQ(callback_entries[root].load(), 0u);
        if (phase == 0) EXPECT_EQ(callback_entries[root].load(), size);
        for (size_t i = 0; i < size; ++i) {
          const bool executed = phase == 0 || (root == 2 && i == 0) ||
              (phase == 2 && root < 2 && starts[i]);
          const unsigned expected = executed ? unsigned(i % 29 + root + 1) : 0;
          ASSERT_EQ(actual[root * size + i].load(), expected) << "root=" << root << " index=" << i;
        }
      }
    }
  }
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
