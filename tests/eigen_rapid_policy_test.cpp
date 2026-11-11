// SPDX-License-Identifier: Apache-2.0
namespace rapid_policy_test { void Claimed(unsigned) noexcept; }
#define OOX_EIGEN_TEST_RAPID_CLAIMED(count) rapid_policy_test::Claimed(count)
#include <oox/eigen/rapid_parallel_for.h>
#undef OOX_EIGEN_TEST_RAPID_CLAIMED
#include "test_support/eigen_resident.h"
#include "eigen_test_wait.h"
#include <gtest/gtest.h>
#include <future>
#include <vector>

namespace rapid_policy_test {
thread_local oox::detail::eigen_pool::ThreadPool *cancel_on_claim = nullptr;
void Claimed(unsigned) noexcept { if (cancel_on_claim) cancel_on_claim->Cancel(); }
}

namespace {
using namespace oox::detail::eigen_pool;
using namespace oox::detail::eigen_pool::rapid;
using namespace std::chrono_literals;

static_assert(policy_detail::SupportedPartitioner<AutoPartitioner>);
static_assert(policy_detail::SupportedPartitioner<const StaticPartitioner>);
static_assert(policy_detail::SupportedPartitioner<AffinityPartitioner &>);
static_assert(!policy_detail::SupportedPartitioner<const AffinityPartitioner>);
static_assert(!policy_detail::SupportedPartitioner<int>);

TEST(EigenRapidPolicy, PrefixUsesOneNativeBudgetAndUnequalOddSplits) {
  using Range = partitioner_detail::LoopRange;
  policy_detail::SeedFrontier<partitioning::AutoPartitionState, 8> automatic(
      Range{0, 100, 1}, partitioning::AutoPartitionState(3));
  automatic.SplitTo(3);
  ASSERT_EQ(automatic.Size(), 3u);
  const std::array<size_t, 3> auto_begin{0, 50, 25}, auto_end{25, 100, 50}, budgets{1, 3, 1};
  for (size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(automatic.At(i).range.begin, auto_begin[i]);
    EXPECT_EQ(automatic.At(i).range.end, auto_end[i]);
    EXPECT_EQ(automatic.At(i).state.divisor, budgets[i]);
    EXPECT_EQ(automatic.At(i).state.max_depth, 5u);
  }
  policy_detail::SeedFrontier<partitioning::StaticPartitionState, 8> statical(
      Range{0, 100, 1}, partitioning::StaticPartitionState(3, 0));
  statical.SplitTo(3);
  const std::array<size_t, 3> begin{0, 67, 33}, end{33, 100, 67}, hints{0, 2, 1};
  for (size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(statical.At(i).range.begin, begin[i]);
    EXPECT_EQ(statical.At(i).range.end, end[i]);
    EXPECT_EQ(statical.At(i).state.divisor, 1u);
    EXPECT_EQ(statical.At(i).state.Hint(), hints[i]);
  }
  const unsigned workers[] = {1, 2};
  statical.Assign(0, workers, 2);
  EXPECT_EQ(statical.ForSlot(0), 0u);
  EXPECT_EQ(statical.ForSlot(1), 2u);
  EXPECT_EQ(statical.ForSlot(2), 1u);
}

template <typename Policy> void GeneratedOracle() {
  constexpr uint64_t seed = 0x52504f4c;
  uint64_t random = seed;
  for (unsigned workers : {1u, 2u, 3u, 5u, 8u}) {
    ThreadPool pool(workers, true, true, WorkerIdleMode::ResidentBusy);
    RapidDomainState domain(pool);
    RapidStartGroup group{&domain, {0, workers}};
    Policy policy;
    for (unsigned limit : {0u, workers / 2, workers}) {
      pool.SetResidentLimit(limit);
      for (unsigned test = 0; test < 50; ++test) {
        random = random * 6364136223846793005ULL + 1;
        const size_t n = test < 40 ? test : test < 48 ? 41 + random % 1024
            : test == 48 ? 65537 : 1048579;
        const size_t grain = test >= 48 ? 17 : test % 7;
        const size_t begin = test % 2 ? size_t(-1) - n : random % 4096;
        SCOPED_TRACE(::testing::Message() << "seed=" << seed << " case=" << test
            << " workers=" << workers << " limit=" << limit << " n=" << n << " grain=" << grain);
        std::vector<std::atomic<unsigned>> actual(n);
        std::vector<unsigned> expected(n);
        for (size_t i = 0; i < n; ++i) expected[i] = unsigned(i % 31 + 1);
        std::atomic<bool> outside{false};
        eigen_test_support::WaitForResidentWorkers(group, 5s);
        rapid::ParallelFor(group, begin, begin + n, [&](size_t i) {
          if (i < begin || i - begin >= n) { outside = true; return; }
          actual[i - begin].fetch_add(unsigned((i - begin) % 31 + 1), std::memory_order_relaxed);
        }, policy, grain);
        ASSERT_FALSE(outside.load());
        for (size_t i = 0; i < n; ++i) ASSERT_EQ(actual[i].load(), expected[i]) << i;
      }
    }
  }
}

TEST(EigenRapidPolicy, AutoMatchesGeneratedSerialOracle) { GeneratedOracle<AutoPartitioner>(); }
TEST(EigenRapidPolicy, SimpleMatchesGeneratedSerialOracle) { GeneratedOracle<SimplePartitioner>(); }
TEST(EigenRapidPolicy, StaticMatchesGeneratedSerialOracle) { GeneratedOracle<StaticPartitioner>(); }
TEST(EigenRapidPolicy, AffinityMatchesGeneratedSerialOracle) { GeneratedOracle<AffinityPartitioner>(); }

TEST(EigenRapidPolicy, StaticFullCohortNeedsNoOrdinaryTasks) {
  ThreadPool pool(3, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  RapidStartGroup group{&domain, {0, 3}};
  eigen_test_support::WaitForResidentWorkers(group, 5s);
  std::array<unsigned, 100> workers{};
  partitioner_detail::Metrics metrics;
  size_t activated = 0;
  rapid::ParallelFor(group, 0, workers.size(), [&](size_t i) {
    workers[i] = static_cast<unsigned>(pool.CurrentThreadId());
  }, StaticPartitioner{}, 1, &metrics, &activated);
  EXPECT_EQ(activated, 2u);
  EXPECT_EQ(metrics.range_tasks.load(), 0u);
  for (size_t i = 0; i < workers.size(); ++i)
    EXPECT_EQ(workers[i], i < 33 ? 0u : i < 67 ? 1u : 2u) << i;
}

TEST(EigenRapidPolicy, AffinityRoutesCapturedOwnersAndRemembersWorkerZero) {
  ThreadPool pool(3, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  RapidStartGroup group{&domain, {0, 3}};
  AffinityPartitioner policy;
  std::atomic<size_t> *history = nullptr;
  {
    auto state = policy.MakeState(pool);
    history = state.slots;
    state.NoteExecution(2, state.Hint());
    state.head = 16;
    state.NoteExecution(0, state.Hint());
    EXPECT_EQ(state.Hint(), 0u);
    state.head = 32;
    state.NoteExecution(1, state.Hint());
  }
  for (unsigned repeat = 0; repeat < 3; ++repeat) {
    eigen_test_support::WaitForResidentWorkers(group, 5s);
    std::array<unsigned, 100> actual{};
    rapid::ParallelFor(group, 0, actual.size(), [&](size_t i) {
      actual[i] = static_cast<unsigned>(pool.CurrentThreadId());
    }, policy, 34);
    for (size_t i = 0; i < actual.size(); ++i)
      EXPECT_EQ(actual[i], i < 33 ? 2u : i < 67 ? 0u : 1u) << i;
    EXPECT_EQ(policy.MakeState(pool).slots, history);
  }
}

TEST(EigenRapidPolicy, FallbackRetainsTheSelectedPolicy) {
  ThreadPool pool(3, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  RapidStartGroup group{&domain, {0, 3}};
  pool.SetResidentLimit(0);
  partitioner_detail::Metrics simple, statical;
  size_t activated = 99;
  rapid::ParallelFor(group, 0, 257, [](size_t) {}, SimplePartitioner{}, 1, &simple, &activated);
  EXPECT_EQ(activated, 0u);
  EXPECT_EQ(simple.range_tasks.load(), 256u);
  rapid::ParallelFor(group, 0, 257, [](size_t) {}, StaticPartitioner{}, 1, &statical);
  EXPECT_EQ(statical.range_tasks.load(), 2u);
}

TEST(EigenRapidPolicy, PartialDomainAndGrainLimitReleaseCapturedHelpers) {
  ThreadPool pool(8, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  RapidStartGroup group{&domain, {2, 7}};
  for (unsigned repeat = 0; repeat < 16; ++repeat) {
    eigen_test_support::WaitForResidentWorkers(group, 5s);
    std::array<std::atomic<unsigned>, 10> visits{};
    size_t activated = 0;
    // Five helpers are available in the domain, but grain permits only three.
    rapid::ParallelFor(group, 0, visits.size(), [&](size_t i) { ++visits[i]; },
        SimplePartitioner{}, 3, nullptr, &activated);
    EXPECT_EQ(activated, 3u);
    for (const auto &v : visits) EXPECT_EQ(v.load(), 1u);
    eigen_test_support::WaitForResidentWorkers(group, 5s);
  }
}

TEST(EigenRapidPolicy, UnusedPrefixSlotsAreExplicitlyEmpty) {
  policy_detail::SeedFrontier<partitioning::SimplePartitionState, 8> frontier(
      {0, 6, 3}, partitioning::SimplePartitionState{});
  frontier.SplitTo(8);
  ASSERT_EQ(frontier.Size(), 2u);
  for (size_t slot = 2; slot < 8; ++slot) EXPECT_EQ(frontier.ForSlot(slot), frontier.Size());
}

template <typename Policy> void NestedAndConcurrent() {
  ThreadPool pool(5, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  RapidStartGroup group{&domain, {0, 5}};
  Policy policy;
  std::array<std::atomic<unsigned>, 17 * 7> actual{};
  rapid::ParallelFor(group, 0, 17, [&](size_t i) {
    size_t activated = 99;
    rapid::ParallelFor(group, 0, 7, [&](size_t j) { ++actual[i * 7 + j]; }, policy, 1, nullptr, &activated);
    EXPECT_EQ(activated, 0u);
  }, policy);
  for (const auto &v : actual) EXPECT_EQ(v.load(), 1u);
  auto run = [&] {
    std::vector<std::atomic<unsigned>> values(4097);
    rapid::ParallelFor(group, 0, values.size(), [&](size_t i) { ++values[i]; }, policy, 3);
    for (size_t i = 0; i < values.size(); ++i) EXPECT_EQ(values[i].load(), 1u) << i;
  };
  auto a = std::async(std::launch::async, run);
  auto b = std::async(std::launch::async, run);
  eigen_test_support::GetReady(a, "first concurrent policy loop");
  eigen_test_support::GetReady(b, "second concurrent policy loop");
}
TEST(EigenRapidPolicy, AllPoliciesSupportNestedAndConcurrentCalls) {
  NestedAndConcurrent<AutoPartitioner>();
  NestedAndConcurrent<SimplePartitioner>();
  NestedAndConcurrent<StaticPartitioner>();
  NestedAndConcurrent<AffinityPartitioner>();
}

template <typename Policy> void FailureContracts() {
  ThreadPool pool(4, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState domain(pool);
  RapidStartGroup group{&domain, {0, 4}};
  Policy policy;
  std::atomic<unsigned> active{0}, late{0};
  std::atomic<bool> alive{true};
  EXPECT_THROW(rapid::ParallelFor(group, 0, 4097, [&](size_t i) {
    struct Exit { std::atomic<unsigned> &n; ~Exit() { --n; } } exit{active};
    ++active;
    if (!alive.load()) ++late;
    if (i == 13) throw std::runtime_error("policy body");
  }, policy), std::runtime_error);
  alive = false;
  EXPECT_EQ(active.load(), 0u);
  std::array<std::atomic<unsigned>, 257> visits{};
  rapid::ParallelFor(group, 0, visits.size(), [&](size_t i) { ++visits[i]; }, policy);
  for (const auto &v : visits) EXPECT_EQ(v.load(), 1u);
  EXPECT_EQ(late.load(), 0u);
  alive = true;
  rapid::ParallelFor(group, 0, 4097, [&](size_t) {
    if (!alive.load()) ++late;
    pool.Cancel();
  }, policy);
  alive = false;
  EXPECT_EQ(late.load(), 0u);
}
TEST(EigenRapidPolicy, ExceptionsAndCancellationJoinAllPolicies) {
  FailureContracts<AutoPartitioner>();
  FailureContracts<SimplePartitioner>();
  FailureContracts<StaticPartitioner>();
  FailureContracts<AffinityPartitioner>();
}

template <typename Policy> void CancelBeforePublication() {
  for (size_t size : {size_t{2}, size_t{257}}) {
    ThreadPool pool(4, true, true, WorkerIdleMode::ResidentBusy);
    RapidDomainState domain(pool);
    RapidStartGroup group{&domain, {0, 4}};
    eigen_test_support::WaitForResidentWorkers(group, 5s);
    std::atomic<unsigned> calls{0};
    size_t activated = 0;
    rapid_policy_test::cancel_on_claim = &pool;
    struct Reset { ~Reset() { rapid_policy_test::cancel_on_claim = nullptr; } } reset;
    rapid::ParallelFor(group, 0, size, [&](size_t) { ++calls; }, Policy{}, 1, nullptr, &activated);
    EXPECT_TRUE(pool.IsCancelled());
    EXPECT_EQ(calls.load(), 0u);
    EXPECT_EQ(activated, std::min(size_t{3}, size - 1));
  }
}
TEST(EigenRapidPolicy, CancellationBetweenCaptureAndPublicationStillJoinsCommands) {
  CancelBeforePublication<AutoPartitioner>();
  CancelBeforePublication<SimplePartitioner>();
  CancelBeforePublication<StaticPartitioner>();
  CancelBeforePublication<AffinityPartitioner>();
}

TEST(EigenRapidPolicy, EmptyInvalidAndParkingGroupsRetainTheirContracts) {
  size_t activated = 99;
  unsigned calls = 0;
  rapid::ParallelFor({}, 0, 10, [&](size_t) { ++calls; }, SimplePartitioner{}, 1, nullptr, &activated);
  EXPECT_EQ(activated, 0u);
  EXPECT_EQ(calls, 0u);
  ThreadPool pool(2, false, true);
  RapidDomainState domain(pool);
  EXPECT_THROW(rapid::ParallelFor({&domain, {0, 3}}, 0, 10, [](size_t) {}, AutoPartitioner{}), std::invalid_argument);
  EXPECT_THROW(rapid::ParallelFor({&domain, {0, 2}}, 0, 10, [](size_t) {}, StaticPartitioner{}), std::invalid_argument);
}
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
