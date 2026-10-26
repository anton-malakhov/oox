// SPDX-License-Identifier: Apache-2.0
#include <array>
#include <cstdlib>
#include <iostream>
#include <new>
#include "eigen_partitioner_test_support.h"
#include <oox/eigen/rapid_auto.h>
#include <oox/eigen/rapid_parallel_for.h>
#include "test_support/eigen_resident.h"

thread_local int fail_after = -1;
thread_local bool injected = false;
std::atomic<size_t> allocation_attempts{0};
void *operator new(std::size_t size) {
  allocation_attempts.fetch_add(1, std::memory_order_relaxed);
  if (fail_after == 0) {
    fail_after = -1;
    injected = true;
    throw std::bad_alloc();
  }
  if (fail_after > 0)
    --fail_after;
  if (void *p = std::malloc(size ? size : 1))
    return p;
  throw std::bad_alloc();
}
void operator delete(void *p) noexcept { std::free(p); }
void operator delete(void *p, std::size_t) noexcept { std::free(p); }


void verify_affinity_publication_failure() {
  using namespace oox::detail::eigen_pool;
  struct Tracked final : Task {
    explicit Tracked(std::atomic<bool> &destroyed) : destroyed(destroyed) {}
    ~Tracked() override { destroyed.store(true, std::memory_order_release); }
    void operator()() override { delete this; }
    std::atomic<bool> &destroyed;
  };
  std::atomic<bool> entered{false}, released{false};
  std::array<std::atomic<bool>, 256> destroyed{};
  bool caught_publication = false;
  {
    ThreadPool pool(2, false, true);
    pool.RunOnThread(MakeTask([&] {
      entered.store(true, std::memory_order_release);
      entered.notify_one();
      released.wait(false, std::memory_order_acquire);
    }), 1);
    entered.wait(false, std::memory_order_acquire);
    // Keep the sender deque full while the recipient is unavailable.
    for (unsigned i = 0; i < 1024; ++i)
      pool.Schedule(MakeTask([] {}));
    for (size_t i = 0; i < destroyed.size(); ++i) {
      auto *task = new Tracked(destroyed[i]);
      injected = false;
      // First fail proxy allocation, then forbid any allocation after it.
      fail_after = i == 0 ? 0 : 1;
      try {
        pool.ScheduleWithAffinity(task, 1);
      } catch (const std::bad_alloc &) {
        caught_publication = injected &&
            destroyed[i].load(std::memory_order_acquire);
      }
      fail_after = -1;
      if (injected) {
        if (!caught_publication) {
          std::cerr << "failed affinity publication retained useful work\n";
          std::_Exit(4);
        }
      } else if (i == 0 || !destroyed[i].load(std::memory_order_acquire)) {
        std::cerr << "bounded affinity fallback did not complete inline\n";
        std::_Exit(4);
      }
    }
    released.store(true, std::memory_order_release);
    released.notify_one();
  }
  if (!caught_publication) {
    std::cerr << "affinity proxy allocation failure was not injected\n";
    std::_Exit(5);
  }
}

void verify_reentrant_discard_after_allocation_failure() {
  using namespace oox::detail::eigen_pool;
  struct Reentrant final : Task {
    Reentrant(ThreadPool &pool, bool &discarded) : pool(pool), discarded(discarded) {}
    void operator()() override { delete this; }
    void Discard() noexcept override {
      pool.Cancel();
      discarded = true;
      delete this;
    }
    ThreadPool &pool;
    bool &discarded;
  };
  ThreadPool pool(2, false, true);
  bool discarded = false;
  auto *task = new Reentrant(pool, discarded);
  fail_after = 0;
  bool caught = false;
  try {
    pool.ScheduleWithAffinity(task, 1);
  } catch (const std::bad_alloc &) {
    caught = true;
  }
  fail_after = -1;
  if (!caught || !discarded || !pool.IsCancelled())
    std::_Exit(6);
}

void verify_rapid_auto_allocation_failures() {
  using namespace oox::detail::eigen_pool;
  using namespace oox::detail::eigen_pool::rapid;
  ThreadPool pool(4, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState state(pool);
  unsigned hits = 0;
  for (int ordinal = 0; ordinal < 64; ++ordinal) {
    injected = false;
    fail_after = ordinal % 32;
    bool caught = false;
    try {
      ParallelForAuto({&state, {0, 4}}, 0, 65537, [](size_t) {},
                         1, nullptr, nullptr);
    } catch (const std::bad_alloc &) { caught = true; }
    fail_after = -1;
    if (caught != injected) {
      std::cerr << "rapid auto allocation ordinal=" << ordinal << " mismatch\n";
      std::_Exit(10);
    }
    hits += injected;
    std::array<std::atomic<unsigned>, 257> visits{};
    ParallelForAuto({&state, {0, 4}}, 0, visits.size(), [&](size_t i) { ++visits[i]; });
    for (auto &v : visits) if (v != 1) std::_Exit(11);
  }
  if (!hits) std::_Exit(12);
}

void verify_terminal_pairs_do_not_allocate() {
  using namespace oox::detail::eigen_pool;
  using namespace oox::detail::eigen_pool::rapid;
  ThreadPool pool(4, true, true, WorkerIdleMode::ResidentBusy);
  RapidDomainState state(pool);
  RapidStartGroup group{&state, {0, 4}};
  for (unsigned test = 0; test < 32; ++test) {
    eigen_test_support::WaitForResidentWorkers(group, std::chrono::seconds(2));
    std::array<std::atomic<unsigned>, 2> visits{};
    size_t activated = 0;
    const auto before = allocation_attempts.load(std::memory_order_relaxed);
    injected = false;
    fail_after = 0;
    try {
      ParallelForAuto(group, 0, 2, [&](size_t i) { ++visits[i]; }, 1, nullptr, &activated);
    } catch (...) {
      fail_after = -1;
      std::cerr << "terminal pair allocated: case=" << test << '\n';
      std::_Exit(13);
    }
    fail_after = -1;
    if (injected || allocation_attempts.load(std::memory_order_relaxed) != before ||
        activated != 1 || visits[0] != 1 || visits[1] != 1) std::_Exit(14);
  }
}

template <typename Policy> void verify_rapid_policy_allocation_failures() {
  using namespace oox::detail::eigen_pool;
  ThreadPool pool(4, true, true, WorkerIdleMode::ResidentBusy);
  rapid::RapidDomainState state(pool);
  rapid::RapidStartGroup group{&state, {0, 4}};
  unsigned hits = 0;
  for (int ordinal = 0; ordinal < 32; ++ordinal) {
    Policy policy; // Include the first Affinity-history allocation in injection.
    eigen_test_support::WaitForResidentWorkers(group, std::chrono::seconds(2));
    injected = false;
    fail_after = ordinal;
    bool caught = false;
    try {
      rapid::ParallelFor(group, 0, 4097, [](size_t) {}, policy);
    } catch (const std::bad_alloc &) { caught = true; }
    fail_after = -1;
    if (caught != injected) {
      std::cerr << "rapid policy allocation ordinal=" << ordinal << " mismatch\n";
      std::_Exit(18);
    }
    hits += injected;
    // A failed preparation/publication must neither strand claimed workers nor
    // leave a callback from the failed launch reachable by the next invocation.
    eigen_test_support::WaitForResidentWorkers(group, std::chrono::seconds(2));
    std::array<std::atomic<unsigned>, 257> visits{};
    rapid::ParallelFor(group, 0, visits.size(), [&](size_t i) { ++visits[i]; }, policy);
    for (auto &v : visits) if (v != 1) std::_Exit(19);
  }
  if (!hits) std::_Exit(20);
}

void verify_failed_donation_releases_reservation() {
  using namespace oox::detail::eigen_pool;
  using State = partitioning::LazyAutoPartitionState;
  ThreadPool pool(2, true, true, WorkerIdleMode::ResidentBusy);
  bool called = false, caught = false;
  auto body = [&](size_t) { called = true; };
  auto *region = NewSmallObject<partitioner_detail::Region>(pool, nullptr);
  using Work = partitioner_detail::Work<decltype(body), State>;
  partitioner_detail::Branch<Work, true> branch(nullptr);
  State state(2);
  if (!region->ReserveDonation()) std::_Exit(15);
  injected = false;
  fail_after = 0;
  try {
    partitioner_detail::OfferWork(*region, &body, {0, 32, 1}, state, branch,
                                 0, partitioning::Split{});
  } catch (const std::bad_alloc &) { caught = true; }
  fail_after = -1;
  if (!caught || !injected || called || branch.Parent() || !region->IsComplete() ||
      !region->ReserveDonation())
    std::_Exit(16);
  region->ReleaseDonation();
  region->TaskComplete();
}

void verify_failed_first_theft_does_not_promote_root() {
  using namespace oox::detail::eigen_pool;
  using State = partitioning::LazyAutoPartitionState;
  ThreadPool pool(1, false, false);
  bool called = false, caught = false;
  auto body = [&](size_t) { called = true; };
  partitioner_detail::Metrics metrics;
  auto *region = NewSmallObject<partitioner_detail::Region>(pool, &metrics);
  using Work = partitioner_detail::Work<decltype(body), State>;
  {
    partitioner_detail::Branch<Work, true> branch(nullptr);
    State state(1);
    state.unpublished = &region->RapidPublication();
    partitioner_detail::ActiveRange<decltype(body), State> active(
        *region, &body, {0, 32, 1}, state, branch);
    injected = false;
    fail_after = 0;
    auto *child = active.Steal();
    fail_after = -1;
    try { region->Rethrow(); } catch (const std::bad_alloc &) { caught = true; }
    if (child || !caught || !injected || called || branch.Parent() ||
        !region->IsComplete() || metrics.range_tasks != 0 || metrics.donated_ranges != 0) {
      std::cerr << "seed=0 case=0 first theft allocation failure promoted root or retained work\n";
      std::_Exit(17);
    }
  }
  region->CloseAndWait();
  region->TaskComplete();
}

int main(int argc, char **argv) try {
  eigen_partitioner_test::Select(argc, argv);
  verify_reentrant_discard_after_allocation_failure();
  verify_rapid_auto_allocation_failures();
  verify_rapid_policy_allocation_failures<oox::detail::eigen_pool::AutoPartitioner>();
  verify_rapid_policy_allocation_failures<oox::detail::eigen_pool::SimplePartitioner>();
  verify_rapid_policy_allocation_failures<oox::detail::eigen_pool::StaticPartitioner>();
  verify_rapid_policy_allocation_failures<oox::detail::eigen_pool::AffinityPartitioner>();
  verify_terminal_pairs_do_not_allocate();
  verify_failed_donation_releases_reservation();
  verify_failed_first_theft_does_not_promote_root();
  verify_affinity_publication_failure();
  using namespace oox::detail::eigen_pool;
  ThreadPool pool(8, true, true);
  eigen_partitioner_test::Run(pool, 0, 4097, [](std::size_t) {});
  unsigned hits = 0;
  for (int ordinal = 0; ordinal < 32; ++ordinal) {
    injected = false;
    fail_after = ordinal;
    bool caught = false;
    try {
      eigen_partitioner_test::Run(pool, 0, 4097, [](std::size_t i) {
        if (i % 64 == 0)
          std::this_thread::yield();
      });
    } catch (const std::bad_alloc &) {
      caught = true;
    }
    fail_after = -1;
    if (caught != injected) {
      std::cerr << "allocation ordinal=" << ordinal
                << " propagation mismatch\n";
      return 1;
    }
    hits += injected;
    std::array<std::atomic<unsigned>, 257> visits{};
    eigen_partitioner_test::Run(pool, 0, visits.size(),
                             [&](std::size_t i) { visits[i].fetch_add(1); });
    for (auto &v : visits)
      if (v != 1)
        return 2;
  }
  if (hits == 0)
    return 3;
  std::cout << "allocation cases=32 injected=" << hits << " reuse=PASS\n";
} catch (const std::exception &error) {
  fail_after = -1;
  std::cerr << "allocation test setup/execution failed: " << error.what() << '\n';
  return 15;
}
