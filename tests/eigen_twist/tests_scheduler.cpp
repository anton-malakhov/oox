// SPDX-License-Identifier: Apache-2.0
#include "oox/eigen/rapid_auto.h"
#include "oox/eigen/rapid_parallel_for.h"
#include <twist/assist/assert.hpp>
#include <twist/sim.hpp>
#include <array>
#include <iostream>
#include <stdexcept>
#include <string_view>

namespace {
namespace pool = oox::detail::eigen_pool;
namespace rapid = pool::rapid;
namespace sync = pool::scheduler_sync;

void OrdinaryTasks() {
  pool::ThreadPool workers(3, false, true);
  std::array<sync::atomic<unsigned>, 7> visits{};
  sync::atomic<unsigned> done{0};
  for (size_t i = 0; i < visits.size(); ++i)
    workers.Schedule(pool::MakeTask([&, i] {
      visits[i].fetch_add(1);
      done.fetch_add(1);
    }));
  workers.Wait([&] { return done.load() == visits.size(); });
  for (auto &value : visits) TWIST_ASSERT(value.load() == 1);
}

void WaitReady(pool::ThreadPool &workers) {
  const pool::DomainId domain{0, static_cast<unsigned>(workers.NumThreads())};
  while (workers.ResidentAvailableWorkers(domain) < workers.ResidentCapacity(domain))
    sync::this_thread::yield();
}

template <typename Launch>
void CheckLoop(Launch launch, size_t count = 17) {
  std::array<sync::atomic<unsigned>, 17> visits{};
  std::array<size_t, 17> output{};
  launch([&](size_t i) {
    TWIST_ASSERT(i < count);
    TWIST_ASSERT(visits[i].fetch_add(1) == 0);
    if (i % 3 == 0) sync::this_thread::yield();
    output[i] = 7 * i + 3;
  });
  // Independent serial oracle, evaluated only after synchronous return.
  for (size_t i = 0; i < count; ++i) {
    TWIST_ASSERT(visits[i].load() == 1);
    TWIST_ASSERT(output[i] == 7 * i + 3);
  }
}

void OrdinaryAuto() {
  pool::ThreadPool workers(3, false, true);
  CheckLoop([&](auto body) { pool::ParallelFor(workers, 0, 17, body, 1); });
}

enum class Mode { Fixed, Lazy, Auto, Simple, Static, Affinity };

template <Mode mode, unsigned threads = 3>
void ResidentLoops() {
  pool::ThreadPool workers(threads, false, true, pool::WorkerIdleMode::ResidentBusy);
  rapid::RapidDomainState state(workers);
  rapid::RapidStartGroup group{&state, {0, threads}};
  pool::AffinityPartitioner affinity;
  for (unsigned repeat = 0; repeat < 3; ++repeat) {
    WaitReady(workers);
    CheckLoop([&](auto body) {
      if constexpr (mode == Mode::Fixed) {
        rapid::ParallelForResidentRanges(group, 0, 17, [&](size_t first, size_t last) {
          for (auto i = first; i < last; ++i) body(i);
        });
      } else if constexpr (mode == Mode::Lazy) {
        rapid::ParallelForAuto(group, 0, 17, body, 1);
      } else if constexpr (mode == Mode::Auto) {
        rapid::ParallelFor(group, 0, 17, body, pool::AutoPartitioner{}, 1);
      } else if constexpr (mode == Mode::Simple) {
        rapid::ParallelFor(group, 0, 17, body, pool::SimplePartitioner{}, 1);
      } else if constexpr (mode == Mode::Static) {
        rapid::ParallelFor(group, 0, 17, body, pool::StaticPartitioner{}, 1);
      } else {
        rapid::ParallelFor(group, 0, 17, body, affinity, 1);
      }
    });
  }
}

void LazyRequiresTailSteal() {
  pool::ThreadPool workers(3, false, true, pool::WorkerIdleMode::ResidentBusy);
  rapid::RapidDomainState state(workers);
  rapid::RapidStartGroup group{&state, {0, 3}};
  WaitReady(workers);
  std::array<sync::atomic<unsigned>, 17> visits{};
  pool::partitioner_detail::Metrics metrics;
  rapid::ParallelForAuto(group, 0, 17, [&](size_t i) {
    // The last initial owner reserves [12,13). Progress requires someone
    // else to steal its unreserved tail containing iteration 16.
    if (i == 12)
      while (visits[16].load() == 0) sync::this_thread::yield();
    TWIST_ASSERT(visits[i].fetch_add(1) == 0);
  }, 1, &metrics);
  for (auto &value : visits) TWIST_ASSERT(value.load() == 1);
  TWIST_ASSERT(metrics.donated_ranges.load() > 0);
}

void NestedLazy() {
  pool::ThreadPool workers(3, false, true, pool::WorkerIdleMode::ResidentBusy);
  rapid::RapidDomainState state(workers);
  rapid::RapidStartGroup group{&state, {0, 3}};
  WaitReady(workers);
  std::array<sync::atomic<unsigned>, 15> visits{};
  rapid::ParallelForAuto(group, 0, 5, [&](size_t outer) {
    rapid::ParallelForAuto(group, 0, 3, [&](size_t inner) {
      visits[outer * 3 + inner].fetch_add(1);
      sync::this_thread::yield();
    }, 1);
  }, 1);
  for (auto &value : visits) TWIST_ASSERT(value.load() == 1);
}

void ConcurrentCallers() {
  pool::ThreadPool workers(3, false, true, pool::WorkerIdleMode::ResidentBusy);
  rapid::RapidDomainState state(workers);
  rapid::RapidStartGroup group{&state, {0, 3}};
  WaitReady(workers);
  sync::thread external([&] {
    CheckLoop([&](auto body) { rapid::ParallelForAuto(group, 0, 17, body, 1); });
  });
  CheckLoop([&](auto body) { rapid::ParallelForAuto(group, 0, 17, body, 1); });
  external.join();
}

void LazyExceptionAndReuse() {
  pool::ThreadPool workers(3, false, true, pool::WorkerIdleMode::ResidentBusy);
  rapid::RapidDomainState state(workers);
  rapid::RapidStartGroup group{&state, {0, 3}};
  WaitReady(workers);
  bool caught = false;
  try {
    rapid::ParallelForAuto(group, 0, 17, [&](size_t i) {
      if (i == 12) throw std::runtime_error("expected failure");
      sync::this_thread::yield();
    }, 1);
  } catch (const std::runtime_error &) { caught = true; }
  TWIST_ASSERT(caught);
  WaitReady(workers);
  CheckLoop([&](auto body) { rapid::ParallelForAuto(group, 0, 17, body, 1); });
}

void DelayedCohortFinalize() {
  pool::ThreadPool workers(3, false, true, pool::WorkerIdleMode::ResidentBusy);
  struct Command final : pool::ResidentTask {
    sync::atomic<unsigned> calls{0};
    sync::atomic<bool> hold{false};
    void Run(size_t slot) noexcept override {
      TWIST_ASSERT(slot == 1 || slot == 2);
      calls.fetch_add(1);
      while (hold.load()) sync::this_thread::yield();
    }
  } old_command, new_command;
  std::array<unsigned, 2> old_workers{}, new_workers{};
  sync::atomic<size_t> old_remaining{2}, new_remaining{2};
  WaitReady(workers);
  TWIST_ASSERT(workers.ClaimResidentWorkers({0, 3}, old_workers.data(), 2) == 2);
  rapid::ResidentCohort old_cohort(workers, old_workers.data(), 2, old_remaining);
  old_cohort.Publish(old_command);
  workers.HelpResidentUntil(old_cohort);
  TWIST_ASSERT(old_command.calls.load() == 2);
  // Keep the old batch descriptor retained while its workers start a new epoch.
  WaitReady(workers);
  TWIST_ASSERT(workers.ClaimResidentWorkers({0, 3}, new_workers.data(), 2) == 2);
  new_command.hold.store(true);
  rapid::ResidentCohort new_cohort(workers, new_workers.data(), 2, new_remaining);
  new_cohort.Publish(new_command);
  while (new_command.calls.load() != 2) sync::this_thread::yield();
  TWIST_ASSERT(old_cohort.IsComplete());
  TWIST_ASSERT(!new_cohort.IsComplete());
  old_cohort.Finalize();
  TWIST_ASSERT(!new_cohort.IsComplete());
  new_command.hold.store(false);
  workers.HelpResidentUntil(new_cohort);
  new_cohort.Finalize();
}

void LazyWithOutsiders() {
  pool::ThreadPool workers(4, false, true, pool::WorkerIdleMode::ResidentBusy);
  workers.SetResidentLimit(2);
  rapid::RapidDomainState state(workers);
  rapid::RapidStartGroup group{&state, {0, 2}};
  WaitReady(workers);
  CheckLoop([&](auto body) { rapid::ParallelForAuto(group, 0, 17, body, 1); });
}

void CancelAndDrain() {
  std::array<sync::atomic<unsigned>, 7> visits{};
  {
    pool::ThreadPool workers(3, false, true);
    for (size_t i = 0; i < visits.size(); ++i) {
      workers.Schedule(pool::MakeTask([&, i] {
        TWIST_ASSERT(visits[i].fetch_add(1) == 0);
        if (i == 0) workers.Cancel();
      }));
    }
    workers.Cancel();
  }
  for (auto &value : visits) TWIST_ASSERT(value.load() <= 1);
}

void CancelFromCallback() {
  sync::atomic<bool> started{false}, finished{false};
  {
    pool::ThreadPool workers(3, false, true);
    workers.Schedule(pool::MakeTask([&] {
      started.store(true);
      sync::this_thread::yield();
      workers.Cancel();
      finished.store(true);
    }));
    while (!started.load()) sync::this_thread::yield();
    workers.Cancel();
  }
  TWIST_ASSERT(started.load() && finished.load());
}
}

int main(int argc, char **argv) {
  struct Scenario { const char *name; void (*run)(); unsigned seeds; };
  const Scenario scenarios[]{
      {"ordinary_tasks", OrdinaryTasks, 32}, {"ordinary_auto", OrdinaryAuto, 32},
      {"fixed_direct", ResidentLoops<Mode::Fixed, 2>, 32},
      {"fixed_packed", ResidentLoops<Mode::Fixed>, 32},
      {"fixed_epoch", ResidentLoops<Mode::Fixed, 9>, 8},
      {"lazy", ResidentLoops<Mode::Lazy>, 32},
      {"native_auto", ResidentLoops<Mode::Auto>, 32},
      {"native_simple", ResidentLoops<Mode::Simple>, 32},
      {"native_static", ResidentLoops<Mode::Static>, 32},
      {"native_affinity", ResidentLoops<Mode::Affinity>, 32},
      {"lazy_tail_steal", LazyRequiresTailSteal, 32},
      {"nested_lazy", NestedLazy, 32}, {"concurrent_callers", ConcurrentCallers, 32},
      {"lazy_exception_reuse", LazyExceptionAndReuse, 32},
      {"delayed_cohort_finalize", DelayedCohortFinalize, 32},
      {"lazy_outsiders", LazyWithOutsiders, 32},
      {"cancel_drain", CancelAndDrain, 32},
      {"cancel_from_callback", CancelFromCallback, 32}};
  bool selected = false;
  for (const auto &scenario : scenarios) {
    if (argc > 1 && std::string_view(argv[1]) != scenario.name) continue;
    selected = true;
    for (unsigned seed = 1; seed <= scenario.seeds; ++seed) {
      std::cout << "case=" << scenario.name << " seed=" << seed << '\n' << std::flush;
      twist::sim::sched::RandomScheduler scheduler{{.seed = seed}};
      twist::sim::Simulator simulator{&scheduler};
      const auto result = simulator.Run(scenario.run);
      if (!result.Ok()) {
        std::cerr << "FAIL case=" << scenario.name << " seed=" << seed
                  << " status=" << static_cast<int>(result.status) << '\n'
                  << result.std_out << result.std_err;
        return 1;
      }
    }
  }
  return selected ? 0 : 2;
}
