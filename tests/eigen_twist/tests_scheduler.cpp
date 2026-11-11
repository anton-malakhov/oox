// SPDX-License-Identifier: Apache-2.0
#include "oox/eigen/parallel_for.h"
#include <twist/assist/assert.hpp>
#include <twist/sim.hpp>
#include <array>
#include <iostream>
#include <string_view>

namespace {
namespace pool = oox::detail::eigen_pool;
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
