// SPDX-License-Identifier: Apache-2.0
#include "oox/eigen/parallel_for.h"
#include "oox/eigen/stealable_range.h"

#include <twist/assist/assert.hpp>
#include <twist/sim.hpp>

#include <array>
#include <cstdint>
#include <iostream>
#include <limits>

namespace {
namespace pool = oox::detail::eigen_pool;
namespace detail = pool::partitioner_detail;
namespace sync = pool::scheduler_sync;

void Yield() { sync::this_thread::yield(); }

// The range returns an actual Task pointer, but this stack-owned test task does
// not self-delete. Its lifetime spans every thief and the unpublication join.
struct CountingTask final : pool::Task {
  void operator()() override { calls.fetch_add(1); }
  void Discard() noexcept override { TWIST_ASSERT(false); }
  sync::atomic<unsigned> calls{0};
};

struct CountingRange final : pool::StealableRange {
  explicit CountingRange(CountingTask &task) : task(task) {}
  pool::Task *Steal() noexcept override {
    TWIST_ASSERT_M(active.fetch_add(1) == 0, "two thieves entered one range");
    ++calls;
    Yield();
    TWIST_ASSERT(active.fetch_sub(1) == 1);
    return &task;
  }
  CountingTask &task;
  sync::atomic<unsigned> active{0}, calls{0};
};

void NestedPublication() {
  sync::atomic<uintptr_t> slot{0};
  CountingTask first, second;
  CountingRange outer(first), inner(second);
  outer.Publish(slot);
  (*pool::StealableRange::TrySteal(slot))();
  inner.Publish(slot);
  (*pool::StealableRange::TrySteal(slot))();
  inner.Unpublish();
  (*pool::StealableRange::TrySteal(slot))();
  outer.Unpublish();
  TWIST_ASSERT(first.calls.load() == 2);
  TWIST_ASSERT(second.calls.load() == 1);
  TWIST_ASSERT(slot.load() == 0);
  TWIST_ASSERT(pool::StealableRange::TrySteal(slot) == nullptr);
}

void ConcurrentThieves() {
  sync::atomic<uintptr_t> slot{0};
  CountingTask task;
  CountingRange range(task);
  range.Publish(slot);
  const auto steal = [&] {
    for (unsigned attempt = 0; attempt < 3; ++attempt) {
      if (auto *work = pool::StealableRange::TrySteal(slot)) (*work)();
      Yield();
    }
  };
  sync::thread first(steal), second(steal);
  steal();
  first.join();
  second.join();
  range.Unpublish();
  TWIST_ASSERT(range.calls.load() != 0);
  TWIST_ASSERT(range.calls.load() == task.calls.load());
  TWIST_ASSERT(range.active.load() == 0);
}

void UnpublishJoinsThief() {
  sync::atomic<uintptr_t> slot{0};
  sync::atomic<bool> entered{false}, release{false}, returned{false};
  sync::atomic<bool> closing{false}, closed{false};
  CountingTask task;
  struct HeldRange final : pool::StealableRange {
    HeldRange(CountingTask &task, sync::atomic<bool> &entered,
              sync::atomic<bool> &release, sync::atomic<bool> &returned)
        : task(task), entered(entered), release(release), returned(returned) {}
    pool::Task *Steal() noexcept override {
      entered.store(true);
      while (!release.load()) Yield();
      returned.store(true);
      return &task;
    }
    CountingTask &task;
    sync::atomic<bool> &entered, &release, &returned;
  } range(task, entered, release, returned);
  range.Publish(slot);
  sync::thread thief([&] {
    auto *work = pool::StealableRange::TrySteal(slot);
    TWIST_ASSERT(work == &task);
    (*work)();
  });
  while (!entered.load()) Yield();
  sync::thread owner([&] {
    closing.store(true);
    range.Unpublish();
    TWIST_ASSERT_M(returned.load(), "unpublication outran pinned thief");
    closed.store(true);
  });
  while (!closing.load()) Yield();
  for (unsigned attempt = 0; attempt < 3; ++attempt) Yield();
  TWIST_ASSERT(!closed.load());
  release.store(true);
  owner.join();
  thief.join();
  TWIST_ASSERT(closed.load() && slot.load() == 0);
  TWIST_ASSERT(task.calls.load() == 1);
}

void ReusedStackPublication() {
  sync::atomic<uintptr_t> slot{0};
  sync::atomic<bool> stop{false};
  CountingTask task;
  sync::thread thief([&] {
    while (!stop.load()) {
      if (auto *work = pool::StealableRange::TrySteal(slot)) (*work)();
      Yield();
    }
  });
  unsigned completed_callbacks = 0;
  for (unsigned generation = 0; generation < 4; ++generation) {
    CountingRange range(task);
    range.Publish(slot);
    Yield();
    range.Unpublish();
    TWIST_ASSERT(range.active.load() == 0);
    completed_callbacks += range.calls.load();
  }
  stop.store(true);
  thief.join();
  TWIST_ASSERT(task.calls.load() == completed_callbacks);
  TWIST_ASSERT(slot.load() == 0);
}

void ActiveRangeExactOnce() {
  // A real one-worker executor services any ordinary descendants. The two
  // extra simulated thieves exercise the real ActiveRange independently of
  // resident activation, which the scheduler component tests separately.
  pool::ThreadPool workers(1, false, false);
  constexpr size_t size = 17;
  for (size_t grain : {size_t{1}, size_t{3}}) {
    const size_t begin = grain == 1 ? 11 : std::numeric_limits<size_t>::max() - size;
    std::array<sync::atomic<unsigned>, size> visits{}, values{};
    sync::atomic<unsigned> stolen{0};
    auto body = [&](size_t index) {
      TWIST_ASSERT(index >= begin && index - begin < size);
      visits[index - begin].fetch_add(1);
      values[index - begin].fetch_add(unsigned(index % 23 + 1));
      // Retain the owner's first callback until a real tail was stolen; the
      // oracle must exercise theft, not just an uncontended owner-only run.
      if (index == begin) while (stolen.load() == 0) Yield();
      Yield();
    };
    using State = pool::partitioning::LazyAutoPartitionState;
    using Work = detail::Work<decltype(body), State>;
    auto *region = pool::NewSmallObject<detail::Region>(workers, nullptr, true);
    sync::atomic<uintptr_t> slot{0};
    {
      detail::Branch<Work, true> branch(nullptr);
      detail::ActiveRange<decltype(body), State> owner(
          *region, &body, {begin, begin + size, grain}, State(workers.NumThreads()), branch);
      owner.Publish(slot);
      const auto steal = [&] {
        for (unsigned attempt = 0; attempt < 4; ++attempt) {
          if (auto *task = pool::StealableRange::TrySteal(slot)) {
            stolen.fetch_add(1);
            (*task)();
          }
          Yield();
        }
      };
      sync::thread first(steal), second(steal);
      size_t chunks = 0;
      owner.Run(chunks);
      owner.Close();
      first.join();
      second.join();
    }
    workers.Wait([&] { return region->IsComplete(); });
    region->CloseAndWait();
    region->TaskComplete();
    TWIST_ASSERT(stolen.load() != 0);
    TWIST_ASSERT(slot.load() == 0);
    // Independent serial oracle for both coverage and computed values.
    for (size_t index = 0; index < size; ++index) {
      TWIST_ASSERT(visits[index].load() == 1);
      TWIST_ASSERT(values[index].load() == unsigned((begin + index) % 23 + 1));
    }
  }
}

struct Scenario { const char *name; void (*run)(); unsigned seeds; bool dfs; };

bool Check(const Scenario &scenario) {
  std::cout << "random case=" << scenario.name << " seeds=1.." << scenario.seeds
            << '\n' << std::flush;
  for (unsigned seed = 1; seed <= scenario.seeds; ++seed) {
    std::cout << "case=" << scenario.name << " seed=" << seed << '\n' << std::flush;
    twist::sim::sched::RandomScheduler scheduler{{.seed = seed}};
    twist::sim::Simulator simulator{&scheduler};
    const auto result = simulator.Run(scenario.run);
    if (!result.Ok()) {
      std::cerr << "FAIL range case=" << scenario.name << " seed=" << seed
                << '\n' << result.std_out << result.std_err;
      return false;
    }
  }
  if (!scenario.dfs) return true;
  // Non-isolated Twist aborts on an exhausted step budget. Unbounded runs of
  // spurious weak-CAS failures are covered by seeded random schedules instead.
  twist::sim::sched::DfsScheduler scheduler{{.max_preempts = 2, .max_steps = 200,
      .spurious_failures = false}};
  size_t explored = 0, complete = 0, pruned = 0;
  bool more = true;
  while (more && explored < 10000) {
    twist::sim::Simulator simulator{&scheduler};
    const auto result = simulator.Run(scenario.run);
    ++explored;
    if (result.Failure()) return false;
    if (result.Ok()) ++complete;
    else ++pruned;
    more = scheduler.NextSchedule();
  }
  std::cout << "dfs case=" << scenario.name << " max_preempts=2 max_steps=200"
            << " max_schedules=10000 explored=" << explored << " complete=" << complete
            << " pruned=" << pruned << " budget_exhausted=" << more << '\n';
  return complete != 0;
}
} // namespace

int main() {
  const Scenario scenarios[]{
      {"nested_publication", NestedPublication, 32, true},
      {"concurrent_thieves", ConcurrentThieves, 64, false},
      {"unpublish_joins_thief", UnpublishJoinsThief, 64, false},
      {"reused_stack_publication", ReusedStackPublication, 64, false},
      {"active_range_exact_once", ActiveRangeExactOnce, 16, false}};
  for (const auto &scenario : scenarios)
    if (!Check(scenario)) return 1;
  std::cout << "PASS real range publication and reservation under bounded Twist simulation\n";
}
