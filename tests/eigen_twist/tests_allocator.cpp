// SPDX-License-Identifier: Apache-2.0
#include "oox/eigen/small_object_pool.h"

#include <twist/assist/assert.hpp>
#include <twist/ed/std/atomic.hpp>
#include <twist/ed/std/condition_variable.hpp>
#include <twist/ed/std/mutex.hpp>
#include <twist/ed/std/thread.hpp>
#include <twist/sim.hpp>

#include <array>
#include <cstdint>
#include <iostream>

namespace {
namespace pool = oox::detail::eigen_pool;
using Thread = twist::ed::std::thread;
using Count = twist::ed::std::atomic<unsigned>;

struct Payload {
  Payload(unsigned value, Count &counter) : id(value), check(value ^ 0x5a5a), destroyed(counter) {}
  ~Payload() { destroyed.fetch_add(1); }
  unsigned id, check;
  Count &destroyed;
};
static_assert(pool::uses_small_object_pool<Payload>);

void Verify(Payload *value, unsigned expected) {
  TWIST_ASSERT(value != nullptr);
  TWIST_ASSERT(value->id == expected);
  TWIST_ASSERT(value->check == (expected ^ 0x5a5a));
}

void LocalReuse() {
  Count destroyed{0};
  Thread owner([&] {
    auto *first = pool::NewSmallObject<Payload>(1, destroyed);
    const auto address = reinterpret_cast<std::uintptr_t>(first);
    Verify(first, 1);
    pool::DeleteSmallObject(first);
    auto *second = pool::NewSmallObject<Payload>(2, destroyed);
    TWIST_ASSERT(reinterpret_cast<std::uintptr_t>(second) == address);
    Verify(second, 2);
    pool::DeleteSmallObject(second);
  });
  owner.join();
  TWIST_ASSERT(destroyed.load() == 2);
}

void RemoteReturnAndReuse() {
  Count destroyed{0};
  Payload *value = nullptr;
  twist::ed::std::atomic<bool> ready{false}, returned{false};
  Thread owner([&] {
    value = pool::NewSmallObject<Payload>(3, destroyed);
    const auto address = reinterpret_cast<std::uintptr_t>(value);
    ready.store(true, std::memory_order_release);
    while (!returned.load(std::memory_order_acquire)) twist::ed::std::this_thread::yield();
    auto *again = pool::NewSmallObject<Payload>(4, destroyed);
    TWIST_ASSERT(reinterpret_cast<std::uintptr_t>(again) == address);
    Verify(again, 4);
    pool::DeleteSmallObject(again);
  });
  while (!ready.load(std::memory_order_acquire)) twist::ed::std::this_thread::yield();
  Verify(value, 3);
  pool::DeleteSmallObject(value);
  returned.store(true, std::memory_order_release);
  owner.join();
  TWIST_ASSERT(destroyed.load() == 2);
}

void ReturnAfterOwnerExit() {
  Count destroyed{0};
  std::array<Payload *, 3> values{};
  Thread owner([&] {
    for (unsigned i = 0; i < values.size(); ++i)
      values[i] = pool::NewSmallObject<Payload>(10 + i, destroyed);
    auto *cached = pool::NewSmallObject<Payload>(99, destroyed);
    pool::DeleteSmallObject(cached);
  });
  owner.join();
  TWIST_ASSERT(destroyed.load() == 1);
  Thread first([&] { Verify(values[0], 10); pool::DeleteSmallObject(values[0]); });
  Thread second([&] { Verify(values[1], 11); pool::DeleteSmallObject(values[1]); });
  Verify(values[2], 12);
  pool::DeleteSmallObject(values[2]);
  first.join();
  second.join();
  TWIST_ASSERT(destroyed.load() == 4);
}

void RemoteReturnDuringClose() {
  Count destroyed{0};
  Payload *value = nullptr;
  twist::ed::std::mutex mutex;
  twist::ed::std::condition_variable changed;
  bool ready = false;
  Thread owner([&] {
    {
      std::lock_guard lock(mutex);
      value = pool::NewSmallObject<Payload>(42, destroyed);
      ready = true;
    }
    changed.notify_one();
  });
  {
    std::unique_lock lock(mutex);
    changed.wait(lock, [&] { return ready; });
  }
  Verify(value, 42);
  pool::DeleteSmallObject(value);
  owner.join();
  TWIST_ASSERT(destroyed.load() == 1);
}

struct Scenario { const char *name; void (*run)(); bool dfs; };

bool Check(const Scenario &scenario) {
  std::cout << "random case=" << scenario.name << " seeds=1..128\n";
  for (unsigned seed = 1; seed <= 128; ++seed) {
    twist::sim::sched::RandomScheduler scheduler{{.seed = seed}};
    twist::sim::Simulator simulator{&scheduler};
    const auto result = simulator.Run(scenario.run);
    if (!result.Ok()) {
      std::cerr << "FAIL allocator case=" << scenario.name << " seed=" << seed
                << '\n' << result.std_out << result.std_err;
      return false;
    }
  }
  if (!scenario.dfs) return true;
  twist::sim::sched::DfsScheduler scheduler{{.max_preempts = 2, .max_steps = 200}};
  std::size_t explored = 0, complete = 0, pruned = 0;
  bool more = true;
  while (more && explored < 50000) {
    twist::sim::Simulator simulator{&scheduler};
    const auto result = simulator.Run(scenario.run);
    ++explored;
    if (result.Failure()) {
      std::cerr << "FAIL allocator case=" << scenario.name << " schedule=" << explored
                << '\n' << result.std_out << result.std_err;
      return false;
    }
    if (result.Ok()) ++complete;
    else ++pruned;
    more = scheduler.NextSchedule();
  }
  std::cout << "dfs case=" << scenario.name << " max_preempts=2 max_steps=200"
            << " max_schedules=50000 explored=" << explored << " complete=" << complete
            << " pruned=" << pruned << " budget_exhausted=" << more << '\n';
  return complete != 0;
}
} // namespace

int main() {
  const Scenario scenarios[]{
      {"local_reuse", LocalReuse, false},
      {"remote_return_and_reuse", RemoteReturnAndReuse, false},
      {"return_after_owner_exit", ReturnAfterOwnerExit, false},
      {"remote_return_during_close", RemoteReturnDuringClose, true}};
  for (const auto &scenario : scenarios)
    if (!Check(scenario)) return 1;
  std::cout << "PASS real small-object allocator under bounded Twist simulation\n";
}
