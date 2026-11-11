// SPDX-License-Identifier: Apache-2.0
#include "oox/eigen/run_queue.h"
#include "oox/eigen/mpmc_queue.h"

#include <twist/assist/assert.hpp>
#include <twist/ed/std/thread.hpp>
#include <twist/sim.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <iostream>
#include <vector>

namespace {
namespace pool = oox::detail::eigen_pool;
using Thread = twist::ed::std::thread;
using Values = std::vector<unsigned>;

void Yield() { twist::ed::std::this_thread::yield(); }

// Independent oracle: the consumed multiset must equal successful insertions.
void ExactOnce(Values expected, Values actual) {
  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());
  TWIST_ASSERT_M(actual == expected, "queue lost, duplicated, or corrupted a value");
}

void RunQueueLastItem() {
  pool::RunQueue<unsigned, 4> queue;
  TWIST_ASSERT(queue.PushFront(1));
  unsigned stolen = 0;
  Thread thief([&] { stolen = queue.PopBack(); });
  Values actual;
  if (auto value = queue.PopFront()) actual.push_back(value);
  thief.join();
  if (stolen) actual.push_back(stolen);
  while (auto value = queue.PopFront()) actual.push_back(value);
  ExactOnce({1}, actual);
  TWIST_ASSERT(queue.Empty());
}

void RunQueueWrapAndSteal() {
  pool::RunQueue<unsigned, 4> queue;
  Values accepted, owner, thief_a, thief_b;
  for (unsigned value = 1; value <= 4; ++value) {
    TWIST_ASSERT(queue.PushFront(value));
    accepted.push_back(value);
  }
  TWIST_ASSERT(!queue.PushFront(99));
  const auto steal = [&](Values &result) {
    for (unsigned attempt = 0; attempt < 12; ++attempt) {
      if (auto value = queue.PopBack()) result.push_back(value);
      Yield();
    }
  };
  Thread first([&] { steal(thief_a); });
  Thread second([&] { steal(thief_b); });
  for (unsigned value = 5; value <= 20; ++value) {
    if (auto popped = queue.PopFront()) owner.push_back(popped);
    if (queue.PushFront(value)) accepted.push_back(value);
    Yield();
  }
  first.join();
  second.join();
  while (auto value = queue.PopFront()) owner.push_back(value);
  owner.insert(owner.end(), thief_a.begin(), thief_a.end());
  owner.insert(owner.end(), thief_b.begin(), thief_b.end());
  ExactOnce(accepted, owner);
  TWIST_ASSERT(queue.Empty());
}

void RunQueueRemoteProducer() {
  pool::RunQueue<unsigned, 4> queue;
  Values remote_accepted, remote_taken, owner_accepted, owner_taken;
  Thread remote([&] {
    for (unsigned value = 101; value <= 108; ++value) {
      if (queue.PushBack(value)) remote_accepted.push_back(value);
      if (auto popped = queue.PopBack()) remote_taken.push_back(popped);
      Yield();
    }
  });
  for (unsigned value = 1; value <= 8; ++value) {
    if (queue.PushFront(value)) owner_accepted.push_back(value);
    if (auto popped = queue.PopFront()) owner_taken.push_back(popped);
    Yield();
  }
  remote.join();
  while (auto value = queue.PopFront()) owner_taken.push_back(value);
  owner_accepted.insert(owner_accepted.end(), remote_accepted.begin(), remote_accepted.end());
  owner_taken.insert(owner_taken.end(), remote_taken.begin(), remote_taken.end());
  ExactOnce(owner_accepted, owner_taken);
  TWIST_ASSERT(queue.Empty());
}

void MailboxLastItem() {
  pool::rigtorp::MPMCQueue<unsigned> queue(1);
  TWIST_ASSERT(queue.try_push(1));
  unsigned stolen = 0;
  bool took = false;
  Thread thief([&] { took = queue.try_pop(stolen); });
  unsigned value = 0;
  Values actual;
  if (queue.try_pop(value)) actual.push_back(value);
  thief.join();
  if (took) actual.push_back(stolen);
  while (queue.try_pop(value)) actual.push_back(value);
  ExactOnce({1}, actual);
  TWIST_ASSERT(queue.empty());
}

void MailboxManyToMany() {
  pool::rigtorp::MPMCQueue<unsigned> queue(2);
  std::array<Values, 2> accepted, consumed;
  const auto produce = [&](unsigned producer) {
    for (unsigned i = 1; i <= 8; ++i) {
      const unsigned value = producer * 100 + i;
      if (queue.try_push(value)) accepted[producer].push_back(value);
      Yield();
    }
  };
  const auto consume = [&](unsigned consumer) {
    for (unsigned attempt = 0; attempt < 12; ++attempt) {
      unsigned value = 0;
      if (queue.try_pop(value)) consumed[consumer].push_back(value);
      Yield();
    }
  };
  Thread first([&] { produce(0); });
  Thread second([&] { produce(1); });
  Thread third([&] { consume(0); });
  consume(1);
  first.join();
  second.join();
  third.join();
  unsigned value = 0;
  while (queue.try_pop(value)) consumed[1].push_back(value);
  accepted[0].insert(accepted[0].end(), accepted[1].begin(), accepted[1].end());
  consumed[0].insert(consumed[0].end(), consumed[1].begin(), consumed[1].end());
  ExactOnce(accepted[0], consumed[0]);
  TWIST_ASSERT(queue.empty());
}

struct Scenario {
  const char *name;
  void (*run)();
  bool dfs;
};

bool Check(const Scenario &scenario) {
  constexpr unsigned first_seed = 1, last_seed = 128;
  std::cout << "random case=" << scenario.name << " seeds=" << first_seed
            << ".." << last_seed << " spurious_failures=1/7 time_slice=15\n";
  for (unsigned seed = first_seed; seed <= last_seed; ++seed) {
    twist::sim::sched::RandomScheduler scheduler{{.seed = seed}};
    twist::sim::Simulator simulator{&scheduler};
    const auto result = simulator.Run(scenario.run);
    if (!result.Ok()) {
      std::cerr << "FAIL random case=" << scenario.name << " seed=" << seed
                << " status=" << static_cast<int>(result.status) << '\n'
                << result.std_out << result.std_err;
      return false;
    }
  }
  if (!scenario.dfs) return true;
  constexpr std::size_t max_schedules = 50000;
  twist::sim::sched::DfsScheduler scheduler{{.max_preempts = 2, .max_steps = 200,
      .spurious_failures = true}};
  std::size_t explored = 0, complete = 0, pruned = 0;
  bool more = true;
  while (more && explored < max_schedules) {
    twist::sim::Simulator simulator{&scheduler};
    const auto result = simulator.Run(scenario.run);
    ++explored;
    if (result.Failure()) {
      std::cerr << "FAIL dfs case=" << scenario.name << " schedule=" << explored
                << " max_preempts=2 max_steps=200\n" << result.std_out << result.std_err;
      return false;
    }
    if (result.Ok()) ++complete;
    else ++pruned;
    more = scheduler.NextSchedule();
  }
  std::cout << "dfs case=" << scenario.name << " max_preempts=2 max_steps=200"
            << " max_schedules=" << max_schedules << " explored=" << explored
            << " complete=" << complete << " pruned=" << pruned
            << " budget_exhausted=" << more << '\n';
  return complete != 0;
}
} // namespace

int main() {
  const Scenario scenarios[]{
      {"run_queue_last_item", RunQueueLastItem, true},
      {"run_queue_wrap_and_steal", RunQueueWrapAndSteal, false},
      {"run_queue_remote_producer", RunQueueRemoteProducer, false},
      {"mailbox_last_item", MailboxLastItem, true},
      {"mailbox_many_to_many", MailboxManyToMany, false}};
  for (const auto &scenario : scenarios)
    if (!Check(scenario)) return 1;
  std::cout << "PASS real Eigen queues under bounded Twist simulation\n";
}
