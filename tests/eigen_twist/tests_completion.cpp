// SPDX-License-Identifier: Apache-2.0
#include "oox/eigen/parallel_for.h"

#include <twist/assist/assert.hpp>
#include <twist/sim.hpp>

#include <array>
#include <iostream>

namespace {
namespace pool = oox::detail::eigen_pool;
namespace detail = pool::partitioner_detail;
namespace sync = pool::scheduler_sync;

// The production join/allocator own every transition. Only destruction is
// instrumented here, with an external counter that outlives every node.
struct Owner final : detail::PeerJoin<Owner>, pool::SmallObjectAllocated<Owner> {
  using Join = detail::PeerJoin<Owner>;
  Owner(Join *parent, sync::atomic<unsigned> &destroyed)
      : Join(parent), destroyed(destroyed) {}
  ~Owner() { destroyed.fetch_add(1); }
  sync::atomic<unsigned> &destroyed;
};
using Join = Owner::Join;

detail::Region *NewTree(pool::ThreadPool &workers) {
  auto *region = pool::NewSmallObject<detail::Region>(workers, nullptr, true);
  TWIST_ASSERT(region->IsComplete());
  region->AddTask();
  TWIST_ASSERT(!region->IsComplete());
  return region;
}

void ExecutionFinishesBeforePeer() {
  pool::ThreadPool workers(1, false, false);
  sync::atomic<unsigned> destroyed{0};
  auto *region = NewTree(workers);
  auto *node = pool::NewSmallObject<Owner>(Join::Root(*region), destroyed);
  Join::Release(node); // This task's own branch is done.
  node->FinishExecution();
  TWIST_ASSERT(destroyed.load() == 0);
  TWIST_ASSERT(!region->IsComplete());
  sync::thread peer([&] { Join::Release(node); });
  peer.join();
  TWIST_ASSERT(destroyed.load() == 1);
  TWIST_ASSERT(region->IsComplete());
  region->TaskComplete();
}

void BranchesFinishBeforeExecution() {
  pool::ThreadPool workers(1, false, false);
  sync::atomic<unsigned> destroyed{0};
  auto *region = NewTree(workers);
  auto *node = pool::NewSmallObject<Owner>(Join::Root(*region), destroyed);
  Join::Release(node);
  sync::thread peer([&] { Join::Release(node); });
  peer.join();
  TWIST_ASSERT(region->IsComplete());
  TWIST_ASSERT(destroyed.load() == 0);
  // Callback/tree completion permits the caller to release Region before
  // trailing task cleanup, which must use only the still-live join object.
  region->TaskComplete();
  node->FinishExecution();
  TWIST_ASSERT(destroyed.load() == 1);
}

void ConcurrentLastBranchesAndExecution() {
  pool::ThreadPool workers(1, false, false);
  sync::atomic<unsigned> destroyed{0};
  auto *region = NewTree(workers);
  auto *node = pool::NewSmallObject<Owner>(Join::Root(*region), destroyed);
  sync::thread first([&] { Join::Release(node); });
  sync::thread second([&] { Join::Release(node); });
  node->FinishExecution();
  first.join();
  second.join();
  TWIST_ASSERT(destroyed.load() == 1);
  TWIST_ASSERT(region->IsComplete());
  region->TaskComplete();
}

void DescendantKeepsFinishedAncestorAlive() {
  pool::ThreadPool workers(1, false, false);
  sync::atomic<unsigned> root_destroyed{0}, child_destroyed{0};
  auto *region = NewTree(workers);
  auto *root = pool::NewSmallObject<Owner>(Join::Root(*region), root_destroyed);
  auto *child = pool::NewSmallObject<Owner>(root, child_destroyed);
  Join::Release(root);
  root->FinishExecution();
  TWIST_ASSERT(root_destroyed.load() == 0);
  Join::Release(child);
  child->FinishExecution();
  TWIST_ASSERT(root_destroyed.load() == 0 && child_destroyed.load() == 0);
  TWIST_ASSERT(!region->IsComplete());
  sync::thread last([&] { Join::Release(child); });
  last.join();
  TWIST_ASSERT(root_destroyed.load() == 1 && child_destroyed.load() == 1);
  TWIST_ASSERT(region->IsComplete());
  region->TaskComplete();
}

void FirstChildPromotesAndSecondExtendsTree() {
  pool::ThreadPool workers(1, false, false);
  std::array<sync::atomic<unsigned>, 2> visits{};
  auto body = [&](size_t index) {
    TWIST_ASSERT(index < visits.size());
    visits[index].fetch_add(1);
    sync::this_thread::yield();
  };
  using State = pool::partitioning::LazyAutoPartitionState;
  using Work = detail::Work<decltype(body), State>;
  using WorkJoin = detail::PeerJoin<Work>;
  auto *region = pool::NewSmallObject<detail::Region>(workers, nullptr, true);
  TWIST_ASSERT(region->IsComplete());
  auto *first = pool::NewSmallObject<Work>(*region, &body,
      detail::LoopRange{0, 1, 1}, State(workers.NumThreads()), 0, nullptr);
  TWIST_ASSERT(!region->IsComplete());
  TWIST_ASSERT(WorkJoin::IsRoot(first->parent));
  auto *second = pool::NewSmallObject<Work>(*region, &body,
      detail::LoopRange{1, 2, 1}, State(workers.NumThreads()), 0, first);
  TWIST_ASSERT(second->parent == first);
  sync::thread stolen_first([&] { (*first)(); });
  sync::thread stolen_second([&] { (*second)(); });
  stolen_first.join();
  stolen_second.join();
  TWIST_ASSERT(visits[0].load() == 1 && visits[1].load() == 1);
  TWIST_ASSERT(!region->IsComplete()); // The original owner's branch remains.
  WorkJoin::Release(second);
  TWIST_ASSERT_M(region->IsComplete(), "one promotion must cover the whole descendant tree");
  region->CloseAndWait();
  region->TaskComplete();
}
} // namespace

int main() {
  struct Scenario { const char *name; void (*run)(); };
  const Scenario scenarios[]{
      {"execution_before_peer", ExecutionFinishesBeforePeer},
      {"branches_before_execution", BranchesFinishBeforeExecution},
      {"concurrent_last_branches", ConcurrentLastBranchesAndExecution},
      {"descendant_keeps_ancestor", DescendantKeepsFinishedAncestorAlive},
      {"first_child_promotion", FirstChildPromotesAndSecondExtendsTree}};
  for (const auto &scenario : scenarios) {
    for (unsigned seed = 1; seed <= 64; ++seed) {
      std::cout << "case=" << scenario.name << " seed=" << seed << '\n' << std::flush;
      twist::sim::sched::RandomScheduler scheduler{{.seed = seed}};
      twist::sim::Simulator simulator{&scheduler};
      const auto result = simulator.Run(scenario.run);
      if (!result.Ok()) {
        std::cerr << "FAIL completion case=" << scenario.name << " seed=" << seed
                  << '\n' << result.std_out << result.std_err;
        return 1;
      }
    }
  }
  std::cout << "PASS real PeerJoin lifetime and Region completion under bounded Twist simulation\n";
}
