// SPDX-License-Identifier: MPL-2.0
#pragma once

#include "queue_traversal.h"
#include "scheduler_trace.h"
#include <atomic>
#include <cstdint>
#include <span>

namespace oox::detail::eigen_pool {
struct Task;

namespace internal {

// A resident polls its command between steps of the current queue traversal.
class ResidentQueueScan {
public:
  explicit ResidentQueueScan(uint64_t epoch) noexcept : epoch_(epoch) {}

  bool RoundFinished() const noexcept { return traversal_.Empty(); }
  bool RetryRequested() const noexcept { return retry_; }

  void AfterCommand() noexcept {
    traversal_ = {};
    retry_ = true;
  }

  template <class NextRandom>
  bool Prepare([[maybe_unused]] unsigned worker, uint64_t epoch, unsigned pool_size,
               std::span<const unsigned> coprime_steps, NextRandom next_random) {
    if (!RoundFinished()) return true;
    if (!retry_ && epoch == epoch_) return false;

    // A publication during a round triggers another pass, including queues
    // visited before the publication. Draw randomness only when starting it.
    traversal_ = QueueTraversal(pool_size, coprime_steps, next_random());
    epoch_ = epoch;
    retry_ = false;
    OOX_SCHED_EVENT("resident_queue_scan", ::oox::scheduler_trace::Current(), worker);
    return true;
  }

  template <class Workers>
  Task *TryTake(unsigned worker, Workers &queues) {
    auto &local = queues[worker];
    retry_ |= !local.affinity_mailbox.Empty();
    if (auto *task = local.PopFront()) return task;

    const unsigned victim = traversal_.Next();
    auto &source = queues[victim];
    const bool pending = source.MayHaveStealableWork();
    retry_ |= pending;
    auto *task = pending ? source.PopBack(true) : nullptr;
    if (task && victim != worker) {
      OOX_SCHED_EVENT("steal_queue", task->trace_id, victim, worker);
#ifdef OOX_EIGEN_ENABLE_STATS
      local.statistics.successful_steals.fetch_add(1, std::memory_order_relaxed);
#endif
    }
    return task;
  }

private:
  // Ordinary-work publication epoch at round start. A change triggers another round.
  uint64_t epoch_;
  QueueTraversal traversal_;
  // Request a round even at the same epoch: on entry, after a Rapid command,
  // or when a queue looked nonempty (a contended steal can leave work behind).
  bool retry_ = true;
};

} // namespace internal
} // namespace oox::detail::eigen_pool
