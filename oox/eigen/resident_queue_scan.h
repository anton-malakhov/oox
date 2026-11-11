// SPDX-License-Identifier: MPL-2.0
#pragma once

#include "scheduler_trace.h"
#include <atomic>
#include <cstdint>
#include <span>

namespace oox::detail::eigen_pool {
struct Task;

namespace internal {

// A resident polls its command between queue probes. A complete round visits
// every queue once, using a stride coprime to the number of workers.
class ResidentQueueScan {
public:
  explicit ResidentQueueScan(uint64_t epoch) noexcept : epoch_(epoch) {}

  bool RoundFinished() const noexcept { return remaining_ == 0; }
  bool RetryRequested() const noexcept { return retry_; }

  void AfterCommand() noexcept {
    remaining_ = 0;
    retry_ = true;
  }

  template <class NextRandom>
  bool Prepare([[maybe_unused]] unsigned worker, uint64_t epoch, unsigned pool_size,
               std::span<const unsigned> coprime_steps, NextRandom next_random) {
    if (!RoundFinished()) return true;
    if (!retry_ && epoch == epoch_) return false;

    // A publication during a round triggers another pass, including queues
    // visited before the publication. Draw randomness only when starting it.
    const uint64_t random = next_random();
    victim_ = static_cast<unsigned>((random * pool_size) >> 32);
    stride_ = coprime_steps[(random * coprime_steps.size()) >> 32];
    remaining_ = pool_size;
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

    auto &source = queues[victim_];
    const bool pending = source.MayHaveStealableWork();
    retry_ |= pending;
    auto *task = pending ? source.PopBack(true) : nullptr;
    if (task && victim_ != worker) {
      OOX_SCHED_EVENT("steal_queue", task->trace_id, victim_, worker);
#ifdef OOX_EIGEN_ENABLE_STATS
      local.statistics.successful_steals.fetch_add(1, std::memory_order_relaxed);
#endif
    }
    --remaining_;
    victim_ += stride_;
    const auto pool_size = static_cast<unsigned>(queues.size());
    if (victim_ >= pool_size) victim_ -= pool_size;
    return task;
  }

private:
  uint64_t epoch_;
  unsigned remaining_ = 0;
  unsigned victim_ = 0;
  unsigned stride_ = 1;
  bool retry_ = true;
};

} // namespace internal
} // namespace oox::detail::eigen_pool
