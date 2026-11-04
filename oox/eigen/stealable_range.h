// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <atomic>
#include <cassert>
#include <cstdint>
#include <thread>
#include "scheduler_trace.h"
#include "scheduler_sync.h"

namespace oox::detail::eigen_pool {
struct Task;

// A tagged worker slot pins the owner's LIFO chain of stack ranges during a
// theft. Publication and unpublication acquire the same tag, so every ancestor
// stays alive until the thief has finished traversing the chain.
class StealableRange {
public:
  virtual ~StealableRange() { assert(!slot_); }
  virtual Task *Steal() noexcept = 0;
  void Publish(scheduler_sync::atomic<uintptr_t> &slot) noexcept {
    assert(!slot_ && !(reinterpret_cast<uintptr_t>(this) & locked));
    slot_ = &slot;
    previous_ = Lock(slot);
    if (previous_)
      reinterpret_cast<StealableRange *>(previous_)->OnNestedEntry();
    slot.store(reinterpret_cast<uintptr_t>(this), std::memory_order_release);
  }
  bool IsPublished() const noexcept { return slot_ != nullptr; }
  void Unpublish() noexcept {
    const auto current = Lock(*slot_);
    assert(current == reinterpret_cast<uintptr_t>(this));
    (void)current;
    slot_->store(previous_, std::memory_order_release);
    slot_ = nullptr;
  }
  void WaitForStealer() const noexcept {
    // Ordered after the owner's progress store. A later thief observes that
    // reservation; an earlier thief finishes before the owner uses its bounds.
#ifdef OOX_SCHEDULER_TRACE
    uint64_t spins = 0;
#endif
    while (slot_->load(std::memory_order_seq_cst) & locked) {
#ifdef OOX_SCHEDULER_TRACE
      ++spins;
#endif
      Relax();
    }
    OOX_SCHED_COUNT(OwnerWaitSpins, spins);
  }
  // Called only by the slot's physical owner. The hook releases pending work
  // before an inner operation that may execute without publishing a range.
  static bool PrepareNested(scheduler_sync::atomic<uintptr_t> &slot) noexcept {
    if (!slot.load(std::memory_order_acquire)) return false;
    const uintptr_t current = Lock(slot);
    const bool released = current &&
        reinterpret_cast<StealableRange *>(current)->OnNestedEntry();
    slot.store(current, std::memory_order_release);
    return released;
  }
  static Task *TrySteal(scheduler_sync::atomic<uintptr_t> &slot) noexcept {
    uintptr_t current = slot.load(std::memory_order_acquire);
    if (!current) return nullptr;
    if ((current & locked) || !slot.compare_exchange_strong(current, current | locked,
            std::memory_order_seq_cst, std::memory_order_relaxed)) {
      OOX_SCHED_COUNT(RangeTagContended);
      return nullptr;
    }
    OOX_SCHED_COUNT(RangeLockAcquired);
    Task *task = nullptr;
    auto *range = reinterpret_cast<StealableRange *>(current);
    do {
      task = range->Steal();
      if (task) break;
      // Keep the slot pinned through both the link read and the ancestor's
      // Steal(). Prefer the deepest range, then try unreserved ancestor tails.
      range = reinterpret_cast<StealableRange *>(range->previous_);
    } while (range);
    slot.store(current, std::memory_order_release);
    return task;
  }

protected:
  // Invoked by the owner while the same slot tag pins this range and its
  // ancestors. The hook must not invoke callbacks or acquire the tag again.
  virtual bool OnNestedEntry() noexcept { return false; }

private:
  static constexpr uintptr_t locked = 1;
  static void Relax() noexcept {
    scheduler_sync::Relax();
  }
  static uintptr_t Lock(scheduler_sync::atomic<uintptr_t> &slot) noexcept {
    uintptr_t current = slot.load(std::memory_order_acquire);
    for (;;) {
      if (!(current & locked) && slot.compare_exchange_weak(current, current | locked,
              std::memory_order_seq_cst, std::memory_order_acquire)) return current;
      Relax();
      current = slot.load(std::memory_order_acquire);
    }
  }
  scheduler_sync::atomic<uintptr_t> *slot_ = nullptr;
  uintptr_t previous_ = 0;
};
} // namespace oox::detail::eigen_pool
