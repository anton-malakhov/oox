// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <atomic>
#include <cassert>
#include <cstdint>
#include <thread>
#include "scheduler_trace.h"

namespace oox::detail::eigen_pool {
struct Task;

// A tagged worker slot pins this stack object during a theft. Unpublication
// acquires the same tag, so a stale pointer is never dereferenced by a thief.
class StealableRange {
public:
  virtual ~StealableRange() { assert(!slot_); }
  virtual Task *Steal() noexcept = 0;
  void Publish(std::atomic<uintptr_t> &slot) noexcept {
    assert(!slot_ && !(reinterpret_cast<uintptr_t>(this) & locked));
    slot_ = &slot;
    previous_ = Lock(slot);
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
  static Task *TrySteal(std::atomic<uintptr_t> &slot) noexcept {
    uintptr_t current = slot.load(std::memory_order_acquire);
    if (!current) return nullptr;
    if ((current & locked) || !slot.compare_exchange_strong(current, current | locked,
            std::memory_order_seq_cst, std::memory_order_relaxed)) {
      OOX_SCHED_COUNT(RangeTagContended);
      return nullptr;
    }
    OOX_SCHED_COUNT(RangeLockAcquired);
    Task *task = reinterpret_cast<StealableRange *>(current)->Steal();
    slot.store(current, std::memory_order_release);
    return task;
  }

private:
  static constexpr uintptr_t locked = 1;
  static void Relax() noexcept {
#if defined(__x86_64__)
    asm volatile("pause" ::: "memory");
#elif defined(__aarch64__)
    asm volatile("yield" ::: "memory");
#else
    std::this_thread::yield();
#endif
  }
  static uintptr_t Lock(std::atomic<uintptr_t> &slot) noexcept {
    uintptr_t current = slot.load(std::memory_order_acquire);
    for (;;) {
      if (!(current & locked) && slot.compare_exchange_weak(current, current | locked,
              std::memory_order_seq_cst, std::memory_order_acquire)) return current;
      Relax();
      current = slot.load(std::memory_order_acquire);
    }
  }
  std::atomic<uintptr_t> *slot_ = nullptr;
  uintptr_t previous_ = 0;
};
} // namespace oox::detail::eigen_pool
