// Copyright (c) 2005-2021 Intel Corporation
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "nonblocking_thread_pool.h"
#include <limits>

namespace oox::detail::eigen_pool::partitioner_detail {

inline constexpr unsigned max_depth = std::numeric_limits<size_t>::digits;

struct Metrics {
  std::atomic<size_t> owner_ranges{0};
  std::atomic<size_t> range_tasks{0};
  std::atomic<size_t> initial_tasks{0};
  std::atomic<size_t> stolen_tasks{0};
  std::atomic<size_t> donated_ranges{0};
  std::atomic<size_t> private_chunks{0};
};

class Region final : public SmallObjectAllocated<Region> {
public:
  explicit Region(ThreadPool &pool, Metrics *metrics,
                  bool asynchronous_roots = false)
      : pool(pool), metrics(metrics), asynchronous_roots_(asynchronous_roots) {}
  void AddTask() noexcept { remaining.fetch_add(1, std::memory_order_relaxed); }
  void TaskComplete(bool notify = true) noexcept {
    // The caller and the adaptive tree (or each non-adaptive task) retain us.
    // Copy the pool before releasing: completion may release the caller too.
    ThreadPool *saved_pool = &pool;
    // Mailbox roots have no synchronous root owner whose wakeup can be elided.
    notify = notify || asynchronous_roots_;
    const size_t previous = remaining.fetch_sub(1, std::memory_order_acq_rel);
    if (previous == 1)
      DeleteSmallObject(this);
    else if (previous == 2 && notify)
      saved_pool->NotifyTaskCompletion();
  }
  bool IsComplete() const noexcept {
    return remaining.load(std::memory_order_acquire) == 1;
  }
  std::atomic<bool> &RapidPublication() noexcept { return rapid_unpublished; }
  bool BeginWork() noexcept {
    if (IsCancelled())
      return false;
    const size_t previous = active.fetch_add(1, std::memory_order_acquire);
    if (!(previous & closed))
      return true;
    // Undo admission after closure, waking a caller waiting for active work.
    EndWork();
    return false;
  }
  void EndWork() noexcept {
    if (active.fetch_sub(1, std::memory_order_acq_rel) == closed + 1)
      active.notify_all();
  }
  void CloseAndWait() noexcept {
    size_t state = active.fetch_or(closed, std::memory_order_acq_rel) | closed;
    while (state != closed) {
      active.wait(state, std::memory_order_acquire);
      state = active.load(std::memory_order_acquire);
    }
  }
  bool IsCancelled() const noexcept {
    return cancelled.load(std::memory_order_relaxed) || pool.IsCancelled();
  }
  bool ReserveDonation() noexcept {
    bool expected = false;
    return donation_pending.compare_exchange_strong(expected, true,
                                                    std::memory_order_relaxed);
  }
  void ReleaseDonation() noexcept {
    const bool pending = donation_pending.exchange(false, std::memory_order_release);
    assert(pending);
    (void)pending;
  }
  void Fail(std::exception_ptr error) noexcept {
    std::lock_guard<std::mutex> lock(error_mutex);
    if (!exception)
      exception = std::move(error);
    cancelled.store(true, std::memory_order_release);
  }
  void Rethrow() {
    if (exception)
      std::rethrow_exception(exception);
  }

  ThreadPool &pool;
  Metrics *const metrics;

private:
  const bool asynchronous_roots_;
  std::atomic<size_t> remaining{1};
  static constexpr size_t closed = size_t{1} << (max_depth - 1);
  std::atomic<size_t> active{0};
  std::atomic<bool> cancelled{false};
  std::atomic<bool> rapid_unpublished{false};
  // One not-yet-started donation per region bounds stale/contended idle hints.
  std::atomic<bool> donation_pending{false};
  std::mutex error_mutex;
  std::exception_ptr exception;
};

// Closing prevents queued tasks from accessing the callback or metrics after
// return. Running callbacks finish before CloseAndWait releases the caller.
class WorkLease {
public:
  // Captured commands are joined before the factory may close the Region.
  // Ordinary descendants still acquire their own counted admission.
  struct JoinedOwners {};
  WorkLease(Region &region, JoinedOwners) noexcept
      : region_(region), acquired_(!region.IsCancelled()), owned_(false) {}
  explicit WorkLease(Region &region, const WorkLease *group = nullptr) noexcept
      : region_(region), acquired_(group ? group->acquired_ && !region.IsCancelled()
                                        : region.BeginWork()), owned_(!group) {
    assert(!group || &group->region_ == &region);
  }
  WorkLease(const WorkLease &) = delete;
  WorkLease &operator=(const WorkLease &) = delete;
  ~WorkLease() {
    if (acquired_ && owned_)
      region_.EndWork();
  }
  explicit operator bool() const noexcept { return acquired_; }

private:
  Region &region_;
  const bool acquired_;
  const bool owned_;
};

} // namespace oox::detail::eigen_pool::partitioner_detail
