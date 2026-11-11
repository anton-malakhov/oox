// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "nonblocking_thread_pool.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <exception>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace oox::detail::eigen_pool::rapid {

struct RapidStartGroup {
  ThreadPool *pool = nullptr;
  DomainId domain;

  bool IsEmpty() const noexcept {
    return pool == nullptr || domain.start == domain.limit;
  }

  void Validate() const {
    if (pool && (domain.start >= domain.limit || domain.limit > pool->NumThreads())) {
      throw std::invalid_argument("rapid domain is outside its thread pool");
    }
  }
};

inline ThreadPool *&CurrentResidentPool() noexcept {
  struct Tag {};
  return scheduler_sync::Local<ThreadPool *, Tag>();
}

class ScopedResidentPool {
public:
  explicit ScopedResidentPool(ThreadPool &pool) noexcept
      : previous_(std::exchange(CurrentResidentPool(), &pool)) {}
  ScopedResidentPool(const ScopedResidentPool &) = delete;
  ScopedResidentPool &operator=(const ScopedResidentPool &) = delete;
  ~ScopedResidentPool() { CurrentResidentPool() = previous_; }

private:
  ThreadPool *previous_;
};

using ResidentCohort = internal::ResidentCohort<ThreadPool>;

template <typename F>
class ResidentRegion final : public ResidentTask {
public:
  ResidentRegion(ThreadPool &pool, F &function, size_t begin, size_t end,
                 size_t slots, size_t helpers) noexcept
      : pool_(pool), function_(function), begin_(begin), end_(end),
        slots_(slots), remaining_(helpers) {}

  void RunCaller() noexcept { Run(0); }

  scheduler_sync::atomic<size_t> &CompletionCounter() noexcept { return remaining_; }

  bool IsComplete() const noexcept {
    return remaining_.load(std::memory_order_acquire) == 0;
  }

  // Both the caller body and all captured helpers must have returned. The
  // captured-command join then makes exception_ immutable and visible here.
  void RethrowAfterJoin() {
    assert(IsComplete());
    if (exception_) std::rethrow_exception(exception_);
  }

  void Run(size_t slot) noexcept final {
    ScopedResidentPool context(pool_);
    const size_t work = end_ - begin_;
    const size_t quotient = work / slots_;
    const size_t remainder = work % slots_;
    const size_t first =
        begin_ + slot * quotient + std::min(slot, remainder);
    const size_t last = first + quotient + (slot < remainder ? 1 : 0);
    try {
      if (!pool_.IsCancelled() &&
          !cancelled_.load(std::memory_order_acquire))
        std::invoke(function_, first, last);
    } catch (...) {
      std::lock_guard<scheduler_sync::mutex> lock(exception_mutex_);
      if (!exception_) {
        exception_ = std::current_exception();
        cancelled_.store(true, std::memory_order_release);
      }
    }
  }

private:
  ThreadPool &pool_;
  F &function_;
  size_t begin_;
  size_t end_;
  size_t slots_;
  scheduler_sync::atomic<size_t> remaining_;
  scheduler_sync::atomic<bool> cancelled_{false};
  scheduler_sync::mutex exception_mutex_;
  std::exception_ptr exception_;
};

// One callback per captured participant. The callback owns its range's loop
// and checks cancellation at its own safe points, like an ordinary pool task.
template <typename F>
void ParallelForResidentRanges(RapidStartGroup group, size_t begin, size_t end,
                               F &&function) {
  if (group.IsEmpty() || begin >= end) {
    return;
  }
  group.Validate();
  ThreadPool &pool = *group.pool;
  if (!pool.UsesResidentBusyWait()) {
    throw std::invalid_argument("resident Rapid requires a resident-busy pool");
  }
  if (CurrentResidentPool() == group.pool || group.domain.Size() == 1) {
    if (!pool.IsCancelled()) {
      ScopedResidentPool context(pool);
      std::invoke(function, begin, end);
    }
    return;
  }
  constexpr size_t kMaximumParticipants = 64;
  // The pool may contain arbitrarily many 64-bit availability words. A single
  // invocation deliberately keeps one compact completion cohort.
  const size_t slots = std::min(
      {end - begin, group.domain.Size(), kMaximumParticipants});
  std::array<unsigned, kMaximumParticipants - 1> workers{};
  const size_t helpers = pool.ClaimResidentWorkers(
      group.domain, workers.data(), slots - 1);
  ResidentRegion region(pool, function, begin, end, helpers + 1, helpers);
  ResidentCohort cohort(pool, workers.data(), helpers, region.CompletionCounter());
  cohort.Publish(region);
  region.RunCaller();
  pool.HelpResidentUntil(cohort);
  cohort.Finalize();
  region.RethrowAfterJoin();
}

} // namespace oox::detail::eigen_pool::rapid
