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

class RapidDomainState {
public:
  explicit RapidDomainState(ThreadPool &pool) noexcept : pool_(pool) {}
  ThreadPool &Pool() noexcept { return pool_; }

private:
  ThreadPool &pool_;
};

struct RapidStartGroup {
  RapidDomainState *state = nullptr;
  DomainId domain;

  bool IsEmpty() const noexcept {
    return state == nullptr || domain.IsEmpty();
  }

  void Validate() const {
    if (state && !domain.IsValidFor(state->Pool().NumThreads())) {
      throw std::invalid_argument("rapid domain is outside its thread pool");
    }
  }

  RapidStartGroup Subgroup(DomainId child) const {
    Validate();
    if (!state || !child.IsValidFor(state->Pool().NumThreads()) ||
        child.start < domain.start || child.limit > domain.limit) {
      throw std::invalid_argument("rapid subgroup is outside its parent");
    }
    return {state, child};
  }
};

inline thread_local RapidDomainState *current_resident_state = nullptr;

class ScopedResidentState {
public:
  explicit ScopedResidentState(RapidDomainState &state) noexcept
      : previous_(std::exchange(current_resident_state, &state)) {}
  ~ScopedResidentState() { current_resident_state = previous_; }

private:
  RapidDomainState *previous_;
};

template <typename F>
class ResidentRegion final : public ResidentTask {
public:
  ResidentRegion(RapidDomainState &state, F &function, size_t begin, size_t end,
                 size_t slots, size_t helpers) noexcept
      : state_(state), function_(function), begin_(begin), end_(end),
        slots_(slots), remaining_(helpers) {}

  void Run(size_t slot) noexcept final {
    RunSlot(slot);
  }

  void RunCaller() noexcept { RunSlot(0); }

  std::atomic<size_t> &CompletionCounter() noexcept { return remaining_; }

  bool IsComplete() const noexcept {
    return remaining_.load(std::memory_order_acquire) == 0;
  }

  void Rethrow() {
    std::lock_guard<std::mutex> lock(exception_mutex_);
    if (exception_) {
      std::rethrow_exception(exception_);
    }
  }

private:
  void RunSlot(size_t slot) noexcept {
    ScopedResidentState context(state_);
    const size_t work = end_ - begin_;
    const size_t quotient = work / slots_;
    const size_t remainder = work % slots_;
    const size_t first =
        begin_ + slot * quotient + std::min(slot, remainder);
    const size_t last = first + quotient + (slot < remainder ? 1 : 0);
    try {
      if (!state_.Pool().IsCancelled() &&
          !cancelled_.load(std::memory_order_acquire))
        std::invoke(function_, first, last);
    } catch (...) {
      std::lock_guard<std::mutex> lock(exception_mutex_);
      if (!exception_) {
        exception_ = std::current_exception();
        cancelled_.store(true, std::memory_order_release);
      }
    }
  }

  RapidDomainState &state_;
  F &function_;
  size_t begin_;
  size_t end_;
  size_t slots_;
  std::atomic<size_t> remaining_;
  std::atomic<bool> cancelled_{false};
  std::mutex exception_mutex_;
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
  ThreadPool &pool = group.state->Pool();
  if (!pool.UsesResidentBusyWait()) {
    throw std::invalid_argument("resident Rapid requires a resident-busy pool");
  }
  if (current_resident_state == group.state || group.domain.Size() == 1) {
    if (!pool.IsCancelled())
      std::invoke(function, begin, end);
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
  using Function = std::remove_reference_t<F>;
  Function &callable = function;
  ResidentRegion<Function> region(*group.state, callable, begin, end,
                                  helpers + 1, helpers);
  for (size_t helper = 0; helper < helpers; ++helper) {
    pool.PublishResident(region, region.CompletionCounter(), workers[helper],
                         helper + 1);
  }
  region.RunCaller();
  pool.HelpResidentUntil(region);
  region.Rethrow();
}

} // namespace oox::detail::eigen_pool::rapid
