// Copyright (c) 2005-2021 Intel Corporation
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "partitioner_task.h"
#include <algorithm>

namespace oox::detail::eigen_pool::partitioner_detail {

// Only an idle thief allocates a descendant. The owner reserves a contiguous
// batch before executing it; theft may shrink only its not-yet-reserved tail.
template <typename F, typename State>
class ActiveRange final : public StealableRange {
  using Node = Work<F, State>;
  using Join = PeerJoin<Node>;
public:
  ActiveRange(Region &region, F *function, LoopRange range, State state,
              Branch<Node, true> &branch) noexcept
      : region_(region), function_(function), state_(state), branch_(branch),
        parent_(branch.Parent()), head_(range.begin), end_(range.end),
        begin_(range.begin), grain_(range.grain), owner_(scheduler_sync::this_thread::get_id()) {
    state_.divisor = 0;
  }
  void Close() noexcept {
    if (IsPublished()) {
      Unpublish();
      branch_.SetParent(parent_);
    }
  }
  Task *Steal() noexcept final {
    const size_t first = head_.load(std::memory_order_seq_cst);
    const size_t last = end_.load(std::memory_order_relaxed);
    if (first >= last) {
      OOX_SCHED_COUNT(RangeRejectedEmpty);
      return nullptr;
    }
    if (last - first <= grain_) {
      OOX_SCHED_COUNT(RangeRejectedGrain);
      return nullptr;
    }
    if (region_.IsCancelled()) return nullptr;
    if (state_.unpublished && state_.unpublished->load(std::memory_order_acquire)) {
      OOX_SCHED_COUNT(RangeRejectedPublishing);
      return nullptr;
    }
    if (region_.pool.HasResidentCommand()) {
      OOX_SCHED_COUNT(RangeRejectedCommand);
      return nullptr;
    }
    const size_t middle = first + (last - first) / 2;
    try {
      state_.CheckDemand(true);
      auto *child = NewSmallObject<Node>(region_, function_, LoopRange{middle, last, grain_},
          State(state_, partitioning::Split{}), 0, parent_);
      // Construction happens on the thief, but completion notification and
      // Auto's stolen-peer feedback must retain the actual donating owner.
      child->SetPublishingThreadId(owner_);
      OOX_SCHED_EVENT("split", child->trace_id, middle, last, trace_parent_);
      parent_ = child;
      end_.store(middle, std::memory_order_release);
      if (region_.metrics) region_.metrics->donated_ranges.fetch_add(1, std::memory_order_relaxed);
      return child;
    } catch (...) {
      region_.Fail(std::current_exception());
      return nullptr;
    }
  }
  void Run(size_t &chunks) {
    size_t cursor = begin_, quantum = grain_;
    // Grow local batches without constructing a private split tree. A thief
    // can access the unreserved remainder even while a callback is running.
    const size_t limit = std::max(grain_, (end_.load(std::memory_order_relaxed) - begin_ - 1) / 8 + 1);
    while (!region_.IsCancelled()) {
      const size_t last = end_.load(std::memory_order_acquire);
      if (cursor >= last) break;
      const size_t next = cursor + std::min(quantum, last - cursor);
      // SC orders this reservation against the slot tag: a later thief sees
      // at least next; an earlier thief is joined and its shorter end reloaded.
      head_.store(next, std::memory_order_seq_cst);
      WaitForStealer();
      const size_t stop = std::min(next, end_.load(std::memory_order_acquire));
      {
        OOX_SCHED_SCOPE("chunk", ::oox::scheduler_trace::NextId(), cursor, stop);
        for (size_t i = cursor; i < stop; ++i) std::invoke(*function_, i);
      }
      cursor = stop;
      ++chunks;
      quantum = std::min(quantum, limit);
      quantum += std::min(quantum, limit - quantum);
    }
  }

private:
  Region &region_;
  F *function_;
  State state_;
  Branch<Node, true> &branch_;
  Join *parent_;
  scheduler_sync::atomic<size_t> head_, end_;
  const size_t begin_, grain_;
  const scheduler_sync::thread::id owner_;
#ifdef OOX_SCHEDULER_TRACE
  const uint64_t trace_parent_ = ::oox::scheduler_trace::Current();
#endif
};

template <typename F, typename State>
void RunCooperativeRange(Region &region, F *function, LoopRange &range,
                         State &state, Branch<Work<F, State>, true> &branch,
                         size_t &chunks) {
  // Unregistered callers have no exclusive worker slot. They retain
  // cooperative donation, using the same completion and cancellation.
  // The prefix consumed the initial fanout. A running owner keeps one
  // contiguous remainder, not an eagerly expanded private range tree.
  state.divisor = 0;
  size_t quantum = range.grain;
  const auto polling_limit = [&] {
    const size_t width = size_t{1} << std::min(state.max_depth, max_depth - 1);
    return std::max(range.grain, (range.end - range.begin - 1) / width + 1);
  };
  size_t limit = polling_limit();
  while (range.begin != range.end && !region.IsCancelled()) {
    if (range.IsDivisible() &&
        (region.pool.HasWaitingHelper() || region.pool.HasResidentAvailableWorker(
            {0, static_cast<unsigned>(region.pool.NumThreads())})) &&
        region.ReserveDonation()) {
      state.CheckDemand(true);
      LoopRange right(range, partitioning::Split{});
      OfferWork(region, function, right, state, branch, 0, partitioning::Split{});
      quantum = range.grain;
      limit = polling_limit();
      if (region.metrics) region.metrics->donated_ranges.fetch_add(1, std::memory_order_relaxed);
      continue;
    }
    const size_t remaining = range.end - range.begin;
    const size_t count = std::min(quantum, remaining);
    const size_t next = range.begin + count;
    {
      OOX_SCHED_SCOPE("chunk", ::oox::scheduler_trace::NextId(), range.begin, next);
      for (size_t i = range.begin; i < next; ++i) std::invoke(*function, i);
    }
    range.begin = next;
    ++chunks;
    // Back off checks on regular work, but bound the unchecked tail by
    // Auto's depth budget so late skew can still supply idle helpers.
    quantum = std::min(quantum, limit);
    quantum += std::min(quantum, limit - quantum);
  }
}

// Execute one owner's lazy range; publication and local batching stay here,
// while Process owns callback admission, exceptions and final metrics.
template <typename F, typename State, typename Run>
void RunLazyRange(Region &region, F *function, LoopRange range, State state,
                  Branch<Work<F, State>, true> &branch, size_t &chunks, Run &run) {
  if (!range.IsDivisible()) {
    if (range.begin != range.end) run(range);
  } else {
    ActiveRange<F, State> active(region, function, range, state, branch);
    // Close while the polymorphic range is still alive, before its
    // destructor can change the vptr observed by a concurrent thief.
    struct Registration {
      ActiveRange<F, State> &range;
      ~Registration() { range.Close(); }
    } registration{active};
    if (region.pool.PublishStealableRange(active)) {
      active.Run(chunks);
    } else {
      RunCooperativeRange(region, function, range, state, branch, chunks);
    }
  }
}

} // namespace oox::detail::eigen_pool::partitioner_detail
