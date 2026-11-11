// Copyright (c) 2005-2021 Intel Corporation
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "partitioner_task.h"
#include <algorithm>

namespace oox::detail::eigen_pool::partitioner_detail {

// Only an idle thief allocates a descendant. Theft takes the unreserved tail,
// including pending batch callbacks explicitly released by a nested owner.
template <typename F, typename State>
class ActiveRange final : public StealableRange {
  using Node = Work<F, State>;
public:
  ActiveRange(Region &region, F *function, LoopRange range, State state,
              Branch<Node, true> &branch) noexcept
      : region_(region), function_(function), state_(state), branch_(branch),
        head_(range.begin), end_(range.end),
        begin_(range.begin), grain_(range.grain), owner_(scheduler_sync::this_thread::get_id()) {
    state_.divisor = 0;
  }
  bool TryPublishConcurrent(ConcurrentRangeSlot &slot) noexcept {
    const size_t size = end_.load(std::memory_order_relaxed) - begin_;
    // Initial Rapid owners join through their cohort. Each independent stolen
    // subtree then keeps the asynchronous region alive through its own root.
    if (!std::atomic<uint64_t>::is_always_lock_free || !state_.unpublished ||
        size > UINT32_MAX || slot.range.load()) return false;
    concurrent_ = &slot;
    slot.bounds.store(size, std::memory_order_relaxed);
    slot.range.store(this, std::memory_order_seq_cst);
    return true;
  }
  void Close() noexcept {
    if (concurrent_) {
      concurrent_->Close();
      concurrent_ = nullptr;
    }
    if (IsPublished()) Unpublish();
  }
  Task *Steal() noexcept final {
    // Kunpeng-920, 32 workers, VariableCost<Clustered>/16384: the tagged
    // path beat concurrent CAS in all 15 pairs (median 282 vs 308 us).
    if (concurrent_) return StealConcurrent();
    const size_t first = head_.load(std::memory_order_seq_cst);
    const size_t last = end_.load(std::memory_order_relaxed);
    if (first >= last) {
      OOX_SCHED_COUNT(RangeRejectedEmpty);
      return nullptr;
    }
    if (last - first <= grain_ && !nested_seen_) {
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
    // A nested owner retains its running callback, so its final indivisible
    // pending tail can move as one whole task without retaining another half.
    const size_t middle = last - first <= grain_ ? first : first + (last - first) / 2;
    try {
      state_.CheckDemand(true);
      auto *child = NewSmallObject<Node>(region_, function_, LoopRange{middle, last, grain_},
          State(state_, partitioning::Split{}), 0, branch_.Parent());
      // Construction happens on the thief, but completion notification and
      // Auto's stolen-peer feedback must retain the actual donating owner.
      child->SetPublishingThreadId(owner_);
      OOX_SCHED_EVENT("split", child->trace_id, middle, last, trace_parent_);
      // The slot tag serializes this update and Close joins the last thief.
      branch_.SetParent(child);
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
      size_t stop;
      if (concurrent_) {
        auto bounds = concurrent_->bounds.load(std::memory_order_relaxed);
        for (;;) {
          const size_t last = begin_ + uint32_t(bounds);
          if (cursor >= last) return;
          stop = cursor + std::min(quantum, last - cursor);
          const uint64_t reserved = (uint64_t(stop - begin_) << 32) | uint32_t(bounds);
          if (concurrent_->bounds.compare_exchange_weak(bounds, reserved)) break;
        }
      } else {
        const size_t last = end_.load(std::memory_order_acquire);
        if (cursor >= last) break;
        const size_t next = cursor + std::min(quantum, last - cursor);
        // SC orders this reservation against the slot tag: a later thief sees
        // at least next; an earlier thief is joined and its shorter end reloaded.
        head_.store(next, std::memory_order_seq_cst);
        WaitForStealer();
        stop = std::min(next, end_.load(std::memory_order_acquire));
      }
      // A terminal-tail thief may have taken all work before this reservation.
      if (stop <= cursor) break;
      {
        Batch batch(active_batch_, cursor, stop);
        OOX_SCHED_SCOPE("chunk", ::oox::scheduler_trace::NextId(), cursor, stop);
        // Keep the cursor local; nested entry only needs the next boundary.
        for (; cursor < batch.end; ++cursor) {
          batch.next = cursor + 1;
          std::invoke(*function_, cursor);
        }
      }
      ++chunks;
      quantum += std::min(quantum, limit - quantum);
    }
  }

private:
  Task *StealConcurrent() noexcept {
    if (region_.IsCancelled()) return nullptr;
    if (state_.unpublished->load(std::memory_order_acquire)) {
      OOX_SCHED_COUNT(RangeRejectedPublishing);
      return nullptr;
    }
    if (region_.pool.HasResidentCommand()) {
      OOX_SCHED_COUNT(RangeRejectedCommand);
      return nullptr;
    }
    auto bounds = concurrent_->bounds.load(std::memory_order_relaxed);
    for (;;) {
      const uint32_t head = bounds >> 32, end = uint32_t(bounds);
      if (head >= end) {
        OOX_SCHED_COUNT(RangeRejectedEmpty);
        return nullptr;
      }
      if (end - head <= grain_ && !nested_seen_.load()) {
        OOX_SCHED_COUNT(RangeRejectedGrain);
        return nullptr;
      }
      const uint32_t tail = end - head <= grain_ ? head : head + (end - head) / 2;
      const uint64_t desired = (uint64_t(head) << 32) | tail;
      if (!concurrent_->bounds.compare_exchange_weak(bounds, desired)) continue;
      try {
        State state = state_;
        state.CheckDemand(true);
        auto *child = NewSmallObject<Node>(region_, function_, LoopRange{begin_ + tail, begin_ + end, grain_},
            State(state, partitioning::Split{}), 0, nullptr);
        child->SetPublishingThreadId(owner_);
        // No owner-side branch belongs to this independent stolen subtree.
        PeerJoin<Node>::Release(child);
        OOX_SCHED_EVENT("split", child->trace_id, begin_ + tail, begin_ + end, trace_parent_);
        if (region_.metrics) region_.metrics->donated_ranges.fetch_add(1, std::memory_order_relaxed);
#ifdef OOX_TEST_CONCURRENT_STEAL
        OOX_TEST_CONCURRENT_STEAL();
#endif
        return child;
      } catch (...) {
        region_.Fail(std::current_exception());
        return nullptr;
      }
    }
  }

  ConcurrentRangeSlot *concurrent_ = nullptr;
  struct Batch {
    Batch(Batch *&slot, size_t begin, size_t stop) : active(slot), next(begin), end(stop) {
      active = this;
    }
    ~Batch() { active = nullptr; }

    Batch *&active;
    size_t next, end;
    bool released = false;
  };
  bool OnNestedEntry() noexcept final {
    if (!active_batch_ || active_batch_->released) return false;
    // Only the owner changes its active batch. Released
    // callbacks below it are unstarted, and the running callback stays private.
    const size_t next = active_batch_->next;
    if (concurrent_) {
      auto bounds = concurrent_->bounds.load(std::memory_order_relaxed);
      while (!concurrent_->bounds.compare_exchange_weak(bounds,
          (uint64_t(next - begin_) << 32) | uint32_t(bounds))) {}
    } else head_.store(next, std::memory_order_seq_cst);
    // Finish the running callback, then reserve again from the live tail.
    active_batch_->end = next;
    active_batch_->released = true;
    nested_seen_ = true;
    return true;
  }

  Region &region_;
  F *function_;
  State state_;
  Branch<Node, true> &branch_;
  scheduler_sync::atomic<size_t> head_, end_;
  const size_t begin_, grain_;
  const scheduler_sync::thread::id owner_;
  Batch *active_batch_ = nullptr;
  // Monotonic: compact-slot thieves can check this flag concurrently.
  scheduler_sync::atomic<bool> nested_seen_{false};
#ifdef OOX_SCHEDULER_TRACE
  const uint64_t trace_parent_ = ::oox::scheduler_trace::Current();
#endif
};

template <typename F, typename State, typename Run>
void RunCooperativeRange(Region &region, F *function, LoopRange &range,
                         State &state, Branch<Work<F, State>, true> &branch,
                         Run &run) {
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
    const size_t next = range.begin + std::min(quantum, range.end - range.begin);
    run(LoopRange{range.begin, next, range.grain});
    range.begin = next;
    // Back off checks on regular work, but bound the unchecked tail by
    // Auto's depth budget so late skew can still supply idle helpers.
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
      RunCooperativeRange(region, function, range, state, branch, run);
    }
  }
}

} // namespace oox::detail::eigen_pool::partitioner_detail
