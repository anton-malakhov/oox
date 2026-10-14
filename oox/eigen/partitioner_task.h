// Copyright (c) 2005-2021 Intel Corporation
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "partitioner_completion.h"
#include "partitioning.h"

namespace oox::detail::eigen_pool::partitioner_detail {

template <typename State>
inline constexpr bool demand_driven =
    std::is_same_v<State, partitioning::LazyAutoPartitionState>;

struct LoopRange {
  size_t begin, end, grain;
  constexpr LoopRange(size_t first, size_t last, size_t minimum) noexcept
      : begin(first), end(last), grain(minimum) {}
  constexpr LoopRange(LoopRange &source, partitioning::Split) noexcept
      : begin(source.begin + (source.end - source.begin) / 2), end(source.end),
        grain(source.grain) {
    source.end = begin;
  }
  constexpr LoopRange(LoopRange &source, partitioning::ProportionalSplit split) noexcept
      : begin(0), end(source.end), grain(source.grain) {
    const size_t size = source.end - source.begin;
    const size_t total = split.left + split.right;
    // Exact rounded proportion; avoid size*right overflow and upstream's float
    // precision loss for large index ranges. Split weights are worker counts.
    const size_t right = (size / total) * split.right +
        ((size % total) * split.right + total / 2) / total;
    begin = source.end = end - right;
  }
  constexpr bool IsDivisible() const noexcept { return end - begin > grain; }
};

template <typename F, typename State> class Work;

template <bool Enabled> struct TaskOwner {};
template <> struct TaskOwner<true> { std::thread::id owner; };
template <bool Enabled> struct TaskPlacement {};
template <> struct TaskPlacement<true> {
  size_t hint = partitioning::no_affinity;
};

template <bool Enabled> struct DonationPin {};
template <> struct DonationPin<true> { bool pending = false; };

template <typename State>
struct TaskContext : TaskOwner<State::adaptive>, TaskPlacement<State::uses_affinity> {
  // Captured roots use Process<..., false>, which never reads owner.
  struct Ownerless {};
  explicit TaskContext(Ownerless) noexcept {}
  TaskContext() noexcept {
    if constexpr (State::adaptive)
      this->owner = std::this_thread::get_id();
  }
  void SetHint(const State &state) noexcept {
    if constexpr (State::uses_affinity)
      this->hint = state.Hint();
  }
};

template <typename F, typename State, bool CheckSteal = true>
void Process(Region &, F *, LoopRange, State, PeerJoin<Work<F, State>, State::adaptive> *,
             TaskContext<State>, const WorkLease * = nullptr) noexcept;

template <typename F, typename State>
class Work final : public Task, public PeerJoin<Work<F, State>, State::adaptive>,
                   private DonationPin<demand_driven<State>>,
                   public SmallObjectAllocated<Work<F, State>> {
  using Join = PeerJoin<Work, State::adaptive>;
public:
  Work(Region &region, F *function, LoopRange range, State state,
       unsigned depth, Join *parent, bool donation = false) noexcept
      : Join(ParentForChild(region, parent)), region_(region), function_(function), range_(range),
        state_(state) {
    OOX_SCHED_EVENT("range_spec", this->trace_id, range.begin, range.end);
    state_.AlignDepth(depth);
    context_.SetHint(state_);
    if constexpr (demand_driven<State>) this->pending = donation;
    if constexpr (!State::adaptive)
      region_.AddTask();
    if (region_.metrics)
      region_.metrics->range_tasks.fetch_add(1, std::memory_order_relaxed);
  }
  ~Work() override = default;
  std::thread::id PublishingThreadId() const noexcept requires(State::adaptive) {
    return context_.owner;
  }
  void SetPublishingThreadId(std::thread::id owner) noexcept requires(State::adaptive) {
    context_.owner = owner;
  }
  void operator()() final {
    ReleaseDonation();
#ifdef OOX_EIGEN_TEST_LAZY_TASK_ACQUIRED
    if constexpr (demand_driven<State>) OOX_EIGEN_TEST_LAZY_TASK_ACQUIRED();
#endif
    Process(region_, function_, range_, state_, static_cast<Join *>(this),
            context_);
    Complete();
  }
  void Discard() noexcept final {
    ReleaseDonation();
    Join::Release(this);
    Complete();
  }
  void Publish() {
    if constexpr (!State::uses_affinity) {
      region_.pool.Schedule(this);
    } else if constexpr (!State::records_affinity) {
      region_.pool.ScheduleWithAffinity(this, context_.hint);
    } else {
      if (context_.hint == partitioning::no_affinity)
        region_.pool.Schedule(this);
      else
        region_.pool.ScheduleWithAffinity(this, context_.hint);
    }
  }

private:
  static Join *ParentForChild(Region &region, Join *parent) noexcept {
    if constexpr (demand_driven<State>) {
      if (parent == nullptr) {
        region.AddTask();
        return Join::Root(region);
      }
    }
    return parent;
  }
  void ReleaseDonation() noexcept {
    if constexpr (demand_driven<State>) {
      if (std::exchange(this->pending, false)) region_.ReleaseDonation();
    }
  }
  void Complete() noexcept {
    // Adaptive Process/Discard may already have released the Region.
    if constexpr (!State::adaptive)
      region_.TaskComplete();
    this->FinishExecution();
  }

  Region &region_;
  F *function_;
  LoopRange range_;
  [[no_unique_address]] State state_;
  [[no_unique_address]] TaskContext<State> context_;
};

template <typename F, typename State, typename Split>
void OfferWork(Region &region, F *function, LoopRange range,
               State &state, Branch<Work<F, State>, State::adaptive> &branch, unsigned depth, Split split) {
  // No throwing operation separates ownership transfer from publication.
  // Publish consumes the task even if queue publication fails or is cancelled.
  Work<F, State> *child;
  try {
    child = NewSmallObject<Work<F, State>>(
        region, function, range, State(state, split), depth, branch.Parent(),
        demand_driven<State>);
  } catch (...) {
    if constexpr (demand_driven<State>) region.ReleaseDonation();
    throw;
  }
  branch.SetParent(child);
  OOX_SCHED_EVENT("split", child->trace_id, range.begin, range.end, ::oox::scheduler_trace::Current());
  child->Publish();
}

} // namespace oox::detail::eigen_pool::partitioner_detail
