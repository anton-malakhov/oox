// Copyright (c) 2005-2021 Intel Corporation
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "nonblocking_thread_pool.h"
#include <algorithm>
#include <exception>
#include <limits>
#include "partitioning.h"

namespace oox::detail::eigen_pool {
struct AutoPartitioner {
  auto MakeState(ThreadPool &pool) const {
    return partitioning::AutoPartitionState(pool.NumThreads());
  }
};

struct SimplePartitioner {
  auto MakeState(ThreadPool &) const {
    return partitioning::SimplePartitionState{};
  }
};

struct StaticPartitioner {
  auto MakeState(ThreadPool &pool) const {
    const auto current = pool.CurrentThreadId();
    return partitioning::StaticPartitionState(
        pool.NumThreads(), current == std::size_t(-1) ? 0 : current);
  }
};

class AffinityPartitioner {
public:
  auto MakeState(ThreadPool &pool) {
    const auto workers = pool.NumThreads();
    const auto current = pool.CurrentThreadId();
    const auto first = current == std::size_t(-1) ? 0 : current;
    std::unique_lock<std::mutex> lock(mutex_);
    if (history_ && history_.use_count() != 1) {
      lock.unlock();
      return partitioning::AffinityPartitionState(
          std::make_shared<partitioning::AffinityHistory>(workers), first);
    }
    if (!history_ || pool_ != &pool || history_->workers != workers) {
      history_ = std::make_shared<partitioning::AffinityHistory>(workers);
      pool_ = &pool;
    }
    return partitioning::AffinityPartitionState(history_, first);
  }

private:
  std::mutex mutex_;
  ThreadPool *pool_ = nullptr;
  std::shared_ptr<partitioning::AffinityHistory> history_;
};

namespace partitioner_detail {

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
  std::mutex error_mutex;
  std::exception_ptr exception;
};

// Closing prevents queued tasks from accessing the callback or metrics after
// return. Running callbacks finish before CloseAndWait releases the caller.
class WorkLease {
public:
  explicit WorkLease(Region &region) noexcept
      : region_(region), acquired_(region.BeginWork()) {}
  WorkLease(const WorkLease &) = delete;
  WorkLease &operator=(const WorkLease &) = delete;
  ~WorkLease() {
    if (acquired_)
      region_.EndWork();
  }
  explicit operator bool() const noexcept { return acquired_; }

private:
  Region &region_;
  const bool acquired_;
};

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

// The two subtree references and the task-execution bit share one counter.
// The embedded node can outlive execution, or finish before execution returns.
template <typename Owner, bool Enabled = true> struct PeerJoin {
  explicit PeerJoin(PeerJoin *parent) noexcept : parent(parent) {}
  static constexpr uintptr_t root_bit = 1;
  static PeerJoin *Root(Region &region) noexcept {
    static_assert(alignof(Region) > root_bit && alignof(PeerJoin) > root_bit);
    // The final branch releases the tree's single Region reference.
    return reinterpret_cast<PeerJoin *>(
        reinterpret_cast<uintptr_t>(&region) | root_bit);
  }
  static bool IsRoot(PeerJoin *node) noexcept {
    return reinterpret_cast<uintptr_t>(node) & root_bit;
  }
  static bool PeerStolen(PeerJoin *node) noexcept {
    return node && !IsRoot(node) && node->stolen.load(std::memory_order_relaxed);
  }
  bool HasPeer() const noexcept {
    return (references.load(std::memory_order_acquire) & branch_mask) == 2;
  }
  static void Release(PeerJoin *node) noexcept {
    if (node && IsRoot(node)) {
      // The root Process returns this branch before its caller starts waiting.
      reinterpret_cast<Region *>(reinterpret_cast<uintptr_t>(node) & ~root_bit)
          ->TaskComplete(false);
      return;
    }
    while (node) {
      // Capture metadata before the RMW can allow FinishExecution to free us.
      PeerJoin *parent = node->parent;
      std::thread::id caller;
      if constexpr (requires(const Owner &owner) { owner.PublishingThreadId(); }) {
        if (IsRoot(parent))
          caller = static_cast<const Owner *>(node)->PublishingThreadId();
      }
      const unsigned previous =
          node->references.fetch_sub(1, std::memory_order_acq_rel);
      if ((previous & branch_mask) != 1)
        return;
      if (!(previous & executing))
        DeleteSmallObject(static_cast<Owner *>(node));
      if (IsRoot(parent)) {
        // The first task was published by the synchronous caller. That thread
        // cannot be parked while executing this completion; other threads notify.
        auto *region = reinterpret_cast<Region *>(
            reinterpret_cast<uintptr_t>(parent) & ~root_bit);
        region->TaskComplete(caller == std::thread::id{} ||
                             caller != std::this_thread::get_id());
        return;
      }
      node = parent;
    }
  }
  void FinishExecution() noexcept {
    if (references.fetch_sub(executing, std::memory_order_acq_rel) == executing)
      DeleteSmallObject(static_cast<Owner *>(this));
  }

  static constexpr unsigned branch_mask = 3;
  static constexpr unsigned executing = 4;
  PeerJoin *const parent;
  std::atomic<unsigned> references{executing | 2};
  std::atomic<bool> stolen{false};
};

// Non-adaptive policies already join through Region's task count.
template <typename Owner> struct PeerJoin<Owner, false> {
  explicit constexpr PeerJoin(PeerJoin *) noexcept {}
  static constexpr void Release(PeerJoin *) noexcept {}
  void FinishExecution() noexcept { DeleteSmallObject(static_cast<Owner *>(this)); }
};

template <typename Owner, bool Enabled> struct Branch {
  using Join = PeerJoin<Owner, Enabled>;
  explicit Branch(Join *parent) noexcept : parent_(parent) {}
  ~Branch() { Join::Release(parent_); }
  Join *Parent() const noexcept { return parent_; }
  void SetParent(Join *parent) noexcept { parent_ = parent; }

private:
  Join *parent_;
};

template <typename Owner> struct Branch<Owner, false> {
  using Join = PeerJoin<Owner, false>;
  explicit constexpr Branch(Join *) noexcept {}
  constexpr Join *Parent() const noexcept { return nullptr; }
  constexpr void SetParent(Join *) noexcept {}
};

template <bool Enabled> struct TaskOwner {};
template <> struct TaskOwner<true> { std::thread::id owner; };
template <bool Enabled> struct TaskPlacement {};
template <> struct TaskPlacement<true> {
  size_t hint = partitioning::no_affinity;
};

template <typename State>
struct TaskContext : TaskOwner<State::adaptive>, TaskPlacement<State::uses_affinity> {
  TaskContext() noexcept {
    if constexpr (State::adaptive)
      this->owner = std::this_thread::get_id();
  }
  void SetHint(const State &state) noexcept {
    if constexpr (State::uses_affinity)
      this->hint = state.Hint();
  }
};

template <typename F, typename State>
void Process(Region &, F *, LoopRange, State, PeerJoin<Work<F, State>, State::adaptive> *,
             TaskContext<State>) noexcept;

template <typename F, typename State>
class Work final : public Task, public PeerJoin<Work<F, State>, State::adaptive>,
                   public SmallObjectAllocated<Work<F, State>> {
  using Join = PeerJoin<Work, State::adaptive>;
public:
  Work(Region &region, F *function, LoopRange range, State state,
       unsigned depth, Join *parent) noexcept
      : Join(parent), region_(region), function_(function), range_(range),
        state_(state) {
    state_.AlignDepth(depth);
    context_.SetHint(state_);
    if constexpr (!State::adaptive)
      region_.AddTask();
    if (region_.metrics)
      region_.metrics->range_tasks.fetch_add(1, std::memory_order_relaxed);
  }
  ~Work() override = default;
  std::thread::id PublishingThreadId() const noexcept requires(State::adaptive) {
    return context_.owner;
  }
  void operator()() final {
    Process(region_, function_, range_, state_, static_cast<Join *>(this),
            context_);
    Complete();
  }
  void Discard() noexcept final {
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
  auto *child = NewSmallObject<Work<F, State>>(
      region, function, range, State(state, split), depth, branch.Parent());
  branch.SetParent(child);
  child->Publish();
}

// All policies share publication, completion and callback lifetime. Adaptive
// policies additionally use the current sibling's stolen flag and range pool.
template <typename F, typename State>
void Process(Region &region, F *function, LoopRange range,
             State state, PeerJoin<Work<F, State>, State::adaptive> *parent,
             TaskContext<State> context) noexcept {
  using Join = PeerJoin<Work<F, State>, State::adaptive>;
  // Release callback/metrics access before the branch can complete its tree.
  Branch<Work<F, State>, State::adaptive> branch{parent};
  WorkLease work(region);
  if (!work)
    return;
  size_t chunks = 0;
  try {
    if constexpr (State::records_affinity) {
      if (context.hint != partitioning::no_affinity)
        state.NoteExecution(region.pool.CurrentThreadId(), context.hint);
    }
    if constexpr (State::adaptive) {
      if (state.NeedsStealCheck()) {
        const bool stolen = std::this_thread::get_id() != context.owner;
        const bool peer_active =
            stolen && parent && !Join::IsRoot(parent) && parent->HasPeer();
        if (state.CheckStolen(stolen, peer_active)) {
          parent->stolen.store(true, std::memory_order_relaxed);
          if (region.metrics)
            region.metrics->stolen_tasks.fetch_add(1, std::memory_order_relaxed);
        }
      }
    }
    while (range.IsDivisible() && state.IsDivisible() &&
           !region.IsCancelled()) {
      const auto split = state.GetSplit();
      LoopRange right(range, split);
      OfferWork(region, function, right, state, branch, 0, split);
      if (region.metrics)
        region.metrics->initial_tasks.fetch_add(1, std::memory_order_relaxed);
    }
    if (region.metrics)
      region.metrics->owner_ranges.fetch_add(1, std::memory_order_relaxed);
    const auto run = [&](const LoopRange &chunk) {
      for (size_t i = chunk.begin; i < chunk.end; ++i)
        std::invoke(*function, i);
      ++chunks;
    };
    if (!region.IsCancelled()) {
      if constexpr (!State::adaptive) {
        run(range);
      } else if (!range.IsDivisible() || !state.max_depth) {
        run(range);
      } else {
        partitioning::RangePool<LoopRange> ranges(range);
        while (!ranges.Empty() && !region.IsCancelled()) {
          ranges.Fill(state.max_depth);
          if (state.CheckDemand(Join::PeerStolen(branch.Parent()))) {
            if (ranges.Size() > 1) {
              OfferWork(region, function, ranges.Front(), state, branch,
                        ranges.FrontDepth(), partitioning::Split{});
              ranges.PopFront();
              if (region.metrics)
                region.metrics->donated_ranges.fetch_add(
                    1, std::memory_order_relaxed);
              continue;
            }
            if (ranges.IsDivisible(state.max_depth))
              continue;
          }
          run(ranges.Back());
          ranges.PopBack();
        }
      }
    }
  } catch (...) {
    region.Fail(std::current_exception());
  }
  if (region.metrics)
    region.metrics->private_chunks.fetch_add(chunks, std::memory_order_relaxed);
}

} // namespace partitioner_detail

// The same synchronous executor and cancellation contract serve all policies.
template <typename F, typename Partitioner>
  requires requires(Partitioner &part, ThreadPool &pool) { part.MakeState(pool); }
void ParallelFor(ThreadPool &pool, size_t begin, size_t end,
                 F &&function, Partitioner &&partitioner, size_t grain = 1,
                 partitioner_detail::Metrics *metrics = nullptr) {
  if (begin >= end || pool.IsCancelled())
    return;
  grain = std::max<size_t>(grain, 1);
  if (pool.NumThreads() == 1 || end - begin <= grain) {
    if (metrics) {
      metrics->owner_ranges.fetch_add(1, std::memory_order_relaxed);
      metrics->private_chunks.fetch_add(1, std::memory_order_relaxed);
    }
    for (size_t i = begin; i < end && !pool.IsCancelled(); ++i)
      std::invoke(function, i);
    return;
  }
  const auto release = [](partitioner_detail::Region *region) {
    region->TaskComplete();
  };
  std::unique_ptr<partitioner_detail::Region, decltype(release)> region(
      NewSmallObject<partitioner_detail::Region>(pool, metrics), release);
  auto state = partitioner.MakeState(pool);
  using RootWork =
      partitioner_detail::Work<std::remove_reference_t<F>, decltype(state)>;
  using RootJoin = partitioner_detail::PeerJoin<RootWork, decltype(state)::adaptive>;
  RootJoin *parent = nullptr;
  if constexpr (decltype(state)::adaptive) {
    region->AddTask();
    parent = RootJoin::Root(*region);
  }
  partitioner_detail::Process<std::remove_reference_t<F>, decltype(state)>(
      *region, std::addressof(function), {begin, end, grain}, state, parent,
      partitioner_detail::TaskContext<decltype(state)>{});
  pool.Wait([&] { return region->IsComplete(); });
  // Main's pool may retain cancelled queued tasks until destruction. They
  // keep the region alive, but closing forbids further callback/metrics access.
  region->CloseAndWait();
  region->Rethrow();
}
template <typename F>
void ParallelFor(ThreadPool &pool, size_t begin, size_t end, F &&function,
                 size_t grain = 1, partitioner_detail::Metrics *metrics = nullptr) {
  ParallelFor(pool, begin, end, std::forward<F>(function), AutoPartitioner{},
              grain, metrics);
}
} // namespace oox::detail::eigen_pool
