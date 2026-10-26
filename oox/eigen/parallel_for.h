// Copyright (c) 2005-2021 Intel Corporation
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "lazy_range_executor.h"
#include <algorithm>

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
    std::unique_lock<scheduler_sync::mutex> lock(mutex_);
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
  scheduler_sync::mutex mutex_;
  ThreadPool *pool_ = nullptr;
  std::shared_ptr<partitioning::AffinityHistory> history_;
};

namespace partitioner_detail {

// Execute Auto/Affinity's private range buffer; the caller owns admission,
// exception handling and final metrics.
template <typename F, typename State, typename Run>
void RunAdaptiveRange(Region &region, F *function, const LoopRange &range,
                      State &state, Branch<Work<F, State>, true> &branch, Run &run) {
  using Join = PeerJoin<Work<F, State>>;
  partitioning::RangePool<LoopRange> ranges(range);
  while (!ranges.Empty() && !region.IsCancelled()) {
    ranges.Fill(state.max_depth);
    bool demand = Join::PeerStolen(branch.Parent());
    if (!demand && region.pool.UsesResidentBusyWait() && region.pool.HasWaitingHelper()) {
      auto *parent = branch.Parent();
      demand = parent && !Join::IsRoot(parent) && !parent->HasPeer();
    }
    if (state.CheckDemand(demand)) {
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

// All policies share publication, completion and callback lifetime. Adaptive
// policies additionally use the current sibling's stolen flag and range pool.
template <typename F, typename State, bool CheckSteal>
void Process(Region &region, F *function, LoopRange range,
             State state, PeerJoin<Work<F, State>, State::adaptive> *parent,
             TaskContext<State> context, const WorkLease *group_admission) noexcept {
  OOX_SCHED_WORKER(region.pool.CurrentThreadId());
  OOX_SCHED_SCOPE("range", ::oox::scheduler_trace::NextId(), range.begin, range.end);
  using Join = PeerJoin<Work<F, State>, State::adaptive>;
  // Release callback/metrics access before the branch can complete its tree.
  Branch<Work<F, State>, State::adaptive> branch{parent};
  // Only captured commands may borrow the launch's joined admission. Stolen
  // tasks acquire independently and reject callbacks after closure.
  WorkLease work(region, group_admission);
  if (!work)
    return;
  size_t chunks = 0;
  try {
    if constexpr (State::records_affinity) {
      if (context.hint != partitioning::no_affinity)
        state.NoteExecution(region.pool.CurrentThreadId(), context.hint);
    }
    if constexpr (State::adaptive && CheckSteal) {
      if (state.NeedsStealCheck()) {
        const bool stolen = scheduler_sync::this_thread::get_id() != context.owner;
        const bool peer_active =
            stolen && parent && !Join::IsRoot(parent) && parent->HasPeer();
        if (state.CheckStolen(stolen, peer_active)) {
          parent->stolen.store(true, std::memory_order_relaxed);
          if (region.metrics)
            region.metrics->stolen_tasks.fetch_add(1, std::memory_order_relaxed);
        }
      }
    }
    if constexpr (!demand_driven<State>) {
      while (range.IsDivisible() && state.IsDivisible() &&
             !region.IsCancelled()) {
        const auto split = state.GetSplit();
        LoopRange right(range, split);
        OfferWork(region, function, right, state, branch, 0, split);
        if (region.metrics)
          region.metrics->initial_tasks.fetch_add(1, std::memory_order_relaxed);
      }
    }
    if (region.metrics)
      region.metrics->owner_ranges.fetch_add(1, std::memory_order_relaxed);
    const auto run = [&](const LoopRange &chunk) {
      OOX_SCHED_SCOPE("chunk", ::oox::scheduler_trace::NextId(), chunk.begin, chunk.end);
      for (size_t i = chunk.begin; i < chunk.end; ++i)
        std::invoke(*function, i);
      ++chunks;
    };
    if (!region.IsCancelled()) {
      if constexpr (demand_driven<State>) {
        RunLazyRange(region, function, range, state, branch, chunks, run);
      } else if constexpr (!State::adaptive) {
        run(range);
      } else if (!range.IsDivisible() || !state.max_depth) {
        run(range);
      } else {
        RunAdaptiveRange(region, function, range, state, branch, run);
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
    OOX_SCHED_WORKER(pool.CurrentThreadId());
    OOX_SCHED_SCOPE("chunk", ::oox::scheduler_trace::NextId(), begin, end);
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
