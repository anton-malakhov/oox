// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "rapid_cohort_launch.h"

namespace oox::detail::eigen_pool::rapid {
namespace batch_detail {

#if defined(OOX_RAPID_EXPERIMENT_NESTED_LAZY) && OOX_RAPID_EXPERIMENT_NESTED_LAZY
// Research-only opt-in. Ordinary Auto remains the default fallback policy.
struct NestedLazyPartitioner {
  auto MakeState(ThreadPool &pool) const {
    return partitioning::LazyAutoPartitionState(pool.NumThreads());
  }
};
#endif

// Both halves are indivisible under the requested grain. There can be no
// descendant tasks borrowing this descriptor, so joining the one captured
// helper suffices even on cancellation; no heap completion tree is needed.
template <typename F>
bool TryTerminalPair(RapidStartGroup group, size_t begin, size_t end, F &function,
                     size_t grain, partitioner_detail::Metrics *metrics, size_t *activated) {
  auto &pool = group.state->Pool();
  partitioner_detail::LoopRange left(begin, end, grain);
  const partitioner_detail::LoopRange right(left, partitioning::Split{});
  assert(!left.IsDivisible() && !right.IsDivisible());
  auto body = [&](size_t slot, size_t) {
    const auto &range = slot == 0 ? left : right;
    pool.RunInlineWork([&] {
      OOX_SCHED_WORKER(pool.CurrentThreadId());
      OOX_SCHED_SCOPE("chunk", ::oox::scheduler_trace::NextId(), range.begin, range.end);
      if (metrics) metrics->owner_ranges.fetch_add(1, std::memory_order_relaxed);
      for (size_t i = range.begin; i < range.end; ++i) std::invoke(function, i);
      if (metrics) metrics->private_chunks.fetch_add(1, std::memory_order_relaxed);
    });
  };
  ResidentRegion<decltype(body)> region(*group.state, body, 0, 2, 2, 1);
  unsigned worker = 0;
  if (!pool.ClaimResidentWorkers(group.domain, &worker, 1)) return false;
#ifdef OOX_EIGEN_TEST_RAPID_CLAIMED
  OOX_EIGEN_TEST_RAPID_CLAIMED(1);
#endif
  pool.PublishResident(region, region.CompletionCounter(), worker, 1);
  region.RunCaller();
  pool.HelpResidentUntil(region);
  region.RethrowAfterJoin();
  if (activated) *activated = 1;
  return true;
}

template <typename F, size_t Participants = 64>
class Launch final : public ResidentTask {
  static_assert(Participants >= 2 && Participants <= 64);
public:
  using State = partitioning::LazyAutoPartitionState;
  using Work = partitioner_detail::Work<F, State>;
  using Join = partitioner_detail::PeerJoin<Work>;
  using Range = partitioner_detail::LoopRange;
  struct Seed {
    Range range;
    State state;
    Join *parent;
  };

  Launch(RapidStartGroup group, partitioner_detail::Region &region, F *function) noexcept
      : group_(group), region_(region), function_(function) {}

  // Only captured owners receive initial ranges. Descendants are created by
  // the demand-driven executor after another worker becomes available.
  void Prepare(Range range, size_t helpers) noexcept {
    assert(seed_count_ == 0 && helpers < Participants);
    remaining_.store(helpers, std::memory_order_relaxed);
    root_range_ = range;
    root_state_ = State(region_.pool.NumThreads());
    root_state_.unpublished = &region_.RapidPublication();
    seed_count_ = std::min(helpers + 1, std::max(size_t{1}, range.end - range.begin));
    // The factory joins captured commands through ResidentCohort. Region
    // tracks only trees that escape after the first child allocation.
  }

  void Run(size_t slot) noexcept final {
    if (slot >= seed_count_) return;
    ScopedResidentState context(*group_.state);
    region_.pool.RunInlineWork([&] {
#ifdef OOX_EIGEN_TEST_RAPID_DIRECT_SEED
      OOX_EIGEN_TEST_RAPID_DIRECT_SEED(slot, slot);
#endif
      const Seed seed = GetSeed(slot);
      partitioner_detail::Process<F, State, false>(region_, function_, seed.range,
          seed.state, seed.parent, partitioner_detail::TaskContext<State>{
              typename partitioner_detail::TaskContext<State>::Ownerless{}}, admission_);
    });
  }
  scheduler_sync::atomic<size_t> &CompletionCounter() noexcept { return remaining_; }
  size_t SeedCount() const noexcept { return seed_count_; }
  Seed GetSeed(size_t index) const noexcept {
    assert(index < seed_count_);
    Seed seed{root_range_, root_state_, nullptr};
    const size_t size = root_range_.end - root_range_.begin;
    const size_t quotient = size / seed_count_, remainder = size % seed_count_;
    seed.range.begin += index * quotient + std::min(index, remainder);
    seed.range.end = seed.range.begin + quotient + (index < remainder ? 1 : 0);
    seed.state.divisor = root_state_.divisor / seed_count_ +
        (index < root_state_.divisor % seed_count_ ? 1 : 0);
    return seed;
  }
  void SetAdmission(const partitioner_detail::WorkLease *admission) noexcept { admission_ = admission; }
  void Publish(ResidentCohort &cohort) noexcept {
    region_.RapidPublication().store(true, std::memory_order_relaxed);
    cohort.Publish(*this);
    region_.RapidPublication().store(false, std::memory_order_release);
  }
private:
  RapidStartGroup group_;
  partitioner_detail::Region &region_;
  F *function_;
  const partitioner_detail::WorkLease *admission_ = nullptr;
  Range root_range_{0, 0, 1};
  State root_state_{0};
  size_t seed_count_ = 0;
  scheduler_sync::atomic<size_t> remaining_{0};
};

template <size_t Participants, typename F>
void Execute(RapidStartGroup group, size_t begin, size_t end, F &function,
             size_t grain, partitioner_detail::Metrics *metrics,
             size_t *activated, size_t capacity) {
  auto &pool = group.state->Pool();
  const auto release_region = [](partitioner_detail::Region *region) { region->TaskComplete(); };
  OOX_SCHED_EVENT("rapid_begin", ::oox::scheduler_trace::Current());
  std::unique_ptr<partitioner_detail::Region, decltype(release_region)> region(
      NewSmallObject<partitioner_detail::Region>(pool, metrics, true), release_region);
  Launch<F, Participants> launch(group, *region, std::addressof(function));
  const size_t helpers = detail::RunCohort<Participants>(group, *region, launch, capacity,
      [&](const unsigned *, size_t count) { launch.Prepare({begin, end, grain}, count); });
  // The cohort join also joins initial owners' in-flight steal constructors;
  // no first-child promotion can appear after the Region ready check below.
  pool.Wait([&] { return region->IsComplete(); });
  OOX_SCHED_EVENT("rapid_tree_joined", ::oox::scheduler_trace::Current());
  // Cancellation may stop Wait with descendants still retained. Completed
  // trees already released callback/metrics admission before their branches.
  if (!region->IsComplete()) region->CloseAndWait();
  OOX_SCHED_EVENT("rapid_closed", ::oox::scheduler_trace::Current());
  region->Rethrow();
  if (activated) *activated = helpers;
  // With joined owners and no retained tree, Region has no callback users.
  // Surviving adaptive task cleanup touches its node, not the Region.
  if (region->IsComplete()) DeleteSmallObject(region.release());
}

} // namespace batch_detail

template <typename F>
void ParallelForAuto(RapidStartGroup group, size_t begin, size_t end,
                     F &&function, size_t grain = 1,
                     partitioner_detail::Metrics *metrics = nullptr,
                     size_t *activated = nullptr) {
  if (activated) *activated = 0;
  if (group.IsEmpty() || begin >= end) return;
  group.Validate();
  auto &pool = group.state->Pool();
  if (!pool.UsesResidentBusyWait())
    throw std::invalid_argument("Rapid auto requires a resident-capable pool");
  if (pool.IsCancelled()) return;
  grain = std::max(grain, size_t{1});
  // Snapshot before RunInlineWork establishes this entry's own ancestry.
  const bool nested = CurrentResidentState() || pool.IsExecutingTask();
  if (nested || pool.NumThreads() == 1 ||
      end - begin <= grain || !pool.HasResidentAvailableWorker(group.domain)) {
    OOX_SCHED_EVENT("rapid_fallback", ::oox::scheduler_trace::Current(),
        (CurrentResidentState() ? 1u : 0u) | (pool.IsExecutingTask() ? 2u : 0u) |
        (pool.NumThreads() == 1 ? 4u : 0u) | (end - begin <= grain ? 8u : 0u),
        pool.ResidentAvailableWorkers(group.domain));
    // Ordinary Auto executes its root directly on this caller. Preserve the
    // Rapid entry's ancestry even if helpers become available mid-callback.
    return pool.RunInlineWork([&] {
#if defined(OOX_RAPID_EXPERIMENT_NESTED_LAZY) && OOX_RAPID_EXPERIMENT_NESTED_LAZY
      if (nested) {
        oox::detail::eigen_pool::ParallelFor(
            pool, begin, end, std::forward<F>(function),
            batch_detail::NestedLazyPartitioner{}, grain, metrics);
        return;
      }
#endif
      oox::detail::eigen_pool::ParallelFor(
          pool, begin, end, std::forward<F>(function), grain, metrics);
    });
  }
  // Overflow-safe size <= 2*grain; preserve Auto's floor/ceil midpoint.
  if (end - begin - grain <= grain) {
    if (batch_detail::TryTerminalPair(group, begin, end, function, grain, metrics, activated)) return;
    return pool.RunInlineWork([&] {
      oox::detail::eigen_pool::ParallelFor(
          pool, begin, end, std::forward<F>(function), grain, metrics);
    });
  }
  const size_t capacity = std::min({size_t{63}, pool.NumThreads() - 1,
      pool.ResidentCapacity(group.domain), (end - begin - 1) / grain});
  // Eligibility may change after sizing; ClaimResidentWorkers is still bounded
  // by this capacity. No partially captured launch can overrun its frame.
  detail::WithCapacity(capacity, [&]<size_t N> {
    batch_detail::Execute<N>(group, begin, end, function, grain, metrics, activated, capacity);
  });
}

} // namespace oox::detail::eigen_pool::rapid
