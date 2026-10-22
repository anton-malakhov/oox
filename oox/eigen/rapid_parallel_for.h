// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "rapid_partitioning.h"
#include "rapid_start.h"

namespace oox::detail::eigen_pool::rapid {
namespace policy_detail {

template <typename F, typename State, size_t Capacity>
class Launch final : public ResidentTask {
  using Work = partitioner_detail::Work<F, State>;
  using Join = partitioner_detail::PeerJoin<Work, State::adaptive>;
public:
  Launch(RapidStartGroup group, partitioner_detail::Region &region, F &function,
         partitioner_detail::LoopRange range, State state) noexcept
      : group_(group), region_(region), function_(function), frontier_(range, std::move(state)) {}

  // Everything which can allocate (including Affinity history) precedes claim.
  // Once workers are claimed, preparation and publication must not throw.
  void Prepare(const unsigned *workers, size_t helpers) noexcept {
    frontier_.SplitTo(helpers + 1);
    frontier_.Assign(region_.pool.CurrentThreadId(), workers, helpers);
    remaining_.store(helpers, std::memory_order_relaxed);
    if constexpr (State::adaptive)
      for (size_t i = 0; i < frontier_.Size(); ++i) region_.AddTask();
  }
  void SetAdmission(const partitioner_detail::WorkLease *admission) noexcept { admission_ = admission; }
  std::atomic<size_t> &CompletionCounter() noexcept { return remaining_; }
  size_t SeedCount() const noexcept { return frontier_.Size(); }

  void Run(size_t slot) noexcept final {
    const size_t index = frontier_.ForSlot(slot);
    if (index == frontier_.Size()) return;
    ScopedResidentState resident(*group_.state);
    region_.pool.RunInlineWork([&] {
      const auto &seed = frontier_.At(index);
      partitioner_detail::TaskContext<State> context{
          typename partitioner_detail::TaskContext<State>::Ownerless{}};
      context.SetHint(seed.state);
      Join *parent = nullptr;
      if constexpr (State::adaptive) parent = Join::Root(region_);
      // The cohort joins the prefix. Each adaptive seed retains one Region
      // reference for its ordinary descendant tree, not another full budget.
      partitioner_detail::Process<F, State, false>(region_, &function_, seed.range,
          seed.state, parent, context, admission_);
    });
  }

private:
  RapidStartGroup group_;
  partitioner_detail::Region &region_;
  F &function_;
  SeedFrontier<State, Capacity> frontier_;
  const partitioner_detail::WorkLease *admission_ = nullptr;
  std::atomic<size_t> remaining_{0};
};

template <size_t Capacity, typename F, typename State>
void Execute(RapidStartGroup group, size_t begin, size_t end, F &function,
             State state, size_t grain, partitioner_detail::Metrics *metrics,
             size_t *activated, size_t capacity) {
  auto &pool = group.state->Pool();
  OOX_SCHED_EVENT("rapid_begin", ::oox::scheduler_trace::Current());
  const auto release = [](partitioner_detail::Region *region) { region->TaskComplete(); };
  std::unique_ptr<partitioner_detail::Region, decltype(release)> region(
      NewSmallObject<partitioner_detail::Region>(pool, metrics, true), release);
  Launch<F, State, Capacity> launch(group, *region, function, {begin, end, grain}, std::move(state));
  std::array<unsigned, Capacity - 1> workers;
  const size_t helpers = pool.ClaimResidentWorkers(group.domain, workers.data(), capacity);
  OOX_SCHED_EVENT("rapid_claimed", ::oox::scheduler_trace::Current(), helpers, capacity);
#ifdef OOX_EIGEN_TEST_RAPID_CLAIMED
  OOX_EIGEN_TEST_RAPID_CLAIMED(helpers);
#endif
  launch.Prepare(workers.data(), helpers);
  OOX_SCHED_EVENT("rapid_prepared", ::oox::scheduler_trace::Current(), helpers, launch.SeedCount());
  ResidentCohort cohort(pool, workers.data(), helpers, launch.CompletionCounter());
  {
    partitioner_detail::WorkLease admission(*region, partitioner_detail::WorkLease::JoinedOwners{});
    launch.SetAdmission(&admission);
    cohort.Publish(launch);
    OOX_SCHED_EVENT("rapid_published", ::oox::scheduler_trace::Current(), helpers);
    launch.Run(0);
    OOX_SCHED_EVENT("rapid_caller_finished", ::oox::scheduler_trace::Current());
    pool.HelpResidentUntil(cohort);
    cohort.Finalize();
    OOX_SCHED_EVENT("rapid_commands_joined", ::oox::scheduler_trace::Current());
    launch.SetAdmission(nullptr);
  }
  // Keep the frontier's Affinity history alive through callback admission close.
  // Queued cancelled children may outlive it, but cannot access borrowed slots.
  pool.Wait([&] { return region->IsComplete(); });
  OOX_SCHED_EVENT("rapid_tree_joined", ::oox::scheduler_trace::Current());
  region->CloseAndWait();
  OOX_SCHED_EVENT("rapid_closed", ::oox::scheduler_trace::Current());
  region->Rethrow();
  if (activated) *activated = helpers;
}

} // namespace policy_detail

// Policy is explicit: Auto means the ordinary Auto range-buffer algorithm.
// The separately named ParallelForAuto in rapid_auto.h keeps its optimized
// lazy-range semantics and is intentionally not redirected through this path.
template <typename F, typename Partitioner>
  requires policy_detail::SupportedPartitioner<Partitioner>
void ParallelFor(RapidStartGroup group, size_t begin, size_t end, F &&function,
                 Partitioner &&partitioner, size_t grain = 1,
                 partitioner_detail::Metrics *metrics = nullptr,
                 size_t *activated = nullptr) {
  if (activated) *activated = 0;
  if (group.IsEmpty() || begin >= end) return;
  group.Validate();
  auto &pool = group.state->Pool();
  if (!pool.UsesResidentBusyWait())
    throw std::invalid_argument("Rapid partitioners require a resident-capable pool");
  if (pool.IsCancelled()) return;
  grain = std::max(grain, size_t{1});
  if (current_resident_state || pool.IsExecutingTask() || pool.NumThreads() == 1 ||
      end - begin <= grain || !pool.HasResidentAvailableWorker(group.domain)) {
    OOX_SCHED_EVENT("rapid_fallback", ::oox::scheduler_trace::Current());
    return pool.RunInlineWork([&] {
      oox::detail::eigen_pool::ParallelFor(pool, begin, end, std::forward<F>(function),
          std::forward<Partitioner>(partitioner), grain, metrics);
    });
  }
  // One state per invocation. Affinity allocation/copy-on-overlap happens
  // before any worker is claimed, so failure cannot strand a resident.
  auto state = partitioner.MakeState(pool);
  const size_t capacity = std::min({size_t{63}, pool.NumThreads() - 1,
      pool.ResidentCapacity(group.domain), (end - begin - 1) / grain});
  if (capacity < 2)
    policy_detail::Execute<2>(group, begin, end, function, std::move(state), grain, metrics, activated, capacity);
  else if (capacity < 4)
    policy_detail::Execute<4>(group, begin, end, function, std::move(state), grain, metrics, activated, capacity);
  else if (capacity < 8)
    policy_detail::Execute<8>(group, begin, end, function, std::move(state), grain, metrics, activated, capacity);
  else if (capacity < 16)
    policy_detail::Execute<16>(group, begin, end, function, std::move(state), grain, metrics, activated, capacity);
  else
    policy_detail::Execute<64>(group, begin, end, function, std::move(state), grain, metrics, activated, capacity);
}

} // namespace oox::detail::eigen_pool::rapid
