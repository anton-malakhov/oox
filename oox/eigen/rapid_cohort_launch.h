// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "parallel_for.h"
#include "rapid_start.h"

namespace oox::detail::eigen_pool::rapid::detail {

// All storage that can throw is prepared before claim. The caller's WorkLease
// outlives every helper borrowing the launch, including cancellation paths.
template <size_t Participants, class Launch, class Prepare>
size_t RunCohort(RapidStartGroup group, partitioner_detail::Region &region,
                 Launch &launch, size_t capacity, Prepare prepare) {
  auto &pool = group.state->Pool();
  std::array<unsigned, Participants - 1> workers;
  const size_t helpers = pool.ClaimResidentWorkers(group.domain, workers.data(), capacity);
  OOX_SCHED_EVENT("rapid_claimed", ::oox::scheduler_trace::Current(), helpers, capacity);
#ifdef OOX_EIGEN_TEST_RAPID_CLAIMED
  OOX_EIGEN_TEST_RAPID_CLAIMED(helpers);
#endif
  prepare(workers.data(), helpers);
  ResidentCohort cohort(pool, workers.data(), helpers, launch.CompletionCounter());
  OOX_SCHED_EVENT("rapid_prepared", ::oox::scheduler_trace::Current(), helpers, launch.SeedCount());
  partitioner_detail::WorkLease admission(region, partitioner_detail::WorkLease::JoinedOwners{});
  launch.SetAdmission(&admission);
  launch.Publish(cohort);
  OOX_SCHED_EVENT("rapid_published", ::oox::scheduler_trace::Current(), helpers);
  launch.Run(0);
  OOX_SCHED_EVENT("rapid_caller_finished", ::oox::scheduler_trace::Current());
  pool.HelpResidentUntil(cohort);
  cohort.Finalize();
  OOX_SCHED_EVENT("rapid_commands_joined", ::oox::scheduler_trace::Current());
  // No captured command can read the borrowed stack lease after this join.
  launch.SetAdmission(nullptr);
  return helpers;
}

template <typename Run>
void WithCapacity(size_t helpers, Run run) {
  if (helpers < 2) run.template operator()<2>();
  else if (helpers < 4) run.template operator()<4>();
  else if (helpers < 8) run.template operator()<8>();
  else if (helpers < 16) run.template operator()<16>();
  else run.template operator()<64>();
}

} // namespace oox::detail::eigen_pool::rapid::detail
