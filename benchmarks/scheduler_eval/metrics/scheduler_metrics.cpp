// SPDX-License-Identifier: Apache-2.0

#include "scheduler_metrics.h"
#include "papi_metrics.h"

#include <benchmark/benchmark.h>

#ifdef OOX_TASK_MODE
#include <oox/oox.h>
#elif defined(EIGEN_MODE)
#include "benchmarks/eigen/eigen_pool.h"
#endif

namespace scheduler_eval {

SchedulerMetrics ReadSchedulerMetrics() {
#if defined(EIGEN_MODE) || defined(OOX_TASK_MODE)
#ifdef OOX_TASK_MODE
  const auto value = oox::internal::get_eigen_pool().GetStatistics();
#else
  const auto value = EigenPool().GetStatistics();
#endif
  return {value.scheduled,           value.executed, value.successful_steals,
          value.failed_steal_rounds, value.sleeps,   value.idle_nanoseconds};
#else
  return {};
#endif
}

void ReportSchedulerMetrics(benchmark::State &state,
                            const SchedulerMetrics &before,
                            const SchedulerMetrics &after) {
#if defined(EIGEN_MODE) || defined(OOX_TASK_MODE)
  state.counters["tasks_scheduled"] = after.scheduled - before.scheduled;
  state.counters["tasks_executed"] = after.executed - before.executed;
  state.counters["successful_steals"] =
      after.successful_steals - before.successful_steals;
  state.counters["failed_steal_rounds"] =
      after.failed_steal_rounds - before.failed_steal_rounds;
  state.counters["worker_sleeps"] = after.sleeps - before.sleeps;
  state.counters["idle_time_ns"] =
      after.idle_nanoseconds - before.idle_nanoseconds;
#else
  static_cast<void>(state);
  static_cast<void>(before);
  static_cast<void>(after);
#endif
}

SchedulerMetricsScope::SchedulerMetricsScope(benchmark::State &state)
    : state_(state), before_(ReadSchedulerMetrics()) {
  BeginPapiMeasurement();
}

SchedulerMetricsScope::~SchedulerMetricsScope() {
  ReportSchedulerMetrics(state_, before_, ReadSchedulerMetrics());
  const auto papi = EndPapiMeasurement();
  if (!papi.error.empty()) {
    state_.SkipWithError(papi.error.c_str());
  } else if (!papi.events.empty()) {
    state_.counters["papi_callback_regions"] = papi.regions;
    for (std::size_t i = 0; i < papi.events.size(); ++i)
      state_.counters["papi_" + papi.events[i]] = papi.values[i];
  }
}

} // namespace scheduler_eval
