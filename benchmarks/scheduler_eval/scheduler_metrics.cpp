// SPDX-License-Identifier: Apache-2.0

#include "scheduler_metrics.h"

#include <benchmark/benchmark.h>

#ifdef EIGEN_MODE
#include "benchmarks/eigen/eigen_pool.h"
#endif

namespace scheduler_eval {

SchedulerMetrics ReadSchedulerMetrics() {
#ifdef EIGEN_MODE
  const auto value = EigenPool().GetStatistics();
  return {value.scheduled,           value.executed, value.successful_steals,
          value.failed_steal_rounds, value.sleeps,   value.idle_nanoseconds};
#else
  return {};
#endif
}

void ReportSchedulerMetrics(benchmark::State &state,
                            const SchedulerMetrics &before,
                            const SchedulerMetrics &after) {
#ifdef EIGEN_MODE
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
    : state_(state), before_(ReadSchedulerMetrics()) {}

SchedulerMetricsScope::~SchedulerMetricsScope() {
  ReportSchedulerMetrics(state_, before_, ReadSchedulerMetrics());
}

} // namespace scheduler_eval
