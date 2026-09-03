// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>

namespace benchmark {
class State;
}

namespace scheduler_eval {

struct SchedulerMetrics {
  std::uint64_t scheduled{};
  std::uint64_t executed{};
  std::uint64_t successful_steals{};
  std::uint64_t failed_steal_rounds{};
  std::uint64_t sleeps{};
  std::uint64_t idle_nanoseconds{};
};

SchedulerMetrics ReadSchedulerMetrics();
void ReportSchedulerMetrics(benchmark::State &state,
                            const SchedulerMetrics &before,
                            const SchedulerMetrics &after);

class SchedulerMetricsScope {
public:
  explicit SchedulerMetricsScope(benchmark::State &state);
  ~SchedulerMetricsScope();

private:
  benchmark::State &state_;
  SchedulerMetrics before_;
};

} // namespace scheduler_eval
