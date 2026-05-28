#ifndef OOX_BENCH_TASKBENCH_METRICS_H
#define OOX_BENCH_TASKBENCH_METRICS_H

#include "taskbench_graph.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <vector>

namespace oox_bench::taskbench {

struct RunResult {
  Config config;
  int width = 0;
  int threads = 1;
  std::int64_t tasks = 0;
  std::int64_t edges = 0;
  double wall_s = 0.0;
  double task_granularity_us = 0.0;
  double throughput = 0.0;
  double efficiency = 1.0;
  bool ok = true;
};

inline RunResult build_result(const Config& cfg, const Graph& graph, double wall_s, bool ok = true) {
  RunResult result;
  result.config = cfg;
  result.width = graph.width();
  result.threads = resolved_threads(cfg);
  result.tasks = graph.task_count();
  result.edges = graph.edge_count();
  result.wall_s = wall_s;
  result.ok = ok;
  if (result.tasks > 0 && wall_s > 0.0) {
    result.task_granularity_us = wall_s * static_cast<double>(result.threads) * 1.0e6 /
                                 static_cast<double>(result.tasks);
    const double useful_work = static_cast<double>(result.tasks) * static_cast<double>(cfg.iterations);
    result.throughput = useful_work / wall_s;
  }
  return result;
}

struct SweepPoint {
  double task_granularity_us = 0.0;
  double throughput = 0.0;
};

struct MetgResult {
  bool found = false;
  double metg_us = std::numeric_limits<double>::quiet_NaN();
  double max_efficiency = 0.0;
};

inline MetgResult compute_metg(std::vector<SweepPoint> points, double threshold) {
  MetgResult result;
  if (points.empty()) return result;

  const auto max_it = std::max_element(points.begin(), points.end(), [](const auto& a, const auto& b) {
    return a.throughput < b.throughput;
  });
  if (max_it == points.end() || max_it->throughput <= 0.0) return result;
  const double max_throughput = max_it->throughput;

  std::sort(points.begin(), points.end(), [](const auto& a, const auto& b) {
    return a.task_granularity_us < b.task_granularity_us;
  });

  double prev_g = 0.0;
  double prev_e = 0.0;
  bool have_prev = false;
  for (const auto& point : points) {
    if (point.task_granularity_us <= 0.0) continue;
    const double efficiency = point.throughput / max_throughput;
    result.max_efficiency = std::max(result.max_efficiency, efficiency);
    if (efficiency >= threshold) {
      result.found = true;
      if (!have_prev || prev_e >= threshold || prev_g <= 0.0 || efficiency == prev_e) {
        result.metg_us = point.task_granularity_us;
      } else {
        const double log_prev = std::log(prev_g);
        const double log_curr = std::log(point.task_granularity_us);
        const double alpha = (threshold - prev_e) / (efficiency - prev_e);
        result.metg_us = std::exp(log_prev + alpha * (log_curr - log_prev));
      }
      return result;
    }
    prev_g = point.task_granularity_us;
    prev_e = efficiency;
    have_prev = true;
  }
  return result;
}

} // namespace oox_bench::taskbench

#endif // OOX_BENCH_TASKBENCH_METRICS_H
