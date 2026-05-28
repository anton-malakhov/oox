#ifndef OOX_BENCH_TASKBENCH_RUNNER_OOX_H
#define OOX_BENCH_TASKBENCH_RUNNER_OOX_H

#include "runner_oox_dispatch.h"
#include "taskbench_metrics.h"

#include <chrono>
#include <vector>

namespace oox_bench::taskbench {

inline RunResult run_oox(const Config& cfg) {
  Graph graph(cfg);
  const int width = graph.width();

  using clock = std::chrono::steady_clock;
  const auto start = clock::now();

  if (cfg.pattern == Pattern::Trivial) {
    std::vector<oox::var<Token>> tasks;
    tasks.reserve(static_cast<std::size_t>(cfg.graphs) *
                  static_cast<std::size_t>(cfg.height) *
                  static_cast<std::size_t>(width));

    const std::vector<oox::var<Token>> empty_prev;
    const std::vector<int> empty_deps;
    for (int row = 0; row < cfg.height; ++row) {
      for (int graph_id = 0; graph_id < cfg.graphs; ++graph_id) {
        for (int col = 0; col < width; ++col) {
          tasks.push_back(run_point_with_deps(graph, graph_id, row, col, empty_prev, empty_deps));
        }
      }
    }

    for (auto& task : tasks) {
      oox::wait_for_all(task);
    }

    const auto stop = clock::now();
    const double wall_s = std::chrono::duration<double>(stop - start).count();
    return build_result(cfg, graph, wall_s, true);
  }

  std::vector<std::vector<oox::var<Token>>> prev(static_cast<std::size_t>(cfg.graphs));
  std::vector<std::vector<oox::var<Token>>> curr(static_cast<std::size_t>(cfg.graphs));
  std::vector<std::vector<oox::var<Token>>> keepalive(static_cast<std::size_t>(cfg.graphs));
  for (int graph_id = 0; graph_id < cfg.graphs; ++graph_id) {
    prev[graph_id].resize(static_cast<std::size_t>(width));
    curr[graph_id].resize(static_cast<std::size_t>(width));
    keepalive[graph_id].reserve(
        static_cast<std::size_t>(std::max(0, cfg.height - 1)) * static_cast<std::size_t>(width));
  }

  for (int graph_id = 0; graph_id < cfg.graphs; ++graph_id) {
    for (int col = 0; col < width; ++col) {
      prev[graph_id][col] = run_point_with_deps(graph, graph_id, 0, col, prev[graph_id], {});
    }
  }

  for (int row = 1; row < cfg.height; ++row) {
    for (int graph_id = 0; graph_id < cfg.graphs; ++graph_id) {
      if (row > 1) {
        // Keep prior-row vars alive to avoid dropping in-flight tasks.
        for (auto& v : curr[graph_id]) {
          keepalive[graph_id].push_back(std::move(v));
        }
      }
      for (int col = 0; col < width; ++col) {
        const auto deps = graph.deps(row, col);
        curr[graph_id][col] = run_point_with_deps(graph, graph_id, row, col, prev[graph_id], deps);
      }
      std::swap(prev[graph_id], curr[graph_id]);
    }
  }

  for (int graph_id = 0; graph_id < cfg.graphs; ++graph_id) {
    for (int col = 0; col < width; ++col) {
      oox::wait_for_all(prev[graph_id][col]);
    }
  }

  const auto stop = clock::now();
  const double wall_s = std::chrono::duration<double>(stop - start).count();
  return build_result(cfg, graph, wall_s, true);
}

} // namespace oox_bench::taskbench

#endif // OOX_BENCH_TASKBENCH_RUNNER_OOX_H
