#ifndef OOX_BENCH_TASKBENCH_RUNNER_SERIAL_H
#define OOX_BENCH_TASKBENCH_RUNNER_SERIAL_H

#include "taskbench_metrics.h"

#include <chrono>
#include <vector>

namespace oox_bench::taskbench {

inline RunResult run_serial(const Config& cfg) {
  Graph graph(cfg);
  const int width = graph.width();

  using clock = std::chrono::steady_clock;
  const auto start = clock::now();

  std::vector<std::vector<Token>> prev(static_cast<std::size_t>(cfg.graphs));
  std::vector<std::vector<Token>> curr(static_cast<std::size_t>(cfg.graphs));
  for (int graph_id = 0; graph_id < cfg.graphs; ++graph_id) {
    prev[graph_id].resize(static_cast<std::size_t>(width));
    curr[graph_id].resize(static_cast<std::size_t>(width));
  }

  for (int graph_id = 0; graph_id < cfg.graphs; ++graph_id) {
    for (int col = 0; col < width; ++col) {
      prev[graph_id][col] = graph.execute_point(graph_id, 0, col, std::span<const Token>());
    }
  }

  std::vector<Token> inputs;
  inputs.reserve(OOX_TASKBENCH_MAX_DEPS);
  for (int row = 1; row < cfg.height; ++row) {
    for (int graph_id = 0; graph_id < cfg.graphs; ++graph_id) {
      for (int col = 0; col < width; ++col) {
        inputs.clear();
        const auto deps = graph.deps(row, col);
        for (int dep : deps) {
          inputs.push_back(prev[graph_id][dep]);
        }
        curr[graph_id][col] = graph.execute_point(graph_id, row, col, std::span<const Token>(inputs));
      }
      std::swap(prev[graph_id], curr[graph_id]);
    }
  }

  const auto stop = clock::now();
  const double wall_s = std::chrono::duration<double>(stop - start).count();
  return build_result(cfg, graph, wall_s, true);
}

} // namespace oox_bench::taskbench

#endif // OOX_BENCH_TASKBENCH_RUNNER_SERIAL_H
