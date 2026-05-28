#ifndef OOX_BENCH_TASKBENCH_RUNNER_FOLLY_H
#define OOX_BENCH_TASKBENCH_RUNNER_FOLLY_H

#include "taskbench_metrics.h"

#include <chrono>
#include <stdexcept>
#include <vector>

#if HAVE_FOLLY
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#endif

namespace oox_bench::taskbench {

#if HAVE_FOLLY

inline RunResult run_folly(const Config& cfg) {
  Graph graph(cfg);
  const int width = graph.width();

  using clock = std::chrono::steady_clock;
  const auto start = clock::now();

  folly::CPUThreadPoolExecutor executor(static_cast<std::size_t>(resolved_threads(cfg)));

  std::vector<std::vector<Token>> prev(static_cast<std::size_t>(cfg.graphs));
  std::vector<std::vector<Token>> curr(static_cast<std::size_t>(cfg.graphs));
  for (int graph_id = 0; graph_id < cfg.graphs; ++graph_id) {
    prev[graph_id].resize(static_cast<std::size_t>(width));
    curr[graph_id].resize(static_cast<std::size_t>(width));
  }

  for (int row = 0; row < cfg.height; ++row) {
    std::vector<folly::Future<Token>> futures;
    futures.reserve(static_cast<std::size_t>(cfg.graphs * width));
    for (int graph_id = 0; graph_id < cfg.graphs; ++graph_id) {
      for (int col = 0; col < width; ++col) {
        futures.push_back(folly::via(&executor, [&, graph_id, row, col] {
          std::vector<Token> inputs;
          inputs.reserve(OOX_TASKBENCH_MAX_DEPS);
          if (row > 0) {
            for (int dep : graph.deps(row, col)) {
              inputs.push_back(prev[graph_id][dep]);
            }
          }
          return graph.execute_point(graph_id, row, col, std::span<const Token>(inputs));
        }));
      }
    }

    auto tries = folly::collectAll(futures).get();
    std::size_t idx = 0;
    for (int graph_id = 0; graph_id < cfg.graphs; ++graph_id) {
      for (int col = 0; col < width; ++col) {
        curr[graph_id][col] = std::move(tries[idx++].value());
      }
    }
    std::swap(prev, curr);
  }

  const auto stop = clock::now();
  const double wall_s = std::chrono::duration<double>(stop - start).count();
  return build_result(cfg, graph, wall_s, true);
}

#else

inline RunResult run_folly(const Config&) {
  throw std::runtime_error("folly runner requested, but this executable was not built with HAVE_FOLLY");
}

#endif

} // namespace oox_bench::taskbench

#endif // OOX_BENCH_TASKBENCH_RUNNER_FOLLY_H
