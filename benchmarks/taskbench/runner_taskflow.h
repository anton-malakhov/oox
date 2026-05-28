#ifndef OOX_BENCH_TASKBENCH_RUNNER_TASKFLOW_H
#define OOX_BENCH_TASKBENCH_RUNNER_TASKFLOW_H

#include "taskbench_metrics.h"

#include <chrono>
#include <cstddef>
#include <stdexcept>
#include <vector>

#if HAVE_TF
#include <taskflow/taskflow.hpp>
#endif

namespace oox_bench::taskbench {

#if HAVE_TF

inline std::size_t task_index(const Graph& graph, int graph_id, int row, int col) {
  const auto width = static_cast<std::size_t>(graph.width());
  return (static_cast<std::size_t>(graph_id) * static_cast<std::size_t>(graph.config().height) +
          static_cast<std::size_t>(row)) *
             width +
         static_cast<std::size_t>(col);
}

inline RunResult run_taskflow(const Config& cfg) {
  Graph graph(cfg);
  const int width = graph.width();
  const auto total_tasks = static_cast<std::size_t>(graph.task_count());

  using clock = std::chrono::steady_clock;
  const auto start = clock::now();

  std::vector<Token> tokens(total_tasks);
  std::vector<tf::Task> tasks(total_tasks);
  tf::Taskflow flow("oox-taskbench");

  for (int graph_id = 0; graph_id < cfg.graphs; ++graph_id) {
    for (int row = 0; row < cfg.height; ++row) {
      for (int col = 0; col < width; ++col) {
        const auto idx = task_index(graph, graph_id, row, col);
        tasks[idx] = flow.emplace([&, graph_id, row, col, idx] {
          std::vector<Token> inputs;
          inputs.reserve(OOX_TASKBENCH_MAX_DEPS);
          for (int dep : graph.deps(row, col)) {
            inputs.push_back(tokens[task_index(graph, graph_id, row - 1, dep)]);
          }
          tokens[idx] = graph.execute_point(graph_id, row, col, std::span<const Token>(inputs));
        });
      }
    }
  }

  for (int graph_id = 0; graph_id < cfg.graphs; ++graph_id) {
    for (int row = 1; row < cfg.height; ++row) {
      for (int col = 0; col < width; ++col) {
        const auto curr = task_index(graph, graph_id, row, col);
        for (int dep : graph.deps(row, col)) {
          tasks[task_index(graph, graph_id, row - 1, dep)].precede(tasks[curr]);
        }
      }
    }
  }

  tf::Executor executor(static_cast<unsigned>(resolved_threads(cfg)));
  executor.run(flow).wait();

  const auto stop = clock::now();
  const double wall_s = std::chrono::duration<double>(stop - start).count();
  return build_result(cfg, graph, wall_s, true);
}

#else

inline RunResult run_taskflow(const Config&) {
  throw std::runtime_error("taskflow runner requested, but this executable was not built with HAVE_TF");
}

#endif

} // namespace oox_bench::taskbench

#endif // OOX_BENCH_TASKBENCH_RUNNER_TASKFLOW_H
