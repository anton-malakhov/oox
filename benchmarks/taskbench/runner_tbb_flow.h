#ifndef OOX_BENCH_TASKBENCH_RUNNER_TBB_FLOW_H
#define OOX_BENCH_TASKBENCH_RUNNER_TBB_FLOW_H

#include "taskbench_metrics.h"

#include <chrono>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <vector>

#if HAVE_TBB
#ifndef TBB_USE_ASSERT
#define TBB_USE_ASSERT 0
#endif
#include <oneapi/tbb/flow_graph.h>
#endif

namespace oox_bench::taskbench {

#if HAVE_TBB

inline std::size_t tbb_flow_task_index(const Graph& graph, int graph_id, int row, int col) {
  const auto width = static_cast<std::size_t>(graph.width());
  return (static_cast<std::size_t>(graph_id) * static_cast<std::size_t>(graph.config().height) +
          static_cast<std::size_t>(row)) *
             width +
         static_cast<std::size_t>(col);
}

inline RunResult run_tbb_flow(const Config& cfg) {
  Graph graph(cfg);
  const int width = graph.width();
  const auto total_tasks = static_cast<std::size_t>(graph.task_count());

  using clock = std::chrono::steady_clock;
  const auto start = clock::now();

  oneapi::tbb::flow::graph flow;
  using node_t = oneapi::tbb::flow::continue_node<oneapi::tbb::flow::continue_msg>;
  std::vector<Token> tokens(total_tasks);
  std::vector<std::unique_ptr<node_t>> nodes(total_tasks);
  std::vector<bool> has_predecessor(total_tasks, false);

  for (int graph_id = 0; graph_id < cfg.graphs; ++graph_id) {
    for (int row = 0; row < cfg.height; ++row) {
      for (int col = 0; col < width; ++col) {
        const auto idx = tbb_flow_task_index(graph, graph_id, row, col);
        nodes[idx] = std::make_unique<node_t>(flow, [&, graph_id, row, col, idx](const oneapi::tbb::flow::continue_msg&) {
          std::vector<Token> inputs;
          inputs.reserve(OOX_TASKBENCH_MAX_DEPS);
          for (int dep : graph.deps(row, col)) {
            inputs.push_back(tokens[tbb_flow_task_index(graph, graph_id, row - 1, dep)]);
          }
          tokens[idx] = graph.execute_point(graph_id, row, col, std::span<const Token>(inputs));
          return oneapi::tbb::flow::continue_msg{};
        });
      }
    }
  }

  for (int graph_id = 0; graph_id < cfg.graphs; ++graph_id) {
    for (int row = 1; row < cfg.height; ++row) {
      for (int col = 0; col < width; ++col) {
        const auto curr = tbb_flow_task_index(graph, graph_id, row, col);
        for (int dep : graph.deps(row, col)) {
          oneapi::tbb::flow::make_edge(*nodes[tbb_flow_task_index(graph, graph_id, row - 1, dep)], *nodes[curr]);
          has_predecessor[curr] = true;
        }
      }
    }
  }

  for (std::size_t idx = 0; idx < total_tasks; ++idx) {
    if (!has_predecessor[idx]) {
      nodes[idx]->try_put(oneapi::tbb::flow::continue_msg{});
    }
  }
  flow.wait_for_all();

  const auto stop = clock::now();
  const double wall_s = std::chrono::duration<double>(stop - start).count();
  return build_result(cfg, graph, wall_s, true);
}

#else

inline RunResult run_tbb_flow(const Config&) {
  throw std::runtime_error("tbb-flow runner requested, but this executable was not built with HAVE_TBB");
}

#endif

} // namespace oox_bench::taskbench

#endif // OOX_BENCH_TASKBENCH_RUNNER_TBB_FLOW_H
