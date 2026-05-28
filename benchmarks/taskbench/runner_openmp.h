#ifndef OOX_BENCH_TASKBENCH_RUNNER_OPENMP_H
#define OOX_BENCH_TASKBENCH_RUNNER_OPENMP_H

#include "taskbench_metrics.h"

#include <chrono>
#include <cstddef>
#include <stdexcept>
#include <vector>

#if HAVE_OMP
#include <omp.h>
#endif

namespace oox_bench::taskbench {

#if HAVE_OMP

inline std::size_t openmp_task_index(const Graph& graph, int graph_id, int row, int col) {
  const auto width = static_cast<std::size_t>(graph.width());
  return (static_cast<std::size_t>(graph_id) * static_cast<std::size_t>(graph.config().height) +
          static_cast<std::size_t>(row)) *
             width +
         static_cast<std::size_t>(col);
}

inline void execute_openmp_point(const Graph& graph,
                                 int graph_id,
                                 int row,
                                 int col,
                                 std::vector<Token>& tokens) {
  std::vector<Token> inputs;
  inputs.reserve(OOX_TASKBENCH_MAX_DEPS);
  for (int dep : graph.deps(row, col)) {
    inputs.push_back(tokens[openmp_task_index(graph, graph_id, row - 1, dep)]);
  }
  tokens[openmp_task_index(graph, graph_id, row, col)] =
      graph.execute_point(graph_id, row, col, std::span<const Token>(inputs));
}

inline void spawn_openmp_point(const Graph& graph,
                               int* marks,
                               std::vector<Token>& tokens,
                               int graph_id,
                               int row,
                               int col,
                               const std::vector<int>& deps) {
  if (deps.size() > OOX_TASKBENCH_MAX_DEPS) {
    throw std::runtime_error("dependency count exceeds OOX_TASKBENCH_MAX_DEPS");
  }
  const auto idx = openmp_task_index(graph, graph_id, row, col);
  switch (deps.size()) {
    case 0:
#pragma omp task firstprivate(graph_id, row, col, idx) shared(graph, tokens, marks) depend(out : marks[idx])
      { execute_openmp_point(graph, graph_id, row, col, tokens); }
      break;
    case 1: {
      const auto a = openmp_task_index(graph, graph_id, row - 1, deps[0]);
#pragma omp task firstprivate(graph_id, row, col, idx, a) shared(graph, tokens, marks) depend(in : marks[a]) depend(out : marks[idx])
      { execute_openmp_point(graph, graph_id, row, col, tokens); }
      break;
    }
    case 2: {
      const auto a = openmp_task_index(graph, graph_id, row - 1, deps[0]);
      const auto b = openmp_task_index(graph, graph_id, row - 1, deps[1]);
#pragma omp task firstprivate(graph_id, row, col, idx, a, b) shared(graph, tokens, marks) depend(in : marks[a], marks[b]) depend(out : marks[idx])
      { execute_openmp_point(graph, graph_id, row, col, tokens); }
      break;
    }
    case 3: {
      const auto a = openmp_task_index(graph, graph_id, row - 1, deps[0]);
      const auto b = openmp_task_index(graph, graph_id, row - 1, deps[1]);
      const auto c = openmp_task_index(graph, graph_id, row - 1, deps[2]);
#pragma omp task firstprivate(graph_id, row, col, idx, a, b, c) shared(graph, tokens, marks) depend(in : marks[a], marks[b], marks[c]) depend(out : marks[idx])
      { execute_openmp_point(graph, graph_id, row, col, tokens); }
      break;
    }
    case 4: {
      const auto a = openmp_task_index(graph, graph_id, row - 1, deps[0]);
      const auto b = openmp_task_index(graph, graph_id, row - 1, deps[1]);
      const auto c = openmp_task_index(graph, graph_id, row - 1, deps[2]);
      const auto d = openmp_task_index(graph, graph_id, row - 1, deps[3]);
#pragma omp task firstprivate(graph_id, row, col, idx, a, b, c, d) shared(graph, tokens, marks) depend(in : marks[a], marks[b], marks[c], marks[d]) depend(out : marks[idx])
      { execute_openmp_point(graph, graph_id, row, col, tokens); }
      break;
    }
    case 5: {
      const auto a = openmp_task_index(graph, graph_id, row - 1, deps[0]);
      const auto b = openmp_task_index(graph, graph_id, row - 1, deps[1]);
      const auto c = openmp_task_index(graph, graph_id, row - 1, deps[2]);
      const auto d = openmp_task_index(graph, graph_id, row - 1, deps[3]);
      const auto e = openmp_task_index(graph, graph_id, row - 1, deps[4]);
#pragma omp task firstprivate(graph_id, row, col, idx, a, b, c, d, e) shared(graph, tokens, marks) depend(in : marks[a], marks[b], marks[c], marks[d], marks[e]) depend(out : marks[idx])
      { execute_openmp_point(graph, graph_id, row, col, tokens); }
      break;
    }
    case 6: {
      const auto a = openmp_task_index(graph, graph_id, row - 1, deps[0]);
      const auto b = openmp_task_index(graph, graph_id, row - 1, deps[1]);
      const auto c = openmp_task_index(graph, graph_id, row - 1, deps[2]);
      const auto d = openmp_task_index(graph, graph_id, row - 1, deps[3]);
      const auto e = openmp_task_index(graph, graph_id, row - 1, deps[4]);
      const auto f = openmp_task_index(graph, graph_id, row - 1, deps[5]);
#pragma omp task firstprivate(graph_id, row, col, idx, a, b, c, d, e, f) shared(graph, tokens, marks) depend(in : marks[a], marks[b], marks[c], marks[d], marks[e], marks[f]) depend(out : marks[idx])
      { execute_openmp_point(graph, graph_id, row, col, tokens); }
      break;
    }
    case 7: {
      const auto a = openmp_task_index(graph, graph_id, row - 1, deps[0]);
      const auto b = openmp_task_index(graph, graph_id, row - 1, deps[1]);
      const auto c = openmp_task_index(graph, graph_id, row - 1, deps[2]);
      const auto d = openmp_task_index(graph, graph_id, row - 1, deps[3]);
      const auto e = openmp_task_index(graph, graph_id, row - 1, deps[4]);
      const auto f = openmp_task_index(graph, graph_id, row - 1, deps[5]);
      const auto g = openmp_task_index(graph, graph_id, row - 1, deps[6]);
#pragma omp task firstprivate(graph_id, row, col, idx, a, b, c, d, e, f, g) shared(graph, tokens, marks) depend(in : marks[a], marks[b], marks[c], marks[d], marks[e], marks[f], marks[g]) depend(out : marks[idx])
      { execute_openmp_point(graph, graph_id, row, col, tokens); }
      break;
    }
    case 8: {
      const auto a = openmp_task_index(graph, graph_id, row - 1, deps[0]);
      const auto b = openmp_task_index(graph, graph_id, row - 1, deps[1]);
      const auto c = openmp_task_index(graph, graph_id, row - 1, deps[2]);
      const auto d = openmp_task_index(graph, graph_id, row - 1, deps[3]);
      const auto e = openmp_task_index(graph, graph_id, row - 1, deps[4]);
      const auto f = openmp_task_index(graph, graph_id, row - 1, deps[5]);
      const auto g = openmp_task_index(graph, graph_id, row - 1, deps[6]);
      const auto h = openmp_task_index(graph, graph_id, row - 1, deps[7]);
#pragma omp task firstprivate(graph_id, row, col, idx, a, b, c, d, e, f, g, h) shared(graph, tokens, marks) depend(in : marks[a], marks[b], marks[c], marks[d], marks[e], marks[f], marks[g], marks[h]) depend(out : marks[idx])
      { execute_openmp_point(graph, graph_id, row, col, tokens); }
      break;
    }
  }
}

inline RunResult run_openmp(const Config& cfg) {
  Graph graph(cfg);
  const int width = graph.width();
  const auto total_tasks = static_cast<std::size_t>(graph.task_count());

  using clock = std::chrono::steady_clock;
  const auto start = clock::now();

  std::vector<Token> tokens(total_tasks);
  std::vector<int> marks(total_tasks);
  int* mark_ptr = marks.data();

#pragma omp parallel num_threads(resolved_threads(cfg))
  {
#pragma omp single
    {
    for (int graph_id = 0; graph_id < cfg.graphs; ++graph_id) {
      for (int row = 0; row < cfg.height; ++row) {
        for (int col = 0; col < width; ++col) {
          spawn_openmp_point(graph, mark_ptr, tokens, graph_id, row, col, graph.deps(row, col));
        }
      }
    }
    }
  }

  const auto stop = clock::now();
  const double wall_s = std::chrono::duration<double>(stop - start).count();
  return build_result(cfg, graph, wall_s, true);
}

#else

inline RunResult run_openmp(const Config&) {
  throw std::runtime_error("openmp runner requested, but this executable was not built with HAVE_OMP");
}

#endif

} // namespace oox_bench::taskbench

#endif // OOX_BENCH_TASKBENCH_RUNNER_OPENMP_H
