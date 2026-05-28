#ifndef OOX_BENCH_TASKBENCH_RUNNER_OOX_DISPATCH_H
#define OOX_BENCH_TASKBENCH_RUNNER_OOX_DISPATCH_H

#include "taskbench_graph.h"

#include <array>
#include <cstddef>
#include <utility>
#include <span>
#include <stdexcept>
#include <vector>

#include <oox/oox.h>

namespace oox_bench::taskbench {

template <std::size_t>
using token_cref_t = const Token&;

template <std::size_t... Is>
Token execute_point_fn(const Graph* graph,
                       int graph_id,
                       int row,
                       int col,
                       token_cref_t<Is>... inputs) {
  const std::array<Token, sizeof...(Is)> input_array{inputs...};
  return graph->execute_point(graph_id, row, col, std::span<const Token>(input_array));
}

template <std::size_t... Is>
oox::var<Token> run_point_with_deps_impl(const Graph& graph,
                                         int graph_id,
                                         int row,
                                         int col,
                                         const std::vector<oox::var<Token>>& prev,
                                         const std::vector<int>& deps,
                                         std::index_sequence<Is...>) {
  return oox::run(execute_point_fn<Is...>, &graph, graph_id, row, col, prev[deps[Is]]...);
}

template <std::size_t N = 0>
oox::var<Token> run_point_with_deps_dispatch(const Graph& graph,
                                             int graph_id,
                                             int row,
                                             int col,
                                             const std::vector<oox::var<Token>>& prev,
                                             const std::vector<int>& deps) {
  if (deps.size() == N) {
    return run_point_with_deps_impl(graph, graph_id, row, col, prev, deps, std::make_index_sequence<N>{});
  }
  if constexpr (N < OOX_TASKBENCH_MAX_DEPS) {
    return run_point_with_deps_dispatch<N + 1>(graph, graph_id, row, col, prev, deps);
  }
  throw std::runtime_error("dependency count exceeds OOX_TASKBENCH_MAX_DEPS");
}

inline oox::var<Token> run_point_with_deps(const Graph& graph,
                                           int graph_id,
                                           int row,
                                           int col,
                                           const std::vector<oox::var<Token>>& prev,
                                           const std::vector<int>& deps) {
  if (deps.size() > OOX_TASKBENCH_MAX_DEPS) {
    throw std::runtime_error("dependency count exceeds OOX_TASKBENCH_MAX_DEPS");
  }
  return run_point_with_deps_dispatch(graph, graph_id, row, col, prev, deps);
}

} // namespace oox_bench::taskbench

#endif // OOX_BENCH_TASKBENCH_RUNNER_OOX_DISPATCH_H
