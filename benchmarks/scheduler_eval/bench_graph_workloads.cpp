// SPDX-License-Identifier: Apache-2.0

#include "graph_workloads.h"

#include <benchmark/benchmark.h>

namespace {

template <scheduler_eval::GraphKind Kind, bool Nested>
void Bfs(benchmark::State &state) {
  const auto graph = scheduler_eval::MakeGraph(Kind, state.range(0));
  for (auto _ : state) {
    auto levels = Nested ? scheduler_eval::BfsNested(graph, 64)
                         : scheduler_eval::BfsFlat(graph);
    benchmark::DoNotOptimize(levels.data());
  }
  state.SetItemsProcessed(state.iterations() * graph.edges.size());
}

#define REGISTER_BFS(kind)                                                     \
  BENCHMARK_TEMPLATE(Bfs, scheduler_eval::GraphKind::kind, false)              \
      ->RangeMultiplier(4)                                                     \
      ->Range(1 << 10, 1 << 18)                                                \
      ->UseRealTime();                                                         \
  BENCHMARK_TEMPLATE(Bfs, scheduler_eval::GraphKind::kind, true)               \
      ->RangeMultiplier(4)                                                     \
      ->Range(1 << 10, 1 << 18)                                                \
      ->UseRealTime()

REGISTER_BFS(Tree);
REGISTER_BFS(ParallelChains);
REGISTER_BFS(Phases);
REGISTER_BFS(SquareGrid);

} // namespace
