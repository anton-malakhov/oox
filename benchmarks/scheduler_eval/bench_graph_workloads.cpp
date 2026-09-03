// SPDX-License-Identifier: Apache-2.0

#include "graph_workloads.h"
#include "scheduler_metrics.h"

#include <benchmark/benchmark.h>

#include <chrono>
#include <vector>

namespace {

enum class BfsPolicy { Flat, Fixed, Adaptive };

template <scheduler_eval::GraphKind Kind, BfsPolicy Policy>
void Bfs(benchmark::State &state) {
  scheduler_eval::SchedulerMetricsScope metrics(state);
  const auto graph = scheduler_eval::MakeGraph(Kind, state.range(0));
  std::uint64_t nested_launches = 0;
  std::uint64_t sequential_inner_loops = 0;
  std::uint64_t learned_sequential_limit = 0;
  for (auto _ : state) {
    scheduler_eval::BfsMetrics bfs_metrics;
    std::vector<int> levels;
    if constexpr (Policy == BfsPolicy::Flat)
      levels = scheduler_eval::BfsFlat(graph);
    else if constexpr (Policy == BfsPolicy::Fixed)
      levels = scheduler_eval::BfsNested(graph, 64, &bfs_metrics);
    else
      levels = scheduler_eval::BfsAdaptive(graph, std::chrono::microseconds(20),
                                           1.8, &bfs_metrics);
    nested_launches += bfs_metrics.nested_launches;
    sequential_inner_loops += bfs_metrics.sequential_inner_loops;
    learned_sequential_limit = bfs_metrics.learned_sequential_limit;
    benchmark::DoNotOptimize(levels.data());
  }
  state.SetItemsProcessed(state.iterations() * graph.edges.size());
  state.counters["nested_launches"] = nested_launches;
  state.counters["sequential_inner_loops"] = sequential_inner_loops;
  state.counters["learned_sequential_limit"] = learned_sequential_limit;
}

#define REGISTER_BFS(kind)                                                     \
  BENCHMARK_TEMPLATE(Bfs, scheduler_eval::GraphKind::kind, BfsPolicy::Flat)    \
      ->RangeMultiplier(4)                                                     \
      ->Range(1 << 10, 1 << 18)                                                \
      ->UseRealTime();                                                         \
  BENCHMARK_TEMPLATE(Bfs, scheduler_eval::GraphKind::kind, BfsPolicy::Fixed)   \
      ->RangeMultiplier(4)                                                     \
      ->Range(1 << 10, 1 << 18)                                                \
      ->UseRealTime();                                                         \
  BENCHMARK_TEMPLATE(Bfs, scheduler_eval::GraphKind::kind,                     \
                     BfsPolicy::Adaptive)                                      \
      ->RangeMultiplier(4)                                                     \
      ->Range(1 << 10, 1 << 18)                                                \
      ->UseRealTime()

REGISTER_BFS(Tree);
REGISTER_BFS(RandomArity100);
REGISTER_BFS(ParallelChains);
REGISTER_BFS(Phases);
REGISTER_BFS(Phases10Degree2);
REGISTER_BFS(Phases50Degree5);
REGISTER_BFS(TrunkFirst);
REGISTER_BFS(Rmat);
REGISTER_BFS(SquareGrid);
REGISTER_BFS(CubeGrid);
REGISTER_BFS(SmallWorld);

} // namespace
