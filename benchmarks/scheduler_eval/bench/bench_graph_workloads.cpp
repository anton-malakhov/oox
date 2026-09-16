// SPDX-License-Identifier: Apache-2.0

#include "graph_workloads.h"
#include "scheduler_metrics.h"

#include <benchmark/benchmark.h>

#include <chrono>
#include <cstdlib>
#include <fstream>
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
REGISTER_BFS(PaslSquareGrid);
REGISTER_BFS(PaslCubeGrid);
REGISTER_BFS(PaslParallelChains100);
REGISTER_BFS(PaslPhases10Degree2);
REGISTER_BFS(PaslPhases50Degree5);

const bool paper_trees_registered = [] {
  using scheduler_eval::GraphKind;
  for (const auto load : {1000000, 10000000, 100000000}) {
    if (load != 1000000 && !std::getenv("OOX_BENCH_PAPER_SCALE"))
      continue;
    benchmark::RegisterBenchmark("BfsPaslTrees524k/Flat",
        Bfs<GraphKind::PaslTrees524k, BfsPolicy::Flat>)->Arg(load)->UseRealTime();
    benchmark::RegisterBenchmark("BfsPaslTrees524k/Fixed",
        Bfs<GraphKind::PaslTrees524k, BfsPolicy::Fixed>)->Arg(load)->UseRealTime();
    benchmark::RegisterBenchmark("BfsPaslTrees524k/Adaptive",
        Bfs<GraphKind::PaslTrees524k, BfsPolicy::Adaptive>)->Arg(load)->UseRealTime();
  }
  return true;
}();

template <BfsPolicy Policy> void BfsFile(benchmark::State &state) {
  std::ifstream input(std::getenv("OOX_BENCH_GRAPH"), std::ios::binary);
  scheduler_eval::CsrGraph graph;
  std::uint32_t source = 0;
  try {
    graph = scheduler_eval::ReadAdjacencyGraph(input);
    if (const auto *value = std::getenv("OOX_BENCH_SOURCE")) {
      std::size_t consumed = 0;
      const auto parsed = std::stoull(value, &consumed);
      if (value[consumed] != '\0' ||
          (parsed >= graph.VertexCount() && (parsed || graph.VertexCount())))
        throw std::invalid_argument("BFS source vertex is outside the graph");
      source = static_cast<std::uint32_t>(parsed);
    }
  } catch (const std::exception &error) {
    state.SkipWithError(error.what());
    return;
  }
  const auto expected = scheduler_eval::BfsSerial(graph, source);
  const auto run = [&] {
    if constexpr (Policy == BfsPolicy::Flat)
      return scheduler_eval::BfsFlat(graph, source);
    else if constexpr (Policy == BfsPolicy::Fixed)
      return scheduler_eval::BfsNested(graph, 64, nullptr, source);
    else
      return scheduler_eval::BfsAdaptive(graph, std::chrono::microseconds(20),
                                        1.8, nullptr, source);
  };
  if (run() != expected) {
    state.SkipWithError("file BFS differs from serial levels");
    return;
  }
  scheduler_eval::SchedulerMetricsScope metrics(state);
  for (auto _ : state)
    benchmark::DoNotOptimize(run());
  state.SetItemsProcessed(state.iterations() * graph.edges.size());
  state.counters["source_vertex"] = source;
}

const bool file_registered = [] {
  if (std::getenv("OOX_BENCH_GRAPH")) {
    benchmark::RegisterBenchmark("BfsFile/Flat", BfsFile<BfsPolicy::Flat>)->UseRealTime();
    benchmark::RegisterBenchmark("BfsFile/Fixed", BfsFile<BfsPolicy::Fixed>)->UseRealTime();
    benchmark::RegisterBenchmark("BfsFile/Adaptive", BfsFile<BfsPolicy::Adaptive>)->UseRealTime();
  }
  return true;
}();

} // namespace
