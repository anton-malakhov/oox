// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace scheduler_eval {

enum class GraphKind {
  Tree,
  RandomArity100,
  ParallelChains,
  Phases,
  Phases10Degree2,
  Phases50Degree5,
  TrunkFirst,
  Rmat,
  SquareGrid,
  CubeGrid,
  SmallWorld
};

struct BfsMetrics {
  std::uint64_t levels{};
  std::uint64_t nested_launches{};
  std::uint64_t sequential_inner_loops{};
  std::uint64_t learned_sequential_limit{};
};

struct CsrGraph {
  std::vector<std::size_t> offsets;
  std::vector<std::uint32_t> edges;

  std::size_t VertexCount() const {
    return offsets.empty() ? 0 : offsets.size() - 1;
  }
};

CsrGraph MakeGraph(GraphKind kind, std::size_t scale);
std::vector<int> BfsSerial(const CsrGraph &graph);
std::vector<int> BfsFlat(const CsrGraph &graph);
std::vector<int> BfsNested(const CsrGraph &graph, std::size_t edge_cutoff,
                           BfsMetrics *metrics = nullptr);
std::vector<int> BfsAdaptive(const CsrGraph &graph,
                             std::chrono::nanoseconds kappa, double alpha = 1.8,
                             BfsMetrics *metrics = nullptr);

} // namespace scheduler_eval
