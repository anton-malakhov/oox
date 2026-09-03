// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace scheduler_eval {

enum class GraphKind { Tree, ParallelChains, Phases, SquareGrid };

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
std::vector<int> BfsNested(const CsrGraph &graph, std::size_t edge_cutoff);

} // namespace scheduler_eval
