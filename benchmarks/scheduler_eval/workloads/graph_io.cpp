// SPDX-License-Identifier: Apache-2.0

#include "graph_workloads.h"

#include <istream>
#include <limits>
#include <stdexcept>
#include <string>

namespace scheduler_eval {
namespace {

std::uint64_t ReadWord(std::istream &input, unsigned bytes, bool little) {
  std::uint64_t value = 0;
  for (unsigned i = 0; i < bytes; ++i) {
    const auto byte = input.get();
    if (byte == std::char_traits<char>::eof())
      throw std::runtime_error("truncated PASL binary graph");
    value |= std::uint64_t{static_cast<unsigned char>(byte)}
             << (8 * (little ? i : bytes - 1 - i));
  }
  return value;
}

CsrGraph ReadPaslGraph(std::istream &input) {
  // PASL graph/include/graphio.hpp: five uint64 header words followed by
  // n+1 offsets and m neighbors, each using the header's 32/64-bit width.
  const auto magic = ReadWord(input, 8, true);
  const bool little = magic == 0xdeadbeef;
  if (!little && magic != 0xefbeadde00000000ull)
    throw std::runtime_error("invalid PASL graph magic");
  const auto bits = ReadWord(input, 8, little);
  const auto vertices = ReadWord(input, 8, little);
  const auto edges = ReadWord(input, 8, little);
  const auto symmetric = ReadWord(input, 8, little);
  if ((bits != 32 && bits != 64) || symmetric > 1 ||
      vertices > std::numeric_limits<std::uint32_t>::max() ||
      vertices >= std::numeric_limits<std::size_t>::max() ||
      edges > std::numeric_limits<std::size_t>::max())
    throw std::runtime_error("unsupported PASL graph dimensions");
  CsrGraph graph;
  for (std::uint64_t i = 0; i <= vertices; ++i) {
    const auto offset = ReadWord(input, bits / 8, little);
    if (offset > edges ||
        (i == 0 ? offset != 0 : offset < graph.offsets.back()))
      throw std::runtime_error("invalid PASL adjacency offset");
    graph.offsets.push_back(offset);
  }
  if (graph.offsets.back() != edges)
    throw std::runtime_error("PASL edge count differs from final offset");
  for (std::uint64_t i = 0; i < edges; ++i) {
    const auto vertex = ReadWord(input, bits / 8, little);
    if (vertex >= vertices)
      throw std::runtime_error("invalid PASL vertex ID");
    graph.edges.push_back(vertex);
  }
  if (input.peek() != std::char_traits<char>::eof())
    throw std::runtime_error("trailing bytes in PASL graph");
  return graph;
}

} // namespace

CsrGraph ReadAdjacencyGraph(std::istream &input) {
  if (input.peek() != 'A')
    return ReadPaslGraph(input);
  std::string header;
  std::uint64_t vertices, edges;
  if (!(input >> header >> vertices >> edges) || header != "AdjacencyGraph" ||
      vertices > std::numeric_limits<std::uint32_t>::max() ||
      vertices >= std::numeric_limits<std::size_t>::max() ||
      edges > std::numeric_limits<std::size_t>::max())
    throw std::runtime_error("invalid PBBS AdjacencyGraph header");
  CsrGraph graph;
  for (std::uint64_t i = 0; i < vertices; ++i) {
    std::uint64_t offset;
    if (!(input >> offset) || offset > edges ||
        (i == 0 ? offset != 0 : offset < graph.offsets.back()))
      throw std::runtime_error("invalid or truncated adjacency offsets");
    graph.offsets.push_back(offset);
  }
  if (!vertices && edges)
    throw std::runtime_error("edges in an empty graph");
  graph.offsets.push_back(edges);
  for (std::uint64_t i = 0; i < edges; ++i) {
    std::uint64_t vertex;
    if (!(input >> vertex) || vertex >= vertices)
      throw std::runtime_error("invalid or truncated adjacency edges");
    graph.edges.push_back(vertex);
  }
  if (input >> header)
    throw std::runtime_error("trailing data after adjacency graph");
  return graph;
}

} // namespace scheduler_eval
