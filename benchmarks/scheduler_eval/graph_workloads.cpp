// SPDX-License-Identifier: Apache-2.0

#include "graph_workloads.h"
#include "common.h"
#include "granularity_control.h"

#include <algorithm>
#include <atomic>
#include <bit>
#include <cmath>
#include <memory>
#include <utility>

namespace scheduler_eval {
namespace {

using Edge = std::pair<std::uint32_t, std::uint32_t>;

CsrGraph Build(std::size_t vertices, const std::vector<Edge> &input) {
  CsrGraph graph;
  graph.offsets.assign(vertices + 1, 0);
  for (auto [from, to] : input)
    ++graph.offsets[from + 1];
  for (std::size_t i = 1; i < graph.offsets.size(); ++i)
    graph.offsets[i] += graph.offsets[i - 1];
  graph.edges.resize(input.size());
  auto cursor = graph.offsets;
  for (auto [from, to] : input)
    graph.edges[cursor[from]++] = to;
  return graph;
}

std::vector<int> BfsParallel(const CsrGraph &graph, bool nested,
                             std::size_t cutoff,
                             GranularityEstimator *estimator,
                             BfsMetrics *metrics) {
  const auto n = graph.VertexCount();
  if (n == 0) {
    if (metrics)
      *metrics = {};
    return {};
  }
  auto levels = std::make_unique<std::atomic<int>[]>(n);
  ParallelFor(0, n, [&](std::size_t i) { levels[i].store(-1); });
  levels[0].store(0);
  std::vector<std::uint32_t> frontier{0};
  int level = 0;
  std::atomic<std::uint64_t> nested_launches{0};
  std::atomic<std::uint64_t> sequential_inner_loops{0};
  while (!frontier.empty()) {
    std::size_t capacity = 0;
    for (auto vertex : frontier)
      capacity += graph.offsets[vertex + 1] - graph.offsets[vertex];
    std::vector<std::uint32_t> next(capacity);
    std::atomic<std::size_t> size{0};
    ParallelFor(0, frontier.size(), [&](std::size_t i) {
      const auto vertex = frontier[i];
      const auto begin = graph.offsets[vertex], end = graph.offsets[vertex + 1];
      const auto degree = end - begin;
      const auto visit = [&](std::size_t edge) {
        const auto neighbor = graph.edges[edge];
        int unseen = -1;
        if (levels[neighbor].compare_exchange_strong(unseen, level + 1))
          next[size.fetch_add(1, std::memory_order_relaxed)] = neighbor;
      };
      const bool promote = nested && (estimator ? !estimator->IsSmall(degree)
                                                : degree >= cutoff);
      const auto started = Clock::now();
      if (promote) {
        nested_launches.fetch_add(1, std::memory_order_relaxed);
        ParallelFor(begin, end, visit);
      } else {
        if (nested)
          sequential_inner_loops.fetch_add(1, std::memory_order_relaxed);
        for (auto edge = begin; edge < end; ++edge)
          visit(edge);
      }
      if (estimator)
        estimator->Report(degree, Clock::now() - started);
    });
    next.resize(size.load(std::memory_order_relaxed));
    frontier = std::move(next);
    ++level;
  }
  std::vector<int> result(n);
  for (std::size_t i = 0; i < n; ++i)
    result[i] = levels[i].load();
  if (metrics)
    *metrics = {
        static_cast<std::uint64_t>(level),
        nested_launches.load(std::memory_order_relaxed),
        sequential_inner_loops.load(std::memory_order_relaxed),
        estimator ? estimator->SequentialComplexityLimit() : cutoff,
    };
  return result;
}

std::uint64_t Mix(std::uint64_t value) {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31);
}

CsrGraph MakePhased(std::size_t scale, std::size_t phases, std::size_t degree) {
  const std::size_t width = std::max<std::size_t>(2, scale / phases);
  std::vector<Edge> edges;
  for (std::uint32_t vertex = 0; vertex < width; ++vertex)
    edges.emplace_back(0, 1 + vertex);
  for (std::uint32_t phase = 0; phase + 1 < phases; ++phase)
    for (std::uint32_t from = 0; from < width; ++from)
      for (std::uint32_t edge = 0; edge < degree; ++edge)
        edges.emplace_back(1 + phase * width + from,
                           1 + (phase + 1) * width +
                               Mix(from * degree + edge + phase) % width);
  return Build(1 + phases * width, edges);
}

} // namespace

CsrGraph MakeGraph(GraphKind kind, std::size_t scale) {
  std::vector<Edge> edges;
  if (kind == GraphKind::Tree || kind == GraphKind::RandomArity100) {
    const std::size_t vertices = std::max<std::size_t>(2, scale);
    const std::uint32_t arity = kind == GraphKind::RandomArity100 ? 100 : 64;
    for (std::uint32_t child = 1; child < vertices; ++child)
      edges.emplace_back((child - 1) / arity, child);
    return Build(vertices, edges);
  }
  if (kind == GraphKind::ParallelChains) {
    const std::size_t chains = std::max<std::size_t>(2, std::sqrt(scale));
    const std::size_t length = std::max<std::size_t>(2, scale / chains);
    for (std::uint32_t chain = 0; chain < chains; ++chain) {
      const auto first = 1 + chain * length;
      edges.emplace_back(0, first);
      for (std::uint32_t i = 1; i < length; ++i)
        edges.emplace_back(first + i - 1, first + i);
    }
    return Build(1 + chains * length, edges);
  }
  if (kind == GraphKind::Phases) {
    const std::size_t width =
        std::min<std::size_t>(64, std::max<std::size_t>(2, std::sqrt(scale)));
    const std::size_t phases = std::max<std::size_t>(2, scale / width);
    for (std::uint32_t vertex = 0; vertex < width; ++vertex)
      edges.emplace_back(0, 1 + vertex);
    for (std::uint32_t phase = 0; phase + 1 < phases; ++phase)
      for (std::uint32_t from = 0; from < width; ++from)
        for (std::uint32_t to = 0; to < width; ++to)
          edges.emplace_back(1 + phase * width + from,
                             1 + (phase + 1) * width + to);
    return Build(1 + phases * width, edges);
  }
  if (kind == GraphKind::Phases10Degree2)
    return MakePhased(scale, 10, 2);
  if (kind == GraphKind::Phases50Degree5)
    return MakePhased(scale, 50, 5);
  if (kind == GraphKind::TrunkFirst) {
    const std::size_t vertices = std::max<std::size_t>(4, scale);
    const std::size_t trunk = std::max<std::size_t>(2, std::sqrt(vertices));
    for (std::uint32_t vertex = 1; vertex < trunk; ++vertex)
      edges.emplace_back(vertex - 1, vertex);
    for (std::uint32_t vertex = trunk; vertex < vertices; ++vertex)
      edges.emplace_back(trunk - 1, vertex);
    return Build(vertices, edges);
  }
  if (kind == GraphKind::Rmat) {
    const std::size_t vertices = std::bit_ceil(std::max<std::size_t>(2, scale));
    const std::size_t edge_count = 8 * vertices;
    for (std::uint64_t edge = 0; edge < edge_count; ++edge) {
      std::uint32_t from = 0, to = 0;
      for (std::size_t step = vertices / 2, bit = 0; step; step /= 2, ++bit) {
        const auto sample = Mix(edge * 64 + bit) % 1000;
        if (sample >= 550 && sample < 675)
          to += step;
        else if (sample >= 675 && sample < 800)
          from += step;
        else if (sample >= 800) {
          from += step;
          to += step;
        }
      }
      if (from != to) {
        edges.emplace_back(from, to);
        edges.emplace_back(to, from);
      }
    }
    return Build(vertices, edges);
  }
  if (kind == GraphKind::CubeGrid) {
    const std::size_t width =
        std::max<std::size_t>(2, std::cbrt(std::max<std::size_t>(8, scale)));
    for (std::uint32_t plane = 0; plane < width; ++plane)
      for (std::uint32_t row = 0; row < width; ++row)
        for (std::uint32_t column = 0; column < width; ++column) {
          const auto vertex = (plane * width + row) * width + column;
          if (column + 1 < width)
            edges.emplace_back(vertex, vertex + 1);
          if (row + 1 < width)
            edges.emplace_back(vertex, vertex + width);
          if (plane + 1 < width)
            edges.emplace_back(vertex, vertex + width * width);
        }
    return Build(width * width * width, edges);
  }
  if (kind == GraphKind::SmallWorld) {
    const std::size_t vertices = std::max<std::size_t>(4, scale);
    for (std::uint32_t vertex = 0; vertex < vertices; ++vertex) {
      edges.emplace_back(vertex, (vertex + 1) % vertices);
      edges.emplace_back(vertex, (vertex + 2) % vertices);
      edges.emplace_back(vertex, Mix(vertex) % vertices);
    }
    return Build(vertices, edges);
  }
  const std::size_t width = std::max<std::size_t>(2, std::sqrt(scale));
  for (std::uint32_t row = 0; row < width; ++row)
    for (std::uint32_t column = 0; column < width; ++column) {
      const auto vertex = row * width + column;
      if (column + 1 < width)
        edges.emplace_back(vertex, vertex + 1);
      if (row + 1 < width)
        edges.emplace_back(vertex, vertex + width);
    }
  return Build(width * width, edges);
}

std::vector<int> BfsSerial(const CsrGraph &graph) {
  if (graph.VertexCount() == 0)
    return {};
  std::vector<int> levels(graph.VertexCount(), -1);
  std::vector<std::uint32_t> queue{0};
  levels[0] = 0;
  for (std::size_t head = 0; head < queue.size(); ++head) {
    const auto vertex = queue[head];
    for (auto edge = graph.offsets[vertex]; edge < graph.offsets[vertex + 1];
         ++edge) {
      const auto neighbor = graph.edges[edge];
      if (levels[neighbor] == -1) {
        levels[neighbor] = levels[vertex] + 1;
        queue.push_back(neighbor);
      }
    }
  }
  return levels;
}

std::vector<int> BfsFlat(const CsrGraph &graph) {
  return BfsParallel(graph, false, 0, nullptr, nullptr);
}

std::vector<int> BfsNested(const CsrGraph &graph, std::size_t edge_cutoff,
                           BfsMetrics *metrics) {
  return BfsParallel(graph, true, std::max<std::size_t>(1, edge_cutoff),
                     nullptr, metrics);
}

std::vector<int> BfsAdaptive(const CsrGraph &graph,
                             std::chrono::nanoseconds kappa, double alpha,
                             BfsMetrics *metrics) {
  GranularityEstimator estimator(kappa, alpha);
  return BfsParallel(graph, true, 1, &estimator, metrics);
}

} // namespace scheduler_eval
