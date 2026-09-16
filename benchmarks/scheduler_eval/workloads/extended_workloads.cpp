// SPDX-License-Identifier: Apache-2.0

#include "extended_workloads.h"

#include <array>
#include <atomic>
#include <limits>
#include <memory>
#include <random>

namespace scheduler_eval {
namespace {

long double Area(const Point &a, const Point &b, const Point &p) {
  const auto ax = static_cast<long double>(b.x) - a.x;
  const auto ay = static_cast<long double>(b.y) - a.y;
  const auto bx = static_cast<long double>(p.x) - a.x;
  const auto by = static_cast<long double>(p.y) - a.y;
  const auto product = ay * bx;
  return std::fma(ax, by, -product) + std::fma(-ay, bx, product);
}

struct HullPart {
  Point a, b;
  std::vector<Point> candidates;
};

} // namespace

std::vector<Point> QuickHullParallel(const std::vector<Point> &points,
                                     QuickHullMetrics *metrics) {
  if (metrics)
    *metrics = {};
  if (points.empty())
    return {};
  const auto less = [](const Point &a, const Point &b) {
    return a.x < b.x || (a.x == b.x && a.y < b.y);
  };
  const auto [minimum, maximum] =
      std::minmax_element(points.begin(), points.end(), less);
  if (!less(*minimum, *maximum))
    return {*minimum};
  std::vector<Point> hull{*minimum, *maximum};
  std::vector<HullPart> frontier{{*minimum, *maximum, {}},
                                 {*maximum, *minimum, {}}};
  for (const auto &point : points) {
    const auto area = Area(*minimum, *maximum, point);
    if (area > 0)
      frontier[0].candidates.push_back(point);
    else if (area < 0)
      frontier[1].candidates.push_back(point);
  }
  while (!frontier.empty()) {
    std::vector<std::array<HullPart, 2>> children(frontier.size());
    std::vector<Point> pivots(frontier.size());
    EvalParallelFor(0, frontier.size(), [&](std::size_t index) {
      const auto &part = frontier[index];
      if (part.candidates.empty())
        return;
      const auto pivot =
          *std::max_element(part.candidates.begin(), part.candidates.end(),
                            [&](const Point &a, const Point &b) {
                              const auto aa = Area(part.a, part.b, a);
                              const auto ab = Area(part.a, part.b, b);
                              return aa < ab || (aa == ab && less(a, b));
                            });
      pivots[index] = pivot;
      auto &left = children[index][0];
      auto &right = children[index][1];
      left.a = part.a;
      left.b = pivot;
      right.a = pivot;
      right.b = part.b;
      for (const auto &point : part.candidates) {
        if (Area(left.a, left.b, point) > 0)
          left.candidates.push_back(point);
        else if (Area(right.a, right.b, point) > 0)
          right.candidates.push_back(point);
      }
    });
    std::vector<HullPart> next;
    for (std::size_t i = 0; i < frontier.size(); ++i) {
      if (frontier[i].candidates.empty())
        continue;
      hull.push_back(pivots[i]);
      if (metrics)
        ++metrics->partitions;
      for (auto &child : children[i])
        if (!child.candidates.empty())
          next.push_back(std::move(child));
    }
    if (metrics)
      ++metrics->depth;
    frontier = std::move(next);
  }
  std::sort(hull.begin(), hull.end(), less);
  return hull;
}

std::vector<std::string> MakeTrigrams(std::size_t size, std::uint64_t seed) {
  // Synthetic word triples; this is not the PBBS language-model generator.
  static const std::array<const char *, 16> words{
      "a",  "the",  "of", "in", "to",   "and", "is", "for",
      "on", "with", "as", "by", "from", "at",  "it", "be"};
  std::mt19937_64 random(seed);
  std::vector<std::string> result(size);
  for (auto &value : result) {
    value = words[random() % words.size()];
    value += ' ';
    value += words[random() % words.size()];
    value += ' ';
    value += words[random() % words.size()];
  }
  return result;
}

std::vector<std::string>
RemoveDuplicateStrings(const std::vector<std::string> &keys,
                       DedupMetrics *metrics) {
  if (metrics)
    *metrics = {};
  if (keys.empty())
    return {};
  std::size_t capacity = 1;
  while (capacity / 2 < keys.size())
    capacity *= 2;
  constexpr auto empty = std::numeric_limits<std::size_t>::max();
  auto table = std::make_unique<std::atomic<std::size_t>[]>(capacity);
  EvalParallelFor(0, capacity, [&](std::size_t i) { table[i].store(empty); });
  std::atomic<std::uint64_t> probes{0};
  EvalParallelFor(0, keys.size(), [&](std::size_t i) {
    std::uint64_t hash = 14695981039346656037ull;
    for (unsigned char c : keys[i])
      hash = (hash ^ c) * 1099511628211ull;
    auto slot = hash & (capacity - 1);
    std::uint64_t count = 0;
    for (;;) {
      ++count;
      auto expected = empty;
      if (table[slot].compare_exchange_strong(expected, i) ||
          keys[expected] == keys[i])
        break;
      slot = (slot + 1) & (capacity - 1);
    }
    if (metrics)
      probes.fetch_add(count, std::memory_order_relaxed);
  });
  std::vector<std::string> result;
  for (std::size_t i = 0; i < capacity; ++i) {
    const auto index = table[i].load();
    if (index != empty)
      result.push_back(keys[index]);
  }
  std::sort(result.begin(), result.end());
  if (metrics)
    *metrics = {result.size(), probes.load(), capacity};
  return result;
}

} // namespace scheduler_eval
