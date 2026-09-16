// SPDX-License-Identifier: Apache-2.0

#include "extended_workloads.h"

#include <limits>
#include <random>
#include <set>

namespace scheduler_eval {

bool CheckExtendedWorkloads() {
  for (std::uint64_t seed = 0; seed < 131; ++seed) {
    const auto size = seed < 64 ? seed
                      : seed < 128
                          ? 2048 + seed
                          : (std::uint64_t{1} << (16 + seed - 128)) + 1;
    auto keys = MakeKeys64(KeyKind::Uniform, size, seed);
    if (size > 2) {
      keys[0] = std::numeric_limits<std::uint64_t>::max();
      keys[1] = 0;
      keys[2] = std::uint64_t{1} << 63;
    }
    auto expected = keys;
    std::vector<KeyValue64> pairs(size);
    for (std::size_t i = 0; i < size; ++i)
      pairs[i] = {seed % 2 ? keys[i] % 7 : keys[i], i};
    auto expected_pairs = pairs;
    std::stable_sort(
        expected_pairs.begin(), expected_pairs.end(),
        [](const auto &a, const auto &b) { return a.key < b.key; });
    if (RadixSort64PairsParallel(pairs) != expected_pairs) {
      std::cerr << "radix64 pairs seed=" << seed << '\n';
      return false;
    }
    if (SampleSortRecords(pairs) != expected_pairs) {
      std::cerr << "sample records seed=" << seed << '\n';
      return false;
    }
    std::sort(expected.begin(), expected.end());
    for (const auto width : {4u, 8u, 11u}) {
      RadixSortMetrics tuning;
      if (RadixSort64Parallel(keys, &tuning, width) != expected ||
          tuning.passes != (size ? (64u + width - 1) / width : 0u)) {
        std::cerr << "radix width=" << width << " seed=" << seed << '\n';
        return false;
      }
    }
    RadixSortMetrics metrics;
    if (RadixSort64Parallel(keys, &metrics) != expected ||
        metrics.passes != (size ? 8u : 0u) ||
        metrics.pass_nanoseconds.size() != metrics.passes) {
      std::cerr << "radix64 seed=" << seed << " size=" << size << '\n';
      return false;
    }
    auto strings = MakeTrigrams(size, seed);
    if (size > 2) {
      strings[0] = "";
      strings[1] = std::string("a\0b", 3);
      strings[2] = strings[1];
    }
    const std::set<std::string> unique(strings.begin(), strings.end());
    auto sorted_strings = strings;
    std::sort(sorted_strings.begin(), sorted_strings.end());
    if (SampleSortStrings(strings) != sorted_strings) {
      std::cerr << "sample strings seed=" << seed << '\n';
      return false;
    }
    if (RemoveDuplicateStrings(strings) !=
        std::vector<std::string>(unique.begin(), unique.end())) {
      std::cerr << "string dedup seed=" << seed << '\n';
      return false;
    }
    std::mt19937_64 random(seed);
    std::vector<Point> points(size);
    for (auto &point : points)
      point = {double(random() % 101), double(random() % 101)};
    if (seed >= 64)
      points = MakePoints(static_cast<PointKind>(seed % 4), size, seed);
    if (seed % 4 == 0)
      for (auto &point : points)
        point.y = point.x;
    auto hull = ConvexHullSerial(points);
    if (seed % 4 == 0 && !points.empty()) {
      const auto [first, last] = std::minmax_element(
          points.begin(), points.end(),
          [](const auto &a, const auto &b) { return a.x < b.x; });
      hull = {*first};
      if (first->x != last->x)
        hull.push_back(*last);
    }
    std::sort(hull.begin(), hull.end(), [](const Point &a, const Point &b) {
      return a.x < b.x || (a.x == b.x && a.y < b.y);
    });
    const auto actual = QuickHullParallel(points);
    if (hull.size() != actual.size() ||
        !std::equal(hull.begin(), hull.end(), actual.begin(),
                    [](const Point &a, const Point &b) {
                      return a.x == b.x && a.y == b.y;
                    })) {
      std::cerr << "quickhull seed=" << seed << " size=" << size << '\n';
      return false;
    }
  }
  return true;
}

} // namespace scheduler_eval
