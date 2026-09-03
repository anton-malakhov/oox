// SPDX-License-Identifier: Apache-2.0

#include "primary_workloads.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <limits>
#include <memory>
#include <random>

namespace scheduler_eval {
namespace {

constexpr std::size_t block_size = 2048;

bool PointLess(const Point &left, const Point &right) {
  return left.x < right.x || (left.x == right.x && left.y < right.y);
}

bool PointEqual(const Point &left, const Point &right) {
  return left.x == right.x && left.y == right.y;
}

double Cross(const Point &a, const Point &b, const Point &point) {
  return (b.x - a.x) * (point.y - a.y) - (b.y - a.y) * (point.x - a.x);
}

std::size_t NextPowerOfTwo(std::size_t value) {
  std::size_t result = 1;
  while (result < value)
    result <<= 1;
  return result;
}

} // namespace

std::vector<Point> MakePoints(PointKind kind, std::size_t size,
                              std::uint64_t seed) {
  std::mt19937_64 random(seed);
  std::uniform_real_distribution<double> unit(0.0, 1.0);
  std::vector<Point> points(size);
  for (auto &point : points) {
    const auto angle = 2.0 * std::acos(-1.0) * unit(random);
    if (kind == PointKind::UniformSquare) {
      point = {2.0 * unit(random) - 1.0, 2.0 * unit(random) - 1.0};
    } else {
      double radius = 1.0;
      if (kind == PointKind::InDisk)
        radius = std::sqrt(unit(random));
      else if (kind == PointKind::Kuzmin) {
        const auto value = std::max(unit(random), 1.0 / (size + 1.0));
        radius = std::sqrt(1.0 / (value * value) - 1.0);
      }
      point = {radius * std::cos(angle), radius * std::sin(angle)};
    }
  }
  return points;
}

std::vector<Point> ConvexHullSerial(std::vector<Point> points) {
  std::sort(points.begin(), points.end(), PointLess);
  points.erase(std::unique(points.begin(), points.end(), PointEqual),
               points.end());
  if (points.size() < 3)
    return points;
  std::vector<Point> hull(2 * points.size());
  std::size_t size = 0;
  for (const auto &point : points) {
    while (size >= 2 && Cross(hull[size - 2], hull[size - 1], point) <= 0)
      --size;
    hull[size++] = point;
  }
  const auto lower = size + 1;
  for (auto i = points.size() - 1; i-- > 0;) {
    while (size >= lower &&
           Cross(hull[size - 2], hull[size - 1], points[i]) <= 0)
      --size;
    hull[size++] = points[i];
  }
  hull.resize(size - 1);
  return hull;
}

std::vector<Point> ConvexHullParallel(const std::vector<Point> &points,
                                      ConvexHullMetrics *metrics) {
  std::vector<Point> sorted = points, buffer(points.size());
  const auto blocks = (points.size() + block_size - 1) / block_size;
  std::size_t merge_passes = 0;
  ParallelFor(0, blocks, [&](std::size_t block) {
    const auto begin = sorted.begin() + block * block_size;
    std::sort(begin, std::min(sorted.end(), begin + block_size), PointLess);
  });
  for (std::size_t width = block_size; width < sorted.size(); width *= 2) {
    ++merge_passes;
    const auto merges = (sorted.size() + 2 * width - 1) / (2 * width);
    ParallelFor(0, merges, [&](std::size_t merge) {
      const auto begin = std::min(sorted.size(), merge * 2 * width);
      const auto middle = std::min(sorted.size(), begin + width);
      const auto end = std::min(sorted.size(), begin + 2 * width);
      std::merge(sorted.begin() + begin, sorted.begin() + middle,
                 sorted.begin() + middle, sorted.begin() + end,
                 buffer.begin() + begin, PointLess);
    });
    sorted.swap(buffer);
  }
  sorted.erase(std::unique(sorted.begin(), sorted.end(), PointEqual),
               sorted.end());
  if (sorted.size() < 3) {
    if (metrics)
      *metrics = {sorted.size(), merge_passes};
    return sorted;
  }
  std::vector<Point> hull(2 * sorted.size());
  std::size_t size = 0;
  for (const auto &point : sorted) {
    while (size >= 2 && Cross(hull[size - 2], hull[size - 1], point) <= 0)
      --size;
    hull[size++] = point;
  }
  const auto lower = size + 1;
  for (auto i = sorted.size() - 1; i-- > 0;) {
    while (size >= lower &&
           Cross(hull[size - 2], hull[size - 1], sorted[i]) <= 0)
      --size;
    hull[size++] = sorted[i];
  }
  hull.resize(size - 1);
  if (metrics)
    *metrics = {hull.size(), merge_passes};
  return hull;
}

std::vector<std::uint32_t> MakeKeys(KeyKind kind, std::size_t size,
                                    std::uint64_t seed) {
  std::mt19937_64 random(seed);
  std::vector<std::uint32_t> keys(size);
  if (kind == KeyKind::AlmostSorted) {
    for (std::size_t i = 0; i < size; ++i)
      keys[i] = static_cast<std::uint32_t>(i);
    for (std::size_t i = 0; i < size / 100 + 1 && size > 1; ++i)
      std::swap(keys[random() % size], keys[random() % size]);
    return keys;
  }
  std::exponential_distribution<double> exponential(1.0);
  for (auto &key : keys) {
    if (kind == KeyKind::DuplicateHeavy)
      key = static_cast<std::uint32_t>(random() %
                                       std::max<std::size_t>(1, size / 64));
    else if (kind == KeyKind::Exponential)
      key = static_cast<std::uint32_t>(std::min(
          exponential(random) * 100000000.0,
          static_cast<double>(std::numeric_limits<std::uint32_t>::max())));
    else
      key = static_cast<std::uint32_t>(random());
  }
  return keys;
}

std::vector<std::uint32_t>
RemoveDuplicatesSerial(std::vector<std::uint32_t> keys) {
  std::sort(keys.begin(), keys.end());
  keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
  return keys;
}

std::vector<std::uint32_t>
RemoveDuplicatesParallel(const std::vector<std::uint32_t> &keys,
                         DedupMetrics *metrics) {
  if (keys.empty()) {
    if (metrics)
      *metrics = {};
    return {};
  }
  constexpr auto empty = std::numeric_limits<std::uint64_t>::max();
  const auto capacity = NextPowerOfTwo(keys.size() * 2);
  auto table = std::make_unique<std::atomic<std::uint64_t>[]>(capacity);
  std::atomic<std::uint64_t> hash_probes{0};
  ParallelFor(0, capacity, [&](std::size_t i) { table[i].store(empty); });
  ParallelFor(0, keys.size(), [&](std::size_t i) {
    const auto key = static_cast<std::uint64_t>(keys[i]);
    auto slot = (key * 11400714819323198485ull) & (capacity - 1);
    std::uint64_t probes = 0;
    while (true) {
      ++probes;
      auto expected = empty;
      if (table[slot].compare_exchange_weak(expected, key) || expected == key)
        break;
      slot = (slot + 1) & (capacity - 1);
    }
    if (metrics)
      hash_probes.fetch_add(probes, std::memory_order_relaxed);
  });
  std::vector<std::uint32_t> result;
  result.reserve(keys.size());
  for (std::size_t i = 0; i < capacity; ++i)
    if (table[i].load() != empty)
      result.push_back(static_cast<std::uint32_t>(table[i].load()));
  std::sort(result.begin(), result.end());
  if (metrics)
    *metrics = {result.size(), hash_probes.load(std::memory_order_relaxed),
                capacity};
  return result;
}

std::vector<std::uint32_t> RadixSortSerial(std::vector<std::uint32_t> keys) {
  std::stable_sort(keys.begin(), keys.end());
  return keys;
}

std::vector<std::uint32_t>
RadixSortParallel(const std::vector<std::uint32_t> &keys,
                  RadixSortMetrics *metrics) {
  if (keys.empty()) {
    if (metrics)
      *metrics = {};
    return {};
  }
  std::vector<std::uint32_t> input = keys, output(keys.size());
  const auto blocks = (keys.size() + block_size - 1) / block_size;
  using Counts = std::array<std::size_t, 256>;
  std::vector<Counts> counts(blocks), offsets(blocks);
  for (unsigned shift = 0; shift < 32; shift += 8) {
    ParallelFor(0, blocks, [&](std::size_t block) {
      counts[block].fill(0);
      const auto end = std::min(input.size(), (block + 1) * block_size);
      for (auto i = block * block_size; i < end; ++i)
        ++counts[block][(input[i] >> shift) & 255];
    });
    std::size_t total = 0;
    for (std::size_t bucket = 0; bucket < 256; ++bucket)
      for (std::size_t block = 0; block < blocks; ++block) {
        offsets[block][bucket] = total;
        total += counts[block][bucket];
      }
    ParallelFor(0, blocks, [&](std::size_t block) {
      auto positions = offsets[block];
      const auto end = std::min(input.size(), (block + 1) * block_size);
      for (auto i = block * block_size; i < end; ++i)
        output[positions[(input[i] >> shift) & 255]++] = input[i];
    });
    input.swap(output);
  }
  if (metrics)
    metrics->passes = 4;
  return input;
}

std::vector<std::uint32_t> SampleSortSerial(std::vector<std::uint32_t> keys) {
  std::sort(keys.begin(), keys.end());
  return keys;
}

std::vector<std::uint32_t>
SampleSortParallel(const std::vector<std::uint32_t> &keys,
                   SampleSortMetrics *metrics) {
  if (keys.size() < block_size) {
    if (metrics)
      *metrics = {keys.empty() ? 0u : 1u, keys.size()};
    return SampleSortSerial(keys);
  }
  const auto buckets =
      std::min<std::size_t>(256, (keys.size() + block_size - 1) / block_size);
  std::vector<std::uint32_t> samples;
  samples.reserve(buckets * 8);
  for (std::size_t i = 0; i < buckets * 8; ++i)
    samples.push_back(keys[(i * keys.size()) / (buckets * 8)]);
  std::sort(samples.begin(), samples.end());
  std::vector<std::uint32_t> splitters(buckets - 1);
  for (std::size_t i = 1; i < buckets; ++i)
    splitters[i - 1] = samples[i * 8];
  const auto blocks = (keys.size() + block_size - 1) / block_size;
  std::vector<std::vector<std::size_t>> offsets(
      blocks, std::vector<std::size_t>(buckets));
  ParallelFor(0, blocks, [&](std::size_t block) {
    const auto end = std::min(keys.size(), (block + 1) * block_size);
    for (auto i = block * block_size; i < end; ++i)
      ++offsets[block]
               [std::upper_bound(splitters.begin(), splitters.end(), keys[i]) -
                splitters.begin()];
  });
  std::vector<std::size_t> bucket_starts(buckets + 1);
  for (std::size_t bucket = 0; bucket < buckets; ++bucket)
    for (std::size_t block = 0; block < blocks; ++block) {
      const auto count = offsets[block][bucket];
      offsets[block][bucket] = bucket_starts[bucket + 1];
      bucket_starts[bucket + 1] += count;
    }
  for (std::size_t bucket = 1; bucket <= buckets; ++bucket) {
    const auto shift = bucket_starts[bucket - 1];
    bucket_starts[bucket] += shift;
    for (std::size_t block = 0; block < blocks; ++block)
      offsets[block][bucket - 1] += shift;
  }
  std::vector<std::uint32_t> output(keys.size());
  ParallelFor(0, blocks, [&](std::size_t block) {
    auto positions = offsets[block];
    const auto end = std::min(keys.size(), (block + 1) * block_size);
    for (auto i = block * block_size; i < end; ++i) {
      const auto bucket =
          std::upper_bound(splitters.begin(), splitters.end(), keys[i]) -
          splitters.begin();
      output[positions[bucket]++] = keys[i];
    }
  });
  ParallelFor(0, buckets, [&](std::size_t bucket) {
    std::sort(output.begin() + bucket_starts[bucket],
              output.begin() + bucket_starts[bucket + 1]);
  });
  if (metrics) {
    std::size_t largest_bucket = 0;
    for (std::size_t bucket = 0; bucket < buckets; ++bucket)
      largest_bucket = std::max(largest_bucket, bucket_starts[bucket + 1] -
                                                    bucket_starts[bucket]);
    *metrics = {buckets, largest_bucket};
  }
  return output;
}

} // namespace scheduler_eval
