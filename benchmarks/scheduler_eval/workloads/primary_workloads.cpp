// SPDX-License-Identifier: Apache-2.0

#include "primary_workloads.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <limits>
#include <memory>
#include <random>
#include <stdexcept>

namespace scheduler_eval {
namespace {

constexpr std::size_t block_size = 2048;

bool PointLess(const Point &left, const Point &right) {
  return left.x < right.x || (left.x == right.x && left.y < right.y);
}

bool PointEqual(const Point &left, const Point &right) {
  return left.x == right.x && left.y == right.y;
}

long double Cross(const Point &a, const Point &b, const Point &point) {
  const auto ax = static_cast<long double>(b.x) - a.x;
  const auto ay = static_cast<long double>(b.y) - a.y;
  const auto bx = static_cast<long double>(point.x) - a.x;
  const auto by = static_cast<long double>(point.y) - a.y;
  const auto product = ay * bx;
  return std::fma(ax, by, -product) + std::fma(-ay, bx, product);
}

std::size_t NextPowerOfTwo(std::size_t value) {
  std::size_t result = 1;
  while (result < value)
    result <<= 1;
  return result;
}

template <unsigned DigitBits = 8, typename Value, typename Key>
std::vector<Value> RadixSortParallelImpl(const std::vector<Value> &values,
                                         Key key,
                                         RadixSortMetrics *metrics) {
  if (values.empty()) {
    if (metrics)
      *metrics = {};
    return {};
  }
  std::vector<Value> input = values, output(values.size());
  const auto blocks = (values.size() + block_size - 1) / block_size;
  constexpr std::size_t bucket_count = std::size_t{1} << DigitBits;
  using Counts = std::array<std::size_t, bucket_count>;
  std::vector<Counts> counts(blocks), offsets(blocks);
  constexpr auto key_bits = sizeof(key(values.front())) * 8;
  if (metrics)
    *metrics = {};
  for (unsigned shift = 0; shift < key_bits; shift += DigitBits) {
    const auto start = metrics ? Clock::now() : Clock::time_point{};
    EvalParallelFor(0, blocks, [&](std::size_t block) {
      counts[block].fill(0);
      const auto end = std::min(input.size(), (block + 1) * block_size);
      for (auto i = block * block_size; i < end; ++i)
        ++counts[block][(key(input[i]) >> shift) & (bucket_count - 1)];
    });
    std::size_t total = 0;
    for (std::size_t bucket = 0; bucket < bucket_count; ++bucket)
      for (std::size_t block = 0; block < blocks; ++block) {
        offsets[block][bucket] = total;
        total += counts[block][bucket];
      }
    EvalParallelFor(0, blocks, [&](std::size_t block) {
      auto positions = offsets[block];
      const auto end = std::min(input.size(), (block + 1) * block_size);
      for (auto i = block * block_size; i < end; ++i)
        output[positions[(key(input[i]) >> shift) & (bucket_count - 1)]++] = input[i];
    });
    input.swap(output);
    if (metrics)
      metrics->pass_nanoseconds.push_back(Nanoseconds(start));
  }
  if (metrics)
    metrics->passes = (key_bits + DigitBits - 1) / DigitBits;
  return input;
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
  EvalParallelFor(0, blocks, [&](std::size_t block) {
    const auto begin = sorted.begin() + block * block_size;
    std::sort(begin, std::min(sorted.end(), begin + block_size), PointLess);
  });
  for (std::size_t width = block_size; width < sorted.size(); width *= 2) {
    ++merge_passes;
    const auto merges = (sorted.size() + 2 * width - 1) / (2 * width);
    EvalParallelFor(0, merges, [&](std::size_t merge) {
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
  if (kind == KeyKind::ReverseSorted) {
    for (std::size_t i = 0; i < size; ++i)
      keys[i] = static_cast<std::uint32_t>(size - i);
    return keys;
  }
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

std::vector<KeyValue> MakeKeyValues(KeyKind kind, std::size_t size,
                                    std::uint64_t seed) {
  const auto keys = MakeKeys(kind, size, seed);
  std::vector<KeyValue> pairs(size);
  for (std::size_t i = 0; i < size; ++i)
    pairs[i] = {keys[i], static_cast<std::uint32_t>(i)};
  return pairs;
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
  EvalParallelFor(0, capacity, [&](std::size_t i) { table[i].store(empty); });
  EvalParallelFor(0, keys.size(), [&](std::size_t i) {
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

std::vector<std::uint64_t> MakeKeys64(KeyKind kind, std::size_t size,
                                     std::uint64_t seed) {
  const auto high = MakeKeys(kind, size, seed);
  const auto low = MakeKeys(kind, size, seed ^ 0x9e3779b97f4a7c15ull);
  std::vector<std::uint64_t> result(size);
  for (std::size_t i = 0; i < size; ++i)
    result[i] = (std::uint64_t{high[i]} << 32) | low[i];
  return result;
}

std::vector<std::uint64_t>
RadixSort64Parallel(const std::vector<std::uint64_t> &keys,
                    RadixSortMetrics *metrics, unsigned digit_bits) {
  const auto key = [](std::uint64_t value) { return value; };
  switch (digit_bits) {
  case 4: return RadixSortParallelImpl<4>(keys, key, metrics);
  case 8: return RadixSortParallelImpl<8>(keys, key, metrics);
  case 11: return RadixSortParallelImpl<11>(keys, key, metrics);
  default: throw std::invalid_argument("radix digit width must be 4, 8 or 11");
  }
}

std::vector<KeyValue64>
RadixSort64PairsParallel(const std::vector<KeyValue64> &keys,
                         RadixSortMetrics *metrics) {
  return RadixSortParallelImpl(
      keys, [](const KeyValue64 &value) { return value.key; }, metrics);
}

std::vector<std::uint32_t>
RadixSortParallel(const std::vector<std::uint32_t> &keys,
                  RadixSortMetrics *metrics) {
  return RadixSortParallelImpl(
      keys, [](std::uint32_t key) { return key; }, metrics);
}

std::vector<KeyValue> RadixSortPairsSerial(std::vector<KeyValue> pairs) {
  std::stable_sort(pairs.begin(), pairs.end(),
                   [](const KeyValue &left, const KeyValue &right) {
                     return left.key < right.key;
                   });
  return pairs;
}

std::vector<KeyValue>
RadixSortPairsParallel(const std::vector<KeyValue> &pairs,
                       RadixSortMetrics *metrics) {
  return RadixSortParallelImpl(
      pairs, [](const KeyValue &pair) { return pair.key; }, metrics);
}

std::vector<std::uint32_t> SampleSortSerial(std::vector<std::uint32_t> keys) {
  std::sort(keys.begin(), keys.end());
  return keys;
}

template <typename Value, typename Less>
static std::vector<Value>
SampleSortParallelImpl(const std::vector<Value> &keys,
                       SampleSortMetrics *metrics, std::size_t depth, Less less) {
  if (keys.size() < block_size) {
    if (metrics)
      *metrics = {keys.empty() ? 0u : 1u, keys.size()};
    auto sorted = keys;
    std::sort(sorted.begin(), sorted.end(), less);
    return sorted;
  }
  const auto buckets =
      std::min<std::size_t>(256, (keys.size() + block_size - 1) / block_size);
  std::vector<Value> samples;
  samples.reserve(buckets * 8);
  for (std::size_t i = 0; i < buckets * 8; ++i)
    samples.push_back(keys[(i * keys.size()) / (buckets * 8)]);
  std::sort(samples.begin(), samples.end(), less);
  std::vector<Value> splitters(buckets - 1);
  for (std::size_t i = 1; i < buckets; ++i)
    splitters[i - 1] = samples[i * 8];
  const auto blocks = (keys.size() + block_size - 1) / block_size;
  std::vector<std::vector<std::size_t>> offsets(
      blocks, std::vector<std::size_t>(buckets));
  EvalParallelFor(0, blocks, [&](std::size_t block) {
    const auto end = std::min(keys.size(), (block + 1) * block_size);
    for (auto i = block * block_size; i < end; ++i)
      ++offsets[block]
               [std::upper_bound(splitters.begin(), splitters.end(), keys[i], less) -
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
  std::vector<Value> output(keys.size());
  EvalParallelFor(0, blocks, [&](std::size_t block) {
    auto positions = offsets[block];
    const auto end = std::min(keys.size(), (block + 1) * block_size);
    for (auto i = block * block_size; i < end; ++i) {
      const auto bucket =
          std::upper_bound(splitters.begin(), splitters.end(), keys[i], less) -
          splitters.begin();
      output[positions[bucket]++] = keys[i];
    }
  });
  EvalParallelFor(0, buckets, [&](std::size_t bucket) {
#ifndef RAPID_START_MODE
    const auto size = bucket_starts[bucket + 1] - bucket_starts[bucket];
    if (depth < 32 && size > 4 * block_size && size < keys.size()) {
      std::vector<Value> subproblem(
          output.begin() + bucket_starts[bucket],
          output.begin() + bucket_starts[bucket + 1]);
      auto sorted = SampleSortParallelImpl(subproblem, nullptr, depth + 1, less);
      std::copy(sorted.begin(), sorted.end(),
                output.begin() + bucket_starts[bucket]);
      return;
    }
#endif
    std::sort(output.begin() + bucket_starts[bucket],
              output.begin() + bucket_starts[bucket + 1], less);
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

std::vector<std::uint32_t>
SampleSortParallel(const std::vector<std::uint32_t> &keys,
                   SampleSortMetrics *metrics) {
  return SampleSortParallelImpl(keys, metrics, 0,
                                [](auto a, auto b) { return a < b; });
}

std::vector<std::string>
SampleSortStrings(const std::vector<std::string> &keys,
                   SampleSortMetrics *metrics) {
  return SampleSortParallelImpl(keys, metrics, 0,
      [](const auto &a, const auto &b) { return a < b; });
}

std::vector<KeyValue64>
SampleSortRecords(const std::vector<KeyValue64> &keys,
                   SampleSortMetrics *metrics) {
  return SampleSortParallelImpl(keys, metrics, 0,
      [](const auto &a, const auto &b) {
        return a.key < b.key || (a.key == b.key && a.value < b.value);
      });
}

} // namespace scheduler_eval
