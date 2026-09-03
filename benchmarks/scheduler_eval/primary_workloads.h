// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "common.h"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace scheduler_eval {

struct Point {
  double x{};
  double y{};
};

enum class PointKind { UniformSquare, InDisk, OnCircle, Kuzmin };
enum class KeyKind { Uniform, Exponential, DuplicateHeavy, AlmostSorted };

struct ConvexHullMetrics {
  std::size_t hull_vertices{};
  std::size_t merge_passes{};
};

struct DedupMetrics {
  std::size_t unique_items{};
  std::uint64_t hash_probes{};
  std::size_t table_capacity{};
};

struct RadixSortMetrics {
  std::size_t passes{};
};

struct SampleSortMetrics {
  std::size_t buckets{};
  std::size_t largest_bucket{};
};

std::vector<Point> MakePoints(PointKind kind, std::size_t size,
                              std::uint64_t seed = 1);
std::vector<Point> ConvexHullSerial(std::vector<Point> points);
std::vector<Point>
ConvexHullParallel(const std::vector<Point> &points,
                   ConvexHullMetrics *metrics = nullptr);

std::vector<std::uint32_t> MakeKeys(KeyKind kind, std::size_t size,
                                    std::uint64_t seed = 1);
std::vector<std::uint32_t>
RemoveDuplicatesSerial(std::vector<std::uint32_t> keys);
std::vector<std::uint32_t>
RemoveDuplicatesParallel(const std::vector<std::uint32_t> &keys,
                         DedupMetrics *metrics = nullptr);
std::vector<std::uint32_t> RadixSortSerial(std::vector<std::uint32_t> keys);
std::vector<std::uint32_t>
RadixSortParallel(const std::vector<std::uint32_t> &keys,
                  RadixSortMetrics *metrics = nullptr);
std::vector<std::uint32_t> SampleSortSerial(std::vector<std::uint32_t> keys);
std::vector<std::uint32_t>
SampleSortParallel(const std::vector<std::uint32_t> &keys,
                   SampleSortMetrics *metrics = nullptr);

} // namespace scheduler_eval
