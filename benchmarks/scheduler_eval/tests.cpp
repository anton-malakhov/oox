// SPDX-License-Identifier: Apache-2.0

#include "benchmarks/eigen/intrusive_ptr.h"
#include "common.h"
#include "granularity_control.h"
#include "graph_workloads.h"
#include "primary_workloads.h"
#include "synthetic_workloads.h"
#include "workloads.h"

#ifdef EIGEN_MODE
#include "benchmarks/eigen/eigen_pool.h"
#endif

#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <vector>

namespace {

bool Close(double left, double right) {
  return std::abs(left - right) <= 1e-9 * std::max(1.0, std::abs(right));
}

bool Report(const char *name, bool ok) {
  if (!ok)
    std::cerr << name << " failed\n";
  return ok;
}

bool CheckScan() {
  std::vector<std::uint64_t> values(1024, 1);
  scheduler_eval::ExclusiveScan(values);
  for (std::size_t i = 0; i < values.size(); ++i)
    if (values[i] != i)
      return false;
  return true;
}

bool CheckSpmv() {
  for (const auto kind : {scheduler_eval::SparseKind::Balanced,
                          scheduler_eval::SparseKind::Hyperbolic,
                          scheduler_eval::SparseKind::Triangle}) {
    const auto matrix = scheduler_eval::MakeSparseMatrix(31, 47, kind);
    std::vector<double> input(matrix.columns, 2.0), actual;
    scheduler_eval::Spmv(matrix, input, actual);
    const auto expected = scheduler_eval::SpmvSerial(matrix, input);
    for (std::size_t i = 0; i < actual.size(); ++i)
      if (!Close(actual[i], expected[i]))
        return false;
  }
  return true;
}

#ifdef SCHEDULER_EVAL_HAS_NESTED_BFS
bool CheckBfs() {
  const scheduler_eval::CsrGraph empty;
  if (!scheduler_eval::BfsSerial(empty).empty() ||
      !scheduler_eval::BfsFlat(empty).empty() ||
      !scheduler_eval::BfsNested(empty, 8).empty() ||
      !scheduler_eval::BfsAdaptive(empty, std::chrono::microseconds(20))
           .empty())
    return false;
  for (const auto kind :
       {scheduler_eval::GraphKind::Tree,
        scheduler_eval::GraphKind::RandomArity100,
        scheduler_eval::GraphKind::ParallelChains,
        scheduler_eval::GraphKind::Phases,
        scheduler_eval::GraphKind::Phases10Degree2,
        scheduler_eval::GraphKind::Phases50Degree5,
        scheduler_eval::GraphKind::TrunkFirst, scheduler_eval::GraphKind::Rmat,
        scheduler_eval::GraphKind::SquareGrid,
        scheduler_eval::GraphKind::CubeGrid,
        scheduler_eval::GraphKind::SmallWorld}) {
    const auto graph = scheduler_eval::MakeGraph(kind, 1024);
    const auto expected = scheduler_eval::BfsSerial(graph);
    if (scheduler_eval::BfsFlat(graph) != expected ||
        scheduler_eval::BfsNested(graph, 8) != expected ||
        scheduler_eval::BfsAdaptive(graph, std::chrono::microseconds(20)) !=
            expected)
      return false;
  }
  return true;
}
#endif

bool CheckSyntheticCosts() {
  using scheduler_eval::CostKind;
  for (const auto kind :
       {CostKind::Constant, CostKind::Uniform, CostKind::Exponential,
        CostKind::Pareto, CostKind::Linear, CostKind::Clustered,
        CostKind::Periodic, CostKind::Shuffled, CostKind::PhaseChanging}) {
    const auto costs = scheduler_eval::MakeIterationCosts(kind, 2048, 17);
    if (scheduler_eval::RunCostLoop(costs) !=
        scheduler_eval::RunCostLoopSerial(costs))
      return false;
  }
  return true;
}

bool CheckPrimaryWorkloads() {
  using scheduler_eval::KeyKind;
  using scheduler_eval::PointKind;
  for (const auto kind : {PointKind::UniformSquare, PointKind::InDisk,
                          PointKind::OnCircle, PointKind::Kuzmin}) {
    for (const auto size :
         std::array<std::size_t, 7>{0, 1, 2, 3, 17, 257, 4099}) {
      const auto points = scheduler_eval::MakePoints(kind, size, 29 + size);
      auto expected = scheduler_eval::ConvexHullSerial(points);
      scheduler_eval::ConvexHullMetrics metrics;
      auto actual = scheduler_eval::ConvexHullParallel(points, &metrics);
      const auto less = [](const auto &left, const auto &right) {
        return left.x < right.x || (left.x == right.x && left.y < right.y);
      };
      std::sort(expected.begin(), expected.end(), less);
      std::sort(actual.begin(), actual.end(), less);
      if (expected.size() != actual.size() ||
          metrics.hull_vertices != actual.size() ||
          (size > 2048 && metrics.merge_passes == 0))
        return false;
      for (std::size_t i = 0; i < expected.size(); ++i)
        if (expected[i].x != actual[i].x || expected[i].y != actual[i].y)
          return false;
    }
  }
  for (const auto kind : {KeyKind::Uniform, KeyKind::Exponential,
                          KeyKind::DuplicateHeavy, KeyKind::AlmostSorted,
                          KeyKind::ReverseSorted}) {
    for (const auto size :
         std::array<std::size_t, 7>{0, 1, 2, 7, 255, 2047, 4099}) {
      const auto keys = scheduler_eval::MakeKeys(kind, size, 41 + size);
      const auto pairs =
          scheduler_eval::MakeKeyValues(kind, size, 41 + size);
      scheduler_eval::DedupMetrics dedup_metrics;
      scheduler_eval::RadixSortMetrics radix_metrics;
      scheduler_eval::RadixSortMetrics pair_metrics;
      scheduler_eval::SampleSortMetrics sample_metrics;
      const auto dedup =
          scheduler_eval::RemoveDuplicatesParallel(keys, &dedup_metrics);
      const auto radix =
          scheduler_eval::RadixSortParallel(keys, &radix_metrics);
      const auto radix_pairs =
          scheduler_eval::RadixSortPairsParallel(pairs, &pair_metrics);
      const auto sample =
          scheduler_eval::SampleSortParallel(keys, &sample_metrics);
      if (dedup != scheduler_eval::RemoveDuplicatesSerial(keys) ||
          radix != scheduler_eval::RadixSortSerial(keys) ||
          radix_pairs != scheduler_eval::RadixSortPairsSerial(pairs) ||
          sample != scheduler_eval::SampleSortSerial(keys) ||
          dedup_metrics.unique_items != dedup.size() ||
          dedup_metrics.hash_probes < keys.size() ||
          (size > 0 && dedup_metrics.table_capacity < 2 * size) ||
          radix_metrics.passes != (size == 0 ? 0u : 4u) ||
          pair_metrics.passes != (size == 0 ? 0u : 4u) ||
          sample_metrics.buckets != (size == 0 ? 0u
                                               : std::min<std::size_t>(
                                                     256, (size + 2047) / 2048)) ||
          sample_metrics.largest_bucket > size)
        return false;
    }
  }
  return true;
}

bool CheckGranularityEstimator() {
  scheduler_eval::GranularityEstimator estimator(std::chrono::microseconds(20));
  if (!estimator.IsSmall(1) || estimator.IsSmall(2))
    return false;
  estimator.Report(64, std::chrono::microseconds(10));
  if (estimator.SequentialComplexityLimit() != 64 || !estimator.IsSmall(115) ||
      estimator.IsSmall(116))
    return false;
  estimator.Report(128, std::chrono::microseconds(21));
  return estimator.SequentialComplexityLimit() == 64;
}

bool CheckSchedulerMetrics() {
#ifdef EIGEN_MODE
  const auto before = EigenPool().GetStatistics();
  std::atomic<bool> completed{false};
  EigenPoolWrapper scheduler;
  scheduler.run(
      [&] { completed.store(true, std::memory_order_release); });
  while (!completed.load(std::memory_order_acquire))
    scheduler.execute_something_else();
  const auto after = EigenPool().GetStatistics();
  return after.scheduled - before.scheduled == 1 &&
         after.executed - before.executed == 1;
#else
  return true;
#endif
}

struct PointerValue : intrusive_ref_counter<PointerValue> {};

bool CheckIntrusivePtrOrdering() {
  IntrusivePtr<PointerValue> left(new PointerValue);
  IntrusivePtr<PointerValue> right(new PointerValue);
  return left < right || right < left;
}

} // namespace

int main() {
  scheduler_eval::Initialize();
  const std::vector<double> values(1003, 1.25);
  const bool bfs_ok =
#ifdef SCHEDULER_EVAL_HAS_NESTED_BFS
      CheckBfs();
#else
      true;
#endif
  bool ok = Report("scan", CheckScan());
  ok &= Report("spmv", CheckSpmv());
  ok &= Report("bfs", bfs_ok);
  ok &= Report("synthetic costs", CheckSyntheticCosts());
  ok &= Report("primary workloads", CheckPrimaryWorkloads());
  ok &= Report("intrusive pointer ordering", CheckIntrusivePtrOrdering());
  ok &= Report("granularity estimator", CheckGranularityEstimator());
  ok &= Report("scheduler metrics", CheckSchedulerMetrics());
  ok &= Report("blocked reduce",
               Close(scheduler_eval::BlockedReduce(values, 37), 1253.75));
  if (!ok)
    std::cerr << "scheduler evaluation correctness test failed\n";
  return ok ? 0 : 1;
}
