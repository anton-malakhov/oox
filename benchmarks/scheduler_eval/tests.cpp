// SPDX-License-Identifier: Apache-2.0

#include "benchmarks/eigen/intrusive_ptr.h"
#include "common.h"
#include "granularity_control.h"
#include "graph_workloads.h"
#include "synthetic_workloads.h"
#include "workloads.h"

#ifdef EIGEN_MODE
#include "benchmarks/eigen/eigen_pool.h"
#endif

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
  std::atomic<std::size_t> completed{0};
  ParallelFor(0, 4096, [&](std::size_t) { ++completed; });
  const auto after = EigenPool().GetStatistics();
  return completed.load() == 4096 && after.scheduled > before.scheduled &&
         after.scheduled - before.scheduled == after.executed - before.executed;
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
  const bool ok = CheckScan() && CheckSpmv() && bfs_ok &&
                  CheckSyntheticCosts() && CheckIntrusivePtrOrdering() &&
                  CheckGranularityEstimator() && CheckSchedulerMetrics() &&
                  Close(scheduler_eval::BlockedReduce(values, 37), 1253.75);
  if (!ok)
    std::cerr << "scheduler evaluation correctness test failed\n";
  return ok ? 0 : 1;
}
