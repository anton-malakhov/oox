// SPDX-License-Identifier: Apache-2.0

#include "benchmarks/eigen/intrusive_ptr.h"
#include "common.h"
#include "graph_workloads.h"
#include "synthetic_workloads.h"
#include "workloads.h"

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
  for (const auto kind : {scheduler_eval::GraphKind::Tree,
                          scheduler_eval::GraphKind::ParallelChains,
                          scheduler_eval::GraphKind::Phases,
                          scheduler_eval::GraphKind::SquareGrid}) {
    const auto graph = scheduler_eval::MakeGraph(kind, 1024);
    const auto expected = scheduler_eval::BfsSerial(graph);
    if (scheduler_eval::BfsFlat(graph) != expected ||
        scheduler_eval::BfsNested(graph, 8) != expected)
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
                  Close(scheduler_eval::BlockedReduce(values, 37), 1253.75);
  if (!ok)
    std::cerr << "scheduler evaluation correctness test failed\n";
  return ok ? 0 : 1;
}
