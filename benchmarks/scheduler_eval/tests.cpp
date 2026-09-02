// SPDX-License-Identifier: Apache-2.0

#include "common.h"
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
    for (const auto order : {scheduler_eval::RowOrder::Sorted,
                             scheduler_eval::RowOrder::Shuffled,
                             scheduler_eval::RowOrder::Shifted}) {
      const auto matrix =
          scheduler_eval::MakeSparseMatrix(31, 47, kind, 1, order);
      std::vector<double> input(matrix.columns, 2.0), actual;
      scheduler_eval::Spmv(matrix, input, actual);
      const auto expected = scheduler_eval::SpmvSerial(matrix, input);
      for (std::size_t i = 0; i < actual.size(); ++i)
        if (!Close(actual[i], expected[i]))
          return false;
    }
  }
  return true;
}

// A permutation must preserve the multiset of per-row costs exactly.
bool CheckPermutationPreservesWork() {
  const auto sorted = scheduler_eval::MakeSparseMatrix(
      257, 4099, scheduler_eval::SparseKind::Hyperbolic, 1,
      scheduler_eval::RowOrder::Sorted);
  for (const auto order : {scheduler_eval::RowOrder::Shuffled,
                           scheduler_eval::RowOrder::Shifted}) {
    const auto permuted = scheduler_eval::MakeSparseMatrix(
        257, 4099, scheduler_eval::SparseKind::Hyperbolic, 1, order);
    if (permuted.values.size() != sorted.values.size())
      return false;
    std::vector<std::size_t> a, b;
    for (std::size_t r = 0; r < sorted.rows; ++r) {
      a.push_back(sorted.row_index[r + 1] - sorted.row_index[r]);
      b.push_back(permuted.row_index[r + 1] - permuted.row_index[r]);
    }
    std::sort(a.begin(), a.end());
    std::sort(b.begin(), b.end());
    if (a != b)
      return false;
    // And must actually move heavy rows away from the front.
    if (order != scheduler_eval::RowOrder::Sorted &&
        permuted.row_index[1] == sorted.row_index[1] &&
        permuted.row_index[2] == sorted.row_index[2])
      return false;
  }
  return true;
}

} // namespace

int main() {
  scheduler_eval::Initialize();
  const std::vector<double> values(1003, 1.25);
  const bool ok = CheckScan() && CheckSpmv() &&
                  CheckPermutationPreservesWork() &&
                  Close(scheduler_eval::BlockedReduce(values, 37), 1253.75);
  if (!ok)
    std::cerr << "scheduler evaluation correctness test failed\n";
  return ok ? 0 : 1;
}
