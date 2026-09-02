// SPDX-License-Identifier: Apache-2.0

#include "common.h"
#include "workloads.h"
#include "benchmarks/eigen/intrusive_ptr.h"

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
  const bool ok = CheckScan() && CheckSpmv() && CheckIntrusivePtrOrdering() &&
                  Close(scheduler_eval::BlockedReduce(values, 37), 1253.75);
  if (!ok)
    std::cerr << "scheduler evaluation correctness test failed\n";
  return ok ? 0 : 1;
}
