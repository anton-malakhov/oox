// SPDX-License-Identifier: Apache-2.0

#include "common.h"
#include "nested_workloads.h"

#include <cmath>
#include <cstddef>
#include <iostream>

namespace {

bool Close(double left, double right) {
  return std::abs(left - right) <= 1e-9 * std::max(1.0, std::abs(right));
}

bool CheckMatrices() {
  auto left = scheduler_eval::MakeDenseMatrix(7, 5);
  auto right = scheduler_eval::MakeDenseMatrix(5, 9);
  scheduler_eval::DenseMatrix actual(7, 9);
  scheduler_eval::Multiply(left, right, actual);
  for (std::size_t row = 0; row < actual.rows; ++row)
    for (std::size_t column = 0; column < actual.columns; ++column) {
      double expected = 0;
      for (std::size_t i = 0; i < left.columns; ++i)
        expected += left(row, i) * right(i, column);
      if (!Close(actual(row, column), expected))
        return false;
    }
  scheduler_eval::DenseMatrix transposed(left.columns, left.rows);
  scheduler_eval::Transpose(left, transposed, 3);
  for (std::size_t row = 0; row < left.rows; ++row)
    for (std::size_t column = 0; column < left.columns; ++column)
      if (!Close(left(row, column), transposed(column, row)))
        return false;
  return true;
}

} // namespace

int main() {
  scheduler_eval::Initialize();
  const bool ok = CheckMatrices();
  if (!ok)
    std::cerr << "nested scheduler evaluation correctness test failed\n";
  return ok ? 0 : 1;
}
