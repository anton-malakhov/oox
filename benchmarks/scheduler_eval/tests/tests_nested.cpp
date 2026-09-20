// SPDX-License-Identifier: Apache-2.0

#include "common.h"
#include "nested_workloads.h"

#include <atomic>
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

bool CheckDeepNesting() {
  std::size_t case_index = 0;
  const auto check = [&](std::size_t first, std::size_t size,
                         std::size_t depth, std::size_t fanout) {
    ++case_index;
    std::vector<std::atomic<std::uint64_t>> visits(size), values(size);
    std::atomic<bool> outside{false};
    scheduler_eval::DeepNestedFor(first, first + size, depth, fanout,
                                 [&](std::size_t i) {
      if (i < first || i - first >= size) {
        outside.store(true, std::memory_order_relaxed);
        return;
      }
      visits[i - first].fetch_add(1, std::memory_order_relaxed);
      values[i - first].fetch_add(
          (std::uint64_t(i) + 1) * (std::uint64_t(i) + 3),
          std::memory_order_relaxed);
    });
    bool ok = !outside.load(std::memory_order_relaxed);
    // Flat serial oracle, independent of recursive subdivision and scheduling.
    for (std::size_t offset = 0; offset < size; ++offset) {
      const auto i = std::uint64_t(first + offset);
      ok &= visits[offset].load(std::memory_order_relaxed) == 1 &&
            values[offset].load(std::memory_order_relaxed) == (i + 1) * (i + 3);
    }
    if (!ok)
      std::cerr << "deep nesting case=" << case_index << " first=" << first
                << " size=" << size << " depth=" << depth
                << " fanout=" << fanout << '\n';
    return ok;
  };
  for (std::size_t size : {0, 1, 2, 7, 31, 257})
    for (std::size_t depth : {0, 1, 2, 4, 8, 16})
      for (std::size_t fanout : {1, 2, 3, 4}) {
        if (!check(5, size, depth, fanout) ||
            !check(std::size_t(-1) - size, size, depth, fanout))
          return false;
      }
  for (std::size_t depth : {2, 4, 8})
    if (!check(0, 1 << 16, depth, 2) || !check(0, 1 << 16, depth, 4))
      return false;
  return check(0, 1 << 16, 16, 2) && check(11, (1 << 16) + 3, 8, 3);
}

} // namespace

int main() {
  scheduler_eval::Initialize();
  const bool ok = CheckMatrices() && CheckDeepNesting();
  if (!ok)
    std::cerr << "nested scheduler evaluation correctness test failed\n";
  return ok ? 0 : 1;
}
