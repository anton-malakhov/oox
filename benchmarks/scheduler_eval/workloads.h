// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "common.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <random>
#include <vector>

namespace scheduler_eval {

enum class SparseKind { Balanced, Hyperbolic, Triangle };

struct SparseMatrix {
  std::size_t rows{};
  std::size_t columns{};
  std::vector<double> values;
  std::vector<std::size_t> column_index;
  std::vector<std::size_t> row_index;
};

inline SparseMatrix MakeSparseMatrix(std::size_t rows, std::size_t columns,
                                     SparseKind kind, std::uint64_t seed = 1) {
  SparseMatrix matrix;
  matrix.rows = rows;
  matrix.columns = columns;
  matrix.row_index.push_back(0);
  std::mt19937_64 random(seed);
  double harmonic = 0;
  for (std::size_t i = 1; i <= rows; ++i)
    harmonic += 1.0 / i;
  const auto average = std::max<std::size_t>(1, (columns + 8) / 9);
  for (std::size_t row = 0; row < rows; ++row) {
    std::size_t count = average;
    if (kind == SparseKind::Hyperbolic) {
      count = std::max<std::size_t>(
          1, static_cast<std::size_t>(average * rows / harmonic / (row + 1)));
    } else if (kind == SparseKind::Triangle) {
      count = std::max<std::size_t>(1, 2 * average * (rows - row) / (rows + 1));
    }
    const std::size_t width =
        kind == SparseKind::Triangle
            ? std::max<std::size_t>(1, columns * (row + 1) / rows)
            : columns;
    count = std::min(count, width);
    for (std::size_t entry = 0; entry < count; ++entry) {
      matrix.values.push_back(0.5 + static_cast<double>((row + entry) % 17));
      const auto segment = std::max<std::size_t>(1, width / count);
      matrix.column_index.push_back(std::min(
          width - 1,
          entry * segment + static_cast<std::size_t>(random() % segment)));
    }
    matrix.row_index.push_back(matrix.values.size());
  }
  return matrix;
}

inline void Spmv(const SparseMatrix &matrix, const std::vector<double> &input,
                 std::vector<double> &output) {
  assert(input.size() == matrix.columns);
  output.resize(matrix.rows);
  ParallelFor(0, matrix.rows, [&](std::size_t row) {
    double sum = 0;
    for (std::size_t i = matrix.row_index[row]; i < matrix.row_index[row + 1];
         ++i)
      sum += matrix.values[i] * input[matrix.column_index[i]];
    output[row] = sum;
  });
}

inline std::vector<double> SpmvSerial(const SparseMatrix &matrix,
                                      const std::vector<double> &input) {
  std::vector<double> output(matrix.rows);
  for (std::size_t row = 0; row < matrix.rows; ++row)
    for (std::size_t i = matrix.row_index[row]; i < matrix.row_index[row + 1];
         ++i)
      output[row] += matrix.values[i] * input[matrix.column_index[i]];
  return output;
}

inline double BlockedReduce(const std::vector<double> &data,
                            std::size_t block_size) {
  const auto blocks = (data.size() + block_size - 1) / block_size;
  std::vector<double> partial(blocks);
  ParallelFor(0, blocks, [&](std::size_t block) {
    double sum = 0;
    for (auto i = block * block_size;
         i < std::min(data.size(), (block + 1) * block_size); ++i)
      sum += data[i];
    partial[block] = sum;
  });
  return std::accumulate(partial.begin(), partial.end(), 0.0);
}

inline void ExclusiveScan(std::vector<std::uint64_t> &data) {
  const std::size_t size = data.size();
  assert(size && (size & (size - 1)) == 0);
  for (std::size_t stride = 2; stride <= size; stride <<= 1) {
    ParallelFor(0, size / stride, [&](std::size_t block) {
      const auto end = (block + 1) * stride - 1;
      data[end] += data[end - stride / 2];
    });
  }
  data.back() = 0;
  for (std::size_t stride = size; stride >= 2; stride >>= 1) {
    ParallelFor(0, size / stride, [&](std::size_t block) {
      const auto end = (block + 1) * stride - 1;
      const auto left = end - stride / 2;
      const auto value = data[left];
      data[left] = data[end];
      data[end] += value;
    });
  }
}

} // namespace scheduler_eval
