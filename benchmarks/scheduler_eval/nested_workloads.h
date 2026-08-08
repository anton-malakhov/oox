// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "common.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <vector>

namespace scheduler_eval {

struct DenseMatrix {
  DenseMatrix(std::size_t rows, std::size_t columns)
      : rows(rows), columns(columns), values(rows * columns) {}
  double &operator()(std::size_t row, std::size_t column) {
    return values[row * columns + column];
  }
  double operator()(std::size_t row, std::size_t column) const {
    return values[row * columns + column];
  }
  std::size_t rows;
  std::size_t columns;
  std::vector<double> values;
};

inline void Multiply(const DenseMatrix &left, const DenseMatrix &right,
                     DenseMatrix &output) {
  assert(left.columns == right.rows && output.rows == left.rows &&
         output.columns == right.columns);
  ParallelFor(0, output.rows, [&](std::size_t row) {
    ParallelFor(0, output.columns, [&](std::size_t column) {
      double sum = 0;
      for (std::size_t i = 0; i < left.columns; ++i)
        sum += left(row, i) * right(i, column);
      output(row, column) = sum;
    });
  });
}

inline void Transpose(const DenseMatrix &input, DenseMatrix &output,
                      std::size_t blocks = 16) {
  assert(input.rows == output.columns && input.columns == output.rows);
  const auto row_blocks = std::min(blocks, input.rows);
  const auto column_blocks = std::min(blocks, input.columns);
  const auto row_size = (input.rows + row_blocks - 1) / row_blocks;
  const auto column_size = (input.columns + column_blocks - 1) / column_blocks;
  ParallelFor(0, row_blocks, [&](std::size_t row_block) {
    ParallelFor(0, column_blocks, [&](std::size_t column_block) {
      for (std::size_t row = row_block * row_size;
           row < std::min(input.rows, (row_block + 1) * row_size); ++row)
        for (std::size_t column = column_block * column_size;
             column < std::min(input.columns, (column_block + 1) * column_size);
             ++column)
          output(column, row) = input(row, column);
    });
  });
}

inline DenseMatrix MakeDenseMatrix(std::size_t rows, std::size_t columns) {
  DenseMatrix matrix(rows, columns);
  for (std::size_t row = 0; row < rows; ++row)
    for (std::size_t column = 0; column < columns; ++column)
      matrix(row, column) =
          static_cast<double>((row * 17 + column * 13) % 23) / 7.0;
  return matrix;
}

} // namespace scheduler_eval
