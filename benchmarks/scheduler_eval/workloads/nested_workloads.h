// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "common.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
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

// Outer levels branch by fanout; the innermost loop covers every remaining item.
template <typename F>
void DeepNestedFor(std::size_t begin, std::size_t end, std::size_t depth,
                   std::size_t fanout, const F &body) {
  assert(fanout > 0);
  if (begin >= end)
    return;
  if (depth == 0) {
    for (auto i = begin; i < end; ++i)
      body(i);
    return;
  }
  const auto children = depth == 1 ? end - begin : std::min(fanout, end - begin);
  const auto block = (end - begin) / children;
  const auto extra = (end - begin) % children;
  EvalParallelFor(
      0, children,
      [&](std::size_t child) {
        const auto first = begin + child * block + std::min(child, extra);
        DeepNestedFor(first, first + block + (child < extra), depth - 1, fanout,
                      body);
      },
      1);
}

// Both shapes have exactly five ParallelFor levels and 16384 output leaves.
// NarrowAncestors has no nonleaf range wider than eight; BatchedAncestor adds
// a width-32 nonleaf range, where lazy local batching can reserve siblings.
// Their different numbers of inner roots make them separate workload cases,
// rather than a pure measurement of the cost of batching.
enum class NestedFiveShape { NarrowAncestors, BatchedAncestor };
enum class NestedFivePattern {
  Balanced,
  EarlyInner,
  LateInner,
  EarlyLeaf,
  LateLeaf
};

struct NestedFiveConfig {
  NestedFiveShape shape = NestedFiveShape::NarrowAncestors;
  NestedFivePattern pattern = NestedFivePattern::Balanced;
  // Only the second level changes grain. All other levels use grain one.
  std::size_t ancestor_grain = 1;
};

inline constexpr std::size_t NestedFiveLeafCount = 16384;
inline constexpr std::size_t NestedFiveWorkSteps = 1 << 20;

inline std::array<std::size_t, 5> NestedFiveExtents(NestedFiveShape shape) {
  return shape == NestedFiveShape::NarrowAncestors
             ? std::array<std::size_t, 5>{8, 4, 4, 4, 32}
             : std::array<std::size_t, 5>{8, 32, 4, 4, 4};
}

inline std::size_t NestedFiveInnerSpan(NestedFiveShape shape) {
  const auto extents = NestedFiveExtents(shape);
  return extents[2] * extents[3] * extents[4];
}

inline std::size_t NestedFiveEarlyInnerFirst(NestedFiveShape shape) {
  // On a width-32 owner range, callback three can begin a grown local batch
  // [3,7): four through six may be reserved while seven and later stay public.
  // This placement does not force any particular steal or execution order.
  return shape == NestedFiveShape::BatchedAncestor
             ? 3 * NestedFiveInnerSpan(shape)
             : 0;
}

inline std::size_t NestedFiveLeafSteps(std::size_t index,
                                     const NestedFiveConfig &config) {
  assert(index < NestedFiveLeafCount);
  if (config.pattern == NestedFivePattern::Balanced)
    return NestedFiveWorkSteps / NestedFiveLeafCount;
  // The varying base sums to 188416 steps. Every skew placement distributes
  // the same remaining 860160 steps, so all five patterns do equal total work.
  constexpr auto extra = NestedFiveWorkSteps - NestedFiveLeafCount * 23 / 2;
  const auto base = 8 + index % 8;
  const auto span = NestedFiveInnerSpan(config.shape);
  const auto first = NestedFiveEarlyInnerFirst(config.shape);
  switch (config.pattern) {
  case NestedFivePattern::EarlyInner:
    return base + (index >= first && index - first < span ? extra / span : 0);
  case NestedFivePattern::LateInner:
    return base + (index >= NestedFiveLeafCount - span ? extra / span : 0);
  case NestedFivePattern::EarlyLeaf:
    return base + (index == first + 3 ? extra : 0);
  case NestedFivePattern::LateLeaf:
    return base + (index == NestedFiveLeafCount - 5 ? extra : 0);
  case NestedFivePattern::Balanced:
    break;
  }
  return base;
}

inline std::uint64_t NestedFiveLeafValue(std::size_t index,
                                       const NestedFiveConfig &config) {
  std::uint64_t value = (std::uint64_t(index) + 0x9e3779b97f4a7c15ULL) ^
                        ((std::uint64_t(index / 32) + 1) *
                         0xd1b54a32d192ed03ULL);
  for (std::size_t step = 0, steps = NestedFiveLeafSteps(index, config);
       step < steps; ++step)
    value = (value ^ (value >> 27)) * 0x3c79ac492ba7b653ULL + step +
            (index & 31);
  // Zero is reserved for an output poison value in the benchmark/oracle.
  return value | 1;
}

template <typename EmitLeaf>
void NestedFiveFor(const NestedFiveConfig &config, const EmitLeaf &emit_leaf) {
  assert(config.ancestor_grain > 0);
  const auto extents = NestedFiveExtents(config.shape);
  EvalParallelFor(0, extents[0], [&](std::size_t a) {
    EvalParallelFor(0, extents[1], [&](std::size_t b) {
      EvalParallelFor(0, extents[2], [&](std::size_t c) {
        EvalParallelFor(0, extents[3], [&](std::size_t d) {
          EvalParallelFor(0, extents[4], [&](std::size_t e) {
            const auto index =
                (((a * extents[1] + b) * extents[2] + c) * extents[3] + d) *
                    extents[4] + e;
            emit_leaf(index, NestedFiveLeafValue(index, config));
          }, 1);
        }, 1);
      }, 1);
    }, config.ancestor_grain);
  }, 1);
}

inline void NestedFive(const NestedFiveConfig &config,
                       std::vector<std::uint64_t> &output) {
  assert(output.size() == NestedFiveLeafCount);
  NestedFiveFor(config, [&](std::size_t index, std::uint64_t value) {
    output[index] = value;
  });
}

inline void Multiply(const DenseMatrix &left, const DenseMatrix &right,
                     DenseMatrix &output) {
  assert(left.columns == right.rows && output.rows == left.rows &&
         output.columns == right.columns);
  EvalParallelFor(0, output.rows, [&](std::size_t row) {
    EvalParallelFor(0, output.columns, [&](std::size_t column) {
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
  EvalParallelFor(0, row_blocks, [&](std::size_t row_block) {
    EvalParallelFor(0, column_blocks, [&](std::size_t column_block) {
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
