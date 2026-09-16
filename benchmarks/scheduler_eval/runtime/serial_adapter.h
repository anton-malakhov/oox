// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "benchmarks/eigen/util.h"

inline int GetThreadIndex() { return 0; }

template <typename F>
void ParallelFor(std::size_t first, std::size_t last, F &&body,
                 std::size_t = 1) {
  for (auto i = first; i < last; ++i)
    body(i);
}

inline void InitParallel(std::size_t) {}
