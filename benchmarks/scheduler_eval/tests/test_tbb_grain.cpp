// SPDX-License-Identifier: Apache-2.0
#include <tbb/tbb.h>
#include <cstddef>
#include <iostream>
#include <limits>
#include <vector>

namespace grain_test {
struct Capture { size_t begin = 0, end = 0, grain = 0, calls = 0; };
Capture capture;
}

// Observe the real blocked_range at our wrapper's TBB boundary.
namespace tbb {
template <typename Body>
void parallel_for(const blocked_range<size_t> &range, const Body &body,
                  const simple_partitioner &, task_group_context &) {
  grain_test::capture = {range.begin(), range.end(), range.grainsize(),
                         grain_test::capture.calls + 1};
  if (!range.empty()) body(range);
}
}

#define TBB_MODE TBB_SIMPLE
#include "benchmarks/eigen/parallel_for.h"

int main() {
  struct Case { size_t begin, end, grain; };
  constexpr size_t seed = 0x41b5;
  constexpr size_t maximum = std::numeric_limits<size_t>::max();
  constexpr Case edges[] = {
      {0, 1009, 64}, {7, 7, 0}, {11, 12, 0}, {5, 70, 7},
      {1021, 1088, 9}, {100, 101, 1048576},
      {0, 262147, 128}, {maximum - 17, maximum, 3}};
  for (size_t index = 0; index < 8 + 128 * 8; ++index) {
    const size_t first = (seed + index * 17) % 1024;
    const size_t length = index < 8 ? 0 : (index - 8) / 8;
    const Case c = index < 8 ? edges[index] :
        Case{first, first + length, (index - 8) % 8};
    std::vector<unsigned> actual(c.end - c.begin), expected(c.end - c.begin);
    for (size_t i = c.begin; i < c.end; ++i) ++expected[i - c.begin];
    grain_test::capture = {};
    bool outside = false;
    ParallelFor(c.begin, c.end, [&](size_t i) {
      if (i < c.begin || i >= c.end) { outside = true; return; }
      ++actual[i - c.begin];
    }, c.grain);
    const auto &seen = grain_test::capture;
    if (seen.calls != 1 || seen.begin != c.begin || seen.end != c.end ||
        seen.grain != (c.grain ? c.grain : 1) || outside || actual != expected) {
      std::cerr << "seed=" << seed << " case=" << index
                << " range=[" << c.begin << ',' << c.end << ") requested="
                << c.grain << " observed=" << seen.grain
                << " intercepted=" << seen.calls << " outside=" << outside << '\n';
      return 1;
    }
  }
}
