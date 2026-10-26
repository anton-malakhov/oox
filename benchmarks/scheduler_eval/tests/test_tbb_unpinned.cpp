// SPDX-License-Identifier: Apache-2.0
#include "benchmarks/eigen/parallel_for.h"
#include <array>

// Under LeakSanitizer this also checks that an unpinned startup does not
// register an unused pinning observer whose proxy survives process shutdown.
int main() {
  InitParallel(static_cast<size_t>(GetNumThreads()));
  std::array<int, 64> values{};
  ParallelFor(0, values.size(), [&](size_t i) { values[i] = int(i + 1); });
  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i] != int(i + 1)) {
      std::cerr << "unpinned TBB index=" << i << " expected=" << i + 1
                << " actual=" << values[i] << '\n';
      return 1;
    }
  }
}
