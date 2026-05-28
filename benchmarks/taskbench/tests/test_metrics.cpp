#include "taskbench_metrics.h"

#include <cassert>
#include <cmath>
#include <vector>

namespace tb = oox_bench::taskbench;

int main() {
  std::vector<tb::SweepPoint> points{
      {1.0, 10.0},
      {10.0, 40.0},
      {100.0, 100.0},
  };
  const auto result = tb::compute_metg(points, 0.5);
  assert(result.found);
  assert(result.metg_us > 10.0);
  assert(result.metg_us < 100.0);
  assert(std::abs(result.max_efficiency - 1.0) < 1e-12);

  const auto missing = tb::compute_metg({{1.0, 1.0}, {2.0, 2.0}}, 1.5);
  assert(!missing.found);
  assert(std::abs(missing.max_efficiency - 1.0) < 1e-12);
}
