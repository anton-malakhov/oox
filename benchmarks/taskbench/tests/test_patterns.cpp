#include "taskbench_graph.h"

#include <cassert>
#include <vector>

namespace tb = oox_bench::taskbench;

int main() {
  tb::Config cfg;
  cfg.height = 3;
  cfg.width = 4;
  cfg.graphs = 1;
  cfg.pattern = tb::Pattern::Stencil;
  tb::Graph stencil(cfg);
  assert((stencil.deps(1, 0) == std::vector<int>{0, 1}));
  assert((stencil.deps(1, 1) == std::vector<int>{0, 1, 2}));
  assert((stencil.deps(1, 3) == std::vector<int>{2, 3}));
  assert(stencil.edge_count() == 20);

  cfg.pattern = tb::Pattern::Sweep;
  tb::Graph sweep(cfg);
  assert((sweep.deps(1, 0) == std::vector<int>{0}));
  assert((sweep.deps(1, 2) == std::vector<int>{1, 2}));

  cfg.pattern = tb::Pattern::Nearest;
  cfg.radix = 3;
  tb::Graph nearest(cfg);
  assert((nearest.deps(1, 2) == std::vector<int>{1, 2, 3}));

  cfg.pattern = tb::Pattern::Trivial;
  tb::Graph trivial(cfg);
  assert(trivial.deps(2, 2).empty());
  assert(trivial.edge_count() == 0);

  cfg.pattern = tb::Pattern::Random;
  cfg.radix = 2;
  cfg.seed = 42;
  tb::Graph random_a(cfg);
  tb::Graph random_b(cfg);
  assert(random_a.deps(2, 1) == random_b.deps(2, 1));
}
