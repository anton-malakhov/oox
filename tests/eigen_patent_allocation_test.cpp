// SPDX-License-Identifier: Apache-2.0
#include <cstdlib>
#include <iostream>
#include <new>
#include <oox/eigen/patent_parallel_for.h>

thread_local int fail_after = -1;
thread_local bool injected = false;
void *operator new(std::size_t size) {
  if (fail_after == 0) {
    fail_after = -1;
    injected = true;
    throw std::bad_alloc();
  }
  if (fail_after > 0)
    --fail_after;
  if (void *p = std::malloc(size ? size : 1))
    return p;
  throw std::bad_alloc();
}
void operator delete(void *p) noexcept { std::free(p); }
void operator delete(void *p, std::size_t) noexcept { std::free(p); }

int main() {
  using namespace oox::detail::eigen_pool;
  ThreadPool pool(8, true, true);
  rapid::RapidDomainState state(pool);
  rapid::RapidStartGroup group{&state, {0, 8}};
  rapid::ParallelForPatent(group, 0, 4097, [](std::size_t) {});
  unsigned hits = 0;
  for (int ordinal = 0; ordinal < 32; ++ordinal) {
    injected = false;
    fail_after = ordinal;
    bool caught = false;
    try {
      rapid::ParallelForPatent(group, 0, 4097, [](std::size_t i) {
        if (i % 64 == 0)
          std::this_thread::yield();
      });
    } catch (const std::bad_alloc &) {
      caught = true;
    }
    fail_after = -1;
    if (caught != injected) {
      std::cerr << "allocation ordinal=" << ordinal
                << " propagation mismatch\n";
      return 1;
    }
    hits += injected;
    std::array<std::atomic<unsigned>, 257> visits{};
    rapid::ParallelForPatent(group, 0, visits.size(),
                             [&](std::size_t i) { visits[i].fetch_add(1); });
    for (auto &v : visits)
      if (v != 1)
        return 2;
  }
  if (hits == 0)
    return 3;
  std::cout << "allocation cases=32 injected=" << hits << " reuse=PASS\n";
}
