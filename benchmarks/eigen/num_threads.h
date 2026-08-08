#pragma once

#include "modes.h"
#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <string>
#include <thread>

inline int GetNumThreads() {
  // TODO(blonded04): actually you need a way to programmatically find number of cores on 1 NUMA node
  static int result = [] {
    if (const char *envThreads = std::getenv("BENCH_NUM_THREADS")) {
      return std::stoi(envThreads);
    }
    if (const char *envThreads = std::getenv("PARLAY_NUM_THREADS")) {
      return std::stoi(envThreads);
    }
    if (const char *envThreads = std::getenv("OMP_NUM_THREADS")) {
      return std::stoi(envThreads);
    }
    return static_cast<int>(std::max(1u, std::thread::hardware_concurrency()));
  }();
  return result;
}
