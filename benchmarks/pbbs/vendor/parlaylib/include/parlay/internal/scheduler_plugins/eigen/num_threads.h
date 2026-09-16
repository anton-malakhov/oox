#pragma once

#include "modes.h"
#include <cstddef>
#include <string>
#include <thread>

namespace Eigen::internal {

inline int GetNumThreads() {
  // cache result to avoid calling getenv on every call
  static int threads = []() -> int {
    if (const char *envThreads = std::getenv("BENCH_NUM_THREADS")) {
      return std::stoi(envThreads);
    }
    // left just for compatibility
    if (const char *envThreads = std::getenv("OMP_NUM_THREADS")) {
      return std::stoi(envThreads);
    }
    return static_cast<int>(std::thread::hardware_concurrency());
  }();
  return threads;
}
}