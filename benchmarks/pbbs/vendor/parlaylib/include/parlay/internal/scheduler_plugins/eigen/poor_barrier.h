#pragma once

#include <atomic>
#include <cstddef>

#include "util.h"

struct SpinBarrier {
  SpinBarrier(size_t count) : remain_(count) {}

  void Notify(size_t count = 1) { remain_.fetch_sub(count); }

  void Wait() {
    while (remain_.load()) {
      CpuRelax();
    }
  }

  std::atomic<size_t> remain_;
};
