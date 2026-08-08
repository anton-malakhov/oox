// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "../rapid_start.h"
#include "benchmarks/eigen/num_threads.h"
#include "benchmarks/eigen/tbb_pinner.h"
#include "benchmarks/eigen/thread_index.h"
#include "benchmarks/eigen/util.h"

#include <chrono>
#include <climits>
#include <stdexcept>
#include <thread>
#include <utility>

namespace rapid_start_eval {

class Runtime {
public:
  explicit Runtime(std::size_t threads)
      : threads_(Validate(threads)),
        control_(tbb::global_control::max_allowed_parallelism, threads_),
        pinner_() {
    group_.init(static_cast<int>(threads_));
    WaitUntilReady();
    Run(0, threads_, [](std::size_t) {});
  }

  template <typename F> void Run(std::size_t from, std::size_t to, F &&func) {
    if (to > static_cast<std::size_t>(INT_MAX))
      throw std::out_of_range("Rapid Start supports ranges up to INT_MAX");
    group_.parallel_ranges(static_cast<int>(from), static_cast<int>(to),
                           [&](int begin, int end, int) {
                             for (int i = begin; i < end; ++i)
                               func(static_cast<std::size_t>(i));
                           });
  }

private:
  static std::size_t Validate(std::size_t threads) {
    if (threads == 0 || threads > Harness::MAX_THREADS)
      throw std::invalid_argument(
          "Rapid Start requires between 1 and 64 workers");
    return threads;
  }

  mask_t ExpectedMask() const {
    return threads_ == Harness::MAX_THREADS
               ? ~mask_t{1}
               : (mask_t{1} << threads_) - mask_t{2};
  }

  void WaitUntilReady() {
    const auto expected = ExpectedMask();
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (group_.start_mask.load(std::memory_order_acquire) != expected) {
      if (std::chrono::steady_clock::now() >= deadline)
        throw std::runtime_error(
            "Rapid Start workers did not register in time");
      std::this_thread::yield();
    }
  }

  std::size_t threads_;
  tbb::global_control control_;
  PinningObserver pinner_;
  Harness::RapidStart group_;
};

inline Runtime &GetRuntime() {
  static Runtime runtime(static_cast<std::size_t>(GetNumThreads()));
  return runtime;
}

} // namespace rapid_start_eval

template <typename F>
void ParallelFor(std::size_t from, std::size_t to, F &&func, std::size_t = 1) {
  rapid_start_eval::GetRuntime().Run(from, to, std::forward<F>(func));
}

inline void InitParallel(std::size_t threads) {
  if (threads != static_cast<std::size_t>(GetNumThreads()))
    throw std::invalid_argument("Rapid Start worker-count mismatch");
  rapid_start_eval::GetRuntime();
}
