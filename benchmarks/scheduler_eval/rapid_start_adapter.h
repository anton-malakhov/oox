// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "benchmarks/eigen/eigen_pool.h"
#include "benchmarks/eigen/thread_index.h"
#include "benchmarks/eigen/util.h"
#include "oox/eigen/rapid_start.h"

#include <stdexcept>
#include <utility>

namespace rapid_start_eval {

class Runtime {
public:
  explicit Runtime(std::size_t threads)
      : threads_(Validate(threads)), state_(EigenPool()),
        group_{&state_, {0, static_cast<unsigned>(threads_)}} {
    Run(0, threads_, [](std::size_t) {});
  }

  template <typename F>
  void Run(std::size_t from, std::size_t to, F &&func,
           std::size_t grain = 1) {
#if EIGEN_MODE == EIGEN_RAPID
    oox::detail::eigen_pool::rapid::ParallelFor(group_, from, to,
                                                std::forward<F>(func));
#elif EIGEN_MODE == EIGEN_RAPID_MAILBOX
    oox::detail::eigen_pool::rapid::ParallelForMailbox(
        group_, from, to, std::forward<F>(func), grain);
#elif EIGEN_MODE == EIGEN_RAPID_LAZY_STEALING
    oox::detail::eigen_pool::rapid::ParallelForLazyStealing(
        group_, from, to, std::forward<F>(func), grain);
#elif EIGEN_MODE == EIGEN_RAPID_TIMESPAN_LAZY_STEALING
    oox::detail::eigen_pool::rapid::ParallelForTimespanLazyStealing(
        group_, from, to, std::forward<F>(func), grain);
#else
#error "Unsupported Rapid Start policy"
#endif
  }

private:
  static std::size_t Validate(std::size_t threads) {
    if (threads == 0 || threads >= (std::size_t{1} << 16))
      throw std::invalid_argument(
          "Rapid Start requires between 1 and 65535 workers");
    return threads;
  }

  std::size_t threads_;
  oox::detail::eigen_pool::rapid::RapidDomainState state_;
  oox::detail::eigen_pool::rapid::RapidStartGroup group_;
};

inline Runtime &GetRuntime() {
  static Runtime runtime(static_cast<std::size_t>(GetNumThreads()));
  return runtime;
}

} // namespace rapid_start_eval

template <typename F>
void ParallelFor(std::size_t from, std::size_t to, F &&func,
                 std::size_t grain = 1) {
  rapid_start_eval::GetRuntime().Run(from, to, std::forward<F>(func), grain);
}

inline void InitParallel(std::size_t threads) {
  if (threads != static_cast<std::size_t>(GetNumThreads()))
    throw std::invalid_argument("Rapid Start worker-count mismatch");
  rapid_start_eval::GetRuntime();
}
