// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "benchmarks/eigen/util.h"
#include <oox/oox.h>

#include <stdexcept>

inline int GetThreadIndex() {
  return oox::internal::get_eigen_pool().CurrentThreadId();
}

namespace scheduler_eval::detail {

template <typename F>
oox::var<int> TaskRange(std::size_t first, std::size_t last, std::size_t grain,
                        const F *body) {
  if (last - first <= grain) {
    for (auto i = first; i < last; ++i)
      (*body)(i);
    return 0;
  }
  const auto middle = first + (last - first) / 2;
  auto right = oox::run([=] { return TaskRange(middle, last, grain, body); });
  auto left = TaskRange(first, middle, grain, body);
  return oox::run([](int, int) { return 0; }, std::move(left),
                  std::move(right));
}

} // namespace scheduler_eval::detail

template <typename F>
void ParallelFor(std::size_t first, std::size_t last, F &&body,
                 std::size_t grain = 0) {
  if (first >= last)
    return;
  if (grain == 0) {
    const auto lanes = std::size_t{8} * GetNumThreads();
    grain = std::min(std::size_t{1024}, (last - first - 1) / lanes + 1);
  }
  auto done = oox::run([&] {
    return scheduler_eval::detail::TaskRange(
        first, last, std::max(grain, std::size_t{1}), &body);
  });
  // Nested calls rely on the pool's cooperative wait (workers execute queued
  // work), not a passive OS wait. Re-run nested/saturated-pool tests whenever
  // queue order, helping, or completion notification changes.
  static_cast<void>(oox::wait_and_get(done));
}

inline void InitParallel(std::size_t threads) {
  auto &pool = oox::internal::get_eigen_pool();
  if (pool.NumThreads() != threads)
    throw std::runtime_error(
        "OOX_TASKS worker count differs from requested threads; configure "
        "-DOOX_EIGEN_THREADS to match --threads");
  auto warmup = oox::run([] { return 0; });
  static_cast<void>(oox::wait_and_get(warmup));
}
