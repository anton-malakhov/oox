// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "oox/eigen/rapid_start.h"
#include <chrono>
#include <thread>

namespace eigen_test_support {

// Test/benchmark startup only: wait for an idle snapshot, without reserving it.
// The caller supplies the timeout; production launches need no preparation.
inline void WaitForResidentWorkers(
    oox::detail::eigen_pool::rapid::RapidStartGroup group,
    std::chrono::steady_clock::duration timeout) {
  if (group.IsEmpty())
    return;
  group.Validate();
  auto &pool = group.state->Pool();
  if (!pool.UsesResidentBusyWait())
    throw std::invalid_argument("resident warmup requires a resident-busy pool");
  const size_t current = pool.CurrentThreadId();
  // The creator of a main-thread pool is never a resident waiter. Subtract a
  // background caller only when it would otherwise be included in capacity.
  const size_t capacity = pool.ResidentCapacity(group.domain);
  const size_t expected = capacity -
      (current < pool.ResidentLimit() && group.domain.Contains(current) &&
       pool.ResidentCapacity({static_cast<unsigned>(current),
                              static_cast<unsigned>(current + 1)}) ? 1 : 0);
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (pool.ResidentAvailableWorkers(group.domain) < expected &&
         !pool.IsCancelled()) {
    if (std::chrono::steady_clock::now() >= deadline)
      throw std::runtime_error("resident warmup workers did not become ready");
    std::this_thread::yield();
  }
}

} // namespace eigen_test_support
