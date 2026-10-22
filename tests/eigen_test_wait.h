// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <thread>

namespace eigen_test_support {

// A failed synchronization test must not unwind into a blocking future or
// thread-pool destructor. CTest owns the process and supplies an outer timeout.
[[noreturn]] inline void FailWait(const char *operation) noexcept {
  std::fprintf(stderr, "Eigen test synchronization timeout: %s\n", operation);
  std::fflush(stderr);
  std::_Exit(EXIT_FAILURE);
}

template <typename Predicate>
void WaitUntil(Predicate ready, const char *operation,
               std::chrono::steady_clock::duration timeout = std::chrono::seconds(10)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!ready()) {
    if (std::chrono::steady_clock::now() >= deadline) FailWait(operation);
    std::this_thread::yield();
  }
}

template <typename Future>
void GetReady(Future &future, const char *operation,
              std::chrono::steady_clock::duration timeout = std::chrono::seconds(10)) {
  if (future.wait_for(timeout) != std::future_status::ready)
    FailWait(operation);
  future.get();
}

} // namespace eigen_test_support
