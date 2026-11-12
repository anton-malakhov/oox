// SPDX-License-Identifier: Apache-2.0
#pragma once

#if defined(OOX_EIGEN_TWIST) && OOX_EIGEN_TWIST
#include <scheduler_sync_twist.h>
#else
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <utility>

namespace oox::detail::eigen_pool::scheduler_sync {

using std::atomic;
using std::atomic_flag;
using std::atomic_thread_fence;
using std::condition_variable;
using std::mutex;
using std::thread;
using std::once_flag;
using std::call_once;
namespace this_thread = std::this_thread;
using std::atomic_signal_fence;

template <typename T, typename Tag> T &Local() {
  static thread_local T value{};
  return value;
}

inline void Relax() noexcept {
#if defined(__x86_64__)
  asm volatile("pause" ::: "memory");
#elif defined(__aarch64__)
  asm volatile("yield" ::: "memory");
#else
  this_thread::yield();
#endif
}

} // namespace oox::detail::eigen_pool::scheduler_sync
#endif
