// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <utility>

#if defined(OOX_EIGEN_TWIST) && OOX_EIGEN_TWIST
#if defined(OOX_SCHEDULER_TRACE) || defined(OOX_EIGEN_THREAD_POOL_TESTING) || defined(OOX_EIGEN_SMALL_OBJECT_POOL_TESTING)
#error "Twist scheduler targets do not support native trace/global-counter instrumentation"
#endif
#include <twist/ed/std/atomic.hpp>
#include <twist/ed/std/condition_variable.hpp>
#include <twist/ed/std/mutex.hpp>
#include <twist/ed/std/thread.hpp>
#include <twist/ed/static/thread_local/var.hpp>
#include <twist/ed/static/var.hpp>
#endif

namespace oox::detail::eigen_pool::scheduler_sync {

// Only dedicated simulator targets select Twist. Normal builds retain the
// standard types and memory orders, without wrappers or runtime dispatch.
#if defined(OOX_EIGEN_TWIST) && OOX_EIGEN_TWIST
template <typename T> class atomic : public twist::ed::std::atomic<T> {
  using Base = twist::ed::std::atomic<T>;
public:
  using Base::Base;
  using Base::operator=;
  atomic() : Base(T{}) {}
};
using twist::ed::std::atomic_flag;
// The pinned simulator explores SC interleavings but its fence handler only
// accepts acquire/release/acq_rel. Retain both HB effects for an SC fence here.
// This does not validate hardware weak-memory executions or SC-fence removal.
inline void atomic_thread_fence(std::memory_order order) noexcept {
  if (order == std::memory_order_relaxed) return;
  twist::ed::std::atomic_thread_fence(
      order == std::memory_order_seq_cst ? std::memory_order_acq_rel : order);
}
using twist::ed::std::condition_variable;
using twist::ed::std::mutex;
using twist::ed::std::thread;
namespace this_thread = twist::ed::std::this_thread;
// Twist does not provide call_once. Model its completion/exception contract
// without blocking the simulator's host thread on a native once flag.
struct once_flag { mutex lock; bool complete = false; };
template <typename F> void call_once(once_flag &flag, F &&function) {
  std::lock_guard<mutex> guard(flag.lock);
  if (!flag.complete) {
    std::forward<F>(function)();
    flag.complete = true;
  }
}
#else
using std::atomic;
using std::atomic_flag;
using std::atomic_thread_fence;
using std::condition_variable;
using std::mutex;
using std::thread;
using std::once_flag;
using std::call_once;
namespace this_thread = std::this_thread;
#endif
using std::atomic_signal_fence;

template <typename T, typename Tag> T &Local() {
#if defined(OOX_EIGEN_TWIST) && OOX_EIGEN_TWIST
  struct Value { T value{}; };
  TWISTED_STATIC_THREAD_LOCAL(Value, storage);
  return storage->value;
#else
  static thread_local T value{};
  return value;
#endif
}

inline void Relax() noexcept {
#if defined(OOX_EIGEN_TWIST) && OOX_EIGEN_TWIST
  this_thread::yield();
#elif defined(__x86_64__)
  asm volatile("pause" ::: "memory");
#elif defined(__aarch64__)
  asm volatile("yield" ::: "memory");
#else
  this_thread::yield();
#endif
}

} // namespace oox::detail::eigen_pool::scheduler_sync
