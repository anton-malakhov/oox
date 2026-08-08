// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#if defined(__APPLE__) || defined(__linux__)
#include <pthread.h>
#endif

namespace Eigen::internal {

class StackBounds {
public:
  StackBounds() noexcept {
#if defined(__APPLE__)
    base_ = reinterpret_cast<std::uintptr_t>(pthread_get_stackaddr_np(pthread_self()));
    size_ = pthread_get_stacksize_np(pthread_self());
#elif defined(__linux__)
    pthread_attr_t attributes;
    void* limit = nullptr;
    if (pthread_getattr_np(pthread_self(), &attributes) == 0) {
      pthread_attr_getstack(&attributes, &limit, &size_);
      pthread_attr_destroy(&attributes);
    }
    base_ = reinterpret_cast<std::uintptr_t>(limit) + size_;
#endif
    if (base_ == 0 || size_ == 0) {
      int anchor;
      base_ = reinterpret_cast<std::uintptr_t>(&anchor);
      size_ = 16 * 1024 * 1024;
    }
  }

  bool HalfConsumed(const void* anchor) const noexcept {
    assert(base_ > size_ / 2);
    return reinterpret_cast<std::uintptr_t>(anchor) < base_ - size_ / 2;
  }

private:
  std::uintptr_t base_{0};
  std::size_t size_{0};
};

inline bool IsStackHalfConsumed() noexcept {
  static thread_local StackBounds bounds;
  int anchor;
  return bounds.HalfConsumed(&anchor);
}

} // namespace Eigen::internal
