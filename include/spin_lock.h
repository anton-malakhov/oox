#pragma once

#include <atomic>

class SpinLock {
public:
  void lock() noexcept {
    while (true) {
      if (!locked.exchange(true, std::memory_order_acquire)) {
        return;
      }
      while (locked.load(std::memory_order_relaxed)) {
        __builtin_ia32_pause();
      }
    }
  }

  bool try_lock() noexcept {
    return !locked.load(std::memory_order_relaxed) &&
           !locked.exchange(true, std::memory_order_acquire);
  }

  void unlock() noexcept { locked.store(false, std::memory_order_release); }

private:
  std::atomic<bool> locked = {0};
};
