#pragma once
#include "modes.h"
#include "num_threads.h"

#ifdef EIGEN_MODE
#include "eigen_pool.h"
#endif

#include <cstddef>
#include <iostream>
#include <sched.h>
#include <string>
#include <thread>
#include <cassert>
#if defined(__x86_64__)
// for rdtsc
#include "x86intrin.h"
#endif

#ifdef TASKFLOW_MODE

inline tf::Executor& tfExecutor() {
  static tf::Executor exec(GetNumThreads());
  return exec;
}

#endif

using Timestamp = uint64_t;
// using Timestamp = std::chrono::system_clock::time_point;

inline Timestamp Now() {
#if defined(__x86_64__)
  return __rdtsc();
#elif defined(__aarch64__)
  // System timer of ARMv8 runs at a different frequency than the CPU's.
  // The frequency is fixed, typically in the range 1-50MHz.  It can be
  // read at CNTFRQ special register.  We assume the OS has set up
  // the virtual timer properly.
  asm volatile("isb");
  Timestamp virtual_timer_value;
  asm volatile("mrs %0, cntvct_el0" : "=r"(virtual_timer_value));
  return virtual_timer_value;
#else
#error "Unsupported architecture"
#endif
  // return std::chrono::duration_cast<std::chrono::nanoseconds>(
  //            std::chrono::high_resolution_clock::now().time_since_epoch())
  //     .count();
}

inline void CpuRelax() {
#if defined(__x86_64__)
  asm volatile("pause\n" : : : "memory");
#elif defined(__aarch64__)
  asm volatile("yield\n" : : : "memory");
#else
#error "Unsupported architecture"
#endif
}

inline void PinThread(size_t slot_number) {
  cpu_set_t mask;
  auto mask_size = sizeof(mask);
  if (sched_getaffinity(0, mask_size, &mask)) {
    std::cerr << "Error in sched_getaffinity" << std::endl;
    return;
  }
  // clear all bits in current_affinity except slot_numbers'th non-zero bit
  size_t nonzero_bits = 0;
  for (size_t i = 0; i < CPU_SETSIZE; ++i) {
    if (CPU_ISSET(i, &mask)) {
      if (nonzero_bits == slot_number) {
        CPU_ZERO(&mask);
        CPU_SET(i, &mask);
        break;
      }
      ++nonzero_bits;
    }
  }

  if (auto err = sched_setaffinity(0, mask_size, &mask)) {
    std::cerr << "Error in sched_setaffinity, slot_number = " << slot_number
              << ", err = " << err << std::endl;
  }
}

namespace detail {

constexpr std::size_t StackSize = std::size_t{16} * 1024 * 1024;

struct StackBase {
  StackBase() noexcept {
    // Stacks are growing top-down. Highest address is called "stack base",
    // and the lowest is "stack limit".
#if __TBB_USE_WINAPI
    suppress_unused_warning(stack_size);
    NT_TIB* pteb = (NT_TIB*)NtCurrentTeb();
    __TBB_ASSERT(&pteb < pteb->StackBase && &pteb > pteb->StackLimit, "invalid stack info in TEB");
    return reinterpret_cast<std::uintptr_t>(pteb->StackBase);
#else
    // There is no portable way to get stack base address in Posix, so we use
    // non-portable method (on all modern Linux) or the simplified approach
    // based on the common sense assumptions. The most important assumption
    // is that the main thread's stack size is not less than that of other threads.

    // Points to the lowest addressable byte of a stack.
    void* stack_limit = nullptr;
#if __linux__ && !__bg__
    pthread_attr_t np_attr_stack;
    if (0 == pthread_getattr_np(pthread_self(), &np_attr_stack)) {
        if (0 == pthread_attr_getstack(&np_attr_stack, &stack_limit, &size_)) {
            assert( &stack_limit > stack_limit && "stack size must be positive" );
        }
        pthread_attr_destroy(&np_attr_stack);
    }
#endif /* __linux__ */
    if (stack_limit) {
        base_ = reinterpret_cast<std::uintptr_t>(stack_limit) + size_;
    } else {
        // Use an anchor as a base stack address.
        int anchor{};
        base_ = reinterpret_cast<std::uintptr_t>(&anchor);
    }
    // std::cerr << "stack_base: " << std::hex << base_ << ", stack_limit: " << stack_limit <<
    //              ", stack size: " << std::dec << (size_ / 1024) << "KB" << std::endl;
#endif /* __TBB_USE_WINAPI */
  }

  std::uintptr_t calculate_stack_half() {
    assert(size_ != 0 && "Stack size cannot be zero");
    assert(base_ > size_ / 2 && "Stack anchor calculation overflow");
    return base_ - size_ / 2;
  }

  std::uintptr_t base_ = 0;
  std::size_t size_ = 0;
};

}

inline bool is_stack_half_full() {
  static thread_local detail::StackBase stack_base;

  auto stack_half = stack_base.calculate_stack_half();
  int anchor = 0;
  auto anchor_ptr = reinterpret_cast<std::uintptr_t>(&anchor);
  // std::cerr << "stack size: " << std::dec << ((stack_base.base_ - anchor_ptr) / 1024) << "KB"\
  //           << ", anchor: " << std::hex << anchor_ptr << ", base: " << stack_base.base_ << ", half: " << stack_half << std::endl;
  return anchor_ptr < stack_half;
}

#ifdef __cpp_lib_hardware_interference_size
using std::hardware_constructive_interference_size;
using std::hardware_destructive_interference_size;
#else
constexpr std::size_t hardware_constructive_interference_size = 128;
constexpr std::size_t hardware_destructive_interference_size = 128;
#endif
