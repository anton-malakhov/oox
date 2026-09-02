#pragma once
#include "modes.h"
#include "num_threads.h"

#ifdef EIGEN_MODE
#include "eigen_pool.h"
#endif

#include <chrono>
#include <cstddef>
#include <iostream>
#if defined(__linux__)
#include <sched.h>
#endif
#include <cstdint>
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

// Frequency of the counter read by Now(), in Hz. On AArch64 this is CNTFRQ_EL0;
// it varies by platform and generation, so Now() ticks are not assumed to be
// nanoseconds. On x86_64 the TSC frequency is calibrated once against
// steady_clock. Any comparison between Now() deltas and a nanosecond quantity
// must go through TicksToNanoseconds / NanosecondsToTicks below.
inline std::uint64_t TimerFrequencyHz() {
  static const std::uint64_t frequency = [] {
#if defined(__aarch64__)
    std::uint64_t value;
    asm volatile("mrs %0, cntfrq_el0" : "=r"(value));
    return value ? value : std::uint64_t{24000000};
#elif defined(__x86_64__)
    const auto t0 = std::chrono::steady_clock::now();
    const std::uint64_t c0 = __rdtsc();
    while (std::chrono::steady_clock::now() - t0 <
           std::chrono::milliseconds(50)) {
    }
    const std::uint64_t c1 = __rdtsc();
    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - t0)
                        .count();
    return ns > 0 ? static_cast<std::uint64_t>((c1 - c0) * 1.0e9 / ns)
                  : std::uint64_t{1000000000};
#else
    return std::uint64_t{1000000000};
#endif
  }();
  return frequency;
}

inline std::uint64_t TicksToNanoseconds(std::uint64_t ticks) {
  const long double hz = static_cast<long double>(TimerFrequencyHz());
  return static_cast<std::uint64_t>(static_cast<long double>(ticks) * 1.0e9L / hz);
}

inline std::uint64_t NanosecondsToTicks(std::uint64_t ns) {
  const long double hz = static_cast<long double>(TimerFrequencyHz());
  return static_cast<std::uint64_t>(static_cast<long double>(ns) * hz / 1.0e9L);
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
#if defined(__linux__)
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
#else
  (void)slot_number;
#endif
}

#ifdef __cpp_lib_hardware_interference_size
using std::hardware_constructive_interference_size;
using std::hardware_destructive_interference_size;
#else
constexpr std::size_t hardware_constructive_interference_size = 128;
constexpr std::size_t hardware_destructive_interference_size = 128;
#endif
