// SPDX-License-Identifier: Apache-2.0

#pragma once

#ifdef SERIAL
#include "serial_adapter.h"
#elif defined(OOX_TASK_MODE)
#include "oox_task_adapter.h"
#elif defined(RAPID_MAILBOX_MODE)
#include "eigen_mailbox_adapter.h"
#elif defined(RAPID_GROUP_MODE)
#include "eigen_rapid_adapter.h"
#elif defined(RAPID_START_MODE)
#include "rapid_start_adapter.h"
#else
#include "benchmarks/eigen/parallel_for.h"
#include "benchmarks/eigen/thread_index.h"
#endif

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <numeric>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "eval_parallel.h"
#include "benchmarks/eigen/arguments.h"

namespace scheduler_eval {

inline int ExecutionThreads() {
#ifdef SERIAL
  return 1;
#else
  return GetNumThreads();
#endif
}

using Clock = std::chrono::steady_clock;

inline std::uint64_t Nanoseconds(Clock::time_point origin) {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                           origin)
          .count());
}

inline std::size_t Argument(int argc, char **argv, std::string_view name,
                            std::size_t fallback) {
  for (int i = 1; i < argc; ++i) {
    if (std::string_view(argv[i]) == name)
      return benchmark_arguments::Number<std::size_t>(
          i + 1 < argc ? std::string_view(argv[i + 1]) : std::string_view{}, name);
  }
  return fallback;
}

inline std::uint64_t Initialize() {
  const auto origin = Clock::now();
  InitParallel(static_cast<std::size_t>(GetNumThreads()));
  return Nanoseconds(origin);
}

inline std::string ModeName() {
#ifdef SCHEDULER_EVAL_MODE_NAME
  return SCHEDULER_EVAL_MODE_NAME;
#else
  return GetParallelMode();
#endif
}

struct Summary {
  double minimum{};
  double mean{};
  double median{};
  double p95{};
  double p99{};
  double maximum{};
};

inline Summary Summarize(std::vector<std::uint64_t> values) {
  if (values.empty())
    return {};
  std::sort(values.begin(), values.end());
  const auto percentile = [&](double p) {
    const auto index = static_cast<std::size_t>(
        std::llround(p * static_cast<double>(values.size() - 1)));
    return static_cast<double>(values[index]);
  };
  return {static_cast<double>(values.front()),
          std::accumulate(values.begin(), values.end(), 0.0) / values.size(),
          percentile(0.5),
          percentile(0.95),
          percentile(0.99),
          static_cast<double>(values.back())};
}

inline void PrintSummary(const Summary &summary) {
  std::cout << "{\"min\":" << summary.minimum << ",\"mean\":" << summary.mean
            << ",\"median\":" << summary.median << ",\"p95\":" << summary.p95
            << ",\"p99\":" << summary.p99 << ",\"max\":" << summary.maximum
            << '}';
}

inline std::string JsonEscape(std::string_view value) {
  std::string result;
  for (unsigned char c : value) {
    if (c < 0x20) {
      constexpr char hex[] = "0123456789abcdef";
      result += "\\u00";
      result.push_back(hex[c >> 4]);
      result.push_back(hex[c & 15]);
      continue;
    }
    if (c == '"' || c == '\\')
      result.push_back('\\');
    result.push_back(static_cast<char>(c));
  }
  return result;
}

} // namespace scheduler_eval
