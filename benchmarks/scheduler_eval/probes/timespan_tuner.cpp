// SPDX-License-Identifier: Apache-2.0

#include "common.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

int main(int argc, char **argv) {
  using namespace scheduler_eval;
  const auto iterations = Argument(argc, argv, "--iterations", 10000);
  const auto warmup_iterations = Argument(argc, argv, "--warmup", 10);
  const auto workers = static_cast<std::size_t>(GetNumThreads());
  if (iterations == 0 || workers == 0) {
    std::cerr << "iterations and worker count must be nonzero\n";
    return 2;
  }
  const auto initialization = Initialize();
  std::vector<std::uint64_t> all_arrivals;
  std::vector<std::uint64_t> maxima;
  all_arrivals.reserve(iterations * workers);
  maxima.reserve(iterations);
  const auto run_once = [&](std::vector<std::uint64_t> *result) {
    std::vector<std::uint64_t> arrivals(workers);
    std::atomic<std::size_t> started{0};
    std::atomic_thread_fence(std::memory_order_seq_cst);
    const auto origin = Clock::now();
    ParallelFor(0, workers, [&](std::size_t task) {
      arrivals[task] = Nanoseconds(origin);
      started.fetch_add(1, std::memory_order_release);
      while (started.load(std::memory_order_acquire) != workers)
        CpuRelax();
    });
    if (result)
      *result = std::move(arrivals);
  };
  for (std::size_t iteration = 0; iteration < warmup_iterations; ++iteration)
    run_once(nullptr);
  for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
    std::vector<std::uint64_t> arrivals;
    run_once(&arrivals);
    maxima.push_back(*std::max_element(arrivals.begin(), arrivals.end()));
    all_arrivals.insert(all_arrivals.end(), arrivals.begin(), arrivals.end());
  }
  const auto maximum_summary = Summarize(maxima);
  const auto p99_ns = static_cast<std::uint64_t>(maximum_summary.p99);
  std::cout << "{\"schema\":2,\"tool\":\"timespan_tuner\",\"mode\":\""
            << JsonEscape(ModeName()) << "\",\"workers\":" << workers
            << ",\"initialization_ns\":" << initialization
            << ",\"warmup_iterations\":" << warmup_iterations
            << ",\"iterations\":" << iterations << ",\"all_arrivals_ns\":";
  PrintSummary(Summarize(all_arrivals));
  std::cout << ",\"iteration_maximum_ns\":";
  PrintSummary(maximum_summary);
  // Both units are emitted so the INIT_TIME gate (which is in Now() ticks) can
  // be set without a manual CNTFRQ_EL0 / TSC conversion.
  std::cout << ",\"timer_frequency_hz\":" << TimerFrequencyHz()
            << ",\"recommended_init_time_ns\":" << p99_ns
            << ",\"recommended_init_time_ticks\":" << NanosecondsToTicks(p99_ns)
            << "}\n";
}
