// SPDX-License-Identifier: Apache-2.0

#include "common.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace {

std::string Scenario(int argc, char **argv) {
  for (int i = 1; i + 1 < argc; ++i)
    if (std::string_view(argv[i]) == "--scenario")
      return argv[i + 1];
  return "spin";
}

} // namespace

int main(int argc, char **argv) {
  using namespace scheduler_eval;
  const auto iterations = Argument(argc, argv, "--iterations", 10);
  const auto tasks_per_worker = Argument(argc, argv, "--tasks-per-worker", 100);
  const auto scenario = Scenario(argc, argv);
  const auto workers = static_cast<std::size_t>(GetNumThreads());
  const auto tasks =
      scenario == "multitask" ? workers * tasks_per_worker : workers;
  if ((scenario != "spin" && scenario != "barrier" &&
       scenario != "multitask") ||
      iterations == 0 || tasks == 0) {
    std::cerr
        << "expected a nonzero run with --scenario spin|barrier|multitask\n";
    return 2;
  }
  const auto initialization = Initialize();

  std::vector<std::uint64_t> all_arrivals;
  std::vector<std::uint64_t> spreads;
  std::cout << "{\"schema\":1,\"tool\":\"scheduling_dist\",\"mode\":\""
            << JsonEscape(ModeName()) << "\",\"scenario\":\""
            << JsonEscape(scenario) << "\",\"workers\":" << workers
            << ",\"initialization_ns\":" << initialization
            << ",\"iterations\":[";
  for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
    std::vector<std::uint64_t> arrivals(tasks);
    std::vector<int> worker_ids(tasks);
    std::atomic<std::size_t> started{0};
    std::atomic_thread_fence(std::memory_order_seq_cst);
    const auto origin = Clock::now();
    ParallelFor(0, tasks, [&](std::size_t task) {
      arrivals[task] = Nanoseconds(origin);
      worker_ids[task] = GetThreadIndex();
      started.fetch_add(1, std::memory_order_release);
      if (scenario == "barrier") {
        while (started.load(std::memory_order_acquire) != tasks)
          CpuRelax();
      } else {
        const auto work = scenario == "multitask" ? 100u : 10000u;
        for (std::size_t i = 0; i < work; ++i)
          CpuRelax();
      }
    });
    const auto [minimum, maximum] =
        std::minmax_element(arrivals.begin(), arrivals.end());
    spreads.push_back(*maximum - *minimum);
    all_arrivals.insert(all_arrivals.end(), arrivals.begin(), arrivals.end());
    if (iteration)
      std::cout << ',';
    std::cout << "{\"arrival_ns\":[";
    for (std::size_t i = 0; i < arrivals.size(); ++i) {
      if (i)
        std::cout << ',';
      std::cout << arrivals[i];
    }
    std::cout << "],\"worker\":[";
    for (std::size_t i = 0; i < worker_ids.size(); ++i) {
      if (i)
        std::cout << ',';
      std::cout << worker_ids[i];
    }
    std::cout << "]}";
  }
  std::cout << "],\"arrival_summary_ns\":";
  PrintSummary(Summarize(all_arrivals));
  std::cout << ",\"spread_summary_ns\":";
  PrintSummary(Summarize(spreads));
  std::cout << "}\n";
}
