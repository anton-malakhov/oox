// SPDX-License-Identifier: Apache-2.0

#include "common.h"

#include <cstddef>
#include <cstdint>
#include <vector>

struct Event {
  std::uint64_t start{};
  std::uint64_t end{};
  int worker{};
};

int main(int argc, char **argv) {
  using namespace scheduler_eval;
  const auto workers = static_cast<std::size_t>(GetNumThreads());
  const auto tasks = Argument(argc, argv, "--tasks", workers * 4);
  const auto work = Argument(argc, argv, "--work", 10000);
  const auto iterations = Argument(argc, argv, "--iterations", 1024);
  if (tasks == 0 || iterations == 0) {
    std::cerr << "tasks and iterations must be nonzero\n";
    return 2;
  }
  const auto initialization = Initialize();
  std::vector<Event> events(tasks * iterations);
  const auto origin = Clock::now();
  for (std::size_t iteration = 0; iteration < iterations; ++iteration)
    ParallelFor(0, tasks, [&](std::size_t task) {
      auto &event = events[iteration * tasks + task];
      event.start = Nanoseconds(origin);
      event.worker = GetThreadIndex();
      for (std::size_t i = 0; i < work; ++i)
        CpuRelax();
      event.end = Nanoseconds(origin);
    });
  std::cout << "{\"schema\":1,\"tool\":\"trace_spin\",\"mode\":\""
            << JsonEscape(ModeName()) << "\",\"workers\":" << workers
            << ",\"initialization_ns\":" << initialization
            << ",\"iterations\":" << iterations << ",\"traceEvents\":[";
  for (std::size_t task = 0; task < events.size(); ++task) {
    if (task)
      std::cout << ',';
    const auto &event = events[task];
    std::cout << "{\"name\":\"task\",\"cat\":\"scheduler\",\"ph\":\"X\""
              << ",\"ts\":" << event.start / 1000.0
              << ",\"dur\":" << (event.end - event.start) / 1000.0
              << ",\"pid\":1,\"tid\":" << event.worker
              << ",\"args\":{\"iteration\":" << task / tasks
              << ",\"task\":" << task % tasks << "}}";
  }
  std::cout << "]}\n";
}
