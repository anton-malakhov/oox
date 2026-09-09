// SPDX-License-Identifier: Apache-2.0
// Identical ready-task workload for legacy, fixed-group, and demand backends.

#include <oox/eigen/nonblocking_thread_pool.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {
using namespace oox::detail::eigen_pool;

std::uint64_t Work(std::size_t index, unsigned cost) {
  std::uint64_t value = index + 1;
  for (unsigned i = 0; i < cost; ++i)
    value = (value ^ (value >> 23)) * 0x9e3779b97f4a7c15ULL + i;
  return value;
}

template <typename Pool>
int Run(std::string_view mode, int threads, std::size_t count, unsigned cost) {
  Pool pool(threads, false, false);
  std::vector<std::uint64_t> values(count);
  std::promise<void> warmed;
  pool.Schedule(MakeTask([&] { warmed.set_value(); }));
  warmed.get_future().wait();
  std::atomic<std::size_t> remaining{count};
  std::promise<void> done;
  auto completed = done.get_future();
  const auto start = std::chrono::steady_clock::now();
  pool.Schedule(MakeTask([&] {
    for (std::size_t i = 0; i < count; ++i) {
      pool.Schedule(MakeTask([&, i] {
        const unsigned iterations = i % 64 == 0 ? cost * 16 : cost;
        values[i] = Work(i, iterations);
        if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1)
          done.set_value();
      }));
    }
  }));
  completed.get();
  const auto stop = std::chrono::steady_clock::now();
  std::uint64_t checksum = 0;
  for (std::size_t i = 0; i < count; ++i) {
    const auto expected = Work(i, i % 64 == 0 ? cost * 16 : cost);
    if (values[i] != expected)
      throw std::runtime_error("result differs from serial oracle");
    checksum += values[i];
  }
  const auto stats = pool.GetDemandStatistics();
  const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
  std::cout << "{\"mode\":\"" << mode << "\",\"threads\":" << threads
            << ",\"tasks\":" << count << ",\"nanoseconds\":" << ns
            << ",\"checksum\":" << checksum << ",\"groups\":" << stats.groups
            << ",\"offers\":" << stats.offers << ",\"remote_groups\":"
            << stats.remote_groups << ",\"feedback\":" << stats.feedback
            << ",\"grouped_tasks\":" << stats.grouped_tasks << "}\n";
  return 0;
}
} // namespace

int main(int argc, char **argv) {
  try {
    const std::string_view mode = argc > 1 ? argv[1] : "demand";
    const int threads = argc > 2 ? std::stoi(argv[2]) : 4;
    const std::size_t tasks = argc > 3 ? std::stoull(argv[3]) : 65536;
    const unsigned cost = argc > 4 ? std::stoul(argv[4]) : 32;
    if (!tasks || cost > 1000000)
      throw std::invalid_argument("tasks must be positive and cost <= 1000000");
    if (mode == "legacy")
      return Run<ThreadPool>(mode, threads, tasks, cost);
    if (mode == "fixed")
      return Run<ThreadPoolTempl<StlThreadEnvironment, TaskGroupPolicy<64, 1, false>>>(
          mode, threads, tasks, cost);
    if (mode == "demand")
      return Run<DemandThreadPool>(mode, threads, tasks, cost);
    throw std::invalid_argument("mode must be legacy, fixed, or demand");
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
