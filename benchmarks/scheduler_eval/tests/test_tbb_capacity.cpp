// SPDX-License-Identifier: Apache-2.0
#include "benchmarks/eigen/parallel_for.h"
#include <atomic>
#include <cstdint>
#include <random>
#include <set>
#include <mutex>
#include <thread>
#include <vector>

int RetryAfterRejection() {
  const auto control_limit = [] {
    return tbb::global_control::active_value(tbb::global_control::max_allowed_parallelism);
  };
  const size_t before = control_limit();
  const size_t available = std::min(before,
      static_cast<size_t>(tbb::this_task_arena::max_concurrency()));
  const size_t retry = std::min(size_t{8}, available);
  std::cerr << "initial_control=" << before << " available_capacity=" << available
            << " retry_participants=" << retry << '\n';
  if (retry <= 2) {
    std::cerr << "SKIP: resource limits prevent a supported retry larger than two\n";
    return 77;
  }
  bool rejected = false;
  tbb::task_arena narrow(1);
  narrow.execute([&] {
    try { InitParallel(2); }
    catch (const std::invalid_argument &error) {
      rejected = true;
      std::cerr << "narrow rejection: " << error.what() << '\n';
    }
  });
  const size_t after = control_limit();
  std::cerr << "control_after_rejection=" << after << '\n';
  bool ok = rejected && after == before;
  if (!ok) std::cerr << "FAIL: rejection changed process control or was not reported\n";
  try { InitParallel(retry); }
  catch (const std::invalid_argument &error) {
    std::cerr << "FAIL: supported retry rejected: " << error.what() << '\n';
    return 1;
  }
  std::set<std::thread::id> participants;
  std::mutex mutex;
  SpinBarrier barrier(retry);
  ParallelFor(0, retry, [&](size_t) {
    { std::lock_guard<std::mutex> lock(mutex); participants.insert(std::this_thread::get_id()); }
    barrier.Notify();
    barrier.Wait();
  }, 1);
  std::cerr << "retry initialization accepted; distinct execution threads (including caller)="
            << participants.size() << '\n';
  return ok && participants.size() == retry ? 0 : 1;
}

int main(int argc, char **argv) {
  if (argc == 2 && std::string(argv[1]) == "--retry-after-rejection")
    return RetryAfterRejection();
  if (argc != 1) return 2;
  tbb::global_control ceiling(tbb::global_control::max_allowed_parallelism, 2);
  const size_t capacity = std::min(size_t{2},
      static_cast<size_t>(tbb::this_task_arena::max_concurrency()));
  const auto rejects = [](size_t requested) {
    try { InitParallel(requested); }
    catch (const std::invalid_argument &) { return true; }
    std::cerr << "accepted unsupported warmup participants=" << requested << '\n';
    return false;
  };
  // Before the capacity check, this deadlocks inside the blocking warmup.
  if (!rejects(capacity + 1)) return 1;
  InitParallel(capacity);
  // The current arena can be narrower than the process-wide control.
  tbb::task_arena narrow(1);
  if (!narrow.execute([&] { return rejects(2); })) return 1;

  const std::vector<size_t> sizes{0, 1, 2, 3, 7, 15, 16, 17, 63, 64, 65,
                                255, 256, 257, 1023, 1024, 1025,
                                65537, 131071, 262145};
  for (size_t test = 0; test < sizes.size(); ++test) {
    const uint64_t seed = 0x194 + test;
    const size_t size = sizes[test];
    std::cerr << "case=" << test << " seed=" << seed << " size=" << size << '\n';
    std::mt19937_64 random(seed);
    std::vector<uint64_t> input(size), actual(size), expected(size);
    std::vector<std::atomic<unsigned>> visits(size);
    for (size_t i = 0; i < size; ++i) {
      input[i] = random();
      expected[i] = input[i] * 3 + 7;
      visits[i].store(0);
    }
    ParallelFor(0, size, [&](size_t i) {
      actual[i] = input[i] * 3 + 7;
      visits[i].fetch_add(1, std::memory_order_relaxed);
    });
    for (size_t i = 0; i < size; ++i)
      if (actual[i] != expected[i] || visits[i].load() != 1) {
        std::cerr << "case=" << test << " seed=" << seed << " index=" << i
                  << " expected=" << expected[i] << " actual=" << actual[i]
                  << " visits=" << visits[i].load() << '\n';
        return 1;
      }
  }
}
