// SPDX-License-Identifier: Apache-2.0
// Counts wake notifications reaching parked workers during mailbox loops.
// Build twice with -DOOX_EIGEN_BATCHED_WAKE=0/1 and compare.
#include <oox/eigen/rapid_start.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

using namespace oox::detail::eigen_pool;
using namespace oox::detail::eigen_pool::rapid;
using namespace std::chrono_literals;

int main() {
  ThreadPool pool(8, /*allow_spinning=*/false, false);
  RapidDomainState state(pool);
  RapidStartGroup group{&state, {0, 8}};
  // Let workers park.
  std::this_thread::sleep_for(20ms);
  const size_t before = pool.WorkerWakeNotifications();
  size_t visits_total = 0;
  for (int round = 0; round < 50; ++round) {
    std::vector<std::atomic<unsigned>> visits(1 << 16);
    ParallelForMailbox(group, 0, visits.size(), [&](size_t i) {
      visits[i].fetch_add(1, std::memory_order_relaxed);
    });
    for (auto &v : visits) {
      if (v.load() != 1u) {
        std::fprintf(stderr, "visit mismatch\n");
        return 1;
      }
      ++visits_total;
    }
    std::this_thread::sleep_for(2ms); // let workers park between rounds
  }
  const size_t after = pool.WorkerWakeNotifications();
  std::printf("batched_wake=%d rounds=50 items=%zu wake_notifications=%zu\n",
              OOX_EIGEN_BATCHED_WAKE, visits_total, after - before);
  return 0;
}
