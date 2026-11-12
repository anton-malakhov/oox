// SPDX-License-Identifier: Apache-2.0
#include <oox/eigen/rapid_auto_calibration.h>
#include "test_support/eigen_resident.h"
#include <array>
#include <atomic>
#include <future>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <vector>

using namespace oox::detail::eigen_pool;

void Check(bool condition, const char *message) {
  if (!condition) throw std::runtime_error(message);
}

void CheckAutomaticPool(size_t expected) {
  ThreadPool pool(4, true, true, WorkerIdleMode::ResidentAuto);
  Check(pool.NumThreads() == 4, "worker pool was resized");
  Check(pool.MaxResidentLimit() == expected, "wrong automatic ceiling");
  Check(pool.ResidentLimit() == expected, "wrong initial resident limit");
  pool.SetResidentLimit(std::numeric_limits<size_t>::max());
  Check(pool.ResidentLimit() == expected, "explicit limit escaped automatic ceiling");
  rapid::RapidDomainState state(pool);
  rapid::RapidStartGroup group{&state, {0, 4}};
  for (size_t limit : {size_t{0}, expected, size_t{1}, expected}) {
    pool.SetResidentLimit(limit);
    std::array<std::atomic<unsigned>, 35> visits{};
    rapid::ParallelForAuto(group, 0, 7, [&](size_t outer) {
      rapid::ParallelForAuto(group, 0, 5, [&](size_t inner) {
        visits[outer * 5 + inner].fetch_add(1);
      });
    });
    for (const auto &count : visits)
      Check(count.load() == 1, "nested range was lost or repeated");
  }
  const auto calibration = rapid::CalibrateAutoGroup(group);
  for (const auto &trial : calibration.trials)
    Check(trial.resident_limit <= expected, "calibration escaped automatic ceiling");
  auto completed = std::make_shared<std::promise<std::thread::id>>();
  auto future = completed->get_future();
  pool.RunOnThread(MakeTask([completed] {
    completed->set_value(std::this_thread::get_id());
  }), 3);
  Check(future.wait_for(std::chrono::seconds(5)) == std::future_status::ready,
        "ordinary task did not wake a helper");
  Check(future.get() != std::this_thread::get_id(), "task did not execute on a helper");
}

int main() {
  try {
    const size_t available = AvailableResidentLimit(4);
    Check(available >= 1 && available <= 4, "invalid available CPU count");
    CheckAutomaticPool(available);
    {
      ThreadPool explicit_pool(4, true, true, WorkerIdleMode::ResidentBusy);
      Check(explicit_pool.ResidentLimit() == 4, "explicit resident policy changed");
      Check(explicit_pool.MaxResidentLimit() == 4, "explicit ceiling changed");
    }
#if defined(__linux__)
    std::vector<unsigned long> original(sizeof(cpu_set_t) / sizeof(unsigned long));
    while (sched_getaffinity(0, original.size() * sizeof(unsigned long),
                            reinterpret_cast<cpu_set_t *>(original.data())) != 0) {
      Check(errno == EINVAL, "read test affinity");
      original.resize(original.size() * 2);
    }
    auto affinity = [&] {
      auto mask = original;
      Check(sched_getaffinity(0, mask.size() * sizeof(unsigned long),
                              reinterpret_cast<cpu_set_t *>(mask.data())) == 0, "read affinity");
      return mask;
    };
    {
      auto placement = DiscoverResidentPlacement(4);
      ThreadPool pool(4, true, true, WorkerIdleMode::ResidentAuto);
      Check(affinity() == original, "pool changed creator affinity");
      std::vector<unsigned long> inherited;
      std::thread external([&] { inherited = affinity(); });
      external.join();
      Check(inherited == original, "external client inherited a worker mask");
      rapid::RapidDomainState state(pool);
      rapid::RapidStartGroup group{&state, {0, 4}};
      eigen_test_support::WaitForResidentWorkers(group, std::chrono::seconds(5));
      std::array<std::vector<unsigned long>, 4> masks;
      rapid::ParallelForResidentRanges(group, 0, 4, [&](size_t, size_t) {
        masks.at(pool.CurrentThreadId()) = affinity();
      });
      for (size_t i = 1; i < placement.bindings.size(); ++i) {
        Check(masks[i] == placement.bindings[i].target, "worker missed its assigned CPU");
        for (size_t j = 1; j < i; ++j) Check(masks[j] != masks[i], "workers share CPU");
      }
    }
    struct Restore {
      const std::vector<unsigned long> &mask;
      ~Restore() {
        sched_setaffinity(0, mask.size() * sizeof(unsigned long),
                          reinterpret_cast<const cpu_set_t *>(mask.data()));
      }
    } restore{original};
    std::vector<unsigned long> restricted(original.size());
    size_t count = 0;
    for (size_t word = 0; word < original.size() && count < 2; ++word) {
      auto remaining = original[word];
      while (remaining && count < 2) {
        const auto bit = 1ul << std::countr_zero(remaining);
        restricted[word] |= bit;
        remaining &= ~bit;
        ++count;
        Check(sched_setaffinity(0, restricted.size() * sizeof(unsigned long),
                               reinterpret_cast<const cpu_set_t *>(restricted.data())) == 0,
              "restrict test affinity");
        CheckAutomaticPool(AvailableResidentLimit(4));
      }
    }
    Check(count > 0, "no allowed test CPU");
#endif
    std::cout << "automatic resident limit and nested execution passed\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
