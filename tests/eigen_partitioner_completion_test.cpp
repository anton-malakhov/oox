// SPDX-License-Identifier: Apache-2.0
#include <oox/eigen/parallel_for.h>
#include <array>
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <thread>

using namespace oox::detail::eigen_pool;

namespace {
void Check(bool value, const char *policy, const char *message, unsigned trace) {
  if (!value) {
    std::cerr << policy << " trace=" << trace << ' ' << message << '\n';
    std::_Exit(1);
  }
}

template <class Policy> void LocalCompletion(Policy &policy, const char *name) {
  std::array<std::atomic<unsigned>, 257> visits{};
  std::atomic<bool> started{false}, release{false};
  internal::completion_notifications = 0;
  {
    ThreadPool pool(2, false, true);
    pool.RunOnThread(MakeTask([&] {
      started = true;
      started.notify_one();
      release.wait(false);
    }), 1);
    started.wait(false);
    ParallelFor(pool, 0, visits.size(), [&](size_t i) {
      Check(pool.CurrentThreadId() == 0, name, "unexpected callback worker", 0);
      ++visits[i];
    }, policy);
    release = true;
    release.notify_one();
  }
  for (size_t i = 0; i < visits.size(); ++i)
    Check(visits[i] == 1, name, "local serial visitation mismatch", unsigned(i));
  Check(internal::completion_notifications == 0, name, "local broadcast", 0);
}

template <class Policy>
void RemoteCompletion(Policy &policy, const char *name, bool registered,
                      unsigned trace) {
  std::array<std::atomic<unsigned>, 2> visits{};
  std::atomic<bool> started{false}, release{false};
  internal::completion_notifications = 0;
  internal::completion_waits = 0;
  {
    ThreadPool pool(2, false, registered);
    const auto caller = std::this_thread::get_id();
    std::thread controller([&] {
      // Release the child only after the caller has armed its pool wait.
      internal::completion_waits.wait(0);
      release = true;
      release.notify_one();
    });
    ParallelFor(pool, 0, visits.size(), [&](size_t i) {
      ++visits[i];
      if (i == 1) {
        Check(std::this_thread::get_id() != caller, name, "child on caller", trace);
        started = true;
        started.notify_one();
        release.wait(false);
      } else {
        started.wait(false);
      }
    }, policy);
    controller.join();
  }
  for (auto &value : visits)
    Check(value == 1, name, "remote serial visitation mismatch", trace);
  Check(internal::completion_notifications == 1, name, "missing remote wakeup", trace);
}

template <class Policy> void Test(const char *name) {
  Policy policy;
  LocalCompletion(policy, name);
  for (unsigned trace = 0; trace < 32; ++trace)
    RemoteCompletion(policy, name, trace % 2 != 0, trace);
}
}

int main() {
  Test<AutoPartitioner>("auto");
  Test<AffinityPartitioner>("affinity");
  std::cout << "local completion and 64 remote/external waiter traces PASS\n";
}
