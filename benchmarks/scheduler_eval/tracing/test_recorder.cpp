// SPDX-License-Identifier: Apache-2.0
#include "recorder.h"
#include <atomic>
#include <iostream>
#include <thread>
#include <vector>

namespace {
std::atomic<bool> entered{false}, released{false};
void ReleaseAfterClosing() {
  released.store(true, std::memory_order_release);
  released.notify_one();
}
}

int main(int argc, char **argv) {
  namespace trace = oox::scheduler_trace;
  const bool overflow = argc > 1 && std::string_view(argv[1]) == "overflow";
  const bool racing = argc > 1 && std::string_view(argv[1]) == "racing";
  const bool closing = argc > 1 && std::string_view(argv[1]) == "closing";
  const bool timeout = argc > 1 && std::string_view(argv[1]) == "timeout";
  {
    trace::Scope outside("before_session", trace::NextId());
    trace::Start(4, overflow ? 2 : 4096);
  }
  if (closing) {
    std::thread worker([] {
      trace::Worker(1);
      trace::Scope accepted("accepted_before_close", trace::NextId());
      entered.store(true, std::memory_order_release);
      entered.notify_one();
      released.wait(false, std::memory_order_acquire);
      // This begin must be rejected while the already accepted end, marker,
      // and counter update must remain recordable until its outer scope ends.
      trace::Scope rejected("rejected_after_close", trace::NextId());
      trace::Emit("after_close_marker", trace::Current());
      trace::Count(trace::Counter::RangeLockAcquired, 17);
    });
    entered.wait(false, std::memory_order_acquire);
    trace::SetClosingHook(ReleaseAfterClosing);
    trace::Dump(std::cout, "TEST", "closing", 1, 1, 1, 0, 1, true);
    worker.join();
    return 0;
  }
  if (timeout) {
    trace::Scope held("held_past_timeout", trace::NextId());
    trace::Dump(std::cout, "TEST", "timeout", 1, 1, 1, 0, 1, true);
    return 0;
  }
  std::atomic<bool> stop{false};
  std::atomic<unsigned> seen{0};
  std::vector<std::thread> workers;
  for (unsigned worker = 0; worker < 3; ++worker) {
    workers.emplace_back([&, worker] {
      trace::Worker(worker);
      for (unsigned i = 0; racing ? !stop.load() : i < 100; ++i) {
        trace::Scope task("task", trace::NextId(), i, i + 1);
        trace::Emit("marker", trace::Current());
        trace::Count(trace::Counter::RangeLockAcquired);
        ++seen;
      }
    });
  }
  if (racing) {
    while (seen.load() < 100) std::this_thread::yield();
    trace::Dump(std::cout, "TEST", "racing", 3, 100, 1, 0, 1, true);
    stop = true;
  }
  for (auto &worker : workers) worker.join();
  if (!racing) trace::Dump(std::cout, "TEST", "normal", 3, 100, 1, 0, 1, true);
}
