// SPDX-License-Identifier: Apache-2.0
#include <oox/eigen/nonblocking_thread_pool.h>
#include <gtest/gtest.h>
#include <array>
#include <atomic>
#include <chrono>
#include <limits>
#include <thread>
#include <vector>

namespace {
using oox::detail::eigen_pool::MakeTask;
using oox::detail::eigen_pool::Task;
using oox::detail::eigen_pool::ThreadPool;

template <class Predicate> bool Until(Predicate ready) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (!ready()) {
    if (std::chrono::steady_clock::now() >= deadline) return false;
    std::this_thread::yield();
  }
  return true;
}

struct BatchTask final : Task {
  BatchTask(ThreadPool &pool, std::atomic<unsigned> &event,
            std::atomic<size_t> &destroyed, bool cancel_run = false,
            bool cancel_discard = false)
      : pool(pool), event(event), destroyed(destroyed),
        cancel_run(cancel_run), cancel_discard(cancel_discard) {}
  ~BatchTask() { destroyed.fetch_add(1, std::memory_order_release); }
  void operator()() final {
    EXPECT_EQ(event.fetch_add(1), 0u);
    if (cancel_run) pool.Cancel();
    delete this;
  }
  void Discard() noexcept final {
    EXPECT_EQ(event.fetch_add(2), 0u);
    if (cancel_discard) pool.Cancel();
    delete this;
  }
  ThreadPool &pool;
  std::atomic<unsigned> &event;
  std::atomic<size_t> &destroyed;
  bool cancel_run, cancel_discard;
};

TEST(EigenPoolBatch, BatchSaturationAndExternalFallbackMatchSerialOracle) {
  constexpr unsigned seed = 0x52e72809;
  const std::array<size_t, 8> counts{0, 1, 3, 31, 1023, 1024, 1025, 4097};
  for (bool external : {false, true}) {
    for (size_t count : counts) {
      SCOPED_TRACE(testing::Message() << "seed=" << seed << " count=" << count
                                     << " external=" << external);
      std::vector<std::atomic<unsigned>> events(count);
      std::atomic<size_t> destroyed{0};
      std::atomic<bool> entered{false}, release{false};
      {
        ThreadPool pool(1, false, false);
        pool.Schedule(MakeTask([&] {
          entered.store(true, std::memory_order_release);
          release.wait(false, std::memory_order_acquire);
        }));
        const bool blocked = Until([&] { return entered.load(std::memory_order_acquire); });
        EXPECT_TRUE(blocked);
        std::vector<Task *> tasks;
        for (size_t index = 0; index < count; ++index)
          tasks.push_back(new BatchTask(pool, events[index], destroyed));
        pool.RunOnThreadBatch(tasks.data(), tasks.size(),
            external ? std::numeric_limits<size_t>::max() : 0);
        for (auto *task : tasks) EXPECT_EQ(task, nullptr);
        // A deliberately serial bounded-mailbox model: retain the first 1024
        // IDs while the worker is blocked, execute the remainder inline.
        for (size_t index = 0; index < count; ++index)
          EXPECT_EQ(events[index].load(), index < 1024 ? 0u : 1u) << index;
        release.store(true, std::memory_order_release);
        release.notify_one();
        EXPECT_TRUE(Until([&] { return destroyed.load(std::memory_order_acquire) == count; }));
      }
      for (size_t index = 0; index < count; ++index)
        EXPECT_EQ(events[index].load(), 1u) << index;
      EXPECT_EQ(destroyed.load(), count);
    }
  }
}

TEST(EigenPoolBatch, BatchInlineReentrantCancellationConsumesEveryPointer) {
  constexpr size_t count = 2063;
  std::vector<std::atomic<unsigned>> events(count);
  std::atomic<size_t> destroyed{0};
  std::atomic<bool> entered{false}, release{false};
  {
    ThreadPool pool(1, false, false);
    pool.Schedule(MakeTask([&] {
      entered.store(true, std::memory_order_release);
      release.wait(false, std::memory_order_acquire);
    }));
    EXPECT_TRUE(Until([&] { return entered.load(std::memory_order_acquire); }));
    std::vector<Task *> tasks;
    for (size_t index = 0; index < count; ++index)
      tasks.push_back(new BatchTask(pool, events[index], destroyed,
                                  index == 1024, index % 7 == 0));
    pool.RunOnThreadBatch(tasks.data(), tasks.size(), 0);
    for (auto *task : tasks) EXPECT_EQ(task, nullptr);
    EXPECT_TRUE(pool.IsCancelled());
    // First 1024 are queued/discarded; ID 1024 cancels inline; all later IDs
    // are rejected. No concurrent consumer can change this reference result.
    for (size_t index = 0; index < count; ++index)
      EXPECT_EQ(events[index].load(), index == 1024 ? 1u : 2u) << index;
    EXPECT_EQ(destroyed.load(), count);
    release.store(true, std::memory_order_release);
    release.notify_one();
  }
}

TEST(EigenPoolBatch, CancelledBatchDiscardsNonNullInputsExactlyOnce) {
  ThreadPool pool(2, false, false);
  pool.Cancel();
  std::array<std::atomic<unsigned>, 17> events{};
  std::atomic<size_t> destroyed{0};
  std::vector<Task *> tasks;
  for (auto &event : events) {
    tasks.push_back(nullptr);
    tasks.push_back(new BatchTask(pool, event, destroyed, false, true));
  }
  pool.RunOnThreadBatch(tasks.data(), tasks.size(), 0);
  for (auto *task : tasks) EXPECT_EQ(task, nullptr);
  for (auto &event : events) EXPECT_EQ(event.load(), 2u);
  EXPECT_EQ(destroyed.load(), events.size());
}

} // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
