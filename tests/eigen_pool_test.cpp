// SPDX-License-Identifier: Apache-2.0

#include <oox/eigen/nonblocking_thread_pool.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <ctime>
#include <cstdlib>
#include <future>
#include <memory>
#include <thread>
#include <vector>

namespace {

using oox::detail::eigen_pool::MakeTask;
using oox::detail::eigen_pool::ThreadPool;
using namespace std::chrono_literals;

struct MoveAwareCallable {
  bool *moved_from;
  bool *ran;

  MoveAwareCallable(bool &moved, bool &called)
      : moved_from(&moved), ran(&called) {}
  MoveAwareCallable(const MoveAwareCallable &) = default;
  MoveAwareCallable(MoveAwareCallable &&other) noexcept : ran(other.ran) {
    *other.moved_from = true;
    moved_from = other.moved_from;
  }
  void operator()() { *ran = true; }
};

TEST(EigenPool, MakeTaskCopiesLvalueCallable) {
  bool moved_from = false;
  bool ran = false;
  MoveAwareCallable callable(moved_from, ran);
  (*MakeTask(callable))();
  EXPECT_FALSE(moved_from);
  EXPECT_TRUE(ran);
}

TEST(EigenPoolDeathTest, UnhandledTaskExceptionFailsFast) {
  EXPECT_EXIT({
    std::set_terminate([] { std::_Exit(86); });
    ThreadPool pool(1, false, false);
    pool.Schedule(MakeTask([] { throw std::bad_alloc{}; }));
    std::this_thread::sleep_for(2s);
    std::_Exit(0);
  }, testing::ExitedWithCode(86), "");
}

TEST(EigenPool, RejectsNonPositiveThreadCounts) {
  EXPECT_THROW(ThreadPool(0), std::invalid_argument);
  EXPECT_THROW(ThreadPool(-1), std::invalid_argument);
}

TEST(EigenPool, OneBackgroundWorkerRunsExternalSubmission) {
  ThreadPool pool(1, false, false);
  std::promise<void> completed;
  auto result = completed.get_future();
  std::thread producer(
      [&] { pool.Schedule(MakeTask([&] { completed.set_value(); })); });
  producer.join();
  EXPECT_EQ(result.wait_for(2s), std::future_status::ready);
}

TEST(EigenPool, OneMainSlotHasFallbackWorker) {
  ThreadPool pool(1, false, true);
  EXPECT_EQ(pool.CurrentThreadId(), 0u);
  std::promise<void> completed;
  auto result = completed.get_future();
  std::thread producer(
      [&] { pool.Schedule(MakeTask([&] { completed.set_value(); })); });
  producer.join();
  EXPECT_EQ(result.wait_for(2s), std::future_status::ready);
}

TEST(EigenPool, SurvivesCreatorThreadExit) {
  std::unique_ptr<ThreadPool> pool;
  std::thread creator([&] { pool = std::make_unique<ThreadPool>(2); });
  creator.join();

  std::promise<void> completed;
  auto result = completed.get_future();
  std::thread producer(
      [&] { pool->Schedule(MakeTask([&] { completed.set_value(); })); });
  producer.join();
  EXPECT_EQ(result.wait_for(2s), std::future_status::ready);
}

TEST(EigenPool, NestedWaitsMakeProgressWithAllWorkersOccupied) {
  ThreadPool pool(2, false, false);
  std::atomic<int> parents_started{0};
  std::atomic<int> parents_completed{0};
  std::promise<void> completed;
  auto result = completed.get_future();

  for (int i = 0; i < 2; ++i) {
    pool.Schedule(MakeTask([&] {
      parents_started.fetch_add(1);
      while (parents_started.load() != 2)
        std::this_thread::yield();
      auto child_done = std::make_shared<std::atomic<bool>>(false);
      pool.Schedule(MakeTask([&, child_done] {
        child_done->store(true, std::memory_order_release);
        pool.NotifyTaskCompletion();
      }));
      pool.Wait([&] { return child_done->load(std::memory_order_acquire); });
      if (parents_completed.fetch_add(1, std::memory_order_acq_rel) == 1) {
        completed.set_value();
      }
    }));
  }

  EXPECT_EQ(result.wait_for(2s), std::future_status::ready);
}

TEST(EigenPool, QueueOverflowDoesNotRecurseInline) {
  ThreadPool pool(2, false, false);
  constexpr int task_count = 5000;
  std::atomic<int> completed_count{0};
  std::promise<void> completed;
  auto result = completed.get_future();

  pool.Schedule(MakeTask([&] {
    for (int i = 0; i < task_count; ++i) {
      pool.Schedule(MakeTask([&] {
        if (completed_count.fetch_add(1, std::memory_order_acq_rel) + 1 ==
            task_count) {
          completed.set_value();
        }
      }));
    }
  }));

  EXPECT_EQ(result.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(completed_count.load(), task_count);
}

TEST(EigenPool, AcceptsConcurrentExternalProducers) {
  ThreadPool pool(4, false, false);
  constexpr int producer_count = 8;
  constexpr int tasks_per_producer = 500;
  constexpr int task_count = producer_count * tasks_per_producer;
  std::atomic<int> completed_count{0};
  std::promise<void> completed;
  auto result = completed.get_future();
  std::vector<std::thread> producers;

  for (int producer = 0; producer < producer_count; ++producer) {
    producers.emplace_back([&] {
      for (int i = 0; i < tasks_per_producer; ++i) {
        pool.Schedule(MakeTask([&] {
          if (completed_count.fetch_add(1, std::memory_order_acq_rel) + 1 ==
              task_count) {
            completed.set_value();
          }
        }));
      }
    });
  }
  for (auto &producer : producers) {
    producer.join();
  }

  EXPECT_EQ(result.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(completed_count.load(), task_count);
}

TEST(EigenPool, NonWorkerWaitParks) {
  ThreadPool pool(1, false, false);
  std::atomic<bool> done{false};
  pool.Schedule(MakeTask([&] {
    std::this_thread::sleep_for(200ms);
    done.store(true, std::memory_order_release);
    pool.NotifyTaskCompletion();
  }));

  const std::clock_t cpu_start = std::clock();
  pool.Wait([&] { return done.load(std::memory_order_acquire); });
  const double cpu_seconds =
      static_cast<double>(std::clock() - cpu_start) / CLOCKS_PER_SEC;
  EXPECT_LT(cpu_seconds, 0.12);
}

TEST(EigenPool, PublicationWakesWorkerInsteadOfExternalWaiter) {
  ThreadPool pool(1, false, false);
  std::atomic<bool> done{false};
  std::atomic<int> waiting{0};
  std::vector<std::thread> waiters;
  for (int i = 0; i < 8; ++i) {
    waiters.emplace_back([&] {
      waiting.fetch_add(1, std::memory_order_release);
      pool.Wait([&] { return done.load(std::memory_order_acquire); });
    });
  }
  while (waiting.load(std::memory_order_acquire) != 8) {
    std::this_thread::yield();
  }

  pool.Schedule(MakeTask([&] {
    done.store(true, std::memory_order_release);
    pool.NotifyTaskCompletion();
  }));
  for (auto &waiter : waiters) {
    waiter.join();
  }
  EXPECT_TRUE(done.load());
}

TEST(EigenPool, RepeatedPublicationDoesNotLoseWakeups) {
  ThreadPool pool(1, false, false);
  for (int iteration = 0; iteration < 5000; ++iteration) {
    auto completed = std::make_shared<std::promise<void>>();
    auto result = completed->get_future();
    pool.Schedule(MakeTask([completed] { completed->set_value(); }));
    if (result.wait_for(1s) != std::future_status::ready) {
      pool.Cancel();
      FAIL() << "lost publication wakeup at iteration " << iteration;
    }
  }
}

TEST(EigenPool, RepeatedCompletionDoesNotLoseWakeups) {
  ThreadPool pool(1, false, false);
  for (int iteration = 0; iteration < 5000; ++iteration) {
    auto entered = std::make_shared<std::atomic<bool>>(false);
    auto released = std::make_shared<std::atomic<bool>>(false);
    auto completed = std::make_shared<std::promise<void>>();
    auto result = completed->get_future();
    pool.Schedule(MakeTask([&, entered, released, completed] {
      entered->store(true, std::memory_order_release);
      entered->notify_one();
      pool.Wait([&] { return released->load(std::memory_order_acquire); });
      completed->set_value();
    }));
    entered->wait(false, std::memory_order_acquire);
    released->store(true, std::memory_order_release);
    pool.NotifyTaskCompletion();
    if (result.wait_for(1s) != std::future_status::ready) {
      pool.Cancel();
      EXPECT_EQ(result.wait_for(1s), std::future_status::ready);
      FAIL() << "lost completion wakeup at iteration " << iteration;
    }
  }
}

TEST(EigenPool, IdleWorkersPark) {
  const std::clock_t cpu_start = std::clock();
  {
    ThreadPool pool(4, false, false);
    std::this_thread::sleep_for(300ms);
  }
  const double cpu_seconds =
      static_cast<double>(std::clock() - cpu_start) / CLOCKS_PER_SEC;
  EXPECT_LT(cpu_seconds, 0.20);
}

TEST(EigenPool, DestructorDrainsPublishedTasks) {
  constexpr int task_count = 3000;
  std::atomic<int> completed{0};
  {
    ThreadPool pool(4, false, false);
    for (int i = 0; i < task_count; ++i) {
      pool.Schedule(MakeTask([&] { completed.fetch_add(1); }));
    }
  }
  EXPECT_EQ(completed.load(), task_count);
}

TEST(EigenPool, CancellationWakesParkedWorkers) {
  ThreadPool pool(4, false, false);
  pool.Cancel();
}

TEST(EigenPool, MakeTaskSupportsPolymorphicDeletion) {
  auto token = std::make_shared<int>(17);
  std::weak_ptr<int> lifetime = token;
  std::unique_ptr<oox::detail::eigen_pool::Task> task(
      MakeTask([token] {}));
  token.reset();
  EXPECT_FALSE(lifetime.expired());
  task.reset();
  EXPECT_TRUE(lifetime.expired());

  struct alignas(128) Payload {
    std::shared_ptr<int> token;
    char padding[512]{};
  };
  token = std::make_shared<int>(23);
  lifetime = token;
  Payload payload{token};
  task.reset(MakeTask([payload = std::move(payload)] {}));
  token.reset();
  EXPECT_FALSE(lifetime.expired());
  task.reset();
  EXPECT_TRUE(lifetime.expired());
}

TEST(EigenPool, AffinitySenderDrainsWhileRecipientIsBlocked) {
  constexpr size_t count = 65537;
  std::vector<std::atomic<unsigned>> visits(count);
  {
    std::atomic<bool> entered{false}, released{false};
    ThreadPool pool(2, false, true);
    pool.RunOnThread(MakeTask([&] {
      entered.store(true, std::memory_order_release);
      entered.notify_one();
      released.wait(false, std::memory_order_acquire);
    }), 1);
    entered.wait(false, std::memory_order_acquire);
    for (size_t i = 0; i < count; ++i) {
      pool.ScheduleWithAffinity(MakeTask([&, i] {
        visits[i].fetch_add(1, std::memory_order_release);
        pool.NotifyTaskCompletion();
      }), 1);
      pool.Wait([&] { return visits[i].load(std::memory_order_acquire) != 0; });
    }
    released.store(true, std::memory_order_release);
    released.notify_one();
  }
  for (size_t i = 0; i < count; ++i)
    EXPECT_EQ(visits[i].load(), 1u) << "case=blocked-recipient item=" << i;
}

TEST(EigenPool, AffinityRecipientWinsAndSenderEntriesRemainSafe) {
  constexpr size_t count = 65537;
  std::vector<std::atomic<unsigned>> visits(count);
  {
    std::atomic<size_t> completed{0};
    std::promise<void> done;
    auto result = done.get_future();
    ThreadPool pool(2, false, true);
    for (size_t i = 0; i < count; ++i)
      pool.ScheduleWithAffinity(MakeTask([&, i] {
        visits[i].fetch_add(1, std::memory_order_relaxed);
        if (completed.fetch_add(1, std::memory_order_acq_rel) + 1 == count)
          done.set_value();
      }), 1);
    EXPECT_EQ(result.wait_for(5s), std::future_status::ready);
    // Destruction drains already-claimed sender entries, including overflow.
  }
  for (size_t i = 0; i < count; ++i)
    EXPECT_EQ(visits[i].load(), 1u) << "case=recipient-wins item=" << i;
}

TEST(EigenPool, AffinityCancellationDiscardsEachUsefulTaskOnce) {
  struct CountedTask : oox::detail::eigen_pool::Task {
    CountedTask(std::atomic<size_t> &ran, std::atomic<size_t> &destroyed)
        : ran(ran), destroyed(destroyed) {}
    ~CountedTask() override { destroyed.fetch_add(1); }
    void operator()() override { ran.fetch_add(1); delete this; }
    std::atomic<size_t> &ran;
    std::atomic<size_t> &destroyed;
  };
  constexpr size_t count = 4097;
  std::atomic<size_t> ran{0}, destroyed{0};
  {
    std::atomic<bool> entered{false}, released{false};
    ThreadPool pool(2, false, true);
    pool.RunOnThread(MakeTask([&] {
      entered.store(true, std::memory_order_release);
      entered.notify_one();
      released.wait(false, std::memory_order_acquire);
    }), 1);
    entered.wait(false, std::memory_order_acquire);
    for (size_t i = 0; i < count; ++i)
      pool.ScheduleWithAffinity(new CountedTask(ran, destroyed), 1);
    pool.Cancel();
    released.store(true, std::memory_order_release);
    released.notify_one();
  }
  EXPECT_EQ(ran.load(), 0u);
  EXPECT_EQ(destroyed.load(), count);
}

} // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
