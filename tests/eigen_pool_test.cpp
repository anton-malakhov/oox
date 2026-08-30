// SPDX-License-Identifier: Apache-2.0

#include <oox/eigen/nonblocking_thread_pool.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <ctime>
#include <future>
#include <memory>
#include <thread>
#include <vector>

namespace {

using oox::detail::eigen_pool::MakeTask;
using oox::detail::eigen_pool::RapidFallbackTask;
using oox::detail::eigen_pool::RapidTask;
using oox::detail::eigen_pool::RegionContext;
using oox::detail::eigen_pool::ThreadPool;
using namespace std::chrono_literals;

struct CountingRapidTask final : RapidTask {
  CountingRapidTask(std::atomic<int> &count, int total,
                    std::promise<void> &completed,
                    std::promise<void> *publishing = nullptr,
                    std::shared_future<void> published = {})
      : count(count), total(total), completed(completed), publishing(publishing),
        published(published) {}

  void AddTickets(size_t count) final {
    tickets.fetch_add(count);
    if (publishing) {
      publishing->set_value();
      published.wait();
    }
  }
  bool TryRun() final {
    if (claimed.exchange(true)) {
      return false;
    }
    if (count.fetch_add(1) + 1 == total) {
      completed.set_value();
    }
    return true;
  }
  void Cancel() noexcept final { claimed.store(true); }
  void ReleaseTicket() final { tickets.fetch_sub(1); }
  RegionContext *Context() noexcept final { return &context; }
  oox::detail::eigen_pool::Task *FallbackTicket() noexcept final {
    return &fallback;
  }

  std::atomic<int> &count;
  int total;
  std::promise<void> &completed;
  std::atomic<bool> claimed{false};
  std::atomic<size_t> tickets{0};
  RegionContext context;
  RapidFallbackTask fallback;
  std::promise<void> *publishing;
  std::shared_future<void> published;
};

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
  std::atomic<int> parents_completed{0};
  std::promise<void> completed;
  auto result = completed.get_future();

  for (int i = 0; i < 2; ++i) {
    pool.Schedule(MakeTask([&] {
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

TEST(EigenPool, RapidOverflowUsesOrdinaryQueueFallback) {
  ThreadPool pool(1, false, false);
  std::promise<void> entered, release, completed;
  auto entered_result = entered.get_future();
  auto release_result = release.get_future().share();
  auto completed_result = completed.get_future();
  pool.Schedule(MakeTask([&] {
    entered.set_value();
    release_result.wait();
  }));
  ASSERT_EQ(entered_result.wait_for(2s), std::future_status::ready);

  constexpr int task_count = 1026;
  std::atomic<int> count{0};
  std::vector<std::unique_ptr<CountingRapidTask>> tasks;
  for (int i = 0; i < task_count; ++i) {
    tasks.push_back(
        std::make_unique<CountingRapidTask>(count, task_count, completed));
    pool.ScheduleRapid(tasks.back().get(), 0);
  }
  release.set_value();
  EXPECT_EQ(completed_result.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(count.load(), task_count);
  for (const auto &task : tasks) {
    EXPECT_EQ(task->tickets.load(), 0u);
  }
}

TEST(EigenPool, CancellationWaitsForRapidPublication) {
  ThreadPool pool(1, false, false);
  std::atomic<int> count{0};
  std::promise<void> completed, publishing, release;
  auto publishing_result = publishing.get_future();
  CountingRapidTask task(count, 1, completed, &publishing,
                         release.get_future().share());
  auto schedule = std::async(std::launch::async,
                             [&] { pool.ScheduleRapid(&task, 0); });
  ASSERT_EQ(publishing_result.wait_for(2s), std::future_status::ready);
  auto cancel = std::async(std::launch::async, [&] { pool.Cancel(); });
  EXPECT_EQ(cancel.wait_for(10ms), std::future_status::timeout);
  release.set_value();
  EXPECT_EQ(schedule.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(cancel.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(task.tickets.load(), 0u);
  EXPECT_EQ(count.load(), 0);
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

} // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
