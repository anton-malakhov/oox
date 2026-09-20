#pragma once
// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Copyright (C) 2016 Dmitry Vyukov <dvyukov@google.com>
//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// PBBS mailbox extensions were imported from EgorkaZ/pbbsbench's
// eigen-mailbox branch. See README.md in this directory for provenance and
// the OOX-specific adaptations retained here.

#include "mpmc_queue.h"
#ifndef OOX_EIGEN_NONBLOCKING_THREAD_POOL_H
#define OOX_EIGEN_NONBLOCKING_THREAD_POOL_H

#include "max_size_vector.h"
#include "run_queue.h"
#include "stl_thread_env.h"
#include "small_object_pool.h"

#include <atomic>
#include <cassert>
#ifdef OOX_EIGEN_ENABLE_STATS
#include <chrono>
#endif
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace oox::detail::eigen_pool {

#ifdef OOX_EIGEN_THREAD_POOL_TESTING
namespace internal {
inline std::atomic<size_t> completion_notifications{0};
inline std::atomic<size_t> completion_waits{0};
}
#endif

struct Task {
  std::atomic<size_t> *outstanding = nullptr;
  virtual void operator()() = 0;
  virtual void Discard() noexcept { delete this; }
  virtual ~Task() = default;
};

template <typename F>
struct UniqueTask final : Task, SmallObjectAllocated<UniqueTask<F>> {

  template <typename G> explicit UniqueTask(G &&f) : f(std::forward<G>(f)) {}

  void operator()() override {
    const auto release = [](UniqueTask *p) { DeleteSmallObject(p); };
    std::unique_ptr<UniqueTask, decltype(release)> self(this, release);
    f();
  }

  void Discard() noexcept override { DeleteSmallObject(this); }

  F f;
};

template <typename F> Task *MakeTask(F &&f) {
  return NewSmallObject<UniqueTask<std::decay_t<F>>>(std::forward<F>(f));
}

// This defines an interface that ThreadPoolDevice can take to use
// custom thread pools underneath.
class ThreadPoolInterface {
public:
  // Submits a closure to be run by a thread in the pool.
  virtual void Schedule(Task *task) = 0;

  // Submits a closure to be run by threads in the range [start, end) in the
  // pool.
  virtual void ScheduleWithHint(Task *task, int /*start*/, int /*end*/) {
    // Just defer to Schedule in case sub-classes aren't interested in
    // overriding this functionality.
    Schedule(task);
  }

  // If implemented, stop processing the closures that have been enqueued.
  // Currently running closures may still be processed.
  // If not implemented, does nothing.
  virtual void Cancel() {}

  // Returns the number of threads in the pool.
  virtual size_t NumThreads() const = 0;

  // Returns a logical thread index between 0 and NumThreads() - 1 if called
  // from one of the threads in the pool. Returns -1 otherwise.
  virtual size_t CurrentThreadId() const = 0;

  virtual ~ThreadPoolInterface() {}
};

template <typename Environment>
class ThreadPoolTempl : public ThreadPoolInterface {
public:
  using TaskPtr = Task *;
  using QueueEntry = uintptr_t;
  using Queue = RunQueue<QueueEntry, 1024>;

#ifdef OOX_EIGEN_ENABLE_STATS
  struct Statistics {
    uint64_t scheduled{};
    uint64_t executed{};
    uint64_t successful_steals{};
    uint64_t failed_steal_rounds{};
    uint64_t sleeps{};
    uint64_t idle_nanoseconds{};
  };
#endif

  ThreadPoolTempl(int num_threads, Environment env = Environment())
      : ThreadPoolTempl(num_threads, true, false, env) {}

  ThreadPoolTempl(int num_threads, bool allow_spinning, bool use_main_thread,
                  Environment env = Environment())
      : env_(env), num_threads_(ValidateThreadCount(num_threads)),
        allow_spinning_(allow_spinning), thread_data_(num_threads_),
        all_coprimes_(num_threads_),
        pool_generation_(NextPoolGeneration()), done_(false),
        cancelled_(false) {
    // Calculate coprimes of all numbers [1, num_threads].
    // Coprimes are used for random walks over all threads in Steal
    // operations. Iteration is based on the fact that if we take
    // a random starting thread index t and calculate num_threads - 1 subsequent
    // indices as (t + coprime) % num_threads, we will cover all threads without
    // repetitions (effectively getting a presudo-random permutation of thread
    // indices).
    for (int i = 1; i <= num_threads_; ++i) {
      all_coprimes_.emplace_back(i);
      ComputeCoprimes(i, &all_coprimes_.back());
    }
    thread_data_.resize(num_threads_);
    const bool needs_fallback_worker = use_main_thread && num_threads_ == 1;
    if (use_main_thread) {
      RegisterCreator(!needs_fallback_worker);
    }
    const int first_background_worker =
        use_main_thread && !needs_fallback_worker ? 1 : 0;
    try {
      for (int i = first_background_worker; i < num_threads_; ++i) {
        thread_data_[i].thread.reset(env_.CreateThread([this, i]() {
          PerThread *pt = GetPerThread();
          const PerThread previous = *pt;
          RegisterThread(pt, i, true);
          WorkerLoop();
          *pt = previous;
        }));
      }
    } catch (...) {
      done_.store(true, std::memory_order_release);
      WakeAll();
      JoinThreads();
      FlushQueues();
      RestoreCreatorRegistration();
      throw;
    }
  }

  ~ThreadPoolTempl() {
    done_.store(true, std::memory_order_release);
    WakeAll();
    JoinThreads();
    FlushQueues();
    RestoreCreatorRegistration();
  }

  void Schedule(TaskPtr p) override {
    // schedule on main thread only when explicitly requested
    ScheduleWithHint(p, 0, num_threads_);
  }

  void RunOnThread(TaskPtr t, size_t threadIndex) {
    // The target is a placement hint: another worker may steal the task.
    if (t == nullptr) {
      return;
    }
    if (cancelled_.load(std::memory_order_acquire)) {
      t->Discard();
      return;
    }
    threadIndex = threadIndex % num_threads_;
    PerThread *pt = GetPerThread();
    const bool local = IsRegistered(pt) && pt->owns_queue &&
                       threadIndex == static_cast<size_t>(pt->thread_id);
    PublishTask(t, static_cast<int>(threadIndex), local);
  }

  void ScheduleWithAffinity(TaskPtr task, size_t hint) {
    if (!task)
      return;
    if (IsCancelled()) {
      task->Discard();
      return;
    }
    PerThread *pt = GetPerThread();
    hint %= num_threads_;
    if (!IsRegistered(pt) || !pt->owns_queue ||
        hint == static_cast<size_t>(pt->thread_id)) {
      RunOnThread(task, hint);
      return;
    }
    const auto discard = [](Task *p) { p->Discard(); };
    std::unique_ptr<Task, decltype(discard)> pending(task, discard);
    auto *proxy = new AffinityProxy(task);
    pending.release();
    AccountTask(task, pt->thread_id);
    // The recipient may claim the work immediately. The local location keeps
    // the proxy alive until publication below consumes that location.
    thread_data_[hint].affinity_mailbox.Push(proxy);
    const QueueEntry entry = reinterpret_cast<QueueEntry>(proxy) | proxy_tag;
    try {
      if (!thread_data_[pt->thread_id].local_tasks.PushFront(entry)) {
        std::lock_guard<std::mutex> lock(overflow_mutex_);
        overflow_tasks_.push_back(entry);
        overflow_nonempty_.store(true, std::memory_order_release);
      }
    } catch (...) {
      // The recipient may be unavailable. Do not leave useful work reachable
      // only from its mailbox when the stealable sender location failed.
      if (TaskPtr unclaimed = proxy->template Extract<AffinityProxy::local_bit>()) {
        auto *outstanding = unclaimed->outstanding;
        unclaimed->Discard();
        TaskFinished(outstanding);
      }
      throw;
    }
    WakeOneWorker();
  }

  void ScheduleWithHint(TaskPtr t, int start, int limit) override {
    if (t == nullptr) {
      return;
    }
    AssertBounds(start, limit);
    if (cancelled_.load(std::memory_order_acquire)) {
      t->Discard();
      return;
    }

    PerThread *pt = GetPerThread();
    if (IsRegistered(pt) && pt->owns_queue && pt->thread_id >= start &&
        pt->thread_id < limit) {
      // Worker thread of this pool, push onto the thread's queue.
      PublishTask(t, pt->thread_id, true);
      return;
    }

    if (pt->rand == 0) {
      pt->rand = GlobalThreadIdHash();
    }
    const int target =
        start + static_cast<int>(Rand(&pt->rand) % (limit - start));
    PublishTask(t, target, false);
  }

  void Cancel() override {
    cancelled_.store(true, std::memory_order_release);
    done_.store(true, std::memory_order_release);

    // Let each thread know it's been cancelled.
#ifdef OOX_EIGEN_THREAD_ENV_SUPPORTS_CANCELLATION
    for (size_t i = 0; i < thread_data_.size(); i++) {
      if (thread_data_[i].thread) {
        thread_data_[i].thread->OnCancel();
      }
    }
#endif
    WakeAll();
  }

  size_t NumThreads() const final { return num_threads_; }

  bool IsCancelled() const noexcept {
    return cancelled_.load(std::memory_order_acquire);
  }

  size_t CurrentThreadId() const final {
    const PerThread *pt = const_cast<ThreadPoolTempl *>(this)->GetPerThread();
    if (IsRegistered(pt)) {
      return pt->thread_id;
    } else {
      return -1;
    }
  }

  // returns true if processed some tasks
  bool JoinMainThread() {
    if (CurrentThreadId() == -1) {
      return false;
    }
    return WorkerLoop(/* external */ true);
  }

  bool TryExecuteSomething() {
    if (CurrentThreadId() == -1) [[unlikely]] {
      return false;
    }
    constexpr bool External = true;
    constexpr bool JustOnce = true;
    return WorkerLoop(External, JustOnce);
  }

#ifdef OOX_EIGEN_ENABLE_STATS
  Statistics GetStatistics() const {
    Statistics result;
    for (const auto &data : thread_data_) {
      result.scheduled += data.statistics.scheduled.load(std::memory_order_relaxed);
      result.executed += data.statistics.executed.load(std::memory_order_relaxed);
      result.successful_steals +=
          data.statistics.successful_steals.load(std::memory_order_relaxed);
      result.failed_steal_rounds +=
          data.statistics.failed_steal_rounds.load(std::memory_order_relaxed);
      result.sleeps += data.statistics.sleeps.load(std::memory_order_relaxed);
      result.idle_nanoseconds +=
          data.statistics.idle_nanoseconds.load(std::memory_order_relaxed);
    }
    return result;
  }
#endif

  template <typename Predicate> void Wait(Predicate ready) {
    const bool registered = IsRegistered(GetPerThread());
    if (ready()) {
      return;
    }
    auto &event = registered ? worker_event_ : waiter_event_;

    while (!ready()) {
      // Help before registering; the second check below prevents lost wakeups.
      if (registered && TryExecuteOne()) {
        continue;
      }
      const uint64_t token = event.PrepareWait();
      if (registered && TryExecuteOne()) {
        event.CancelWait();
        continue;
      }
      if (ready() || cancelled_.load(std::memory_order_acquire)) {
        event.CancelWait();
        return;
      }
#ifdef OOX_EIGEN_THREAD_POOL_TESTING
      internal::completion_waits.fetch_add(1);
      internal::completion_waits.notify_one();
#endif
      event.Wait(token);
    }
  }

  void NotifyTaskCompletion() {
#ifdef OOX_EIGEN_THREAD_POOL_TESTING
    internal::completion_notifications.fetch_add(1);
#endif
    worker_event_.NotifyAll();
    waiter_event_.NotifyAll();
  }

private:
  // Create a single atomic<int> that encodes start and limit information for
  // each thread.
  // We expect num_threads_ < 65536, so we can store them in a single
  // std::atomic<unsigned>.
  // Exposed publicly as static functions so that external callers can reuse
  // this encode/decode logic for maintaining their own thread-safe copies of
  // scheduling and steal domain(s).
  static const int kMaxPartitionBits = 16;
  static const int kMaxThreads = 1 << kMaxPartitionBits;
  static const int kSpinCount = 64;

  class EventCount {
  public:
    uint64_t PrepareWait() {
      // The caller checks for work again after registering. Sequential
      // consistency makes either that check or a concurrent notification win.
      const uint64_t epoch = epoch_.load(std::memory_order_seq_cst);
      waiters_.fetch_add(1, std::memory_order_seq_cst);
      std::atomic_thread_fence(std::memory_order_seq_cst);
      return epoch;
    }

    void CancelWait() { waiters_.fetch_sub(1, std::memory_order_seq_cst); }

    void Wait(uint64_t token) {
      epoch_.wait(token, std::memory_order_acquire);
      CancelWait();
    }

    void NotifyOne() { Notify(false); }
    void NotifyAll() { Notify(true); }

  private:
    void Notify(bool all) {
      std::atomic_thread_fence(std::memory_order_seq_cst);
      if (waiters_.load(std::memory_order_seq_cst) == 0) {
        return;
      }
      epoch_.fetch_add(1, std::memory_order_seq_cst);
      if (all) {
        epoch_.notify_all();
      } else {
        epoch_.notify_one();
      }
    }

    std::atomic<uint64_t> epoch_{0};
    std::atomic<size_t> waiters_{0};
  };

  static int ValidateThreadCount(int count) {
    if (count <= 0 || count >= kMaxThreads) {
      throw std::invalid_argument("thread count must be in [1, 65535]");
    }
    return count;
  }

  void ExecuteTask(TaskPtr p) {
    struct FinishTask {
      ThreadPoolTempl *pool;
      std::atomic<size_t> *outstanding;
      ~FinishTask() { pool->TaskFinished(outstanding); }
    } finish{this, p->outstanding};
    try {
      (*p)();
    } catch (...) {
      // Exception-aware tasks must publish failure before returning. The pool
      // cannot repair an arbitrary task's completion state after it unwinds;
      // continuing here could leave its dependents waiting forever.
      std::terminate();
    }
  }

  void AssertBounds(int start, int end) {
    if (start < 0 || start >= end || end > num_threads_) {
      throw std::invalid_argument("invalid scheduling partition");
    }
  }

  void ComputeCoprimes(int N, MaxSizeVector<unsigned> *coprimes) {
    for (int i = 1; i <= N; i++) {
      unsigned a = i;
      unsigned b = N;
      // If GCD(a, b) == 1, then a and b are coprimes.
      while (b != 0) {
        unsigned tmp = a;
        a = b;
        b = tmp % b;
      }
      if (a == 1) {
        coprimes->push_back(i);
      }
    }
  }

  typedef typename Environment::EnvThread Thread;

  static constexpr QueueEntry proxy_tag = 1;

  struct AffinityProxy final {
    static constexpr uintptr_t local_bit = 1;
    static constexpr uintptr_t mailbox_bit = 2;
    static constexpr uintptr_t locations = local_bit | mailbox_bit;
    static_assert(alignof(Task) > locations);

    explicit AffinityProxy(TaskPtr task) noexcept
        : task_and_locations(reinterpret_cast<uintptr_t>(task) | locations) {}

    template <uintptr_t Location> TaskPtr Extract() noexcept {
      constexpr uintptr_t other = locations ^ Location;
      const uintptr_t previous =
          task_and_locations.fetch_and(other, std::memory_order_acq_rel);
      // The other location can reclaim this proxy immediately after the RMW.
      // Only the last location may access it again.
      if (!(previous & other))
        delete this;
      return reinterpret_cast<TaskPtr>(previous & ~locations);
    }

    std::atomic<uintptr_t> task_and_locations;
    AffinityProxy *next = nullptr;
  };

  // Multiple publishers; only the queue's owning worker consumes. A detached
  // batch is reversed to preserve FIFO mailbox order. Thieves use the sender's
  // local proxy, so they never consume another worker's affinity mailbox.
  class AffinityMailbox {
  public:
    void Push(AffinityProxy *proxy) noexcept {
      auto *head = incoming_.load(std::memory_order_relaxed);
      do {
        proxy->next = head;
      } while (!incoming_.compare_exchange_weak(
          head, proxy, std::memory_order_release, std::memory_order_relaxed));
    }

    AffinityProxy *Pop() noexcept {
      if (!ready_) {
        if (!incoming_.load(std::memory_order_acquire))
          return nullptr;
        auto *batch = incoming_.exchange(nullptr, std::memory_order_acquire);
        while (batch) {
          auto *next = batch->next;
          batch->next = ready_;
          ready_ = batch;
          batch = next;
        }
      }
      auto *result = ready_;
      if (result)
        ready_ = result->next;
      return result;
    }

  private:
    std::atomic<AffinityProxy *> incoming_{nullptr};
    AffinityProxy *ready_ = nullptr;
  };

  static TaskPtr ExtractEntry(QueueEntry entry) noexcept {
    if (entry & proxy_tag)
      return reinterpret_cast<AffinityProxy *>(entry & ~proxy_tag)
          ->template Extract<AffinityProxy::local_bit>();
    return reinterpret_cast<TaskPtr>(entry);
  }

  struct PerThread {
    constexpr PerThread()
        : pool(nullptr), pool_generation(0), rand(0), thread_id(-1),
          owns_queue(false) {}
    ThreadPoolTempl *pool; // Parent pool, or null for normal threads.
    uint64_t pool_generation;
    uint64_t rand;         // Random generator state.
    int thread_id;         // Worker thread index in pool.
    bool owns_queue;
  };

#ifdef OOX_EIGEN_ENABLE_STATS
  struct AtomicStatistics {
    std::atomic<uint64_t> scheduled{0};
    std::atomic<uint64_t> executed{0};
    std::atomic<uint64_t> successful_steals{0};
    std::atomic<uint64_t> failed_steal_rounds{0};
    std::atomic<uint64_t> sleeps{0};
    std::atomic<uint64_t> idle_nanoseconds{0};
  };
#endif

  struct ThreadData {
    ThreadData()
        : thread(), outstanding_tasks(0), local_tasks(),
          mailbox(1024) {}
    std::unique_ptr<Thread> thread;
    std::atomic<size_t> outstanding_tasks;
    Queue local_tasks;
    AffinityMailbox affinity_mailbox;
    rigtorp::mpmc::Queue<TaskPtr> mailbox;
#ifdef OOX_EIGEN_ENABLE_STATS
    AtomicStatistics statistics;
#endif

    bool PushTask(TaskPtr p, bool localThread) {
      if (localThread) {
        return local_tasks.PushFront(reinterpret_cast<QueueEntry>(p));
      } else {
        return mailbox.try_push(p);
      }
    }

    TaskPtr PopFront() {
      while (auto entry = local_tasks.PopFront())
        if (auto *task = ExtractEntry(entry))
          return task;
      while (auto *proxy = affinity_mailbox.Pop())
        if (auto *task = proxy->template Extract<AffinityProxy::mailbox_bit>())
          return task;
      TaskPtr task = nullptr;
      mailbox.try_pop(task);
      return task;
    }

    TaskPtr PopBack(bool force) {
      TaskPtr task = nullptr;
      mailbox.try_pop(task);
      if (!task && force) {
        while (auto entry = local_tasks.PopBack())
          if ((task = ExtractEntry(entry)))
            break;
      }
      return task;
    }
  };

  Environment env_;
  const int num_threads_;
  const bool allow_spinning_;
  MaxSizeVector<ThreadData> thread_data_;
  MaxSizeVector<MaxSizeVector<unsigned>> all_coprimes_;
  const uint64_t pool_generation_;

  std::mutex overflow_mutex_;
  std::atomic<bool> overflow_nonempty_{false};
  std::deque<QueueEntry> overflow_tasks_;
  EventCount worker_event_;
  EventCount waiter_event_;
  std::atomic<bool> done_;
  std::atomic<bool> cancelled_;

  bool creator_registered_ = false;
  std::thread::id creator_thread_id_;
  PerThread creator_previous_registration_;

  // Main worker thread loop. Returns true if processed some tasks
  bool WorkerLoop(bool external = false, bool once = false) {
    bool processed_anything = false;
    for (;;) {
      if (cancelled_.load(std::memory_order_acquire)) {
        return processed_anything;
      }

      if (TryExecuteOne()) {
        processed_anything = true;
        if (once) {
          return true;
        }
        continue;
      }
      if (external || once || ShouldExit()) {
        return processed_anything;
      }

      bool found_work = false;
      if (allow_spinning_) {
        for (int i = 0; i < kSpinCount; ++i) {
          if (cancelled_.load(std::memory_order_acquire)) {
            return processed_anything;
          }
          if (TryExecuteOne()) {
            processed_anything = true;
            found_work = true;
            break;
          }
          std::this_thread::yield();
        }
      }
      if (found_work) {
        continue;
      }

      const uint64_t token = worker_event_.PrepareWait();
      if (TryExecuteOne()) {
        worker_event_.CancelWait();
        processed_anything = true;
        continue;
      }
      if (cancelled_.load(std::memory_order_acquire) || ShouldExit()) {
        worker_event_.CancelWait();
        return processed_anything;
      }
#ifdef OOX_EIGEN_ENABLE_STATS
      const auto idle_begin = std::chrono::steady_clock::now();
#endif
      worker_event_.Wait(token);
#ifdef OOX_EIGEN_ENABLE_STATS
      const auto idle_end = std::chrono::steady_clock::now();
      auto &statistics = thread_data_[GetPerThread()->thread_id].statistics;
      statistics.sleeps.fetch_add(1, std::memory_order_relaxed);
      statistics.idle_nanoseconds.fetch_add(
          std::chrono::duration_cast<std::chrono::nanoseconds>(idle_end -
                                                               idle_begin)
              .count(),
          std::memory_order_relaxed);
#endif
    }
  }

  static uint64_t NextPoolGeneration() {
    static std::atomic<uint64_t> next{1};
    return next.fetch_add(1, std::memory_order_relaxed);
  }

  void RegisterThread(PerThread *pt, int thread_id, bool owns_queue) {
    pt->pool = this;
    pt->pool_generation = pool_generation_;
    pt->rand = GlobalThreadIdHash();
    pt->thread_id = thread_id;
    pt->owns_queue = owns_queue;
  }

  void RegisterCreator(bool owns_queue) {
    creator_thread_id_ = std::this_thread::get_id();
    PerThread *pt = GetPerThread();
    creator_previous_registration_ = *pt;
    RegisterThread(pt, 0, owns_queue);
    creator_registered_ = true;
  }

  void RestoreCreatorRegistration() {
    if (!creator_registered_ ||
        creator_thread_id_ != std::this_thread::get_id()) {
      return;
    }
    PerThread *pt = GetPerThread();
    if (IsRegistered(pt)) {
      *pt = creator_previous_registration_;
    }
    creator_registered_ = false;
  }

  bool IsRegistered(const PerThread *pt) const {
    return pt->pool == this && pt->pool_generation == pool_generation_;
  }

  void AccountTask(TaskPtr task, int target) noexcept {
#ifdef OOX_EIGEN_ENABLE_STATS
    thread_data_[target].statistics.scheduled.fetch_add(
        1, std::memory_order_relaxed);
#endif
    auto &outstanding = thread_data_[target].outstanding_tasks;
    task->outstanding = &outstanding;
    outstanding.fetch_add(1, std::memory_order_relaxed);
  }

  void PublishTask(TaskPtr task, int target, bool local) {
    AccountTask(task, target);
    auto &outstanding = thread_data_[target].outstanding_tasks;
    try {
      if (!thread_data_[target].PushTask(task, local)) {
        std::lock_guard<std::mutex> lock(overflow_mutex_);
        overflow_tasks_.push_back(reinterpret_cast<QueueEntry>(task));
        overflow_nonempty_.store(true, std::memory_order_release);
      }
    } catch (...) {
      outstanding.fetch_sub(1, std::memory_order_relaxed);
      task->Discard();
      throw;
    }
    WakeOneWorker();
  }

  TaskPtr PopOverflow() {
    while (overflow_nonempty_.load(std::memory_order_acquire)) {
      QueueEntry entry;
      {
        std::lock_guard<std::mutex> lock(overflow_mutex_);
        if (overflow_tasks_.empty())
          return nullptr;
        entry = overflow_tasks_.front();
        overflow_tasks_.pop_front();
        overflow_nonempty_.store(!overflow_tasks_.empty(),
                                 std::memory_order_release);
      }
      if (auto *task = ExtractEntry(entry))
        return task;
    }
    return nullptr;
  }

  bool TryExecuteOne() {
    PerThread *pt = GetPerThread();
    assert(IsRegistered(pt));

    TaskPtr task = nullptr;
    if (pt->owns_queue) {
      task = thread_data_[pt->thread_id].PopFront();
    }
    if (!task) {
      task = PopOverflow();
    }
    if (!task) {
      task = GlobalSteal(true);
    }
    if (!task) {
      return false;
    }
#ifdef OOX_EIGEN_ENABLE_STATS
    thread_data_[pt->thread_id].statistics.executed.fetch_add(
        1, std::memory_order_relaxed);
#endif
    ExecuteTask(task);
    return true;
  }

  void TaskFinished(std::atomic<size_t> *outstanding) {
    assert(outstanding != nullptr);
    const size_t previous = outstanding->fetch_sub(1, std::memory_order_seq_cst);
    assert(previous > 0);
    if (previous == 1 && done_.load(std::memory_order_acquire) &&
        NoOutstandingTasks()) {
      WakeAllWorkers();
    }
  }

  bool NoOutstandingTasks() const {
    for (const auto &data : thread_data_) {
      if (data.outstanding_tasks.load(std::memory_order_acquire) != 0) {
        return false;
      }
    }
    return true;
  }

  bool ShouldExit() const {
    return done_.load(std::memory_order_acquire) && NoOutstandingTasks();
  }

  void WakeOneWorker() { worker_event_.NotifyOne(); }

  void WakeAllWorkers() { worker_event_.NotifyAll(); }

  void WakeAll() {
    WakeAllWorkers();
    waiter_event_.NotifyAll();
  }

  void JoinThreads() {
    for (auto &data : thread_data_) {
      data.thread.reset();
    }
  }

  void FlushQueues() {
    for (auto &data : thread_data_) {
      while (TaskPtr task = data.PopFront()) {
        auto *outstanding = task->outstanding;
        task->Discard();
        outstanding->fetch_sub(1, std::memory_order_relaxed);
      }
    }
    std::lock_guard<std::mutex> lock(overflow_mutex_);
    while (!overflow_tasks_.empty()) {
      const auto entry = overflow_tasks_.front();
      overflow_tasks_.pop_front();
      if (auto *task = ExtractEntry(entry)) {
        auto *outstanding = task->outstanding;
        task->Discard();
        outstanding->fetch_sub(1, std::memory_order_relaxed);
      }
    }
    overflow_nonempty_.store(false, std::memory_order_release);
    assert(NoOutstandingTasks());
  }

  // Steal tries to steal work from other worker threads in the range [start,
  // limit) in best-effort manner.
  TaskPtr Steal(unsigned start, unsigned limit, bool force) {
    PerThread *pt = GetPerThread();
    const size_t size = limit - start;
    unsigned r = Rand(&pt->rand);
    // Reduce r into [0, size) range, this utilizes trick from
    // https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
    assert(all_coprimes_[size - 1].size() < (1 << 30));
    unsigned victim = ((uint64_t)r * (uint64_t)size) >> 32;
    unsigned index =
        ((uint64_t)all_coprimes_[size - 1].size() * (uint64_t)r) >> 32;
    unsigned inc = all_coprimes_[size - 1][index];

    for (unsigned i = 0; i < size; i++) {
      assert(start + victim < limit);
      TaskPtr t = thread_data_[start + victim].PopBack(force);
      if (t) {
#ifdef OOX_EIGEN_ENABLE_STATS
        if (static_cast<int>(start + victim) != pt->thread_id)
          thread_data_[pt->thread_id].statistics.successful_steals.fetch_add(
              1, std::memory_order_relaxed);
#endif
        return t;
      }
      victim += inc;
      if (victim >= size) {
        victim -= size;
      }
    }
#ifdef OOX_EIGEN_ENABLE_STATS
    thread_data_[pt->thread_id].statistics.failed_steal_rounds.fetch_add(
        1, std::memory_order_relaxed);
#endif
    return nullptr;
  }

  // Steals work from any other thread in the pool.
  TaskPtr GlobalSteal(bool force) { return Steal(0, num_threads_, force); }

  static inline uint64_t GlobalThreadIdHash() {
    return std::hash<std::thread::id>()(std::this_thread::get_id());
  }

  inline PerThread *GetPerThread() {
    static thread_local PerThread per_thread_;
    PerThread *pt = &per_thread_;
    return pt;
  }

  static inline unsigned Rand(uint64_t *state) {
    uint64_t current = *state;
    // Update the internal state
    *state = current * 6364136223846793005ULL + 0xda3e39cb94b95bdbULL;
    // Generate the random output (using the PCG-XSH-RS scheme)
    return static_cast<unsigned>((current ^ (current >> 22)) >>
                                 (22 + (current >> 61)));
  }
};

typedef ThreadPoolTempl<StlThreadEnvironment> ThreadPool;

} // namespace oox::detail::eigen_pool

#endif // OOX_EIGEN_NONBLOCKING_THREAD_POOL_H
