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

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace oox::detail::eigen_pool {

class RapidRegionBase;

struct DomainId {
  unsigned start = 0;
  unsigned limit = 0;

  size_t Size() const noexcept { return limit - start; }
  bool Contains(size_t worker) const noexcept {
    return worker >= start && worker < limit;
  }
};

struct RegionContext {
  RapidRegionBase *region = nullptr;
  DomainId domain;
  RegionContext *parent = nullptr;
};

struct Task;

class RapidTask {
public:
  virtual void AddTickets(size_t count) = 0;
  virtual bool TryRun() = 0;
  virtual void Cancel() noexcept = 0;
  virtual void ReleaseTicket() = 0;
  virtual RegionContext *Context() noexcept = 0;
  virtual Task *FallbackTicket() noexcept = 0;
  virtual ~RapidTask() = default;
};

struct Task {
  std::atomic<size_t> *outstanding = nullptr;
  RegionContext *region_context = nullptr;
  virtual void operator()() = 0;
  virtual void Discard() noexcept { delete this; }
  virtual ~Task() = default;
};

class RapidFallbackTask final : public Task {
public:
  void Bind(RapidTask *task) noexcept {
    rapid_ = task;
    region_context = task->Context();
  }
  void operator()() final {
    RapidTask *task = rapid_;
    rapid_ = nullptr;
    task->TryRun();
    task->ReleaseTicket();
  }
  void Discard() noexcept final {
    RapidTask *task = rapid_;
    rapid_ = nullptr;
    if (task) {
      task->Cancel();
      task->ReleaseTicket();
    }
  }

private:
  RapidTask *rapid_ = nullptr;
};

template <typename F> struct UniqueTask : Task {

  UniqueTask(F &&f) : f(std::move(f)) {}

  void operator()() override {
    f();
    delete this; // really safe to do heere
  }

  std::decay_t<F> f;
};

template <typename F> Task *MakeTask(F &&f) {
  return new UniqueTask<decltype(std::forward<F>(f))>{std::forward<F>(f)};
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
  using Queue = RunQueue<TaskPtr, 1024>;

  ThreadPoolTempl(int num_threads, Environment env = Environment())
      : ThreadPoolTempl(num_threads, true, false, env) {}

  ThreadPoolTempl(int num_threads, bool allow_spinning, bool use_main_thread,
                  Environment env = Environment())
      : env_(env), num_threads_(ValidateThreadCount(num_threads)),
        allow_spinning_(allow_spinning), thread_data_(num_threads_),
        all_coprimes_(num_threads_),
        global_steal_partition_(EncodePartition(0, num_threads_)),
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
    for (int i = 0; i < num_threads_; i++) {
      SetStealPartition(i, EncodePartition(0, num_threads_));
    }

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

  void SetStealPartitions(
      const std::vector<std::pair<unsigned, unsigned>> &partitions) {
    assert(partitions.size() == static_cast<std::size_t>(num_threads_));

    // Pass this information to each thread queue.
    for (int i = 0; i < num_threads_; i++) {
      const auto &pair = partitions[i];
      unsigned start = pair.first, end = pair.second;
      AssertBounds(start, end);
      unsigned val = EncodePartition(start, end);
      SetStealPartition(i, val);
    }
  }

  void Schedule(TaskPtr p) override {
    // schedule on main thread only when explicitly requested
    ScheduleWithHint(p, 0, num_threads_);
  }

  void RunOnThread(TaskPtr t, size_t threadIndex) {
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
    for (auto &data : thread_data_) {
      // Admission and cancellation share one modification order per target:
      // either this sets the stop bit first, or it observes the publisher.
      data.rapid_publication_state.fetch_or(kRapidCancelled,
                                           std::memory_order_acq_rel);
      while ((data.rapid_publication_state.load(std::memory_order_acquire) &
              kRapidPublisherMask) != 0) {
        std::this_thread::yield();
      }
    }
    CancelRapidQueues();
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

  RegionContext *CurrentRegionContext() const noexcept {
    const PerThread *pt = const_cast<ThreadPoolTempl *>(this)->GetPerThread();
    return pt->region_context;
  }

  void NotifyRapidRegionStart() noexcept { UpdateRapidLinger(); }

  template <typename F>
  decltype(auto) ExecuteInRegion(RegionContext *context, F &&function) {
    PerThread *pt = GetPerThread();
    RegionContext *previous = pt->region_context;
    pt->region_context = context;
    struct RestoreContext {
      PerThread *thread;
      RegionContext *previous;
      ~RestoreContext() { thread->region_context = previous; }
    } restore{pt, previous};
    return std::forward<F>(function)();
  }

  size_t WorkerRegistrationCount() const noexcept {
    return registrations_started_.load(std::memory_order_acquire);
  }

  void ScheduleRapid(RapidTask *rapid, size_t target) {
    if (rapid == nullptr) {
      return;
    }
    target %= static_cast<size_t>(num_threads_);
    struct PublicationGuard {
      explicit PublicationGuard(std::atomic<size_t> &state)
          : state(state), admitted((state.fetch_add(1, std::memory_order_acquire) &
                                    kRapidCancelled) == 0) {}
      ~PublicationGuard() { state.fetch_sub(1, std::memory_order_release); }
      std::atomic<size_t> &state;
      bool admitted;
    } publication(thread_data_[target].rapid_publication_state);
    if (!publication.admitted) {
      rapid->Cancel();
      return;
    }
    if (cancelled_.load(std::memory_order_acquire)) {
      rapid->Cancel();
      return;
    }
    rapid->AddTickets(1);
    if (thread_data_[target].PushRapid(rapid)) {
      if (cancelled_.load(std::memory_order_acquire)) {
        rapid->Cancel();
      }
      WakeOneWorker();
      return;
    }

    TaskPtr fallback = rapid->FallbackTicket();
    static_cast<RapidFallbackTask *>(fallback)->Bind(rapid);
    try {
      PublishTask(fallback, static_cast<int>(target), false);
    } catch (...) {
      fallback->Discard();
      throw;
    }
  }

  template <typename Region> void HelpUntil(Region &region) {
    const bool registered = IsRegistered(GetPerThread());
    auto &event = registered ? worker_event_ : waiter_event_;
    while (!region.IsComplete()) {
      const uint64_t token = event.PrepareWait();
      if (registered && TryExecuteOne()) {
        event.CancelWait();
        continue;
      }
      if (region.IsComplete()) {
        event.CancelWait();
        return;
      }
      event.Wait(token);
    }
  }

  template <typename Predicate> void Wait(Predicate ready) {
    const bool registered = IsRegistered(GetPerThread());
    if (ready()) {
      return;
    }
    auto &event = registered ? worker_event_ : waiter_event_;

    while (!ready()) {
      const uint64_t token = event.PrepareWait();
      if (registered && TryExecuteOne()) {
        event.CancelWait();
        continue;
      }
      if (ready() || cancelled_.load(std::memory_order_acquire)) {
        event.CancelWait();
        return;
      }
      event.Wait(token);
    }
  }

  void NotifyTaskCompletion(bool worker_waiter) {
    (worker_waiter ? worker_event_ : waiter_event_).NotifyAll();
  }

  void NotifyTaskCompletion() {
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
  static const unsigned kRapidFairness = 8;
  static constexpr size_t kRapidCancelled =
      size_t{1} << (sizeof(size_t) * 8 - 1);
  static constexpr size_t kRapidPublisherMask = kRapidCancelled - 1;

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

  inline unsigned EncodePartition(unsigned start, unsigned limit) {
    return (start << kMaxPartitionBits) | limit;
  }

  void ExecuteTask(TaskPtr p) {
    struct FinishTask {
      ThreadPoolTempl *pool;
      std::atomic<size_t> *outstanding;
      ~FinishTask() { pool->TaskFinished(outstanding); }
    } finish{this, p->outstanding};
    PerThread *pt = GetPerThread();
    RegionContext *previous_context = pt->region_context;
    pt->region_context = p->region_context;
    struct RestoreContext {
      PerThread *thread;
      RegionContext *previous;
      ~RestoreContext() { thread->region_context = previous; }
    } restore{pt, previous_context};
    (*p)();
  }

  inline void DecodePartition(unsigned val, unsigned *start, unsigned *limit) {
    *limit = val & (kMaxThreads - 1);
    val >>= kMaxPartitionBits;
    *start = val;
  }

  void AssertBounds(int start, int end) {
    if (start < 0 || start >= end || end > num_threads_) {
      throw std::invalid_argument("invalid scheduling partition");
    }
  }

  inline void SetStealPartition(size_t i, unsigned val) {
    thread_data_[i].steal_partition.store(val, std::memory_order_relaxed);
  }

  inline unsigned GetStealPartition(int i) {
    return thread_data_[i].steal_partition.load(std::memory_order_relaxed);
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

  struct PerThread {
    constexpr PerThread()
        : pool(nullptr), pool_generation(0), rand(0), thread_id(-1),
          owns_queue(false), rapid_streak(0), region_context(nullptr) {}
    ThreadPoolTempl *pool; // Parent pool, or null for normal threads.
    uint64_t pool_generation;
    uint64_t rand;         // Random generator state.
    int thread_id;         // Worker thread index in pool.
    bool owns_queue;
    unsigned rapid_streak;
    RegionContext *region_context;
  };

  struct ThreadData {
    ThreadData()
        : thread(), steal_partition(0), outstanding_tasks(0), local_tasks(),
          mailbox(1024), rapid_publication_state(0), rapid_slot(nullptr),
          rapid_overflow(1024) {}
    std::unique_ptr<Thread> thread;
    std::atomic<unsigned> steal_partition;
    std::atomic<size_t> outstanding_tasks;
    Queue local_tasks;
    rigtorp::mpmc::Queue<TaskPtr> mailbox;
    // High bit rejects new publishers; the remaining bits count active ones.
    std::atomic<size_t> rapid_publication_state;
    std::atomic<RapidTask *> rapid_slot;
    rigtorp::mpmc::Queue<RapidTask *> rapid_overflow;

    bool PushRapid(RapidTask *task) {
      RapidTask *empty = nullptr;
      if (rapid_slot.compare_exchange_strong(empty, task,
                                             std::memory_order_release,
                                             std::memory_order_relaxed)) {
        return true;
      }
      return rapid_overflow.try_push(task);
    }

    RapidTask *PopRapid() {
      if (RapidTask *task =
              rapid_slot.exchange(nullptr, std::memory_order_acquire)) {
        return task;
      }
      RapidTask *task = nullptr;
      rapid_overflow.try_pop(task);
      return task;
    }

    void FlushRapid() {
      while (RapidTask *task = PopRapid()) {
        task->ReleaseTicket();
      }
    }

    bool PushTask(TaskPtr p, bool localThread) {
      if (localThread) {
        return local_tasks.PushFront(p);
      } else {
        return mailbox.try_push(p);
      }
    }

    TaskPtr PopFront() {
      if (auto p = local_tasks.PopFront()) {
        return p;
      }
      TaskPtr task = nullptr;
      mailbox.try_pop(task);
      return task;
    }

    TaskPtr PopBack(bool force) {
      TaskPtr task = nullptr;
      mailbox.try_pop(task);
      if (!task && force) {
        task = local_tasks.PopBack();
      }
      return task;
    }
  };

  Environment env_;
  const int num_threads_;
  const bool allow_spinning_;
  MaxSizeVector<ThreadData> thread_data_;
  MaxSizeVector<MaxSizeVector<unsigned>> all_coprimes_;
  unsigned global_steal_partition_;
  const uint64_t pool_generation_;

  std::mutex overflow_mutex_;
  std::deque<TaskPtr> overflow_tasks_;
  EventCount worker_event_;
  EventCount waiter_event_;
  std::atomic<bool> done_;
  std::atomic<bool> cancelled_;
  std::atomic<size_t> registrations_started_{0};
  std::atomic<unsigned> rapid_linger_iterations_{kSpinCount};
  std::atomic<uint64_t> last_rapid_region_ns_{0};

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
        const unsigned spin_count =
            rapid_linger_iterations_.load(std::memory_order_relaxed);
        for (unsigned i = 0; i < spin_count; ++i) {
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
      worker_event_.Wait(token);
    }
  }

  static uint64_t NextPoolGeneration() {
    static std::atomic<uint64_t> next{1};
    return next.fetch_add(1, std::memory_order_relaxed);
  }

  void UpdateRapidLinger() noexcept {
    const uint64_t now = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
    const uint64_t previous =
        last_rapid_region_ns_.exchange(now, std::memory_order_relaxed);
    if (previous == 0) {
      return;
    }
    const uint64_t interval = now - previous;
    const unsigned target =
        interval < 50'000 ? 256 : (interval < 500'000 ? 128 : 32);
    const unsigned current =
        rapid_linger_iterations_.load(std::memory_order_relaxed);
    rapid_linger_iterations_.store((current * 7 + target) / 8,
                                   std::memory_order_relaxed);
  }

  void RegisterThread(PerThread *pt, int thread_id, bool owns_queue) {
    pt->pool = this;
    pt->pool_generation = pool_generation_;
    pt->rand = GlobalThreadIdHash();
    pt->thread_id = thread_id;
    pt->owns_queue = owns_queue;
    pt->rapid_streak = 0;
    pt->region_context = nullptr;
    registrations_started_.fetch_add(1, std::memory_order_release);
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

  void PublishTask(TaskPtr task, int target, bool local) {
    auto &outstanding = thread_data_[target].outstanding_tasks;
    task->outstanding = &outstanding;
    outstanding.fetch_add(1, std::memory_order_relaxed);
    try {
      if (!thread_data_[target].PushTask(task, local)) {
        std::lock_guard<std::mutex> lock(overflow_mutex_);
        overflow_tasks_.push_back(task);
      }
    } catch (...) {
      outstanding.fetch_sub(1, std::memory_order_relaxed);
      throw;
    }
    WakeOneWorker();
  }

  TaskPtr PopOverflow() {
    std::lock_guard<std::mutex> lock(overflow_mutex_);
    if (overflow_tasks_.empty()) {
      return nullptr;
    }
    TaskPtr task = overflow_tasks_.front();
    overflow_tasks_.pop_front();
    return task;
  }

  bool TryExecuteOne() {
    PerThread *pt = GetPerThread();
    assert(IsRegistered(pt));

    // Preserve the fairness probe when ordinary work exists, but avoid its
    // mutex-backed queues during Rapid-only regions.
    if (pt->rapid_streak >= kRapidFairness && NoOutstandingTasks()) {
      pt->rapid_streak = 0;
    }
    if (pt->rapid_streak < kRapidFairness && TryExecuteRapid()) {
      ++pt->rapid_streak;
      return true;
    }

    TaskPtr task = nullptr;
    if (pt->owns_queue) {
      task = thread_data_[pt->thread_id].PopFront();
    }
    if (!task) {
      task = PopOverflow();
    }
    if (!task) {
      task = LocalSteal(true);
    }
    if (!task) {
      task = GlobalSteal(true);
    }
    if (!task) {
      if (TryExecuteRapid()) {
        ++pt->rapid_streak;
        return true;
      }
      return false;
    }
    pt->rapid_streak = 0;
    ExecuteTask(task);
    return true;
  }

  bool TryExecuteRapid() {
    PerThread *pt = GetPerThread();
    RapidTask *rapid = nullptr;
    if (pt->owns_queue) {
      rapid = thread_data_[pt->thread_id].PopRapid();
    }
    if (!rapid) {
      unsigned start = 0;
      unsigned limit = static_cast<unsigned>(num_threads_);
      if (pt->region_context && pt->region_context->domain.Size() != 0) {
        start = pt->region_context->domain.start;
        limit = pt->region_context->domain.limit;
      }
      for (unsigned worker = start; worker < limit && !rapid; ++worker) {
        if (worker != static_cast<unsigned>(pt->thread_id)) {
          rapid = thread_data_[worker].PopRapid();
        }
      }
    }
    if (!rapid) {
      return false;
    }
    RegionContext *previous = pt->region_context;
    pt->region_context = rapid->Context();
    try {
      rapid->TryRun();
    } catch (...) {
      pt->region_context = previous;
      rapid->ReleaseTicket();
      throw;
    }
    pt->region_context = previous;
    rapid->ReleaseTicket();
    return true;
  }

  void TaskFinished(std::atomic<size_t> *outstanding) {
    assert(outstanding != nullptr);
    const size_t previous = outstanding->fetch_sub(1, std::memory_order_release);
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

  void CancelRapidQueues() {
    for (auto &data : thread_data_) {
      while (RapidTask *rapid = data.PopRapid()) {
        rapid->Cancel();
        rapid->ReleaseTicket();
      }
    }
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
      data.FlushRapid();
    }
    std::lock_guard<std::mutex> lock(overflow_mutex_);
    while (!overflow_tasks_.empty()) {
      TaskPtr task = overflow_tasks_.front();
      overflow_tasks_.pop_front();
      auto *outstanding = task->outstanding;
      task->Discard();
      outstanding->fetch_sub(1, std::memory_order_relaxed);
    }
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
        return t;
      }
      victim += inc;
      if (victim >= size) {
        victim -= size;
      }
    }
    return nullptr;
  }

  // Steals work within threads belonging to the partition.
  TaskPtr LocalSteal(bool force) {
    PerThread *pt = GetPerThread();
    unsigned partition = GetStealPartition(pt->thread_id);
    if (pt->region_context && pt->region_context->domain.Size() != 0) {
      partition = EncodePartition(pt->region_context->domain.start,
                                  pt->region_context->domain.limit);
    }
    // If thread steal partition is the same as global partition, there is no
    // need to go through the steal loop twice.
    if (global_steal_partition_ == partition)
      return nullptr;
    unsigned start, limit;
    DecodePartition(partition, &start, &limit);
    AssertBounds(start, limit);

    return Steal(start, limit, force);
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
