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
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

// Batch publication wakes parked workers once per batch with a count instead
// of once per task. Set to 0 to restore per-task notification for A/B runs.
#ifndef OOX_EIGEN_BATCHED_WAKE
#define OOX_EIGEN_BATCHED_WAKE 1
#endif

namespace oox::detail::eigen_pool {

namespace rapid {
class RapidDomainState;
}

struct DomainId {
  unsigned start = 0;
  unsigned limit = 0;

  bool IsEmpty() const noexcept { return start == limit; }
  bool IsValidFor(size_t workers) const noexcept {
    return start < limit && limit <= workers;
  }
  size_t Size() const noexcept {
    return limit >= start ? limit - start : 0;
  }
  bool Contains(size_t worker) const noexcept {
    return worker >= start && worker < limit;
  }
};

struct RegionContext {
  DomainId domain;
  RegionContext *parent = nullptr;
  bool leave_on_steal = false;
  rapid::RapidDomainState *rapid_state = nullptr;
};

class ScopedRegionContext {
public:
  ScopedRegionContext(RegionContext *&current, RegionContext *replacement)
      : current_(current), previous_(std::exchange(current, replacement)) {}
  ScopedRegionContext(const ScopedRegionContext &) = delete;
  ScopedRegionContext &operator=(const ScopedRegionContext &) = delete;
  ~ScopedRegionContext() { current_ = previous_; }

private:
  RegionContext *&current_;
  RegionContext *previous_;
};

struct Task;

class RapidTask {
public:
  virtual void AddTickets(size_t count) noexcept = 0;
  virtual bool TryRun() = 0;
  virtual void Cancel() noexcept = 0;
  virtual void ReleaseTicket() noexcept = 0;
  virtual RegionContext *Context() noexcept = 0;
  virtual Task *FallbackTicket() noexcept = 0;
  virtual ~RapidTask() = default;
};

class RapidTicketGuard {
public:
  explicit RapidTicketGuard(RapidTask &task) noexcept : task_(task) {}
  RapidTicketGuard(const RapidTicketGuard &) = delete;
  RapidTicketGuard &operator=(const RapidTicketGuard &) = delete;
  ~RapidTicketGuard() { task_.ReleaseTicket(); }

private:
  RapidTask &task_;
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
    assert(rapid_ == nullptr);
    rapid_ = task;
    region_context = task->Context();
  }
  void operator()() final {
    RapidTask *task = rapid_;
    rapid_ = nullptr;
    assert(task != nullptr);
    RapidTicketGuard release(*task);
    task->TryRun();
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
  using Function = std::decay_t<F>;

  explicit UniqueTask(F &&function)
      : function_(std::forward<F>(function)) {}

  void operator()() override {
    std::unique_ptr<UniqueTask> self(this);
    self->function_();
  }

  Function function_;
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
    // repetitions (effectively getting a pseudo-random permutation of thread
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
    threadIndex = threadIndex % num_threads_;
    PerThread *pt = GetPerThread();
    const bool local = IsRegistered(pt) && pt->owns_queue &&
                       threadIndex == static_cast<size_t>(pt->thread_id);
    PublishOrdinaryTask(t, static_cast<int>(threadIndex), local);
  }

  template <typename F> void PublishOrdinaryBatch(F &&publisher) {
    // Batch publication may overlap cancellation. The final reconciliation
    // drains any tasks published after cancellation's own drain.
    if ((ordinary_publication_state_.load(std::memory_order_acquire) &
         kPublicationCancelled) != 0) {
      return;
    }
    // Wake one worker on the first publication so a parked pool starts
    // immediately, then defer the remaining notifications to one NotifyN at
    // the end of the batch (including exceptional exits). Every mature parking
    // protocol (Eigen EventCount, Rayon's jobs event counter, Tokio) tolerates
    // a lost wake because the publishing worker will still pop the work.
    size_t deferred = 0;
    bool first_publication = true;
    struct WakeDeferred {
      ThreadPoolTempl *pool;
      size_t &count;
      ~WakeDeferred() {
        if (count != 0) {
          pool->worker_event_.NotifyN(count);
        }
      }
    } wake_guard{this, deferred};
    auto publish_one = [&](TaskPtr task, size_t thread_index) {
      assert(task != nullptr);
      thread_index %= static_cast<size_t>(num_threads_);
      PerThread *pt = GetPerThread();
      const bool local = IsRegistered(pt) && pt->owns_queue &&
                         thread_index == static_cast<size_t>(pt->thread_id);
#if OOX_EIGEN_BATCHED_WAKE
      const bool wake_now = first_publication;
      first_publication = false;
      TaskPtr inline_task = PublishAdmittedTask(
          task, static_cast<int>(thread_index), local, wake_now);
      if (!wake_now && !inline_task) {
        ++deferred;
      }
      if (inline_task) {
        ExecuteTask(inline_task);
      }
#else
      if (TaskPtr inline_task =
              PublishAdmittedTask(task, static_cast<int>(thread_index), local)) {
        ExecuteTask(inline_task);
      }
#endif
    };
    std::forward<F>(publisher)(publish_one);
  }

  // Pair with every PublishOrdinaryBatch call, including exceptional exits.
  void FinishOrdinaryBatch() { ReconcileCancelledOrdinaryBatch(); }

  void ScheduleWithHint(TaskPtr t, int start, int limit) override {
    if (t == nullptr) {
      return;
    }
    AssertBounds(start, limit);
    PerThread *pt = GetPerThread();
    if (IsRegistered(pt) && pt->owns_queue && pt->thread_id >= start &&
        pt->thread_id < limit) {
      // Worker thread of this pool, push onto the thread's queue.
      PublishOrdinaryTask(t, pt->thread_id, true);
      return;
    }

    if (pt->rand == 0) {
      pt->rand = GlobalThreadIdHash();
    }
    const int target =
        start + static_cast<int>(Rand(&pt->rand) % (limit - start));
    PublishOrdinaryTask(t, target, false);
  }

  void Cancel() override {
    if (ActiveCancellation() == this) {
      return;
    }
    std::call_once(cancellation_once_, [this] {
      ThreadPoolTempl *&active = ActiveCancellation();
      ThreadPoolTempl *previous = std::exchange(active, this);
      struct RestoreCancellation {
        ThreadPoolTempl *&active;
        ThreadPoolTempl *previous;
        ~RestoreCancellation() { active = previous; }
      } restore{active, previous};
      CancelOnce();
    });
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
  decltype(auto) ExecuteInRegion(RegionContext *context, F &&function) noexcept(
      noexcept(std::forward<F>(function)())) {
    PerThread *pt = GetPerThread();
    ScopedRegionContext restore(pt->region_context, context);
    return std::forward<F>(function)();
  }

  size_t WorkerRegistrationCount() const noexcept {
    return registrations_started_.load(std::memory_order_acquire);
  }

  size_t RapidDeregistrationCount() const noexcept {
    return rapid_deregistrations_.load(std::memory_order_acquire);
  }

  // Worker wake notifications that reached a parked worker. Diagnostic.
  size_t WorkerWakeNotifications() const noexcept {
    return worker_event_.Notifications();
  }

  bool IsCancelled() const noexcept {
    return cancelled_.load(std::memory_order_acquire);
  }

  void ScheduleRapid(RapidTask *rapid, size_t target) {
    if (rapid == nullptr) {
      return;
    }
    target %= static_cast<size_t>(num_threads_);
    TaskPtr inline_task = nullptr;
    {
      PublicationGuard publication(
          thread_data_[target].rapid_publication_state);
      if (!publication.IsAdmitted()) {
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
        inline_task =
            PublishAdmittedTask(fallback, static_cast<int>(target), false);
      } catch (...) {
        // Publication owns the extra ticket. On failure Discard cancels the
        // activation and releases that ticket before the exception escapes.
        fallback->Discard();
        throw;
      }
    }
    if (inline_task) {
      ExecuteTask(inline_task);
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
  static ThreadPoolTempl *&ActiveCancellation() noexcept {
    static thread_local ThreadPoolTempl *active = nullptr;
    return active;
  }

  void CancelOnce() {
    cancelled_.store(true, std::memory_order_release);
    ordinary_publication_state_.fetch_or(kPublicationCancelled,
                                         std::memory_order_seq_cst);
    for (auto &data : thread_data_) {
      // Admission and cancellation share one modification order per target:
      // either this sets the stop bit first, or it observes the publisher.
      data.rapid_publication_state.fetch_or(kPublicationCancelled,
                                            std::memory_order_acq_rel);
    }
    while ((ordinary_publication_state_.load(std::memory_order_acquire) &
            kPublicationPublisherMask) != 0) {
      std::this_thread::yield();
    }
    for (auto &data : thread_data_) {
      while ((data.rapid_publication_state.load(std::memory_order_acquire) &
              kPublicationPublisherMask) != 0) {
        std::this_thread::yield();
      }
    }
    CancelRapidQueues();
    DrainCancelledOrdinaryQueues();
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

  // Create a single atomic<int> that encodes start and limit information for
  // each thread.
  // We expect num_threads_ < 65536, so we can store them in a single
  // std::atomic<unsigned>.
  // The packed representation keeps each worker's steal domain in one atomic.
  static constexpr int kMaxPartitionBits = 16;
  static constexpr int kMaxThreads = 1 << kMaxPartitionBits;
  static constexpr int kSpinCount = 64;
  static constexpr unsigned kRapidFairness = 8;
  static constexpr uint64_t kFrequentRapidIntervalNs = 50'000;
  static constexpr uint64_t kOccasionalRapidIntervalNs = 500'000;
  static constexpr unsigned kFrequentRapidLingerIterations = 256;
  static constexpr unsigned kOccasionalRapidLingerIterations = 128;
  static constexpr unsigned kInfrequentRapidLingerIterations = 32;
  static constexpr size_t kPublicationCancelled =
      size_t{1} << (sizeof(size_t) * 8 - 1);
  static constexpr size_t kPublicationPublisherMask =
      kPublicationCancelled - 1;

  class PublicationGuard {
  public:
    explicit PublicationGuard(std::atomic<size_t> &state) noexcept
        : state_(state), admitted_((state.fetch_add(
                                        1, std::memory_order_relaxed) &
                                    kPublicationCancelled) == 0) {}
    PublicationGuard(const PublicationGuard &) = delete;
    PublicationGuard &operator=(const PublicationGuard &) = delete;
    ~PublicationGuard() noexcept {
      state_.fetch_sub(1, std::memory_order_release);
    }

    bool IsAdmitted() const noexcept { return admitted_; }

  private:
    std::atomic<size_t> &state_;
    bool admitted_;
  };

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

    // Wake up to `count` waiters with one epoch advance. Falls back to a
    // broadcast when the request covers every registered waiter.
    void NotifyN(size_t count) {
      if (count == 0) {
        return;
      }
      std::atomic_thread_fence(std::memory_order_seq_cst);
      const size_t waiters = waiters_.load(std::memory_order_seq_cst);
      if (waiters == 0) {
        return;
      }
      notifications_.fetch_add(1, std::memory_order_relaxed);
      epoch_.fetch_add(1, std::memory_order_seq_cst);
      if (count >= waiters) {
        epoch_.notify_all();
        return;
      }
      for (size_t i = 0; i < count; ++i) {
        epoch_.notify_one();
      }
    }

    // Number of notifications that found at least one waiter (i.e. that
    // actually reached the OS). Diagnostic only.
    size_t Notifications() const noexcept {
      return notifications_.load(std::memory_order_relaxed);
    }

  private:
    void Notify(bool all) {
      std::atomic_thread_fence(std::memory_order_seq_cst);
      if (waiters_.load(std::memory_order_seq_cst) == 0) {
        return;
      }
      notifications_.fetch_add(1, std::memory_order_relaxed);
      epoch_.fetch_add(1, std::memory_order_seq_cst);
      if (all) {
        epoch_.notify_all();
      } else {
        epoch_.notify_one();
      }
    }

    std::atomic<uint64_t> epoch_{0};
    std::atomic<size_t> waiters_{0};
    std::atomic<size_t> notifications_{0};
  };

  static int ValidateThreadCount(int count) {
    if (count <= 0 || count >= kMaxThreads) {
      throw std::invalid_argument("thread count must be in [1, 65535]");
    }
    return count;
  }

  static constexpr unsigned EncodePartition(unsigned start,
                                             unsigned limit) noexcept {
    return (start << kMaxPartitionBits) | limit;
  }

  void ExecuteTask(TaskPtr p) {
    struct FinishTask {
      ThreadPoolTempl *pool;
      std::atomic<size_t> *outstanding;
      ~FinishTask() { pool->TaskFinished(outstanding); }
    } finish{this, p->outstanding};
    PerThread *pt = GetPerThread();
    ScopedRegionContext restore(pt->region_context, p->region_context);
    (*p)();
  }

  static constexpr DomainId DecodePartition(unsigned value) noexcept {
    const unsigned limit = value & (kMaxThreads - 1);
    return {value >> kMaxPartitionBits, limit};
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

    RapidTask *StealRapid() {
      // Do not take write ownership of an empty remote inbox.
      RapidTask *task = rapid_slot.load(std::memory_order_relaxed);
      if (task && rapid_slot.compare_exchange_strong(
                      task, nullptr, std::memory_order_acquire,
                      std::memory_order_relaxed)) {
        return task;
      }
      task = nullptr;
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

  std::once_flag cancellation_once_;
  std::mutex ordinary_cancellation_mutex_;
  EventCount worker_event_;
  EventCount waiter_event_;
  std::atomic<bool> done_;
  std::atomic<bool> cancelled_;
  std::atomic<size_t> registrations_started_{0};
  std::atomic<size_t> rapid_deregistrations_{0};
  std::atomic<unsigned> rapid_linger_iterations_{kSpinCount};
  std::atomic<uint64_t> last_rapid_region_ns_{0};

  bool creator_registered_ = false;
  std::thread::id creator_thread_id_;
  PerThread creator_previous_registration_;
  // Generic publication admission is written for every scheduled task. Keep
  // it away from worker-loop counters so submissions do not invalidate a cache
  // line that every worker is reading.
  alignas(OOX_EIGEN_CACHE_LINE_SIZE) std::atomic<size_t>
      ordinary_publication_state_{0};

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
        interval < kFrequentRapidIntervalNs
            ? kFrequentRapidLingerIterations
            : (interval < kOccasionalRapidIntervalNs
                   ? kOccasionalRapidLingerIterations
                   : kInfrequentRapidLingerIterations);
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

  void PublishOrdinaryTask(TaskPtr task, int target, bool local) {
    TaskPtr inline_task = nullptr;
    {
      PublicationGuard publication(ordinary_publication_state_);
      if (!publication.IsAdmitted() ||
          cancelled_.load(std::memory_order_acquire)) {
        task->Discard();
        return;
      }
      inline_task = PublishAdmittedTask(task, target, local);
    }
    if (inline_task) {
      ExecuteTask(inline_task);
    }
  }

  TaskPtr PublishAdmittedTask(TaskPtr task, int target, bool local,
                              bool wake = true) {
    auto &outstanding = thread_data_[target].outstanding_tasks;
    task->outstanding = &outstanding;
    outstanding.fetch_add(1, std::memory_order_relaxed);
    if (!thread_data_[target].PushTask(task, local)) {
      return task;
    }
    if (wake) {
      WakeOneWorker();
    }
    return nullptr;
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
      task = LocalSteal(true);
    }
    if (!task && pt->region_context && pt->region_context->leave_on_steal) {
      pt->region_context = pt->region_context->parent;
      rapid_deregistrations_.fetch_add(1, std::memory_order_relaxed);
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
          rapid = thread_data_[worker].StealRapid();
        }
      }
    }
    if (!rapid) {
      return false;
    }
    ScopedRegionContext restore(pt->region_context, rapid->Context());
    RapidTicketGuard release(*rapid);
    rapid->TryRun();
    return true;
  }

  static void DiscardPublishedTask(TaskPtr task) noexcept {
    assert(task != nullptr);
    auto *outstanding = task->outstanding;
    assert(outstanding != nullptr);
    task->Discard();
    const size_t previous =
        outstanding->fetch_sub(1, std::memory_order_relaxed);
    assert(previous > 0);
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

  void CancelOrdinaryQueues() {
    for (auto &data : thread_data_) {
      while (TaskPtr task = data.PopBack(true)) {
        DiscardPublishedTask(task);
      }
    }
  }

  void DrainCancelledOrdinaryQueues() {
    std::lock_guard<std::mutex> lock(ordinary_cancellation_mutex_);
    CancelOrdinaryQueues();
  }

  void ReconcileCancelledOrdinaryBatch() {
    // This load and cancellation's stop-bit update share the sequentially
    // consistent order. Either cancellation follows and drains this batch, or
    // this observes the stop bit and drains after the batch.
    const size_t state =
        ordinary_publication_state_.load(std::memory_order_seq_cst);
    if ((state & kPublicationCancelled) != 0) {
      DrainCancelledOrdinaryQueues();
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
        DiscardPublishedTask(task);
      }
      data.FlushRapid();
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
    const DomainId domain = DecodePartition(partition);
    AssertBounds(domain.start, domain.limit);

    return Steal(domain.start, domain.limit, force);
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
