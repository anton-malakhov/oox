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
#include "queue_traversal.h"
#include "run_queue.h"
#include "stl_thread_env.h"
#include "small_object_pool.h"
#include "stealable_range.h"
#include "scheduler_trace.h"
#include "resident_command.h"
#include "resident_cohort.h"
#include "resident_queue_scan.h"
#include "resident_worker_mask.h"
#include "resident_worker_set.h"
#include "available_parallelism.h"

#include <atomic>
#include <cassert>
#ifdef OOX_EIGEN_ENABLE_STATS
#include <chrono>
#endif
#include <cstddef>
#include <cstdint>
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

// ResidentBusy is an explicit pool-lifetime policy. Idle workers advertise a
// bit and poll a command slot instead of entering the OS parking protocol.
// ResidentAuto bounds persistent polling by the CPU allowance at construction.
enum class WorkerIdleMode { Park, ResidentBusy, ResidentAuto };

#ifdef OOX_EIGEN_THREAD_POOL_TESTING
namespace internal {
inline scheduler_sync::atomic<size_t> completion_notifications{0};
inline scheduler_sync::atomic<size_t> completion_waits{0};

}
#endif

struct Task {
#ifdef OOX_SCHEDULER_TRACE
  const uint64_t trace_id = ::oox::scheduler_trace::NextId();
#endif
  scheduler_sync::atomic<size_t> *outstanding = nullptr;
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
      : ThreadPoolTempl(num_threads, allow_spinning, use_main_thread,
                        WorkerIdleMode::Park, env) {}

  ThreadPoolTempl(int num_threads, bool allow_spinning, bool use_main_thread,
                  WorkerIdleMode idle_mode, Environment env = Environment())
      : env_(env), num_threads_(ValidateThreadCount(num_threads)),
        allow_spinning_(allow_spinning), thread_data_(num_threads_),
        all_coprimes_(num_threads_),
        pool_generation_(NextPoolGeneration()), idle_mode_(idle_mode),
        resident_workers_(idle_mode_ == WorkerIdleMode::Park ? 0 : num_threads_,
                          use_main_thread && num_threads_ > 1 ? 1 : 0),
        resident_broadcasts_(resident_workers_.WordCount()
            ? std::make_unique<ResidentBroadcast[]>(resident_workers_.WordCount()) : nullptr),
        done_(false),
        cancelled_(false),
        resident_placement_(idle_mode == WorkerIdleMode::ResidentAuto
            ? DiscoverResidentPlacement(num_threads_)
            : ResidentPlacement{idle_mode == WorkerIdleMode::Park ? 0 : size_t(num_threads_), {}}),
        resident_limit_max_(resident_placement_.limit),
        resident_limit_(resident_limit_max_) {
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
#ifdef OOX_SCHEDULER_TRACE
    for (int i = 0; i < num_threads_; ++i) thread_data_[i].trace_worker = i;
#endif
    const bool needs_fallback_worker = use_main_thread && num_threads_ == 1;
    if (use_main_thread) {
      RegisterCreator(!needs_fallback_worker);
    }
    try {
      for (int i = resident_workers_.FirstBackgroundWorker(); i < num_threads_; ++i) {
        thread_data_[i].thread.reset(env_.CreateThread([this, i]() {
          PerThread *pt = GetPerThread();
          const PerThread previous = *pt;
          RegisterThread(pt, i, true);
          WorkerLoop(false, false, ResidentWorkerBinding(resident_placement_, i));
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
    PublishOrdinaryTask(t, static_cast<int>(threadIndex), local);
  }

  bool IsExecutingTask() const noexcept {
    return GetExecutionContext().execution_pool == this;
  }

  // The caller owns the range and callback lifetime; this only preserves task
  // ancestry for nested loops. It does not publish or account an ordinary task.
  template <typename F> void RunInlineWork(F &&body) {
    auto &context = GetExecutionContext();
    struct Restore {
      ExecutionContext &context;
      ThreadPoolTempl *previous;
      ~Restore() { context.execution_pool = previous; }
    } restore{context, std::exchange(context.execution_pool, this)};
    body();
  }

  void ScheduleWithAffinity(TaskPtr task, size_t hint) {
    if (!task)
      return;
    PerThread *pt = GetPerThread();
    hint %= num_threads_;
    if (!IsRegistered(pt) || !pt->owns_queue ||
        hint == static_cast<size_t>(pt->thread_id)) {
      RunOnThread(task, hint);
      return;
    }
    // Discard may reenter Cancel; release publication admission first.
    const auto discard = [](Task *p) { p->Discard(); };
    std::unique_ptr<Task, decltype(discard)> pending(task, discard);
    TaskPtr inline_task = nullptr;
    {
      PublicationGuard publication(ordinary_publication_state_);
      if (!publication.IsAdmitted() || IsCancelled()) {
        return;
      }
      auto *proxy = new AffinityProxy(task);
      pending.release();
      AccountTask(task, pt->thread_id);
      OOX_SCHED_EVENT("offer_affinity", task->trace_id, hint, 0, ::oox::scheduler_trace::Current());
      if (!thread_data_[hint].affinity_mailbox.Push(proxy)) {
        inline_task = proxy->template Extract<AffinityProxy::mailbox_bit>();
        proxy->template Extract<AffinityProxy::local_bit>();
      } else {
        const QueueEntry entry = reinterpret_cast<QueueEntry>(proxy) | proxy_tag;
        if (!thread_data_[pt->thread_id].local_tasks.PushFront(entry))
          inline_task = proxy->template Extract<AffinityProxy::local_bit>();
      }
      HintResidentOrdinaryWork();
      WakeOneWorker();
    }
    if (inline_task) {
      ExecuteTask(inline_task);
    }
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
    scheduler_sync::call_once(cancellation_once_, [this] {
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

  bool UsesResidentBusyWait() const noexcept {
    return idle_mode_ == WorkerIdleMode::ResidentBusy ||
           idle_mode_ == WorkerIdleMode::ResidentAuto;
  }

  size_t MaxResidentLimit() const noexcept { return resident_limit_max_; }

  size_t ResidentLimit() const noexcept {
    return resident_limit_.load(std::memory_order_acquire);
  }

  // In-flight resident commands finish normally. Other workers use parking;
  // increasing eligibility wakes parked workers to avoid lost transitions.
  void SetResidentLimit(size_t limit) {
    if (!UsesResidentBusyWait() && limit != 0)
      throw std::invalid_argument("resident limit requires a resident-capable pool");
    resident_limit_.store(std::min(limit, MaxResidentLimit()), std::memory_order_release);
    WakeAllWorkers();
  }

  size_t ResidentCapacity(DomainId domain) const noexcept {
    return resident_workers_.Capacity(domain.LimitedTo(ResidentLimit()));
  }

  // A demand hint, not an idle-worker reservation. A waiter cancels its
  // registration before executing acquired work.
  bool HasWaitingHelper() const noexcept { return worker_event_.HasWaiters(); }
  bool HasResidentCommand() noexcept {
    auto *pt = GetPerThread();
    if (!IsRegistered(pt) || !pt->owns_queue) return false;
    const unsigned worker = static_cast<unsigned>(pt->thread_id);
    return thread_data_[worker].resident_command.HasCommand(worker, resident_broadcasts_.get());
  }

  void PrepareNestedRange() noexcept {
    auto *pt = GetPerThread();
    if (!IsRegistered(pt) || !pt->owns_queue || !UsesResidentBusyWait()) return;
    auto &data = thread_data_[pt->thread_id];
    const bool released = data.stealable_range.load(std::memory_order_relaxed)
        ? StealableRange::PrepareNested(data.stealable_range) : data.concurrent_range.PrepareNested();
    if (!released) return;
    // Notify only after the slot is unlocked, using the existing demand hints.
    NotifyStealableRange();
  }

  template <class Range> bool PublishStealableRange(Range &range) noexcept {
    auto *pt = GetPerThread();
    if (!IsRegistered(pt) || !pt->owns_queue || !UsesResidentBusyWait()) return false;
    auto &data = thread_data_[pt->thread_id];
    if (!data.stealable_range.load(std::memory_order_relaxed)) {
      if constexpr (requires { range.TryPublishConcurrent(data.concurrent_range); }) {
        if (range.TryPublishConcurrent(data.concurrent_range)) {
          NotifyStealableRange();
          return true;
        }
      }
      data.concurrent_range.PrepareNested();
    }
    range.Publish(data.stealable_range);
    NotifyStealableRange();
    return true;
  }

  size_t ResidentAvailableWorkers(DomainId domain) const noexcept {
    return resident_workers_.Count(domain.LimitedTo(ResidentLimit()));
  }

  // A demand hint can stop at the first available worker in the domain.
  bool HasResidentAvailableWorker(DomainId domain) const noexcept {
    return resident_workers_.Any(domain.LimitedTo(ResidentLimit()));
  }

  size_t ClaimResidentWorkers(DomainId domain, unsigned *workers,
                              size_t capacity) noexcept {
    return resident_workers_.Claim(domain.LimitedTo(ResidentLimit()), workers, capacity);
  }

  void PublishResident(ResidentTask &task, scheduler_sync::atomic<size_t> &completion,
                       unsigned worker, size_t slot, uint64_t epoch = 0) noexcept {
    assert(worker < static_cast<unsigned>(num_threads_));
    OOX_SCHED_WORKER(CurrentThreadId());
    thread_data_[worker].resident_command.Publish(worker, task, completion, slot, epoch);
  }

  size_t PublishResidentBatch(ResidentTask &task, scheduler_sync::atomic<size_t> &completion,
                            const unsigned *workers, size_t count,
                            size_t first_slot = 1, uint64_t epoch = 0,
                            bool packed_completion = false) noexcept {
    auto slot_for = [this](unsigned worker) -> internal::ResidentCommandSlot & {
      assert(worker < static_cast<unsigned>(num_threads_));
      OOX_SCHED_WORKER(CurrentThreadId());
      return thread_data_[worker].resident_command;
    };
    return internal::PublishResidentCommands(resident_broadcasts_.get(), slot_for, task, completion,
        workers, count, first_slot, epoch, packed_completion && SupportsPackedResidentCompletion());
  }

  uint64_t NextResidentEpoch() noexcept {
    return internal::AllocateResidentEpoch(num_threads_, resident_workers_, resident_dispatch_epoch_);
  }

  bool ResidentEpochComplete(uint64_t epoch, const unsigned *workers,
                              size_t count) const noexcept {
    return internal::CohortEpochComplete(thread_data_, epoch, workers, count);
  }

  bool SupportsPackedResidentCompletion() const noexcept {
    return internal::ResidentMaskWord::SupportsPackedCompletion(num_threads_);
  }
  uint64_t ResidentBatchDoneMask(size_t word, const unsigned *workers,
                                size_t count) const noexcept {
    return internal::CohortDoneMask(word, workers, count, num_threads_);
  }
  bool ResidentBatchDone(size_t word, uint64_t mask) const noexcept {
    return resident_workers_.Word(word).Completed(mask);
  }

  // The selected finished-epoch or packed-done acquire must precede release.
  void ReleaseResidentEpochBatch(size_t word) noexcept {
    if (word == internal::no_resident_batch) return;
    resident_broadcasts_[word].ReleaseAfterJoin(word, resident_workers_.Word(word), num_threads_);
  }

  template <typename Region> void HelpResidentUntil(Region &region) {
    unsigned spins = 0;
    while (!region.IsComplete()) {
      if (++spins < kSpinCount) {
        RelaxResidentWait();
        continue;
      }
      spins = 0;
      OOX_SCHED_EVENT("caller_help_probe", ::oox::scheduler_trace::Current());
      if (!TryExecuteSomething())
        RelaxResidentWait();
    }
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
#ifdef OOX_EIGEN_TEST_WAIT_PREPARED
      OOX_EIGEN_TEST_WAIT_PREPARED();
#endif
      if (TaskPtr task = registered ? TryTakeOne() : nullptr) {
        event.CancelWait();
        ExecuteTask(task);
        continue;
      }
      if (registered && UsesResidentBusyWait()) {
        // Keep the caller visible as an idle helper without immediately
        // parking on a short resident command's completion.
        for (unsigned spins = 0; spins < kSpinCount && !ready(); ++spins)
          RelaxResidentWait();
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
  static ThreadPoolTempl *&ActiveCancellation() noexcept {
    struct Tag {};
    return scheduler_sync::Local<ThreadPoolTempl *, Tag>();
  }

  void CancelOnce() {
    cancelled_.store(true, std::memory_order_release);
    ordinary_publication_state_.fetch_or(kPublicationCancelled,
                                         std::memory_order_seq_cst);
    while ((ordinary_publication_state_.load(std::memory_order_acquire) &
            kPublicationPublisherMask) != 0) {
      scheduler_sync::this_thread::yield();
    }
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

  static constexpr int kMaxThreads = 1 << 16;
  static constexpr int kSpinCount = 64;
  static constexpr size_t kPublicationCancelled =
      size_t{1} << (sizeof(size_t) * 8 - 1);
  static constexpr size_t kPublicationPublisherMask =
      kPublicationCancelled - 1;

  class PublicationGuard {
  public:
    explicit PublicationGuard(scheduler_sync::atomic<size_t> &state) noexcept
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
    scheduler_sync::atomic<size_t> &state_;
    bool admitted_;
  };

  class EventCount {
  public:
    uint64_t PrepareWait() {
      // The caller checks for work again after registering. Sequential
      // consistency makes either that check or a concurrent notification win.
      const uint64_t epoch = epoch_.load(std::memory_order_seq_cst);
      waiters_.fetch_add(1, std::memory_order_seq_cst);
      scheduler_sync::atomic_thread_fence(std::memory_order_seq_cst);
      return epoch;
    }

    bool HasWaiters() const noexcept { return waiters_.load(std::memory_order_relaxed) != 0; }

    void CancelWait() { waiters_.fetch_sub(1, std::memory_order_seq_cst); }

    void Wait(uint64_t token) {
      epoch_.wait(token, std::memory_order_acquire);
      CancelWait();
    }

    void NotifyOne() { Notify(false); }
    void NotifyAll() { Notify(true); }

  private:
    void Notify(bool all) {
      scheduler_sync::atomic_thread_fence(std::memory_order_seq_cst);
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

    scheduler_sync::atomic<uint64_t> epoch_{0};
    scheduler_sync::atomic<size_t> waiters_{0};
  };

  static int ValidateThreadCount(int count) {
    if (count <= 0 || count >= kMaxThreads) {
      throw std::invalid_argument("thread count must be in [1, 65535]");
    }
    return count;
  }

  void ExecuteTask(TaskPtr p) {
    OOX_SCHED_WORKER(CurrentThreadId());
    OOX_SCHED_SCOPE("task", p->trace_id);
    auto *context = &GetExecutionContext();
    struct RestoreExecution {
      ExecutionContext *context;
      ThreadPoolTempl *previous;
      ~RestoreExecution() { context->execution_pool = previous; }
    } execution{context, std::exchange(context->execution_pool, this)};
#ifdef OOX_EIGEN_ENABLE_STATS
    const PerThread *pt = GetPerThread();
    const size_t worker = IsRegistered(pt) ? static_cast<size_t>(pt->thread_id) : 0;
    thread_data_[worker].statistics.executed.fetch_add(
        1, std::memory_order_relaxed);
#endif
    struct FinishTask {
      ThreadPoolTempl *pool;
      scheduler_sync::atomic<size_t> *outstanding;
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

    scheduler_sync::atomic<uintptr_t> task_and_locations;
  };

  // Bounded recipient references. Thieves use the sender's local proxy;
  // cancellation can safely drain the mailbox alongside its owner.
  class AffinityMailbox {
  public:
    bool Push(AffinityProxy *proxy) noexcept { return queue_.try_push(proxy); }

    AffinityProxy *Pop() noexcept {
      AffinityProxy *result = nullptr;
      queue_.try_pop(result);
      return result;
    }

    bool Empty() const noexcept { return queue_.empty(); }

  private:
    rigtorp::mpmc::Queue<AffinityProxy *> queue_{1024};
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

  // Dynamic call context is independent of thread registration. In particular,
  // nested pools must restore the enclosing task ancestry.
  struct ExecutionContext {
    ThreadPoolTempl *execution_pool = nullptr;
  };

  static ExecutionContext &GetExecutionContext() noexcept {
    struct Tag {};
    return scheduler_sync::Local<ExecutionContext, Tag>();
  }

#ifdef OOX_EIGEN_ENABLE_STATS
  struct AtomicStatistics {
    scheduler_sync::atomic<uint64_t> scheduled{0};
    scheduler_sync::atomic<uint64_t> executed{0};
    scheduler_sync::atomic<uint64_t> successful_steals{0};
    scheduler_sync::atomic<uint64_t> failed_steal_rounds{0};
    scheduler_sync::atomic<uint64_t> sleeps{0};
    scheduler_sync::atomic<uint64_t> idle_nanoseconds{0};
  };
#endif

  using ResidentBroadcast = internal::ResidentBroadcast;

  struct ThreadData {
    ThreadData()
        : thread(), outstanding_tasks(0), local_tasks(),
          mailbox(1024) {}
    std::unique_ptr<Thread> thread;
    scheduler_sync::atomic<size_t> outstanding_tasks;
    Queue local_tasks;
    AffinityMailbox affinity_mailbox;
    rigtorp::mpmc::Queue<TaskPtr> mailbox;
    internal::ResidentCommandSlot resident_command;
    alignas(128) scheduler_sync::atomic<uint64_t> resident_finished{0};
    // One already-accounted task rescued when a Rapid publisher wins capture.
    // It remains stealable, including by waits inside the winning command.
    alignas(128) scheduler_sync::atomic<TaskPtr> resident_deferred{nullptr};
    scheduler_sync::atomic<uintptr_t> stealable_range{0};
    ConcurrentRangeSlot concurrent_range;
#ifdef OOX_SCHEDULER_TRACE
    int trace_worker = -1;
#endif

#ifdef OOX_EIGEN_ENABLE_STATS
    AtomicStatistics statistics;
#endif

    bool PushTask(TaskPtr p, bool localThread) {
      OOX_SCHED_EVENT(localThread ? "offer_deque" : "offer_mailbox", p->trace_id,
                      trace_worker, 0, ::oox::scheduler_trace::Current());
      if (localThread) {
        return local_tasks.PushFront(reinterpret_cast<QueueEntry>(p));
      } else {
        return mailbox.try_push(p);
      }
    }

    TaskPtr PopFront() {
      if (resident_deferred.load(std::memory_order_relaxed))
        if (auto task = resident_deferred.exchange(nullptr, std::memory_order_acquire)) {
          OOX_SCHED_EVENT("take_deferred", task->trace_id, trace_worker);
          return task;
        }
      while (auto entry = local_tasks.PopFront())
        if (auto *task = ExtractEntry(entry)) {
          OOX_SCHED_EVENT("take_deque", task->trace_id, trace_worker);
          return task;
        }
      while (auto *proxy = affinity_mailbox.Pop())
        if (auto *task = proxy->template Extract<AffinityProxy::mailbox_bit>()) {
          OOX_SCHED_EVENT("take_affinity", task->trace_id, trace_worker);
          return task;
        }
      TaskPtr task = nullptr;
      mailbox.try_pop(task);
      if (task) OOX_SCHED_EVENT("take_mailbox", task->trace_id, trace_worker);
      return task;
    }

    bool MayHaveStealableWork() const {
      return !local_tasks.Empty() || !mailbox.empty() ||
          resident_deferred.load(std::memory_order_acquire) != nullptr ||
          stealable_range.load(std::memory_order_acquire) != 0 ||
          concurrent_range.range.load(std::memory_order_acquire) != nullptr;
    }

    TaskPtr PopBack(bool force) {
      if (resident_deferred.load(std::memory_order_relaxed))
        if (auto task = resident_deferred.exchange(nullptr, std::memory_order_acquire)) {
          OOX_SCHED_EVENT("take_deferred", task->trace_id, trace_worker);
          return task;
        }
      TaskPtr task = nullptr;
      mailbox.try_pop(task);
      if (task) OOX_SCHED_EVENT("take_mailbox", task->trace_id, trace_worker);
      if (!task && force) {
        while (auto entry = local_tasks.PopBack())
          if ((task = ExtractEntry(entry))) {
            OOX_SCHED_EVENT("take_deque", task->trace_id, trace_worker);
            break;
          }
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
  const WorkerIdleMode idle_mode_;
  internal::ResidentWorkerSet resident_workers_;
  std::unique_ptr<ResidentBroadcast[]> resident_broadcasts_;
  alignas(128) scheduler_sync::atomic<uint64_t> resident_dispatch_epoch_{0};

  scheduler_sync::once_flag cancellation_once_;
  scheduler_sync::mutex ordinary_cancellation_mutex_;
  EventCount worker_event_;
  EventCount waiter_event_;
  scheduler_sync::atomic<bool> done_;
  scheduler_sync::atomic<bool> cancelled_;

  bool creator_registered_ = false;
  scheduler_sync::thread::id creator_thread_id_;
  PerThread creator_previous_registration_;
  // Generic publication admission is written for every scheduled task. Keep
  // it away from worker-loop counters so submissions do not invalidate a cache
  // line that every worker is reading.
  alignas(OOX_EIGEN_CACHE_LINE_SIZE) scheduler_sync::atomic<size_t>
      ordinary_publication_state_{0};

  // Resident workers poll this generation. Keep its cache line separate from
  // ordinary publication accounting.
  alignas(128) scheduler_sync::atomic<uint64_t> resident_publication_epoch_{0};
  ResidentPlacement resident_placement_;
  const size_t resident_limit_max_;
  alignas(OOX_EIGEN_CACHE_LINE_SIZE) scheduler_sync::atomic<size_t> resident_limit_;

  bool IsResidentWorker(size_t worker) const noexcept {
    return UsesResidentBusyWait() && worker < ResidentLimit();
  }

  enum class IdleResult { Retry, ExecutedTask, Exit };

  // Main worker thread loop. Returns true if processed some tasks
  bool WorkerLoop(bool external = false, bool once = false,
                  ResidentWorkerBinding binding = {}) {
    PerThread *pt = GetPerThread();
    assert(IsRegistered(pt));
    bool processed_anything = false;
    for (;;) {
      if (cancelled_.load(std::memory_order_acquire)) {
        return processed_anything;
      }

      // Recheck an existing CPU binding before probing ordinary queues.
      if (binding.Requested()) binding.SetResident(IsResidentWorker(pt->thread_id));
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

      const bool resident = IsResidentWorker(static_cast<size_t>(pt->thread_id));
      binding.SetResident(resident);
      if (resident) {
        if (WaitResident(static_cast<unsigned>(pt->thread_id))) {
          processed_anything = true;
        }
        continue;
      }

      const IdleResult idle = WaitForOrdinaryWork(pt);
      if (idle == IdleResult::Exit) return processed_anything;
      if (idle == IdleResult::ExecutedTask) processed_anything = true;
    }
  }

  IdleResult WaitForOrdinaryWork(PerThread *pt) {
    // Extra workers in an oversubscribed automatic pool should park after
    // the failed search, leaving CPU time to its resident participants.
    if (allow_spinning_ && (idle_mode_ != WorkerIdleMode::ResidentAuto ||
                            MaxResidentLimit() == NumThreads())) {
      for (int i = 0; i < kSpinCount; ++i) {
        if (cancelled_.load(std::memory_order_acquire)) return IdleResult::Exit;
        if (TryExecuteOne()) return IdleResult::ExecutedTask;
        scheduler_sync::this_thread::yield();
      }
    }

    // Register before the final checks so a publication racing them wakes us.
    const uint64_t token = worker_event_.PrepareWait();
    if (TaskPtr task = TryTakeOne()) {
      worker_event_.CancelWait();
      ExecuteTask(task);
      return IdleResult::ExecutedTask;
    }
    if (IsResidentWorker(static_cast<size_t>(pt->thread_id))) {
      worker_event_.CancelWait();
      return IdleResult::Retry;
    }
    if (cancelled_.load(std::memory_order_acquire) || ShouldExit()) {
      worker_event_.CancelWait();
      return IdleResult::Exit;
    }
#ifdef OOX_EIGEN_ENABLE_STATS
    const auto idle_begin = std::chrono::steady_clock::now();
#endif
    worker_event_.Wait(token);
#ifdef OOX_EIGEN_ENABLE_STATS
    const auto idle = std::chrono::steady_clock::now() - idle_begin;
    auto &statistics = thread_data_[pt->thread_id].statistics;
    statistics.sleeps.fetch_add(1, std::memory_order_relaxed);
    statistics.idle_nanoseconds.fetch_add(
        std::chrono::duration_cast<std::chrono::nanoseconds>(idle).count(), std::memory_order_relaxed);
#endif
    return IdleResult::Retry;
  }

  static uint64_t NextPoolGeneration() {
#if defined(OOX_EIGEN_TWIST) && OOX_EIGEN_TWIST
    struct Counter { scheduler_sync::atomic<uint64_t> next{1}; };
    TWISTED_STATIC(Counter, counter);
    return counter->next.fetch_add(1, std::memory_order_relaxed);
#else
    static scheduler_sync::atomic<uint64_t> next{1};
    return next.fetch_add(1, std::memory_order_relaxed);
#endif
  }

  void RegisterThread(PerThread *pt, int thread_id, bool owns_queue) {
    pt->pool = this;
    pt->pool_generation = pool_generation_;
    pt->rand = GlobalThreadIdHash();
    pt->thread_id = thread_id;
    pt->owns_queue = owns_queue;
  }

  void RegisterCreator(bool owns_queue) {
    creator_thread_id_ = scheduler_sync::this_thread::get_id();
    PerThread *pt = GetPerThread();
    creator_previous_registration_ = *pt;
    RegisterThread(pt, 0, owns_queue);
    creator_registered_ = true;
  }

  void RestoreCreatorRegistration() {
    if (!creator_registered_ ||
        creator_thread_id_ != scheduler_sync::this_thread::get_id()) {
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

  void PublishOrdinaryTask(TaskPtr task, int target, bool local) {
    const auto discard = [](Task *p) { p->Discard(); };
    std::unique_ptr<Task, decltype(discard)> pending(task, discard);
    TaskPtr inline_task = nullptr;
    {
      PublicationGuard publication(ordinary_publication_state_);
      if (!publication.IsAdmitted() ||
          cancelled_.load(std::memory_order_acquire)) {
        return;
      }
      inline_task = PublishAdmittedTask(pending.release(), target, local);
    }
    if (inline_task) {
      ExecuteTask(inline_task);
    }
  }

  TaskPtr PublishAdmittedTask(TaskPtr task, int target, bool local, bool notify = true) {
    AccountTask(task, target);
    if (!thread_data_[target].PushTask(task, local)) {
      OOX_SCHED_EVENT("inline_queue_full", task->trace_id, target);
      return task;
    }
    if (notify) { HintResidentOrdinaryWork(); WakeOneWorker(); }
    return nullptr;
  }

  TaskPtr TryTakeOne() {
    PerThread *pt = GetPerThread();
    assert(IsRegistered(pt));
    OOX_SCHED_WORKER(pt->thread_id);

    TaskPtr task = nullptr;
    if (pt->owns_queue) {
      task = thread_data_[pt->thread_id].PopFront();
    }
    if (!task) {
      task = GlobalSteal(true);
    }
    return task;
  }

  bool TryExecuteOne() {
    TaskPtr task = TryTakeOne();
    if (!task) return false;
    ExecuteTask(task);
    return true;
  }

  void NotifyStealableRange() noexcept {
    if (HasWaitingHelper()) WakeOneWorker();
    if (HasResidentAvailableWorker({0, static_cast<unsigned>(num_threads_)}))
      HintResidentOrdinaryWork();
  }

  void HintResidentOrdinaryWork() noexcept {
    if (!UsesResidentBusyWait() || ResidentLimit() <= resident_workers_.FirstBackgroundWorker()) {
      return;
    }
    // Publication is only a probe hint, not a reservation of an idle worker.
    // A worker withdraws only after it has actually acquired ordinary work.
    resident_publication_epoch_.fetch_add(1, std::memory_order_release);
  }

  void DeferResidentTask(ThreadData &data, TaskPtr task) noexcept {
    // Cancellation closes admission and drains these slots with other queues.
    // Do not account again: the task retains its original outstanding owner.
    bool published = false;
    {
      PublicationGuard publication(ordinary_publication_state_);
      if (publication.IsAdmitted()) {
        assert(data.resident_deferred.load(std::memory_order_relaxed) == nullptr);
        OOX_SCHED_EVENT("defer", task->trace_id, data.trace_worker);
        data.resident_deferred.store(task, std::memory_order_release);
        HintResidentOrdinaryWork();
        WakeOneWorker();
        published = true;
      }
    }
    if (!published) DiscardPublishedTask(task);
  }

  TaskPtr ProbeOrdinaryWork(unsigned worker, internal::ResidentQueueScan &scan) {
    const uint64_t epoch = resident_publication_epoch_.load(std::memory_order_acquire);
    if (!scan.Prepare(worker, epoch, num_threads_, all_coprimes_.back(),
                      [this] { return Rand(&GetPerThread()->rand); })) return nullptr;
    if (TaskPtr task = scan.TryTake(worker, thread_data_)) return task;
    if (!scan.RoundFinished()) return nullptr;
    OOX_SCHED_EVENT("resident_range_probe", ::oox::scheduler_trace::Current(), worker);
    TaskPtr task = StealActiveRange();
#ifdef OOX_EIGEN_ENABLE_STATS
    if (!task && scan.RetryRequested())
      thread_data_[worker].statistics.failed_steal_rounds.fetch_add(1, std::memory_order_relaxed);
#endif
    return task;
  }

  bool WaitResident(unsigned worker) noexcept {
    if (!IsResidentWorker(worker)) return false;
    // Snapshot before advertising and always probe once for earlier publications.
    internal::ResidentQueueScan scan{resident_publication_epoch_.load(std::memory_order_acquire)};
    auto &availability = resident_workers_.Word(worker / 64);
    availability.Advertise(worker, num_threads_);
    bool processed = false;
    unsigned polls = 0;
    for (;;) {
      const auto command = thread_data_[worker].resident_command.Take(worker, resident_broadcasts_.get());
      if (command.task) {
        thread_data_[worker].resident_command.Run(command, worker);
        command.Complete(worker, num_threads_, resident_workers_.Word(worker / 64),
                         thread_data_[worker].resident_finished);
        processed = true;
        scan.AfterCommand();
        continue;
      }
      if (++polls == kSpinCount) {
        polls = 0;
        if (!IsResidentWorker(worker) && availability.WithdrawResident(worker)) return processed;
        if (TaskPtr ordinary = ProbeOrdinaryWork(worker, scan)) {
#ifdef OOX_EIGEN_TEST_RESIDENT_ACQUIRED
          OOX_EIGEN_TEST_RESIDENT_ACQUIRED(worker);
#endif
          if (availability.WithdrawResident(worker)) {
            ExecuteTask(ordinary);
            return true;
          }
          // The publisher owns this worker. Its command may need the stolen work to join.
          DeferResidentTask(thread_data_[worker], ordinary);
        }
      }
      if ((cancelled_.load(std::memory_order_acquire) || ShouldExit()) && availability.WithdrawResident(worker))
        return processed;
      RelaxResidentWait();
    }
  }

public:
  static void RelaxResidentWait() noexcept {
#if defined(OOX_EIGEN_TWIST) && OOX_EIGEN_TWIST
    scheduler_sync::this_thread::yield();
#elif defined(__x86_64__)
    asm volatile("pause" ::: "memory");
#elif defined(__aarch64__)
    asm volatile("isb sy" ::: "memory");
#else
    scheduler_sync::this_thread::yield();
#endif
  }

private:
  static void DiscardPublishedTask(TaskPtr task) noexcept {
    assert(task != nullptr);
    OOX_SCHED_EVENT("discard", task->trace_id);
    auto *outstanding = task->outstanding;
    assert(outstanding != nullptr);
    task->Discard();
    const size_t previous =
        outstanding->fetch_sub(1, std::memory_order_relaxed);
    assert(previous > 0);
  }

  void TaskFinished(scheduler_sync::atomic<size_t> *outstanding) {
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

  void CancelOrdinaryQueues() {
    // Best-effort drain: a contended local steal may fail before the deque is
    // empty. Recipient-side affinity proxy references are also left for the
    // final owner-side FlushQueues after workers join; Cancel is not a join.
    for (auto &data : thread_data_) {
      while (TaskPtr task = data.PopBack(true)) {
        DiscardPublishedTask(task);
      }
    }
  }

  void DrainCancelledOrdinaryQueues() {
    std::lock_guard<scheduler_sync::mutex> lock(ordinary_cancellation_mutex_);
    CancelOrdinaryQueues();
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
    }
    assert(NoOutstandingTasks());
  }

  // Steal tries to steal work from other worker threads in the range [start,
  // limit) in best-effort manner.
  TaskPtr Steal(unsigned start, unsigned limit, bool force) {
    PerThread *pt = GetPerThread();
    const unsigned size = limit - start;
    internal::QueueTraversal queues(size, all_coprimes_[size - 1], Rand(&pt->rand));
    while (!queues.Empty()) {
      const unsigned victim = start + queues.Next();
      TaskPtr t = thread_data_[victim].PopBack(force);
      if (t) {
        OOX_SCHED_EVENT("steal_queue", t->trace_id, victim, pt->thread_id);
#ifdef OOX_EIGEN_ENABLE_STATS
        if (static_cast<int>(victim) != pt->thread_id)
          thread_data_[pt->thread_id].statistics.successful_steals.fetch_add(
              1, std::memory_order_relaxed);
#endif
        return t;
      }
    }
#ifdef OOX_EIGEN_ENABLE_STATS
    thread_data_[pt->thread_id].statistics.failed_steal_rounds.fetch_add(
        1, std::memory_order_relaxed);
#endif
    return nullptr;
  }

  // Steals work from any other thread in the pool.
  TaskPtr StealActiveRange() {
    if (!UsesResidentBusyWait() || IsCancelled()) return nullptr;
    auto *pt = GetPerThread();
    const unsigned first = (uint64_t{Rand(&pt->rand)} * num_threads_) >> 32;
    for (unsigned offset = 0; offset < static_cast<unsigned>(num_threads_); ++offset) {
      const unsigned victim = (first + offset) % static_cast<unsigned>(num_threads_);
      auto &data = thread_data_[victim];
      auto *task = StealableRange::TrySteal(data.stealable_range);
      if (!task) task = data.concurrent_range.TrySteal();
      if (task) {
        OOX_SCHED_EVENT("steal_range", task->trace_id, victim, pt->thread_id);
        AccountTask(task, victim);
#ifdef OOX_EIGEN_ENABLE_STATS
        thread_data_[pt->thread_id].statistics.successful_steals.fetch_add(1, std::memory_order_relaxed);
#endif
        return task;
      }
    }
    return nullptr;
  }

  TaskPtr GlobalSteal(bool force) {
    if (auto *task = Steal(0, num_threads_, force)) return task;
    return StealActiveRange();
  }

  static inline uint64_t GlobalThreadIdHash() {
    return std::hash<scheduler_sync::thread::id>()(scheduler_sync::this_thread::get_id());
  }

  inline PerThread *GetPerThread() {
    struct Tag {};
    return &scheduler_sync::Local<PerThread, Tag>();
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
