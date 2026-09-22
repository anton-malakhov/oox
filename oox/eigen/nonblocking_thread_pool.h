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
#include <bit>
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
enum class WorkerIdleMode { Park, ResidentBusy };

constexpr uint64_t ResidentClaimMask(uint64_t candidates, unsigned first_bit,
                                     size_t capacity) noexcept {
  if (static_cast<size_t>(std::popcount(candidates)) <= capacity)
    return candidates;
  uint64_t remaining = std::rotr(candidates, static_cast<int>(first_bit));
  uint64_t selected = 0;
  for (size_t count = 0; count < capacity; ++count) {
    selected |= remaining & (~remaining + 1);
    remaining &= remaining - 1;
  }
  return std::rotl(selected, static_cast<int>(first_bit));
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

class ResidentTask {
public:
  virtual void Run(size_t slot) noexcept = 0;
  virtual ~ResidentTask() = default;
};

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
      : ThreadPoolTempl(num_threads, allow_spinning, use_main_thread,
                        WorkerIdleMode::Park, env) {}

  ThreadPoolTempl(int num_threads, bool allow_spinning, bool use_main_thread,
                  WorkerIdleMode idle_mode, Environment env = Environment())
      : env_(env), num_threads_(ValidateThreadCount(num_threads)),
        allow_spinning_(allow_spinning), thread_data_(num_threads_),
        all_coprimes_(num_threads_),
        pool_generation_(NextPoolGeneration()), idle_mode_(idle_mode),
        resident_available_words_(
            idle_mode_ == WorkerIdleMode::ResidentBusy
                ? (static_cast<size_t>(num_threads_) + 63) / 64
                : 0),
        resident_available_(resident_available_words_ == 0
                                ? nullptr
                                : std::make_unique<std::atomic<uint64_t>[]>(
                                      resident_available_words_)),
        done_(false),
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
    for (size_t word = 0; word < resident_available_words_; ++word) {
      resident_available_[word].store(0, std::memory_order_relaxed);
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
      if (!thread_data_[hint].affinity_mailbox.Push(proxy)) {
        inline_task = proxy->template Extract<AffinityProxy::mailbox_bit>();
        proxy->template Extract<AffinityProxy::local_bit>();
      } else {
        const QueueEntry entry = reinterpret_cast<QueueEntry>(proxy) | proxy_tag;
        if (!thread_data_[pt->thread_id].local_tasks.PushFront(entry))
          inline_task = proxy->template Extract<AffinityProxy::local_bit>();
      }
      ReleaseOneResidentForOrdinary();
      WakeOneWorker();
    }
    if (inline_task)
      ExecuteTask(inline_task);
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
    return idle_mode_ == WorkerIdleMode::ResidentBusy;
  }

  size_t ResidentAvailableWorkers(DomainId domain) const noexcept {
    size_t available = 0;
    for (size_t word = 0; word < resident_available_words_; ++word) {
      available += std::popcount(
          resident_available_[word].load(std::memory_order_acquire) &
          ResidentDomainMask(word, domain));
    }
    return available;
  }

  size_t ClaimResidentWorkers(DomainId domain, unsigned *workers,
                              size_t capacity) noexcept {
    if (idle_mode_ != WorkerIdleMode::ResidentBusy || capacity == 0) {
      return 0;
    }
    // The word index is worker / 64 and the bit is worker % 64. Rotating both
    // starting positions avoids repeatedly favoring low-numbered workers.
    const size_t ticket = resident_claim_cursor_.fetch_add(
        std::max<size_t>(capacity, 1), std::memory_order_relaxed);
    const size_t first_word = (ticket / 64) % resident_available_words_;
    const unsigned first_bit = static_cast<unsigned>(ticket % 64);
    size_t claimed = 0;
    for (size_t offset = 0;
         offset < resident_available_words_ && claimed < capacity; ++offset) {
      const size_t word = (first_word + offset) % resident_available_words_;
      const uint64_t allowed = ResidentDomainMask(word, domain);
      uint64_t observed =
          resident_available_[word].load(std::memory_order_acquire);
      while (observed & allowed) {
        const uint64_t candidates = observed & allowed;
        uint64_t selected =
            ResidentClaimMask(candidates, first_bit, capacity - claimed);
        if (resident_available_[word].compare_exchange_weak(
                observed, observed & ~selected, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
          while (selected != 0) {
            const unsigned bit = std::countr_zero(selected);
            workers[claimed++] = static_cast<unsigned>(word * 64 + bit);
            selected &= selected - 1;
          }
          break;
        }
      }
    }
    return claimed;
  }

  void PublishResident(ResidentTask &task, std::atomic<size_t> &completion,
                       unsigned worker, size_t slot) noexcept {
    assert(worker < static_cast<unsigned>(num_threads_));
    ThreadData &data = thread_data_[worker];
    assert(data.resident_task.load(std::memory_order_relaxed) == nullptr);
    data.resident_slot = slot;
    data.resident_completion = &completion;
    data.resident_task.store(&task, std::memory_order_release);
  }

  template <typename Region> void HelpResidentUntil(Region &region) {
    unsigned spins = 0;
    while (!region.IsComplete()) {
      if (++spins < kSpinCount) {
        RelaxResidentWait();
        continue;
      }
      spins = 0;
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
  static ThreadPoolTempl *&ActiveCancellation() noexcept {
    static thread_local ThreadPoolTempl *active = nullptr;
    return active;
  }

  void CancelOnce() {
    cancelled_.store(true, std::memory_order_release);
    ordinary_publication_state_.fetch_or(kPublicationCancelled,
                                         std::memory_order_seq_cst);
    while ((ordinary_publication_state_.load(std::memory_order_acquire) &
            kPublicationPublisherMask) != 0) {
      std::this_thread::yield();
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
#ifdef OOX_EIGEN_ENABLE_STATS
    const PerThread *pt = GetPerThread();
    const size_t worker = IsRegistered(pt) ? static_cast<size_t>(pt->thread_id) : 0;
    thread_data_[worker].statistics.executed.fetch_add(
        1, std::memory_order_relaxed);
#endif
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
          mailbox(1024), resident_task(nullptr),
          resident_ordinary(false) {}
    std::unique_ptr<Thread> thread;
    std::atomic<size_t> outstanding_tasks;
    Queue local_tasks;
    AffinityMailbox affinity_mailbox;
    rigtorp::mpmc::Queue<TaskPtr> mailbox;
    std::atomic<ResidentTask *> resident_task;
    size_t resident_slot = 0;
    std::atomic<size_t> *resident_completion = nullptr;
    std::atomic<bool> resident_ordinary;

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
  const WorkerIdleMode idle_mode_;
  const size_t resident_available_words_;
  std::unique_ptr<std::atomic<uint64_t>[]> resident_available_;
  std::atomic<size_t> resident_claim_cursor_{0};

  std::once_flag cancellation_once_;
  std::mutex ordinary_cancellation_mutex_;
  EventCount worker_event_;
  EventCount waiter_event_;
  std::atomic<bool> done_;
  std::atomic<bool> cancelled_;

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
    PerThread *pt = GetPerThread();
    assert(IsRegistered(pt));
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

      if (idle_mode_ == WorkerIdleMode::ResidentBusy) {
        if (WaitResident(static_cast<unsigned>(pt->thread_id))) {
          processed_anything = true;
        }
        continue;
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

  TaskPtr PublishAdmittedTask(TaskPtr task, int target, bool local) {
    AccountTask(task, target);
    if (!thread_data_[target].PushTask(task, local)) {
      return task;
    }
    ReleaseOneResidentForOrdinary();
    WakeOneWorker();
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
      task = GlobalSteal(true);
    }
    if (!task) {
      return false;
    }
    ExecuteTask(task);
    return true;
  }

  static uint64_t ResidentDomainMask(size_t word, DomainId domain) noexcept {
    const size_t word_begin = word * 64;
    const size_t first = std::max(word_begin, static_cast<size_t>(domain.start));
    const size_t last =
        std::min(word_begin + 64, static_cast<size_t>(domain.limit));
    if (first >= last) {
      return 0;
    }
    const unsigned offset = static_cast<unsigned>(first - word_begin);
    const unsigned count = static_cast<unsigned>(last - first);
    const uint64_t bits = count == 64 ? ~uint64_t{0}
                                      : (uint64_t{1} << count) - 1;
    return bits << offset;
  }

  bool WithdrawResident(unsigned worker) noexcept {
    const size_t word = worker / 64;
    const uint64_t bit = uint64_t{1} << (worker % 64);
    return (resident_available_[word].fetch_and(~bit,
                                                std::memory_order_acq_rel) &
            bit) != 0;
  }

  void ReleaseOneResidentForOrdinary() noexcept {
    if (idle_mode_ != WorkerIdleMode::ResidentBusy) {
      return;
    }
    unsigned worker = 0;
    if (ClaimResidentWorkers({0, static_cast<unsigned>(num_threads_)}, &worker,
                             1) == 1) {
      thread_data_[worker].resident_ordinary.store(true,
                                                   std::memory_order_release);
    }
  }

  bool WaitResident(unsigned worker) noexcept {
    ThreadData &data = thread_data_[worker];
    const size_t word = worker / 64;
    const uint64_t bit = uint64_t{1} << (worker % 64);
    resident_available_[word].fetch_or(bit, std::memory_order_release);
    bool processed = false;
    unsigned polls = 0;
    for (;;) {
      ResidentTask *task = nullptr;
      if (data.resident_task.load(std::memory_order_relaxed))
        task = data.resident_task.exchange(nullptr, std::memory_order_acquire);
      if (task) {
        const size_t slot = data.resident_slot;
        std::atomic<size_t> *completion = data.resident_completion;
        assert(completion != nullptr);
        task->Run(slot);
        // A subsequent launch may claim this worker only after Run returned.
        // Publishing availability before completion guarantees that a caller
        // observing completion can immediately launch the next generation.
        resident_available_[word].fetch_or(bit, std::memory_order_release);
        completion->fetch_sub(1, std::memory_order_release);
        processed = true;
        continue;
      }
      if (data.resident_ordinary.load(std::memory_order_relaxed) &&
          data.resident_ordinary.exchange(false, std::memory_order_acquire)) {
        return processed;
      }
      // Publication may race with advertising this worker as resident. Probe
      // periodically so a missed availability snapshot cannot strand work.
      if (++polls == kSpinCount) {
        polls = 0;
        // A running ordinary task does not make idle residents unavailable.
        // Only queued work needs a scheduler worker; remote affinity tasks
        // also have a stealable sender reference.
        bool pending = !data.affinity_mailbox.Empty();
        for (const auto &source : thread_data_) {
          pending |= !source.local_tasks.Empty() || !source.mailbox.empty();
        }
        if (pending && WithdrawResident(worker))
          return processed;
      }
      if (cancelled_.load(std::memory_order_acquire) || ShouldExit()) {
        if (WithdrawResident(worker)) {
          return processed;
        }
      }
      RelaxResidentWait();
    }
  }

  static void RelaxResidentWait() noexcept {
#if defined(__x86_64__)
    asm volatile("pause" ::: "memory");
#elif defined(__aarch64__)
    asm volatile("yield" ::: "memory");
#else
    std::this_thread::yield();
#endif
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
