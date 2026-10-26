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
#include "stealable_range.h"
#include "scheduler_trace.h"
#include "resident_command.h"

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


#ifdef OOX_EIGEN_THREAD_POOL_TESTING
namespace internal {
inline scheduler_sync::atomic<size_t> completion_notifications{0};
inline scheduler_sync::atomic<size_t> completion_waits{0};
#ifdef OOX_EIGEN_TEST_RESIDENT_CLOCK_BITS
static_assert(OOX_EIGEN_TEST_RESIDENT_CLOCK_BITS > 0 && OOX_EIGEN_TEST_RESIDENT_CLOCK_BITS <= 56);
inline constexpr unsigned resident_packed_clock_bits = OOX_EIGEN_TEST_RESIDENT_CLOCK_BITS;
inline constexpr uint64_t resident_packed_clock_max = (uint64_t{1} << resident_packed_clock_bits) - 1;
#else
inline constexpr unsigned resident_packed_clock_bits = 0;
inline constexpr uint64_t resident_packed_clock_max = 0;
#endif
#ifdef OOX_EIGEN_TEST_GLOBAL_RESIDENT_CLOCK_BITS
static_assert(OOX_EIGEN_TEST_GLOBAL_RESIDENT_CLOCK_BITS > 0 && OOX_EIGEN_TEST_GLOBAL_RESIDENT_CLOCK_BITS < 64);
inline constexpr uint64_t resident_global_clock_max =
    (uint64_t{1} << OOX_EIGEN_TEST_GLOBAL_RESIDENT_CLOCK_BITS) - 1;
#else
inline constexpr uint64_t resident_global_clock_max = ~uint64_t{0};
#endif
#ifdef OOX_EIGEN_TEST_RESIDENT_BATCH_GENERATION_BITS
static_assert(OOX_EIGEN_TEST_RESIDENT_BATCH_GENERATION_BITS > 0 && OOX_EIGEN_TEST_RESIDENT_BATCH_GENERATION_BITS < 64);
inline constexpr uint64_t resident_batch_generation_max =
    ((uint64_t{1} << OOX_EIGEN_TEST_RESIDENT_BATCH_GENERATION_BITS) - 1) & ~uint64_t{1};
#else
inline constexpr uint64_t resident_batch_generation_max = ~uint64_t{1};
#endif
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
        resident_available_words_(
            idle_mode_ == WorkerIdleMode::ResidentBusy
                ? (static_cast<size_t>(num_threads_) + 63) / 64
                : 0),
        resident_available_(resident_available_words_ == 0
                                ? nullptr
                                : std::make_unique<scheduler_sync::atomic<uint64_t>[]>(
                                      resident_available_words_)),
        resident_broadcasts_(resident_available_words_ == 0
                                ? nullptr
                                : std::make_unique<ResidentBroadcast[]>(
                                      resident_available_words_)),
        done_(false),
        cancelled_(false),
        resident_limit_(idle_mode == WorkerIdleMode::ResidentBusy ? num_threads_ : 0),
        first_background_worker_(use_main_thread && num_threads_ > 1 ? 1 : 0) {
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
    return idle_mode_ == WorkerIdleMode::ResidentBusy;
  }

  size_t ResidentLimit() const noexcept {
    return resident_limit_.load(std::memory_order_acquire);
  }

  // In-flight resident commands finish normally. Other workers use parking;
  // increasing eligibility wakes parked workers to avoid lost transitions.
  void SetResidentLimit(size_t limit) {
    if (!UsesResidentBusyWait() && limit != 0)
      throw std::invalid_argument("resident limit requires a resident-capable pool");
    resident_limit_.store(std::min(limit, NumThreads()), std::memory_order_release);
    WakeAllWorkers();
  }

  size_t ResidentCapacity(DomainId domain) const noexcept {
    const size_t first = std::max<size_t>(domain.start, first_background_worker_);
    const size_t last = std::min<size_t>(domain.limit, ResidentLimit());
    return last > first ? last - first : 0;
  }

  // A demand hint, not an idle-worker reservation. A waiter cancels its
  // registration before executing acquired work.
  bool HasWaitingHelper() const noexcept { return worker_event_.HasWaiters(); }
  bool HasResidentCommand() noexcept {
    auto *pt = GetPerThread();
    return IsRegistered(pt) && pt->owns_queue &&
        (thread_data_[pt->thread_id].resident_task.load(std::memory_order_acquire) != nullptr ||
         HasResidentBatch(pt->thread_id));
  }

  bool PublishStealableRange(StealableRange &range) noexcept {
    auto *pt = GetPerThread();
    if (!IsRegistered(pt) || !pt->owns_queue || !UsesResidentBusyWait()) return false;
    range.Publish(thread_data_[pt->thread_id].stealable_range);
    if (HasWaitingHelper()) WakeOneWorker();
    if (HasResidentAvailableWorker({0, static_cast<unsigned>(num_threads_)}))
      HintResidentOrdinaryWork();
    return true;
  }

  size_t ResidentAvailableWorkers(DomainId domain) const noexcept {
    domain.limit = static_cast<unsigned>(std::min<size_t>(domain.limit, ResidentLimit()));
    size_t available = 0;
    for (size_t word = 0; word < resident_available_words_; ++word) {
      available += std::popcount(
          resident_available_[word].load(std::memory_order_acquire) &
          ResidentDomainMask(word, domain));
    }
    return available;
  }

  // Like the count, this is only a snapshot. Stop at the first eligible bit
  // when callers need a Boolean demand hint rather than a participant count.
  bool HasResidentAvailableWorker(DomainId domain) const noexcept {
    domain.limit = static_cast<unsigned>(std::min<size_t>(domain.limit, ResidentLimit()));
    for (size_t word = 0; word < resident_available_words_; ++word) {
      if (resident_available_[word].load(std::memory_order_acquire) &
          ResidentDomainMask(word, domain)) return true;
    }
    return false;
  }

  size_t ClaimResidentWorkers(DomainId domain, unsigned *workers,
                              size_t capacity) noexcept {
    domain.limit = static_cast<unsigned>(std::min<size_t>(domain.limit, ResidentLimit()));
    if (domain.start >= domain.limit)
      return 0;
    if (idle_mode_ != WorkerIdleMode::ResidentBusy || capacity == 0) {
      return 0;
    }
    // The word index is worker / 64 and the bit is worker % 64. Rotating both
    // starting positions avoids repeatedly favoring low-numbered workers.
    // A single-helper launch prefers the first ready recipient;
    // it never waits for a busy preferred worker. Multi-helper claims rotate.
    const uint64_t packed_version = num_threads_ <= kMaxPackedResidentThreads
        ? resident_available_[0].load(std::memory_order_relaxed) >> (2 * num_threads_) : 0;
    const bool packed_rotation = num_threads_ <= kMaxPackedResidentThreads && packed_version < ResidentPackedClockMax();
    const size_t ticket =
        capacity == 1 ? 0 :
        packed_rotation ? static_cast<size_t>(packed_version) :
        resident_claim_cursor_.fetch_add(
        std::max<size_t>(capacity, 1), std::memory_order_relaxed);
    const size_t first_word = (ticket / 64) % resident_available_words_;
    const unsigned first_bit = static_cast<unsigned>(packed_rotation && capacity > 1
        ? first_background_worker_ + ticket % (num_threads_ - first_background_worker_)
        : ticket % 64);
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
        const uint64_t desired = (observed & ~selected) + ResidentPackedClockIncrement(observed);
        if (resident_available_[word].compare_exchange_weak(
                observed, desired, std::memory_order_acq_rel,
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

  void PublishResident(ResidentTask &task, scheduler_sync::atomic<size_t> &completion,
                       unsigned worker, size_t slot, uint64_t epoch = 0) noexcept {
    assert(worker < static_cast<unsigned>(num_threads_));
    ThreadData &data = thread_data_[worker];
    assert(data.resident_task.load(std::memory_order_relaxed) == nullptr);
    data.resident_slot = slot;
    data.resident_completion = epoch ? nullptr : &completion;
    data.resident_epoch = epoch;
#ifdef OOX_SCHEDULER_TRACE
    data.resident_trace_id = ::oox::scheduler_trace::NextId();
    OOX_SCHED_WORKER(CurrentThreadId());
    OOX_SCHED_EVENT("activate", data.resident_trace_id, worker, slot, ::oox::scheduler_trace::Current());
#endif
    data.resident_task.store(&task, std::memory_order_release);
  }

  // A shared descriptor handles one word-sized cohort. Other simultaneous
  // publishers retain the ordinary per-worker publication path.
  size_t PublishResidentBatch(ResidentTask &task, scheduler_sync::atomic<size_t> &completion,
                            const unsigned *workers, size_t count,
                            size_t first_slot = 1, uint64_t epoch = 0,
                            bool packed_completion = false) noexcept {
    if (!count) return size_t(-1);
    const size_t word = workers[0] / 64;
    uint64_t mask = 0;
    bool ordered = count > 1;
    for (size_t i = 0; i < count; ++i) {
      ordered &= workers[i] / 64 == word && (!i || workers[i - 1] < workers[i]);
      mask |= uint64_t{1} << (workers[i] % 64);
    }
    auto &batch = resident_broadcasts_[word];
    if (!ordered || batch.busy.test_and_set(std::memory_order_acquire)) {
      for (size_t i = 0; i < count; ++i)
        PublishResident(task, completion, workers[i], first_slot + i, epoch);
      return size_t(-1);
    }
    assert(completion.load(std::memory_order_relaxed) == count);
    const uint64_t generation = batch.generation.load(std::memory_order_relaxed);
    if (generation == ResidentBatchGenerationMax()) {
      // An inactive nonmember may retain any old generation indefinitely.
      // Freeze the last even value and permanently publish direct commands.
      batch.busy.clear(std::memory_order_release);
      for (size_t i = 0; i < count; ++i)
        PublishResident(task, completion, workers[i], first_slot + i, epoch);
      return size_t(-1);
    }
    batch.generation.store(generation + 1, std::memory_order_release);
#if defined(OOX_EIGEN_THREAD_POOL_TESTING) && defined(OOX_EIGEN_TEST_RESIDENT_BATCH_EDITING)
    OOX_EIGEN_TEST_RESIDENT_BATCH_EDITING(word);
#endif
    batch.task = &task;
    batch.completion = epoch ? nullptr : &completion;
    batch.epoch = epoch;
    batch.packed_completion = packed_completion && epoch &&
        num_threads_ <= kMaxPackedResidentThreads;
    batch.first_slot = first_slot;
#ifdef OOX_SCHEDULER_TRACE
    for (size_t i = 0; i < count; ++i) {
      auto &data = thread_data_[workers[i]];
      data.resident_trace_id = ::oox::scheduler_trace::NextId();
      OOX_SCHED_WORKER(CurrentThreadId());
      OOX_SCHED_EVENT("activate", data.resident_trace_id, workers[i], first_slot + i,
                      ::oox::scheduler_trace::Current());
    }
#endif
    // A reader of this mask must also observe the preceding odd generation.
    // Validation therefore rejects a new mask paired with an old generation.
    batch.mask.store(mask, std::memory_order_release);
#if defined(OOX_EIGEN_THREAD_POOL_TESTING) && defined(OOX_EIGEN_TEST_RESIDENT_BATCH_MASK_READY)
    OOX_EIGEN_TEST_RESIDENT_BATCH_MASK_READY(word);
#endif
    batch.generation.store(generation + 2, std::memory_order_release);
    return word;
  }

  // Allocate after all claims and before any command publication. Successive
  // claims of any one worker therefore have strictly increasing epochs.
  uint64_t NextResidentEpoch() noexcept {
    if (num_threads_ <= kMaxPackedResidentThreads) {
      const uint64_t version = resident_available_[0].load(std::memory_order_acquire) >> (2 * num_threads_);
      return version == ResidentPackedClockMax() ? 0 : version;
    }
    uint64_t observed = resident_dispatch_epoch_.load(std::memory_order_relaxed);
    const uint64_t maximum = ResidentGlobalClockMax();
    while (observed < maximum) {
      if (resident_dispatch_epoch_.compare_exchange_weak(observed, observed + 1,
                                                        std::memory_order_relaxed))
        return observed + 1 == maximum ? 0 : observed + 1;
    }
    // Keep every completed ID monotonic; zero permanently selects counters.
    return 0;
  }

  bool ResidentEpochComplete(uint64_t epoch, const unsigned *workers,
                              size_t count) const noexcept {
    for (size_t i = 0; i < count; ++i)
      if (thread_data_[workers[i]].resident_finished.load(std::memory_order_acquire) < epoch)
        return false;
    return true;
  }

  bool SupportsPackedResidentCompletion() const noexcept {
    return num_threads_ <= kMaxPackedResidentThreads;
  }
  uint64_t ResidentBatchDoneMask(size_t word, const unsigned *workers,
                                size_t count) const noexcept {
    if (word == size_t(-1) || !SupportsPackedResidentCompletion()) return 0;
    uint64_t mask = 0;
    for (size_t i = 0; i < count; ++i)
      mask |= uint64_t{1} << (workers[i] + num_threads_);
    return mask;
  }
  bool ResidentBatchDone(size_t word, uint64_t mask) const noexcept {
    return (resident_available_[word].load(std::memory_order_acquire) & mask) == mask;
  }

  // The selected finished-epoch or packed-done acquire must precede release.
  void ReleaseResidentEpochBatch(size_t word) noexcept {
    if (word == size_t(-1)) return;
    auto &batch = resident_broadcasts_[word];
    if (batch.packed_completion) {
      // Sticky bits belong to this busy-held descriptor, not worker readiness.
      // Direct commands may reuse a worker, but never modify these done bits.
      const uint64_t done = batch.mask.load(std::memory_order_relaxed) << num_threads_;
      resident_available_[word].fetch_and(~done, std::memory_order_relaxed);
    }
    batch.mask.store(0, std::memory_order_release);
    batch.busy.clear(std::memory_order_release);
#if defined(OOX_EIGEN_THREAD_POOL_TESTING) && defined(OOX_EIGEN_TEST_RESIDENT_BATCH_RELEASED)
    OOX_EIGEN_TEST_RESIDENT_BATCH_RELEASED(word);
#endif
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
#if defined(OOX_EIGEN_TWIST) && OOX_EIGEN_TWIST
    struct Tag {};
    return scheduler_sync::Local<ThreadPoolTempl *, Tag>();
#else
    static thread_local ThreadPoolTempl *active = nullptr;
    return active;
#endif
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
  static constexpr int kMaxPackedResidentThreads = 8;
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
#if defined(OOX_EIGEN_TWIST) && OOX_EIGEN_TWIST
    struct Tag {};
    return scheduler_sync::Local<ExecutionContext, Tag>();
#else
    static thread_local ExecutionContext context;
    return context;
#endif
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
          mailbox(1024), resident_task(nullptr) {}
    std::unique_ptr<Thread> thread;
    scheduler_sync::atomic<size_t> outstanding_tasks;
    Queue local_tasks;
    AffinityMailbox affinity_mailbox;
    rigtorp::mpmc::Queue<TaskPtr> mailbox;
    scheduler_sync::atomic<ResidentTask *> resident_task;
    // Owned by this worker, including HasResidentCommand calls inside its work.
    uint64_t resident_batch_seen = 0;
    size_t resident_slot = 0;
    // Null selects the persistent epoch record; otherwise use a legacy counter.
    scheduler_sync::atomic<size_t> *resident_completion = nullptr;
    uint64_t resident_epoch = 0;
    alignas(128) scheduler_sync::atomic<uint64_t> resident_finished{0};
    // One already-accounted task rescued when a Rapid publisher wins capture.
    // It remains stealable, including by waits inside the winning command.
    alignas(128) scheduler_sync::atomic<TaskPtr> resident_deferred{nullptr};
    scheduler_sync::atomic<uintptr_t> stealable_range{0};
#ifdef OOX_SCHEDULER_TRACE
    int trace_worker = -1;
    uint64_t resident_trace_id = 0;
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
  const size_t resident_available_words_;
  // Small pools keep a claim clock above their worker bits. Registration and
  // withdrawal must preserve those upper bits, including during reconfiguration.
  // For <=8 workers: [clock | N sticky shared-cohort done bits | N ready bits].
  // Larger pools use ordinary readiness words and separate epoch records.
  std::unique_ptr<scheduler_sync::atomic<uint64_t>[]> resident_available_;
  std::unique_ptr<ResidentBroadcast[]> resident_broadcasts_;
  scheduler_sync::atomic<size_t> resident_claim_cursor_{0};
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

  // Keep the polled generation off the frequently written claim cursor's
  // cache line without shifting existing scheduler fields.
  alignas(128) scheduler_sync::atomic<uint64_t> resident_publication_epoch_{0};
  alignas(OOX_EIGEN_CACHE_LINE_SIZE) scheduler_sync::atomic<size_t> resident_limit_;
  const size_t first_background_worker_;

  bool IsResidentWorker(size_t worker) const noexcept {
    return UsesResidentBusyWait() && worker < ResidentLimit();
  }

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

      if (IsResidentWorker(static_cast<size_t>(pt->thread_id))) {
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
          scheduler_sync::this_thread::yield();
        }
      }
      if (found_work) {
        continue;
      }

      const uint64_t token = worker_event_.PrepareWait();
      if (TaskPtr task = TryTakeOne()) {
        worker_event_.CancelWait();
        ExecuteTask(task);
        processed_anything = true;
        continue;
      }
      if (IsResidentWorker(static_cast<size_t>(pt->thread_id))) {
        worker_event_.CancelWait();
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

  uint64_t ResidentPackedClockMax() const noexcept {
    if (num_threads_ > kMaxPackedResidentThreads) return 0;
    const uint64_t maximum = ~uint64_t{0} >> (2 * num_threads_);
#ifdef OOX_EIGEN_THREAD_POOL_TESTING
    if (internal::resident_packed_clock_bits)
      return std::min(internal::resident_packed_clock_max, maximum);
#endif
    return maximum;
  }

  static constexpr uint64_t ResidentGlobalClockMax() noexcept {
#ifdef OOX_EIGEN_THREAD_POOL_TESTING
    return internal::resident_global_clock_max;
#else
    return ~uint64_t{0};
#endif
  }

  static constexpr uint64_t ResidentBatchGenerationMax() noexcept {
#ifdef OOX_EIGEN_THREAD_POOL_TESTING
    return internal::resident_batch_generation_max;
#else
    return ~uint64_t{1};
#endif
  }

  uint64_t ResidentPackedClockIncrement(uint64_t observed) const noexcept {
    if (num_threads_ > kMaxPackedResidentThreads || (observed >> (2 * num_threads_)) == ResidentPackedClockMax())
      return 0;
    return uint64_t{1} << (2 * num_threads_);
  }

  bool WithdrawResident(unsigned worker) noexcept {
    const size_t word = worker / 64;
    const uint64_t bit = uint64_t{1} << (worker % 64);
    return (resident_available_[word].fetch_and(~bit,
                                                std::memory_order_acq_rel) &
            bit) != 0;
  }

  void HintResidentOrdinaryWork() noexcept {
    if (!UsesResidentBusyWait() || ResidentLimit() <= first_background_worker_) {
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

  bool HasResidentBatch(unsigned worker) noexcept {
    if (!UsesResidentBusyWait()) return false;
    auto &batch = resident_broadcasts_[worker / 64];
    const uint64_t generation = batch.generation.load(std::memory_order_acquire);
    if ((generation & 1) || generation == thread_data_[worker].resident_batch_seen)
      return false;
    const uint64_t mask = batch.mask.load(std::memory_order_acquire);
    return batch.generation.load(std::memory_order_acquire) == generation &&
        (mask & (uint64_t{1} << (worker % 64)));
  }

  ResidentBroadcast *TakeResidentBatch(unsigned worker, ResidentTask *&task,
                                       size_t &slot,
                                       scheduler_sync::atomic<size_t> *&completion,
                                       uint64_t &epoch, bool &packed_completion) noexcept {
    auto &batch = resident_broadcasts_[worker / 64];
    auto &seen = thread_data_[worker].resident_batch_seen;
    const uint64_t generation = batch.generation.load(std::memory_order_acquire);
    if ((generation & 1) || generation == seen) return nullptr;
#if defined(OOX_EIGEN_THREAD_POOL_TESTING) && defined(OOX_EIGEN_TEST_RESIDENT_BATCH_READER)
    OOX_EIGEN_TEST_RESIDENT_BATCH_READER(worker, generation);
#endif
    const uint64_t mask = batch.mask.load(std::memory_order_acquire);
    if (batch.generation.load(std::memory_order_acquire) != generation) {
#if defined(OOX_EIGEN_THREAD_POOL_TESTING) && defined(OOX_EIGEN_TEST_RESIDENT_BATCH_REJECTED)
      OOX_EIGEN_TEST_RESIDENT_BATCH_REJECTED(worker);
#endif
      return nullptr;
    }
    seen = generation;
    const uint64_t bit = uint64_t{1} << (worker % 64);
    if (!(mask & bit)) return nullptr;
    // Membership is admission: this descriptor cannot be reused until this
    // worker completes. Nonmembers never read its non-atomic metadata.
    task = batch.task;
    completion = batch.completion;
    epoch = batch.epoch;
    packed_completion = batch.packed_completion;
    slot = batch.first_slot + std::popcount(mask & (bit - 1));
    return &batch;
  }

  bool WaitResident(unsigned worker) noexcept {
    if (!IsResidentWorker(worker))
      return false;
    ThreadData &data = thread_data_[worker];
    const size_t word = worker / 64;
    const uint64_t bit = uint64_t{1} << (worker % 64);
    // Snapshot before registration and always scan once: ordinary work may
    // have been published immediately before we registered.
    uint64_t observed_epoch =
        resident_publication_epoch_.load(std::memory_order_acquire);
    bool probe_ordinary = true;
    resident_available_[word].fetch_or(bit, std::memory_order_release);
    bool processed = false;
    unsigned polls = 0;
    unsigned search_left = 0, victim = 0, increment = 1;
    bool retry_queued = false;
    for (;;) {
      ResidentTask *task = nullptr;
      size_t slot = 0;
      scheduler_sync::atomic<size_t> *completion = nullptr;
      uint64_t epoch = 0;
      bool packed_completion = false;
      ResidentBroadcast *batch = nullptr;
      if (data.resident_task.load(std::memory_order_relaxed))
        task = data.resident_task.load(std::memory_order_acquire);
      if (task) {
        // This worker is the sole consumer. Publishers cannot replace this
        // command until the worker advertises its availability after Run.
        data.resident_task.store(nullptr, std::memory_order_relaxed);
        slot = data.resident_slot;
        completion = data.resident_completion;
        epoch = data.resident_epoch;
      } else {
        batch = TakeResidentBatch(worker, task, slot, completion, epoch, packed_completion);
      }
      if (task) {
        assert(completion != nullptr || epoch != 0);
        {
          OOX_SCHED_WORKER(worker);
          OOX_SCHED_SCOPE("resident", data.resident_trace_id, worker, slot);
          task->Run(slot);
        }
        // Advertise before completion so the next launch can capture us.
        // The release already needed to advertise readiness also joins this
        // opted-in shared cohort. Keep done sticky until its caller releases busy.
        resident_available_[word].fetch_or(bit | (packed_completion ? bit << num_threads_ : 0),
                                          std::memory_order_release);
        const size_t before = completion
            ? completion->fetch_sub(1, std::memory_order_release) : 0;
        if (!completion && !packed_completion)
          data.resident_finished.store(epoch, std::memory_order_release);
        if (completion && batch && before == 1) {
          // Join all preceding participant releases before allowing a later
          // publisher to overwrite metadata read by those participants.
          scheduler_sync::atomic_thread_fence(std::memory_order_acquire);
          // Only pool-owned state is touched after the completion RMW: the
          // caller may already have destroyed its command and counter.
          batch->mask.store(0, std::memory_order_release);
          batch->busy.clear(std::memory_order_release);
#if defined(OOX_EIGEN_THREAD_POOL_TESTING) && defined(OOX_EIGEN_TEST_RESIDENT_BATCH_RELEASED)
          OOX_EIGEN_TEST_RESIDENT_BATCH_RELEASED(worker / 64);
#endif
        }
        processed = true;
        probe_ordinary = true;
        search_left = 0;
        continue;
      }
      // After the entry scan, rescan when publication advances the generation.
      // Keep retrying a nonempty scan if a Rapid launch owns our availability
      // bit; clearing the pending-work indication could strand ordinary work.
      if (++polls == kSpinCount) {
        polls = 0;
        if (!IsResidentWorker(worker) && WithdrawResident(worker))
          return processed;
        const uint64_t epoch =
            resident_publication_epoch_.load(std::memory_order_acquire);
        if (probe_ordinary || epoch != observed_epoch) {
          if (!search_left) {
            // An epoch is searched only after finishing this permutation.
            // A later epoch causes another round, even for already-visited queues.
            observed_epoch = epoch;
            auto *pt = GetPerThread();
            const unsigned random = Rand(&pt->rand);
            victim = (uint64_t{random} * num_threads_) >> 32;
            const auto &steps = all_coprimes_[num_threads_ - 1];
            increment = steps[(uint64_t{random} * steps.size()) >> 32];
            search_left = static_cast<unsigned>(num_threads_);
            retry_queued = false;
            OOX_SCHED_EVENT("resident_queue_scan", ::oox::scheduler_trace::Current(), worker);
          }
          retry_queued |= !data.affinity_mailbox.Empty();
          TaskPtr ordinary = data.PopFront();
          // Probe one remote queue per polling interval, so searches do not
          // delay a newly published resident command by a full pool scan.
          if (!ordinary && search_left) {
            auto &source = thread_data_[victim];
            const bool pending = !source.local_tasks.Empty() || !source.mailbox.empty() ||
                source.resident_deferred.load(std::memory_order_acquire) != nullptr ||
                source.stealable_range.load(std::memory_order_acquire) != 0;
            retry_queued |= pending;
            if (pending) ordinary = source.PopBack(true);
            if (ordinary && victim != worker)
              OOX_SCHED_EVENT("steal_queue", ordinary->trace_id, victim, worker);
#ifdef OOX_EIGEN_ENABLE_STATS
            if (ordinary && victim != worker)
              data.statistics.successful_steals.fetch_add(1, std::memory_order_relaxed);
#endif
            --search_left;
            victim += increment;
            if (victim >= static_cast<unsigned>(num_threads_)) victim -= num_threads_;
          }
          // Exhaust ordinary queues before materializing a new task.
          if (!ordinary && !search_left) {
            OOX_SCHED_EVENT("resident_range_probe", ::oox::scheduler_trace::Current(), worker);
            ordinary = StealActiveRange();
          }
          probe_ordinary = search_left != 0 || retry_queued;
#ifdef OOX_EIGEN_ENABLE_STATS
          if (!ordinary && !search_left && retry_queued)
            data.statistics.failed_steal_rounds.fetch_add(1, std::memory_order_relaxed);
#endif
          if (ordinary) {
#ifdef OOX_EIGEN_TEST_RESIDENT_ACQUIRED
            OOX_EIGEN_TEST_RESIDENT_ACQUIRED(worker);
#endif
            if (WithdrawResident(worker)) {
              ExecuteTask(ordinary);
              return true;
            }
            // A publisher owns our bit, possibly before publishing its command.
            // Never hide work that that command might itself need to join.
            DeferResidentTask(data, ordinary);
          }
        }
      }
      if (cancelled_.load(std::memory_order_acquire) || ShouldExit()) {
        if (WithdrawResident(worker)) {
          return processed;
        }
      }
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
        OOX_SCHED_EVENT("steal_queue", t->trace_id, start + victim, pt->thread_id);
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
  TaskPtr StealActiveRange() {
    if (!UsesResidentBusyWait() || IsCancelled()) return nullptr;
    auto *pt = GetPerThread();
    const unsigned first = (uint64_t{Rand(&pt->rand)} * num_threads_) >> 32;
    for (unsigned offset = 0; offset < static_cast<unsigned>(num_threads_); ++offset) {
      const unsigned victim = (first + offset) % static_cast<unsigned>(num_threads_);
      if (auto *task = StealableRange::TrySteal(thread_data_[victim].stealable_range)) {
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
#if defined(OOX_EIGEN_TWIST) && OOX_EIGEN_TWIST
    struct Tag {};
    auto &per_thread_ = scheduler_sync::Local<PerThread, Tag>();
#else
    static thread_local PerThread per_thread_;
#endif
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
