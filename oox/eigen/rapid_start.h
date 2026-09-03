// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "nonblocking_thread_pool.h"
#include "rapid_start_model.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace oox::detail::eigen_pool::rapid {

class RapidDomainState;
class SubtreeLease;

class RapidRegion final {
public:
  using Invoke = void (*)(void *, size_t, size_t);

  RapidRegion(RapidDomainState &state, bool worker_waiter,
              bool leave_on_steal, void *function, Invoke invoke)
      : state_(state), worker_waiter_(worker_waiter),
        leave_on_steal_(leave_on_steal), function_(function), invoke_(invoke) {}

  void Run(size_t begin, size_t end) noexcept;
  void Finish() noexcept;
  void Fail(const std::exception_ptr &exception) noexcept {
    std::lock_guard<std::mutex> lock(exception_mutex_);
    if (!exception_) {
      exception_ = exception;
      cancelled_.store(true, std::memory_order_release);
    }
  }
  bool IsComplete() const noexcept {
    return complete_.load(std::memory_order_acquire);
  }
  void Rethrow() {
    if (exception_) {
      std::rethrow_exception(exception_);
    }
  }
  bool LeaveOnSteal() const noexcept { return leave_on_steal_; }

private:
  RapidDomainState &state_;
  bool worker_waiter_;
  bool leave_on_steal_;
  void *function_;
  Invoke invoke_;
  std::atomic<bool> complete_{false};
  std::atomic<bool> cancelled_{false};
  std::mutex exception_mutex_;
  std::exception_ptr exception_;
};

class alignas(128) Activation final : public RapidTask {
public:
  enum class State : unsigned char { Free, Pending, Running, Complete };

  void Initialize(RapidDomainState &owner, RapidRegion &region,
                  Activation *parent, RegionContext *parent_context,
                  DomainId domain, size_t begin, size_t end) noexcept;
  void AddTickets(size_t count) noexcept final {
    tickets_.fetch_add(count, std::memory_order_relaxed);
  }
  bool TryRun() noexcept final;
  void Cancel() noexcept final;
  void ReleaseTicket() noexcept final;
  RegionContext *Context() noexcept final { return &context_; }
  Task *FallbackTicket() noexcept final { return &fallback_; }
  void ChildComplete() noexcept;

private:
  friend class RapidDomainState;
  void Complete() noexcept;

  RapidDomainState *owner_ = nullptr;
  RapidRegion *region_ = nullptr;
  Activation *parent_ = nullptr;
  RegionContext context_;
  size_t begin_ = 0;
  size_t end_ = 0;
  std::atomic<size_t> tickets_{0};
  std::atomic<unsigned> children_{0};
  uint32_t slot_ = 0;
  std::atomic<uint32_t> next_free_{0};
  std::atomic<State> state_{State::Free};
  RapidFallbackTask fallback_;
};

class RapidDomainState {
public:
  explicit RapidDomainState(ThreadPool &pool, size_t slots_per_worker = 128)
      : pool_(pool),
        activations_(ActivationCapacity(pool.NumThreads(), slots_per_worker)),
        leases_(pool.NumThreads() * 2 - 1) {
    for (size_t i = 0; i < activations_.size(); ++i) {
      activations_[i].slot_ = static_cast<uint32_t>(i);
      activations_[i].next_free_.store(static_cast<uint32_t>(i + 1),
                                       std::memory_order_relaxed);
    }
    activations_.back().next_free_.store(kEmpty, std::memory_order_relaxed);
    size_t cursor = 0;
    BuildTopology({0, static_cast<unsigned>(pool.NumThreads())}, cursor);
  }

  ThreadPool &Pool() noexcept { return pool_; }
  Activation *Acquire();
  std::pair<Activation *, Activation *> TryAcquirePair() noexcept;
  void Release(Activation &activation) noexcept;
  void Execute(Activation &activation) noexcept;
  void Publish(Activation &activation) {
    pool_.ScheduleRapid(&activation, activation.context_.domain.start);
  }
  void BeginRegion() noexcept { pool_.NotifyRapidRegionStart(); }
  SubtreeLease TryLeaseSubtree(DomainId domain) noexcept;

private:
  struct LeaseRecord {
    DomainId domain;
    std::atomic<uint64_t> generation{0};
  };

  static constexpr uint32_t kEmpty =
      std::numeric_limits<uint32_t>::max();
  static size_t ActivationCapacity(size_t workers, size_t slots_per_worker) {
    if (slots_per_worker != 0 &&
        workers > static_cast<size_t>(kEmpty) / slots_per_worker) {
      throw std::length_error("rapid activation capacity exceeds index range");
    }
    return std::max<size_t>(2, workers * slots_per_worker);
  }
  static constexpr uint64_t EncodeHead(uint32_t index,
                                       uint32_t stamp) noexcept {
    return static_cast<uint64_t>(stamp) << 32 | index;
  }
  static constexpr uint32_t HeadIndex(uint64_t head) noexcept {
    return static_cast<uint32_t>(head);
  }
  static constexpr uint32_t HeadStamp(uint64_t head) noexcept {
    return static_cast<uint32_t>(head >> 32);
  }
  size_t BuildTopology(DomainId domain, size_t &cursor) noexcept {
    const size_t current = cursor++;
    leases_[current].domain = domain;
    if (domain.Size() > 1) {
      const unsigned middle =
          domain.start + static_cast<unsigned>(domain.Size() / 2);
      BuildTopology({domain.start, middle}, cursor);
      BuildTopology({middle, domain.limit}, cursor);
    }
    return current;
  }
  void ReleaseLease(size_t index, uint64_t generation) noexcept {
    leases_[index].generation.compare_exchange_strong(
        generation, 0, std::memory_order_release, std::memory_order_relaxed);
  }
  friend class SubtreeLease;
  ThreadPool &pool_;
  std::vector<Activation> activations_;
  std::vector<LeaseRecord> leases_;
  std::atomic<uint64_t> free_head_{EncodeHead(0, 0)};
  std::atomic<uint64_t> next_lease_generation_{1};
};

class SubtreeLease {
public:
  SubtreeLease() = default;
  SubtreeLease(const SubtreeLease &) = delete;
  SubtreeLease &operator=(const SubtreeLease &) = delete;
  SubtreeLease(SubtreeLease &&other) noexcept
      : owner_(std::exchange(other.owner_, nullptr)), index_(other.index_),
        generation_(other.generation_), domain_(other.domain_) {}
  SubtreeLease &operator=(SubtreeLease &&other) noexcept {
    if (this != &other) {
      Reset();
      owner_ = std::exchange(other.owner_, nullptr);
      index_ = other.index_;
      generation_ = other.generation_;
      domain_ = other.domain_;
    }
    return *this;
  }
  ~SubtreeLease() { Reset(); }

  explicit operator bool() const noexcept { return owner_ != nullptr; }
  DomainId Domain() const noexcept { return domain_; }
  uint64_t Generation() const noexcept { return generation_; }
  void Reset() noexcept {
    if (owner_) {
      owner_->ReleaseLease(index_, generation_);
      owner_ = nullptr;
    }
  }

private:
  friend class RapidDomainState;
  SubtreeLease(RapidDomainState &owner, size_t index, uint64_t generation,
               DomainId domain) noexcept
      : owner_(&owner), index_(index), generation_(generation),
        domain_(domain) {}
  RapidDomainState *owner_ = nullptr;
  size_t index_ = 0;
  uint64_t generation_ = 0;
  DomainId domain_;
};

inline SubtreeLease
RapidDomainState::TryLeaseSubtree(DomainId domain) noexcept {
  for (size_t index = 0; index < leases_.size(); ++index) {
    if (leases_[index].domain.start != domain.start ||
        leases_[index].domain.limit != domain.limit) {
      continue;
    }
    uint64_t empty = 0;
    const uint64_t generation =
        next_lease_generation_.fetch_add(1, std::memory_order_relaxed);
    if (leases_[index].generation.compare_exchange_strong(
            empty, generation, std::memory_order_acq_rel,
            std::memory_order_relaxed)) {
      return SubtreeLease(*this, index, generation, domain);
    }
    return {};
  }
  return {};
}

struct RapidStartGroup {
  RapidDomainState *state = nullptr;
  DomainId domain;

  bool IsEmpty() const noexcept {
    return state == nullptr || domain.IsEmpty();
  }

  void Validate() const {
    if (state && !domain.IsValidFor(state->Pool().NumThreads())) {
      throw std::invalid_argument("rapid domain is outside its thread pool");
    }
  }

  RapidStartGroup Subgroup(DomainId child) const {
    Validate();
    if (!state || !child.IsValidFor(state->Pool().NumThreads()) ||
        child.start < domain.start || child.limit > domain.limit) {
      throw std::invalid_argument("rapid subgroup is outside its parent");
    }
    return {state, child};
  }
};

inline void RapidRegion::Run(size_t begin, size_t end) noexcept {
  if (cancelled_.load(std::memory_order_acquire)) {
    return;
  }
  try {
    invoke_(function_, begin, end);
  } catch (...) {
    Fail(std::current_exception());
  }
}

inline void RapidRegion::Finish() noexcept {
  ThreadPool *pool = &state_.Pool();
  const bool worker_waiter = worker_waiter_;
  // Publishing completion may let the waiter destroy this stack region.
  complete_.store(true, std::memory_order_release);
  pool->NotifyTaskCompletion(worker_waiter);
}

inline void Activation::Initialize(RapidDomainState &owner, RapidRegion &region,
                                   Activation *parent,
                                   RegionContext *parent_context,
                                   DomainId domain, size_t begin,
                                   size_t end) noexcept {
  owner_ = &owner;
  region_ = &region;
  parent_ = parent;
  context_ = {domain, parent_context, region.LeaveOnSteal(), &owner};
  begin_ = begin;
  end_ = end;
  // The initial ticket keeps the descriptor alive until completion.
  tickets_.store(1, std::memory_order_relaxed);
  children_.store(0, std::memory_order_relaxed);
  state_.store(State::Pending, std::memory_order_release);
}

inline Activation *RapidDomainState::Acquire() {
  uint64_t head = free_head_.load(std::memory_order_acquire);
  while (true) {
    const uint32_t index = HeadIndex(head);
    if (index == kEmpty) {
      if (pool_.IsCancelled()) {
        return nullptr;
      }
      if (!pool_.TryExecuteSomething()) {
        std::this_thread::yield();
      }
      head = free_head_.load(std::memory_order_acquire);
      continue;
    }
    Activation &activation = activations_[index];
    const uint32_t next = activation.next_free_.load(std::memory_order_relaxed);
    const uint64_t replacement = EncodeHead(next, HeadStamp(head) + 1);
    if (free_head_.compare_exchange_weak(head, replacement,
                                         std::memory_order_acq_rel,
                                         std::memory_order_acquire)) {
      return &activation;
    }
  }
}

inline std::pair<Activation *, Activation *>
RapidDomainState::TryAcquirePair() noexcept {
  uint64_t head = free_head_.load(std::memory_order_acquire);
  while (true) {
    const uint32_t first_index = HeadIndex(head);
    if (first_index == kEmpty) {
      return {};
    }
    Activation &first = activations_[first_index];
    const uint32_t second_index =
        first.next_free_.load(std::memory_order_relaxed);
    if (second_index == kEmpty) {
      return {};
    }
    Activation &second = activations_[second_index];
    const uint32_t next = second.next_free_.load(std::memory_order_relaxed);
    const uint64_t replacement = EncodeHead(next, HeadStamp(head) + 1);
    if (free_head_.compare_exchange_weak(head, replacement,
                                         std::memory_order_acq_rel,
                                         std::memory_order_acquire)) {
      return {&first, &second};
    }
  }
}

inline void RapidDomainState::Release(Activation &activation) noexcept {
  uint64_t head = free_head_.load(std::memory_order_relaxed);
  do {
    activation.next_free_.store(HeadIndex(head), std::memory_order_relaxed);
  } while (!free_head_.compare_exchange_weak(
      head, EncodeHead(activation.slot_, HeadStamp(head) + 1),
      std::memory_order_release, std::memory_order_relaxed));
}

inline bool Activation::TryRun() noexcept {
  State expected = State::Pending;
  if (!state_.compare_exchange_strong(expected, State::Running,
                                      std::memory_order_acq_rel,
                                      std::memory_order_acquire)) {
    return false;
  }
  owner_->Pool().ExecuteInRegion(
      &context_, [&]() noexcept { owner_->Execute(*this); });
  return true;
}

inline void Activation::ReleaseTicket() noexcept {
  const size_t previous = tickets_.fetch_sub(1, std::memory_order_acq_rel);
  assert(previous > 0);
  if (previous == 1) {
    assert(state_.load(std::memory_order_acquire) == State::Complete);
    RapidDomainState *owner = owner_;
    Activation *parent = parent_;
    RapidRegion *region = region_;
    state_.store(State::Free, std::memory_order_release);
    owner->Release(*this);
    if (parent) {
      parent->ChildComplete();
    } else {
      region->Finish();
    }
  }
}

inline void Activation::Cancel() noexcept {
  State expected = State::Pending;
  if (state_.compare_exchange_strong(expected, State::Running,
                                     std::memory_order_acq_rel,
                                     std::memory_order_acquire)) {
    Complete();
  }
}

inline void Activation::ChildComplete() noexcept {
  if (children_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    Complete();
  }
}

inline void Activation::Complete() noexcept {
  state_.store(State::Complete, std::memory_order_release);
  ReleaseTicket();
}

inline void RapidDomainState::Execute(Activation &activation) noexcept {
  const size_t work = activation.end_ - activation.begin_;
  const size_t workers = activation.context_.domain.Size();
  if (work <= 1 || workers <= 1) {
    activation.region_->Run(activation.begin_, activation.end_);
    activation.Complete();
    return;
  }

  const size_t left_workers = workers / 2;
  const size_t whole_work = (work / workers) * left_workers;
  const size_t remainder_work =
      ((work % workers) * left_workers + workers / 2) / workers;
  size_t left_work = whole_work + remainder_work;
  left_work = std::clamp(left_work, size_t{1}, work - 1);
  const unsigned middle_worker =
      activation.context_.domain.start + static_cast<unsigned>(left_workers);
  const size_t middle_work = activation.begin_ + left_work;

  auto [left, right] = TryAcquirePair();
  if (!left) {
    activation.region_->Run(activation.begin_, activation.end_);
    activation.Complete();
    return;
  }
  activation.children_.store(2, std::memory_order_relaxed);
  left->Initialize(*this, *activation.region_, &activation,
                   &activation.context_,
                   {activation.context_.domain.start, middle_worker},
                   activation.begin_, middle_work);
  right->Initialize(*this, *activation.region_, &activation,
                    &activation.context_,
                    {middle_worker, activation.context_.domain.limit},
                    middle_work, activation.end_);
  try {
    Publish(*right);
  } catch (...) {
    activation.region_->Fail(std::current_exception());
    // Failed publication has already canceled and released the right child.
    left->Cancel();
    return;
  }
  left->AddTickets(1);
  left->TryRun();
  left->ReleaseTicket();
}

inline RegionContext *CompatibleParentContext(ThreadPool &pool,
                                              RapidDomainState &state) noexcept {
  RegionContext *parent = pool.CurrentRegionContext();
  if (parent && parent->rapid_state && parent->rapid_state != &state) {
    return nullptr;
  }
  return parent;
}

template <typename F>
void ParallelForRanges(RapidStartGroup group, size_t begin, size_t end,
                       F &&function, bool leave_on_steal = false) {
  if (group.IsEmpty() || begin >= end) {
    return;
  }
  group.Validate();
  using Function = std::remove_reference_t<F>;
  Function *function_ptr = std::addressof(function);
  const auto invoke = [](void *opaque, size_t first, size_t last) {
    Function &callable = *static_cast<Function *>(opaque);
    std::invoke(callable, first, last);
  };
  ThreadPool &pool = group.state->Pool();
  RegionContext *parent = CompatibleParentContext(pool, *group.state);
  DomainId domain = parent ? parent->domain : group.domain;
  if (domain.Size() == 1) {
    if (!pool.IsCancelled()) {
      std::invoke(function, begin, end);
    }
    return;
  }
  RapidRegion region(*group.state,
                     pool.CurrentThreadId() < pool.NumThreads(),
                     leave_on_steal, function_ptr, invoke);
  group.state->BeginRegion();
  Activation *root = group.state->Acquire();
  if (!root) {
    return;
  }
  root->Initialize(*group.state, region, nullptr, parent, domain, begin, end);
  root->AddTickets(1);
  root->TryRun();
  root->ReleaseTicket();
  pool.HelpUntil(region);
  region.Rethrow();
}

class OrdinaryRegion {
public:
  explicit OrdinaryRegion(ThreadPool &pool)
      : pool_(pool), worker_waiter_(pool.CurrentThreadId() < pool.NumThreads()) {
  }

  bool IsComplete() const noexcept {
    return complete_.load(std::memory_order_acquire);
  }
  bool IsCancelled() const noexcept {
    return cancelled_.load(std::memory_order_acquire) || pool_.IsCancelled();
  }
  void AddTask() noexcept { remaining_.fetch_add(1, std::memory_order_relaxed); }
  void TaskComplete() noexcept {
    if (remaining_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      complete_.store(true, std::memory_order_release);
      pool_.NotifyTaskCompletion(worker_waiter_);
    }
  }
  void Fail(const std::exception_ptr &exception) noexcept {
    std::lock_guard<std::mutex> lock(exception_mutex_);
    if (!exception_) {
      exception_ = exception;
      cancelled_.store(true, std::memory_order_release);
    }
  }
  void Rethrow() {
    if (exception_) {
      std::rethrow_exception(exception_);
    }
  }

private:
  ThreadPool &pool_;
  bool worker_waiter_;
  std::atomic<size_t> remaining_{1};
  std::atomic<bool> complete_{false};
  std::atomic<bool> cancelled_{false};
  std::mutex exception_mutex_;
  std::exception_ptr exception_;
};

template <typename F>
void RunOrdinaryRange(OrdinaryRegion &region, F &function, size_t begin,
                      size_t end) noexcept {
  try {
    for (size_t index = begin; index < end && !region.IsCancelled(); ++index) {
      std::invoke(function, index);
    }
  } catch (...) {
    region.Fail(std::current_exception());
  }
}

struct MailboxDomainContext {
  RapidDomainState *state = nullptr;
  DomainId domain;
};

inline thread_local MailboxDomainContext current_mailbox_context;

class ScopedMailboxDomainContext {
public:
  explicit ScopedMailboxDomainContext(MailboxDomainContext replacement)
      : previous_(std::exchange(current_mailbox_context, replacement)) {}
  ScopedMailboxDomainContext(const ScopedMailboxDomainContext &) = delete;
  ScopedMailboxDomainContext &
  operator=(const ScopedMailboxDomainContext &) = delete;
  ~ScopedMailboxDomainContext() { current_mailbox_context = previous_; }

private:
  MailboxDomainContext previous_;
};

template <typename F> class RangeTask final : public Task {
public:
  RangeTask(OrdinaryRegion &region, RapidDomainState &state, F &function,
            DomainId domain, size_t begin, size_t end)
      : region_(region), state_(state), function_(function), domain_(domain),
        begin_(begin), end_(end) {}

  void operator()() final {
    ScopedMailboxDomainContext context({&state_, domain_});
    RunOrdinaryRange(region_, function_, begin_, end_);
    region_.TaskComplete();
    delete this;
  }
  void Discard() noexcept final {
    region_.TaskComplete();
    delete this;
  }

private:
  OrdinaryRegion &region_;
  RapidDomainState &state_;
  F &function_;
  DomainId domain_;
  size_t begin_;
  size_t end_;
};

// Lazy-stealing range coordinator.
//
// One proportional range per slot. The owner claims blocks from its own range
// first; a worker that exhausts its range leaves the Rapid domain once and then
// claims blocks from peer ranges. Three orthogonal policies are compile-time
// parameters so their costs can be measured independently:
//
//   Law      how the owner sizes its next block (see rapid_start_model.h);
//   Victims  which peer range an idle worker probes first;
//   profile  optional cross-call state that warms the first block from the
//            previous invocation of the same loop site.
//
// Only the owner writes a range's statistics and adaptive block; thieves read
// the last published block and claim atomically, so migration and contention
// never feed back into the estimate.
template <typename F, GrainLaw Law, VictimPolicy Victims>
class LazyRangeCoordinator {
public:
  static constexpr bool kTimed = IsTimedLaw(Law);
  static constexpr bool kItem = IsItemLaw(Law);

  LazyRangeCoordinator(ThreadPool &pool, F &function, size_t begin, size_t end,
                       size_t slots, size_t grain, size_t target_block_ns = 0,
                       LoopProfile *profile = nullptr)
      : pool_(pool), function_(function), slots_(slots),
        ranges_(new Range[slots]), grain_(grain),
        target_block_ns_(target_block_ns), profile_(profile) {
    const size_t work = end - begin;
    block_ = HybridBlockSize(work, slots, grain);
    if constexpr (kTimed) {
      scheduling_overhead_ns_ = TimespanDomainSchedulingOverheadNs(
          CalibratedTimespanSchedulingOverheadNs(), slots_);
      if (profile_ && profile_->IsWarm()) {
        // Warm start: size the first block from the previous call's per-item
        // time instead of the structural step function, then keep at least
        // four later steal opportunities per range.
        const size_t item_ns = profile_->item_ns.load(std::memory_order_relaxed);
        const size_t range_work = model_detail::CeilDivide(work, slots);
        const size_t target =
            TargetNanoseconds(1, item_ns, range_work, profile_->Cv(), 0);
        size_t warm = ItemsForDuration(target, item_ns, grain_, range_work);
        warm = std::min(warm, std::max(grain_, model_detail::CeilDivide(
                                                   range_work, size_t{4})));
        block_ = std::max(warm, grain_);
      }
    }
    const size_t quotient = work / slots;
    const size_t remainder = work % slots;
    size_t cursor = begin;
    for (size_t slot = 0; slot < slots; ++slot) {
      ranges_[slot].next.store(cursor, std::memory_order_relaxed);
      ranges_[slot].adaptive_block.store(block_, std::memory_order_relaxed);
      const size_t range_begin = cursor;
      cursor += quotient + (slot < remainder);
      ranges_[slot].end = cursor;
      ranges_[slot].work = cursor - range_begin;
    }
  }

  void ReserveFirstBlock(size_t own_slot) noexcept {
    Range &range = ranges_[own_slot];
    const size_t block = Block(range);
    range.first = range.next.fetch_add(block, std::memory_order_relaxed);
    range.first_block = block;
  }

  void Run(size_t own_slot) noexcept {
    RunOwnedRange(own_slot);
    if constexpr (kTimed) {
      steal_pressure_.fetch_add(1, std::memory_order_relaxed);
    }
    RunPeerRanges(own_slot);
    if constexpr (kTimed) {
      steal_pressure_.fetch_sub(1, std::memory_order_relaxed);
    }
  }

  void Rethrow() {
    if (exception_) {
      std::rethrow_exception(exception_);
    }
  }

  // Aggregate owner-side statistics into the loop profile. Call after all
  // slots have completed; safe to call when no profile was supplied.
  void PublishProfile() noexcept {
    if (!profile_ || IsCancelled()) {
      return;
    }
    if constexpr (kTimed) {
      long double weighted_item_ns = 0.0L;
      long double weighted_cv = 0.0L;
      long double weight = 0.0L;
      size_t block_sum = 0;
      size_t block_count = 0;
      for (size_t slot = 0; slot < slots_; ++slot) {
        const Range &range = ranges_[slot];
        if (range.stats.count == 0) {
          continue;
        }
        const long double w = static_cast<long double>(range.stats.count);
        weighted_item_ns += range.stats.mean * w;
        weighted_cv += range.stats.ItemCv() * w;
        weight += w;
        block_sum += range.adaptive_block.load(std::memory_order_relaxed);
        ++block_count;
      }
      if (weight > 0.0L) {
        profile_->Record(
            model_detail::RoundNonnegativeToSize(weighted_item_ns / weight),
            weighted_cv / weight,
            block_count ? block_sum / block_count : block_);
      }
    }
  }

  // Diagnostics for tests and tracing.
  size_t InitialBlock() const noexcept { return block_; }
  size_t StealPressure() const noexcept {
    return steal_pressure_.load(std::memory_order_relaxed);
  }

private:
  struct alignas(OOX_EIGEN_CACHE_LINE_SIZE) Range {
    std::atomic<size_t> next{0};
    std::atomic<size_t> adaptive_block{1};
    size_t first = 0;
    size_t first_block = 0;
    size_t end = 0;
    size_t work = 0;
    RunningStats stats; // owner-only
  };

  void RunOwnedRange(size_t own_slot) noexcept {
    Range &range = ranges_[own_slot];
    if constexpr (kTimed) {
      RunTimedBlock(range, range.first, range.first_block);
    } else {
      RunClaimedBlock(range, range.first, range.first_block);
    }
    while (!IsCancelled() && RunBlock(own_slot, true)) {
    }
  }

  void RunPeerRanges(size_t own_slot) noexcept {
    bool in_rapid_domain = true;
    while (!IsCancelled() && HasUnclaimedWork()) {
      if (in_rapid_domain) {
        RegionContext *context = pool_.CurrentRegionContext();
        bool executed = false;
        try {
          executed = pool_.TryExecuteSomething();
        } catch (...) {
          Fail(std::current_exception());
          return;
        }
        if (pool_.CurrentRegionContext() != context) {
          in_rapid_domain = false;
        }
        if (executed) {
          continue;
        }
      }
      in_rapid_domain = false;
      if (!StealOnePeerBlock(own_slot)) {
        break;
      }
    }
  }

  bool StealOnePeerBlock(size_t own_slot) noexcept {
    if constexpr (Victims == VictimPolicy::MostRemaining) {
      size_t best = slots_;
      size_t best_remaining = 0;
      for (size_t slot = 0; slot < slots_; ++slot) {
        if (slot == own_slot) {
          continue;
        }
        const size_t next = ranges_[slot].next.load(std::memory_order_relaxed);
        const size_t remaining =
            next < ranges_[slot].end ? ranges_[slot].end - next : 0;
        if (remaining > best_remaining) {
          best_remaining = remaining;
          best = slot;
        }
      }
      if (best != slots_ && RunBlock(best, false)) {
        return true;
      }
      // The chosen victim was drained between the scan and the claim; make one
      // ordinary pass so progress does not depend on a stale snapshot.
      for (size_t k = 1; k < slots_ && !IsCancelled(); ++k) {
        if (RunBlock(VictimCandidate(VictimPolicy::Linear, own_slot, k, slots_),
                     false)) {
          return true;
        }
      }
      return false;
    } else {
      for (size_t k = 1; k < slots_ && !IsCancelled(); ++k) {
        const size_t slot = VictimCandidate(Victims, own_slot, k, slots_);
        if (RunBlock(slot, false)) {
          return true;
        }
      }
      return false;
    }
  }

  size_t Block(Range &range) const noexcept {
    if constexpr (kTimed) {
      return range.adaptive_block.load(std::memory_order_relaxed);
    } else if constexpr (kItem) {
      const size_t next = range.next.load(std::memory_order_relaxed);
      const size_t remaining = next < range.end ? range.end - next : 0;
      if constexpr (Law == GrainLaw::Factoring) {
        return FactoringChunk(remaining, slots_, grain_);
      } else {
        return GuidedChunk(remaining, slots_, grain_);
      }
    } else {
      return block_;
    }
  }

  size_t TargetNanoseconds(size_t completed, size_t elapsed_ns,
                           size_t range_work, long double item_cv,
                           size_t stealing) const noexcept {
    if (target_block_ns_ != 0) {
      return target_block_ns_;
    }
    if constexpr (Law == GrainLaw::Heartbeat) {
      return HeartbeatTargetNanoseconds(scheduling_overhead_ns_);
    } else if constexpr (Law == GrainLaw::SqrtCv) {
      return SqrtCvTargetNanoseconds(scheduling_overhead_ns_, completed,
                                     elapsed_ns, range_work, stealing, slots_,
                                     item_cv);
    } else if constexpr (Law == GrainLaw::FixedSizeChunk) {
      const long double item_ns =
          completed ? static_cast<long double>(elapsed_ns) /
                          static_cast<long double>(completed)
                    : 0.0L;
      const size_t items = FixedSizeChunkItems(
          range_work, scheduling_overhead_ns_, item_cv * item_ns, slots_);
      if (items != 0 && item_ns > 0.0L) {
        return std::max<size_t>(
            model_detail::RoundNonnegativeToSize(
                static_cast<long double>(items) * item_ns),
            scheduling_overhead_ns_);
      }
      // sigma unknown yet (first sample) or P == 1: fall back to the sqrt law.
      return TimespanTargetNanoseconds(scheduling_overhead_ns_, completed,
                                       elapsed_ns, range_work, stealing,
                                       slots_);
    } else {
      return TimespanTargetNanoseconds(scheduling_overhead_ns_, completed,
                                       elapsed_ns, range_work, stealing,
                                       slots_);
    }
  }

  bool IsCancelled() const noexcept {
    return cancelled_.load(std::memory_order_acquire) || pool_.IsCancelled();
  }
  bool HasUnclaimedWork() const noexcept {
    for (size_t slot = 0; slot < slots_; ++slot) {
      if (ranges_[slot].next.load(std::memory_order_relaxed) <
          ranges_[slot].end) {
        return true;
      }
    }
    return false;
  }
  bool RunBlock(size_t slot, bool calibrate) noexcept {
    Range &range = ranges_[slot];
    const size_t block = Block(range);
    const size_t first = range.next.fetch_add(block, std::memory_order_relaxed);
    if constexpr (kTimed) {
      if (calibrate) {
        return RunTimedBlock(range, first, block);
      }
    }
    return RunClaimedBlock(range, first, block);
  }
  bool RunTimedBlock(Range &range, size_t first, size_t block) noexcept {
    if (first >= range.end) {
      return false;
    }
    const size_t completed = std::min(block, range.end - first);
    const auto origin = std::chrono::steady_clock::now();
    const bool ran = RunClaimedBlock(range, first, block);
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::steady_clock::now() - origin)
                             .count();
    if (ran && !IsCancelled()) {
      const size_t cursor = range.next.load(std::memory_order_relaxed);
      const size_t remaining = cursor < range.end ? range.end - cursor : 0;
      const size_t elapsed_ns = static_cast<size_t>(
          std::max<decltype(elapsed)>(elapsed, 1));
      range.stats.Add(static_cast<long double>(elapsed_ns) /
                          static_cast<long double>(completed),
                      static_cast<long double>(completed));
      const size_t target_ns = TargetNanoseconds(
          completed, elapsed_ns, range.work, range.stats.ItemCv(),
          steal_pressure_.load(std::memory_order_relaxed));
      size_t next = TimespanBlockSize(block, completed, elapsed_ns, remaining,
                                      grain_, target_ns);
      if (range.stats.count > 1) {
        next = next > block ? block + (next - block) / 4
                            : block - (block - next) / 4;
      }
      range.adaptive_block.store(next, std::memory_order_relaxed);
    }
    return ran;
  }
  bool RunClaimedBlock(Range &range, size_t first, size_t block) noexcept {
    if (first >= range.end) {
      return false;
    }
    const size_t last = first + std::min(block, range.end - first);
    try {
      for (size_t index = first; index < last && !IsCancelled(); ++index) {
        std::invoke(function_, index);
      }
    } catch (...) {
      Fail(std::current_exception());
    }
    return true;
  }

  void Fail(const std::exception_ptr &exception) noexcept {
    std::lock_guard<std::mutex> lock(exception_mutex_);
    if (!exception_) {
      exception_ = exception;
      cancelled_.store(true, std::memory_order_release);
    }
  }

  ThreadPool &pool_;
  F &function_;
  size_t slots_;
  std::unique_ptr<Range[]> ranges_;
  size_t block_ = 1;
  size_t grain_;
  size_t target_block_ns_;
  LoopProfile *profile_;
  size_t scheduling_overhead_ns_ = 0;
  std::atomic<size_t> steal_pressure_{0};
  std::atomic<bool> cancelled_{false};
  std::mutex exception_mutex_;
  std::exception_ptr exception_;
};

template <typename F>
void ParallelFor(RapidStartGroup group, size_t begin, size_t end,
                 F &&function) {
  auto ranges = [&](size_t first, size_t last) {
    for (size_t i = first;
         i < last && !group.state->Pool().IsCancelled(); ++i) {
      std::invoke(function, i);
    }
  };
  ParallelForRanges(group, begin, end, ranges);
}

inline thread_local RapidDomainState *current_resident_state = nullptr;

class ScopedResidentState {
public:
  explicit ScopedResidentState(RapidDomainState &state) noexcept
      : previous_(std::exchange(current_resident_state, &state)) {}
  ~ScopedResidentState() { current_resident_state = previous_; }

private:
  RapidDomainState *previous_;
};

template <typename F> class ResidentRegion final : public ResidentTask {
public:
  ResidentRegion(RapidDomainState &state, F &function, size_t begin, size_t end,
                 size_t slots, size_t helpers) noexcept
      : state_(state), function_(function), begin_(begin), end_(end),
        slots_(slots), remaining_(helpers) {}

  void Run(size_t slot) noexcept final {
    RunSlot(slot);
  }

  void RunCaller() noexcept { RunSlot(0); }

  std::atomic<size_t> &CompletionCounter() noexcept { return remaining_; }

  bool IsComplete() const noexcept {
    return remaining_.load(std::memory_order_acquire) == 0;
  }

  void Rethrow() {
    std::lock_guard<std::mutex> lock(exception_mutex_);
    if (exception_) {
      std::rethrow_exception(exception_);
    }
  }

private:
  void RunSlot(size_t slot) noexcept {
    ScopedResidentState context(state_);
    const size_t work = end_ - begin_;
    const size_t quotient = work / slots_;
    const size_t remainder = work % slots_;
    const size_t first =
        begin_ + slot * quotient + std::min(slot, remainder);
    const size_t last = first + quotient + (slot < remainder ? 1 : 0);
    try {
      for (size_t index = first;
           index < last && !state_.Pool().IsCancelled(); ++index) {
        if (((index - first) & 63) == 0 &&
            cancelled_.load(std::memory_order_acquire)) {
          break;
        }
        std::invoke(function_, index);
      }
    } catch (...) {
      std::lock_guard<std::mutex> lock(exception_mutex_);
      if (!exception_) {
        exception_ = std::current_exception();
        cancelled_.store(true, std::memory_order_release);
      }
    }
  }

  RapidDomainState &state_;
  F &function_;
  size_t begin_;
  size_t end_;
  size_t slots_;
  std::atomic<size_t> remaining_;
  std::atomic<bool> cancelled_{false};
  std::mutex exception_mutex_;
  std::exception_ptr exception_;
};

inline void PrepareResidentGroup(RapidStartGroup group) {
  group.Validate();
  ThreadPool &pool = group.state->Pool();
  if (!pool.UsesResidentBusyWait()) {
    throw std::invalid_argument("resident Rapid requires a resident-busy pool");
  }
  const size_t current = pool.CurrentThreadId();
  const size_t expected =
      group.domain.Size() - (current < pool.NumThreads() &&
                                     group.domain.Contains(current)
                                 ? 1
                                 : 0);
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (pool.ResidentAvailableWorkers(group.domain) < expected &&
         !pool.IsCancelled()) {
    if (std::chrono::steady_clock::now() >= deadline) {
      throw std::runtime_error("resident Rapid workers did not become ready");
    }
    std::this_thread::yield();
  }
}

template <typename F>
void ParallelForResident(RapidStartGroup group, size_t begin, size_t end,
                         F &&function) {
  if (group.IsEmpty() || begin >= end) {
    return;
  }
  group.Validate();
  ThreadPool &pool = group.state->Pool();
  if (!pool.UsesResidentBusyWait()) {
    ParallelFor(group, begin, end, std::forward<F>(function));
    return;
  }
  if (current_resident_state == group.state || group.domain.Size() == 1) {
    for (size_t index = begin; index < end && !pool.IsCancelled(); ++index) {
      std::invoke(function, index);
    }
    return;
  }
  constexpr size_t kMaximumParticipants = 64;
  // The pool may contain arbitrarily many 64-bit availability words. A single
  // invocation deliberately keeps one compact completion cohort.
  const size_t slots = std::min(
      {end - begin, group.domain.Size(), kMaximumParticipants});
  std::array<unsigned, kMaximumParticipants - 1> workers{};
  const size_t helpers = pool.ClaimResidentWorkers(
      group.domain, workers.data(), slots - 1);
  using Function = std::remove_reference_t<F>;
  Function &callable = function;
  ResidentRegion<Function> region(*group.state, callable, begin, end,
                                  helpers + 1, helpers);
  for (size_t helper = 0; helper < helpers; ++helper) {
    pool.PublishResident(region, region.CompletionCounter(), workers[helper],
                         helper + 1);
  }
  region.RunCaller();
  while (!region.IsComplete()) {
    if (!pool.TryExecuteSomething()) {
      std::this_thread::yield();
    }
  }
  region.Rethrow();
}

template <typename F>
void ParallelForMailbox(RapidStartGroup group, size_t begin, size_t end,
                        F &&function, size_t grain = 1) {
  if (group.IsEmpty() || begin >= end) {
    return;
  }
  group.Validate();
  ThreadPool &pool = group.state->Pool();
  const DomainId domain = current_mailbox_context.state == group.state
                              ? current_mailbox_context.domain
                              : group.domain;
  if (domain.Size() == 1) {
    for (size_t index = begin; index < end && !pool.IsCancelled(); ++index) {
      std::invoke(function, index);
    }
    return;
  }
  group.domain = domain;
  using Function = std::remove_reference_t<F>;
  Function &callable = function;
  OrdinaryRegion ordinary(pool);
  const size_t work = end - begin;
  const size_t work_per_worker =
      model_detail::CeilDivide(work, group.domain.Size());
  // Halve publication density through 512 iterations per worker. Preserve
  // finer tasks beyond that point so irregular work remains stealable.
  const size_t task_density_divisor = work_per_worker <= 512 ? 2 : 1;
  const size_t block =
      HybridBlockSize(end - begin, group.domain.Size(),
                      std::max<size_t>(grain, 1), task_density_divisor);
  try {
    pool.PublishOrdinaryBatch([&](auto &&publish) {
      ParallelForRanges(group, begin, end, [&](size_t first, size_t last) {
        RegionContext *context = pool.CurrentRegionContext();
        const DomainId domain = context ? context->domain : group.domain;
        const size_t target = domain.start;
        while (first < last) {
          const size_t task_end = first + std::min(block, last - first);
          auto *task = new RangeTask<Function>(
              ordinary, *group.state, callable, domain, first, task_end);
          ordinary.AddTask();
          try {
            publish(task, target);
          } catch (...) {
            task->Discard();
            throw;
          }
          first = task_end;
        }
      });
    });
  } catch (...) {
    ordinary.Fail(std::current_exception());
  }
  pool.FinishOrdinaryBatch();
  ordinary.TaskComplete();
  pool.HelpUntil(ordinary);
  ordinary.Rethrow();
}

template <GrainLaw Law, VictimPolicy Victims, typename F>
void ParallelForLazyStealingPolicy(RapidStartGroup group, size_t begin,
                                   size_t end, F &&function, size_t grain = 1,
                                   size_t target_block_ns = 0,
                                   LoopProfile *profile = nullptr) {
  if (group.IsEmpty() || begin >= end) {
    return;
  }
  group.Validate();
  using Function = std::remove_reference_t<F>;
  Function &callable = function;
  ThreadPool &pool = group.state->Pool();
  RegionContext *parent = CompatibleParentContext(pool, *group.state);
  const DomainId domain = parent ? parent->domain : group.domain;
  if (domain.Size() == 1) {
    for (size_t index = begin; index < end && !pool.IsCancelled(); ++index) {
      std::invoke(callable, index);
    }
    return;
  }
  group.domain = domain;
  const size_t slots = std::min(group.domain.Size(), end - begin);
  LazyRangeCoordinator<Function, Law, Victims> coordinator(
      pool, callable, begin, end, slots, std::max<size_t>(grain, 1),
      target_block_ns, profile);
  // Protect one block for every proportional owner before work is published.
  // A worker can then steal later blocks without racing a delayed owner.
  for (size_t slot = 0; slot < slots; ++slot) {
    coordinator.ReserveFirstBlock(slot);
  }
  ParallelForRanges(
      group, 0, slots,
      [&](size_t first_slot, size_t last_slot) {
        for (size_t slot = first_slot; slot < last_slot; ++slot) {
          coordinator.Run(slot);
        }
      },
      true);
  coordinator.PublishProfile();
  coordinator.Rethrow();
}

template <typename F>
void ParallelForLazyStealing(RapidStartGroup group, size_t begin, size_t end,
                             F &&function, size_t grain = 1) {
  ParallelForLazyStealingPolicy<GrainLaw::Fixed, VictimPolicy::Linear>(
      group, begin, end, std::forward<F>(function), grain, 0, nullptr);
}

template <typename F>
void ParallelForTimespanLazyStealing(RapidStartGroup group, size_t begin,
                                     size_t end, F &&function,
                                     size_t grain = 1,
                                     size_t target_block_ns = 0,
                                     LoopProfile *profile = nullptr) {
  ParallelForLazyStealingPolicy<GrainLaw::Sqrt, VictimPolicy::Linear>(
      group, begin, end, std::forward<F>(function), grain, target_block_ns,
      profile);
}

} // namespace oox::detail::eigen_pool::rapid
