// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "nonblocking_thread_pool.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
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

namespace oox::detail::eigen_pool {

class RapidRegionBase {
public:
  RapidRegionBase *ParentRegion() const noexcept { return parent_region_; }
  bool IsComplete() const noexcept {
    return complete_.load(std::memory_order_acquire);
  }

protected:
  RapidRegionBase(RapidRegionBase *parent, DomainId domain)
      : parent_region_(parent), domain_(domain) {}

  RapidRegionBase *parent_region_;
  DomainId domain_;
  std::atomic<bool> complete_{false};
};

namespace rapid {

class RapidDomainState;
class SubtreeLease;

class RapidRegion final : public RapidRegionBase {
public:
  using Invoke = void (*)(void *, size_t, size_t);

  RapidRegion(RapidDomainState &state, RegionContext *parent, DomainId domain,
              bool worker_waiter, bool leave_on_steal, void *function,
              Invoke invoke)
      : RapidRegionBase(parent ? parent->region : nullptr, domain),
        state_(state), parent_context_(parent), worker_waiter_(worker_waiter),
        leave_on_steal_(leave_on_steal), function_(function), invoke_(invoke) {}

  void Run(size_t begin, size_t end) noexcept {
    if (cancelled_.load(std::memory_order_acquire)) {
      return;
    }
    try {
      invoke_(function_, begin, end);
    } catch (...) {
      std::lock_guard<std::mutex> lock(exception_mutex_);
      if (!exception_) {
        exception_ = std::current_exception();
        cancelled_.store(true, std::memory_order_release);
      }
    }
  }

  void Finish() noexcept;
  void Rethrow() {
    if (exception_) {
      std::rethrow_exception(exception_);
    }
  }
  RapidDomainState &State() noexcept { return state_; }
  RegionContext *ParentContext() const noexcept { return parent_context_; }
  DomainId Domain() const noexcept { return domain_; }
  bool LeaveOnSteal() const noexcept { return leave_on_steal_; }

private:
  RapidDomainState &state_;
  RegionContext *parent_context_;
  bool worker_waiter_;
  bool leave_on_steal_;
  void *function_;
  Invoke invoke_;
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
  void AddTickets(size_t count) final {
    tickets_.fetch_add(count, std::memory_order_relaxed);
  }
  bool TryRun() final;
  void Cancel() noexcept final;
  void ReleaseTicket() final;
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
        activations_(std::max<size_t>(2, pool.NumThreads() * slots_per_worker)),
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
  std::pair<Activation *, Activation *> TryAcquirePair();
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

  static constexpr uint32_t kEmpty = UINT32_MAX;
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

  RapidStartGroup Subgroup(DomainId child) const {
    if (!state || child.start < domain.start || child.limit > domain.limit ||
        child.start >= child.limit) {
      throw std::invalid_argument("rapid subgroup is outside its parent");
    }
    return {state, child};
  }
};

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
  context_ = {&region, domain, parent_context, region.LeaveOnSteal(), &owner};
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
RapidDomainState::TryAcquirePair() {
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

inline bool Activation::TryRun() {
  State expected = State::Pending;
  if (!state_.compare_exchange_strong(expected, State::Running,
                                      std::memory_order_acq_rel,
                                      std::memory_order_acquire)) {
    return false;
  }
  owner_->Pool().ExecuteInRegion(&context_, [&] { owner_->Execute(*this); });
  return true;
}

inline void Activation::ReleaseTicket() {
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
  size_t left_work = (work * left_workers + workers / 2) / workers;
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
  Publish(*right);
  left->AddTickets(1);
  left->TryRun();
  left->ReleaseTicket();
}

template <typename F>
void ParallelForRanges(RapidStartGroup group, size_t begin, size_t end,
                       F &&function, bool leave_on_steal = false) {
  if (!group.state || group.domain.Size() == 0 || begin >= end) {
    return;
  }
  using Function = std::remove_reference_t<F>;
  Function *function_ptr = std::addressof(function);
  const auto invoke = [](void *opaque, size_t first, size_t last) {
    Function &callable = *static_cast<Function *>(opaque);
    std::invoke(callable, first, last);
  };
  ThreadPool &pool = group.state->Pool();
  RegionContext *parent = pool.CurrentRegionContext();
  if (parent && parent->rapid_state && parent->rapid_state != group.state) {
    parent = nullptr;
  }
  DomainId domain = parent ? parent->domain : group.domain;
  if (domain.Size() == 1) {
    std::invoke(function, begin, end);
    return;
  }
  RapidRegion region(*group.state, parent, domain,
                     pool.CurrentThreadId() < pool.NumThreads(),
                     leave_on_steal, function_ptr, invoke);
  group.state->BeginRegion();
  Activation *root = group.state->Acquire();
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
  void Fail(std::exception_ptr exception) noexcept {
    std::lock_guard<std::mutex> lock(exception_mutex_);
    if (!exception_) {
      exception_ = std::move(exception);
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

inline size_t HybridBlockSize(size_t work, size_t workers, size_t grain,
                              size_t blocks_per_worker_divisor = 1) noexcept {
  assert(blocks_per_worker_divisor != 0);
  const size_t work_per_worker = work / workers + (work % workers != 0);
  // Bound scheduler metadata for short loops, then expose finer blocks only
  // when each worker has enough useful work to amortize them.
  const size_t blocks_per_worker = work_per_worker <= 8      ? 2
                                   : work_per_worker <= 64   ? 8
                                   : work_per_worker <= 4096 ? 32
                                                             : 64;
  const size_t blocks =
      workers *
      std::max<size_t>(blocks_per_worker / blocks_per_worker_divisor, 1);
  const size_t target = work / blocks + (work % blocks != 0);
  return std::max<size_t>({target, grain, 1});
}

inline size_t CalibratedTimespanSchedulingOverheadNs() noexcept {
  static const size_t overhead_ns = [] {
    constexpr size_t samples = 128;
    constexpr size_t rounds = 5;
    std::atomic<size_t> cursor{0};
    std::atomic<size_t> published_block{1};
    size_t best = std::numeric_limits<size_t>::max();
    // Use the best batch average so preemption changes neither the estimate nor
    // every later block decision. This calibration runs once per process.
    for (size_t round = 0; round < rounds; ++round) {
      size_t total = 0;
      for (size_t sample = 0; sample < samples; ++sample) {
        const auto origin = std::chrono::steady_clock::now();
        const size_t block = published_block.load(std::memory_order_relaxed);
        cursor.fetch_add(block, std::memory_order_relaxed);
        cursor.load(std::memory_order_relaxed);
        published_block.store(block, std::memory_order_relaxed);
        const auto elapsed =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - origin)
                .count();
        total += static_cast<size_t>(
            std::max<decltype(elapsed)>(elapsed, 1));
      }
      best = std::min(best, total / samples + (total % samples != 0));
    }
    return std::max<size_t>(best, 1);
  }();
  return overhead_ns;
}

inline size_t TimespanDomainSchedulingOverheadNs(size_t local_overhead_ns,
                                                 size_t workers) noexcept {
  // A stolen block can be inspected by every worker in the domain, so account
  // for the domain-wide cache/coherence opportunity rather than one claim.
  if (local_overhead_ns >
      std::numeric_limits<size_t>::max() / std::max<size_t>(workers, 1)) {
    return std::numeric_limits<size_t>::max();
  }
  return local_overhead_ns * std::max<size_t>(workers, 1);
}

inline size_t TimespanTargetNanoseconds(size_t scheduling_overhead_ns,
                                        size_t completed, size_t elapsed_ns,
                                        size_t range_work,
                                        size_t stealing_workers,
                                        size_t workers) noexcept {
  if (range_work == 0 || completed == 0) {
    return std::max<size_t>(scheduling_overhead_ns, 1);
  }
  const long double projected_range_ns =
      static_cast<long double>(elapsed_ns) * range_work / completed;
  const long double steal_pressure =
      1.0L + static_cast<long double>(stealing_workers) /
                   std::max<size_t>(workers, 1);
  // Minimize H*T/tau scheduling work plus pressure*tau tail exposure.
  const long double target = std::sqrt(
      static_cast<long double>(std::max<size_t>(scheduling_overhead_ns, 1)) *
      projected_range_ns / steal_pressure);
  if (target >= std::numeric_limits<size_t>::max()) {
    return std::numeric_limits<size_t>::max();
  }
  return std::max<size_t>(
      static_cast<size_t>(target + 0.5L),
      std::max<size_t>(scheduling_overhead_ns, 1));
}

inline size_t TimespanBlockSize(size_t current, size_t completed,
                                size_t elapsed_ns, size_t remaining,
                                size_t grain, size_t target_ns) noexcept {
  if (remaining == 0) {
    return grain;
  }
  const long double ratio = std::clamp(
      static_cast<long double>(target_ns) / std::max<size_t>(elapsed_ns, 1),
      0.25L, 8.0L);
  const long double estimate = static_cast<long double>(completed) * ratio;
  const size_t scaled = estimate >= std::numeric_limits<size_t>::max()
                            ? std::numeric_limits<size_t>::max()
                            : std::max<size_t>(static_cast<size_t>(estimate + 0.5L),
                                               grain);
  const size_t growth_limit =
      current > std::numeric_limits<size_t>::max() / 8
          ? std::numeric_limits<size_t>::max()
          : current * 8;
  const size_t balance_limit =
      std::max(grain, remaining / 4 + (remaining % 4 != 0));
  const size_t upper = std::max(grain, std::min(growth_limit, balance_limit));
  const size_t lower = std::min(
      upper, std::max(grain, current / 4 + (current % 4 != 0)));
  return std::clamp(scaled, lower, upper);
}

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

template <typename F> class RangeTask final : public Task {
public:
  RangeTask(OrdinaryRegion &region, RapidDomainState &state, F &function,
            DomainId domain, size_t begin, size_t end)
      : region_(region), state_(state), function_(function), domain_(domain),
        begin_(begin), end_(end) {}

  void operator()() final {
    const MailboxDomainContext previous_context = current_mailbox_context;
    current_mailbox_context = {&state_, domain_};
    RunOrdinaryRange(region_, function_, begin_, end_);
    current_mailbox_context = previous_context;
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

template <typename F, bool Timespan> class LazyRangeCoordinator {
public:
  LazyRangeCoordinator(ThreadPool &pool, F &function, size_t begin, size_t end,
                       size_t slots, size_t grain, size_t target_block_ns = 0)
      : pool_(pool), function_(function), slots_(slots),
        ranges_(new Range[slots]), block_(HybridBlockSize(end - begin, slots,
                                                         grain)),
        grain_(grain), target_block_ns_(target_block_ns) {
    if constexpr (Timespan) {
      if (target_block_ns_ == 0) {
        scheduling_overhead_ns_ = TimespanDomainSchedulingOverheadNs(
            CalibratedTimespanSchedulingOverheadNs(), slots_);
      }
    }
    const size_t quotient = (end - begin) / slots;
    const size_t remainder = (end - begin) % slots;
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
    range.first = range.next.fetch_add(Block(range), std::memory_order_relaxed);
  }

  void Run(size_t own_slot) noexcept {
    Range &range = ranges_[own_slot];
    if constexpr (Timespan) {
      RunTimedBlock(range, range.first, Block(range));
    } else {
      RunClaimedBlock(range, range.first, block_);
    }
    while (!IsCancelled() && RunBlock(own_slot, true)) {
    }
    if constexpr (Timespan) {
      steal_pressure_.fetch_add(1, std::memory_order_relaxed);
    }
    bool in_rapid_domain = true;
    while (!IsCancelled() && HasUnclaimedWork()) {
      if (in_rapid_domain) {
        RegionContext *context = pool_.CurrentRegionContext();
        const bool executed = pool_.TryExecuteSomething();
        if (pool_.CurrentRegionContext() != context) {
          in_rapid_domain = false;
        }
        if (executed) {
          continue;
        }
      }
      in_rapid_domain = false;
      bool found = false;
      for (size_t offset = 1; offset < slots_ && !IsCancelled(); ++offset) {
        const size_t slot = (own_slot + offset) % slots_;
        if (RunBlock(slot, false)) {
          found = true;
          break;
        }
      }
      if (!found) {
        break;
      }
    }
    if constexpr (Timespan) {
      steal_pressure_.fetch_sub(1, std::memory_order_relaxed);
    }
  }

  void Rethrow() {
    if (exception_) {
      std::rethrow_exception(exception_);
    }
  }

private:
  struct alignas(OOX_EIGEN_CACHE_LINE_SIZE) Range {
    std::atomic<size_t> next{0};
    std::atomic<size_t> adaptive_block{1};
    size_t first = 0;
    size_t end = 0;
    size_t work = 0;
    size_t samples = 0;
  };

  size_t Block(Range &range) const noexcept {
    if constexpr (Timespan) {
      return range.adaptive_block.load(std::memory_order_relaxed);
    }
    return block_;
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
    if constexpr (Timespan) {
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
      const size_t target_ns =
          target_block_ns_ != 0
              ? target_block_ns_
              : TimespanTargetNanoseconds(
                    scheduling_overhead_ns_, completed, elapsed_ns, range.work,
                    steal_pressure_.load(std::memory_order_relaxed), slots_);
      size_t next = TimespanBlockSize(
          block, completed, elapsed_ns, remaining, grain_, target_ns);
      if (range.samples++ != 0) {
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
      std::lock_guard<std::mutex> lock(exception_mutex_);
      if (!exception_) {
        exception_ = std::current_exception();
        cancelled_.store(true, std::memory_order_release);
      }
    }
    return true;
  }

  ThreadPool &pool_;
  F &function_;
  size_t slots_;
  std::unique_ptr<Range[]> ranges_;
  size_t block_;
  size_t grain_;
  size_t target_block_ns_;
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
    for (size_t i = first; i < last; ++i) {
      std::invoke(function, i);
    }
  };
  ParallelForRanges(group, begin, end, ranges);
}

template <typename F>
void ParallelForMailbox(RapidStartGroup group, size_t begin, size_t end,
                        F &&function, size_t grain = 1) {
  if (!group.state || group.domain.Size() == 0 || begin >= end) {
    return;
  }
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
      work / group.domain.Size() + (work % group.domain.Size() != 0);
  // Halve publication density through 512 iterations per worker. Preserve
  // finer tasks beyond that point so irregular work remains stealable.
  const size_t task_density_divisor = work_per_worker <= 512 ? 2 : 1;
  const size_t block =
      HybridBlockSize(end - begin, group.domain.Size(),
                      std::max<size_t>(grain, 1), task_density_divisor);
  try {
    ParallelForRanges(group, begin, end, [&](size_t first, size_t last) {
      RegionContext *context = pool.CurrentRegionContext();
      const DomainId domain = context ? context->domain : group.domain;
      const size_t target = domain.start;
      while (first < last) {
        const size_t task_end = first + std::min(block, last - first);
        auto *task = new RangeTask<Function>(ordinary, *group.state, callable,
                                             domain, first, task_end);
        ordinary.AddTask();
        try {
          pool.RunOnThread(task, target);
        } catch (...) {
          task->Discard();
          throw;
        }
        first = task_end;
      }
    });
  } catch (...) {
    ordinary.Fail(std::current_exception());
  }
  ordinary.TaskComplete();
  pool.HelpUntil(ordinary);
  ordinary.Rethrow();
}

template <bool Timespan, typename F>
void ParallelForLazyStealingImpl(RapidStartGroup group, size_t begin,
                                 size_t end, F &&function, size_t grain,
                                 size_t target_block_ns) {
  if (!group.state || group.domain.Size() == 0 || begin >= end) {
    return;
  }
  using Function = std::remove_reference_t<F>;
  Function &callable = function;
  ThreadPool &pool = group.state->Pool();
  RegionContext *parent = pool.CurrentRegionContext();
  if (parent && parent->rapid_state && parent->rapid_state != group.state) {
    parent = nullptr;
  }
  const DomainId domain = parent ? parent->domain : group.domain;
  if (domain.Size() == 1) {
    for (size_t index = begin; index < end && !pool.IsCancelled(); ++index) {
      std::invoke(callable, index);
    }
    return;
  }
  group.domain = domain;
  const size_t slots = std::min(group.domain.Size(), end - begin);
  LazyRangeCoordinator<Function, Timespan> coordinator(
      pool, callable, begin, end, slots, std::max<size_t>(grain, 1),
      target_block_ns);
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
  coordinator.Rethrow();
}

template <typename F>
void ParallelForLazyStealing(RapidStartGroup group, size_t begin, size_t end,
                             F &&function, size_t grain = 1) {
  ParallelForLazyStealingImpl<false>(group, begin, end,
                                     std::forward<F>(function), grain, 0);
}

template <typename F>
void ParallelForTimespanLazyStealing(RapidStartGroup group, size_t begin,
                                     size_t end, F &&function,
                                     size_t grain = 1,
                                     size_t target_block_ns = 0) {
  ParallelForLazyStealingImpl<true>(group, begin, end,
                                    std::forward<F>(function), grain,
                                    target_block_ns);
}

} // namespace rapid
} // namespace oox::detail::eigen_pool
