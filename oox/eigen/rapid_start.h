// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "nonblocking_thread_pool.h"

#include <algorithm>
#include <atomic>
#include <cassert>
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
              bool worker_waiter, void *function, Invoke invoke)
      : RapidRegionBase(parent ? parent->region : nullptr, domain),
        state_(state), parent_context_(parent), worker_waiter_(worker_waiter),
        function_(function), invoke_(invoke) {}

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

private:
  RapidDomainState &state_;
  RegionContext *parent_context_;
  bool worker_waiter_;
  void *function_;
  Invoke invoke_;
  std::atomic<bool> cancelled_{false};
  std::mutex exception_mutex_;
  std::exception_ptr exception_;
};

class Activation final : public RapidTask {
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
  uint32_t slot_ = 0;
  std::atomic<size_t> tickets_{0};
  std::atomic<unsigned> children_{0};
  std::atomic<State> state_{State::Free};
  std::atomic<uint32_t> next_free_{0};
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
  complete_.store(true, std::memory_order_release);
  pool->NotifyTaskCompletion(worker_waiter_);
}

inline void Activation::Initialize(RapidDomainState &owner, RapidRegion &region,
                                   Activation *parent,
                                   RegionContext *parent_context,
                                   DomainId domain, size_t begin,
                                   size_t end) noexcept {
  owner_ = &owner;
  region_ = &region;
  parent_ = parent;
  context_ = {&region, domain, parent_context};
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
  const size_t right_workers = workers - left_workers;
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
void ParallelFor(RapidStartGroup group, size_t begin, size_t end,
                 F &&function) {
  if (!group.state || group.domain.Size() == 0 || begin >= end) {
    return;
  }
  using Function = std::remove_reference_t<F>;
  Function *function_ptr = std::addressof(function);
  const auto invoke = [](void *opaque, size_t first, size_t last) {
    Function &callable = *static_cast<Function *>(opaque);
    for (size_t i = first; i < last; ++i) {
      std::invoke(callable, i);
    }
  };
  ThreadPool &pool = group.state->Pool();
  RegionContext *parent = pool.CurrentRegionContext();
  DomainId domain = parent ? parent->domain : group.domain;
  RapidRegion region(*group.state, parent, domain,
                     pool.CurrentThreadId() < pool.NumThreads(), function_ptr,
                     invoke);
  group.state->BeginRegion();
  Activation *root = group.state->Acquire();
  root->Initialize(*group.state, region, nullptr, parent, domain, begin, end);
  root->AddTickets(1);
  root->TryRun();
  root->ReleaseTicket();
  pool.HelpUntil(region);
  region.Rethrow();
}

} // namespace rapid
} // namespace oox::detail::eigen_pool
