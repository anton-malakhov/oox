// Copyright (c) 2005-2021 Intel Corporation
// SPDX-License-Identifier: Apache-2.0
//
// Adapted from oneTBB 2021.5.0, partitioner.h, revision
// 70bf10c0a9e65e3a954156f801b0a11c96f7f6bd.
// OOX adapts auto_partitioner state and its private range buffer to Eigen.

#ifndef OOX_EIGEN_TBB_PARTITIONING_H
#define OOX_EIGEN_TBB_PARTITIONING_H

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <type_traits>

namespace oox::detail::eigen_pool::partitioning {

struct Split {};
struct ProportionalSplit {
  std::size_t left, right;
};

inline constexpr std::size_t no_affinity = std::size_t(-1);

struct UnaffinitizedState {
  static constexpr bool adaptive = false;
  static constexpr bool uses_affinity = false;
  static constexpr bool records_affinity = false;
  constexpr std::size_t Hint() const noexcept { return no_affinity; }
  constexpr void NoteExecution(std::size_t, std::size_t) noexcept {}
  constexpr void AlignDepth(unsigned) noexcept {}
};

struct SimplePartitionState : UnaffinitizedState {
  constexpr SimplePartitionState() = default;
  constexpr SimplePartitionState(SimplePartitionState &, Split) noexcept {}
  constexpr bool IsDivisible() const noexcept { return true; }
  constexpr Split GetSplit() const noexcept { return {}; }
};

struct StaticPartitionState : UnaffinitizedState {
  static constexpr bool uses_affinity = true;
  constexpr StaticPartitionState(std::size_t workers, std::size_t first) noexcept
      : divisor(workers), head(first), slots(workers) {}
  constexpr StaticPartitionState(StaticPartitionState &source,
                       ProportionalSplit split) noexcept
      : divisor(split.right), head(0), slots(source.slots) {
    source.divisor -= divisor;
    head = (source.head + source.divisor) % slots;
  }
  constexpr bool IsDivisible() const noexcept { return divisor > 1; }
  constexpr ProportionalSplit GetSplit() const noexcept {
    return {divisor - divisor / 2, divisor / 2};
  }
  constexpr std::size_t Hint() const noexcept { return head; }

  std::size_t divisor, head, slots;
};

// State transitions from auto_partition_type and dynamic_grainsize_mode.
struct AutoPartitionState : UnaffinitizedState {
  static constexpr bool adaptive = true;
  explicit constexpr AutoPartitionState(std::size_t workers) noexcept
      : divisor(2 * workers) {}
  constexpr AutoPartitionState(AutoPartitionState &source, Split) noexcept
      : divisor(source.divisor /= 2), max_depth(source.max_depth) {}

  constexpr Split GetSplit() const noexcept { return {}; }
  constexpr bool NeedsStealCheck() const noexcept { return divisor == 0; }
  constexpr bool CheckDemand(bool peer_stolen) noexcept {
    if (peer_stolen)
      GrowDepth();
    return peer_stolen;
  }
  constexpr bool IsDivisible() noexcept {
    if (divisor > 1)
      return true;
    if (divisor && max_depth) {
      --max_depth;
      divisor = 0;
      return true;
    }
    return false;
  }
  constexpr bool CheckStolen(bool stolen, bool peer_active) noexcept {
    if (divisor)
      return false;
    divisor = 1;
    if (!stolen || !peer_active)
      return false;
    if (!max_depth)
      ++max_depth;
    GrowDepth();
    return true;
  }
  constexpr void GrowDepth() noexcept {
    if (max_depth < std::numeric_limits<std::size_t>::digits)
      ++max_depth;
  }
  constexpr void AlignDepth(unsigned base) noexcept {
    assert(base <= max_depth);
    max_depth -= base;
  }

  std::size_t divisor;
  unsigned max_depth = 5;
};

struct AffinityHistory {
  explicit AffinityHistory(std::size_t count)
      : workers(count), slots(new std::atomic<std::size_t>[count * 16]{}) {}
  const std::size_t workers;
  std::unique_ptr<std::atomic<std::size_t>[]> slots;
};

struct AffinityPartitionState : AutoPartitionState {
  static constexpr bool uses_affinity = true;
  static constexpr bool records_affinity = true;
  static constexpr std::size_t factor = 16;
  enum class Delay { Begin, Pass };

  AffinityPartitionState(std::shared_ptr<AffinityHistory> history,
                         std::size_t first) noexcept
      : AutoPartitionState(0), head(first), slots_count(history->workers * factor),
        slots(history->slots.get()), lifetime(std::move(history)) {
    divisor = slots_count;
  }
  AffinityPartitionState(AffinityPartitionState &source, Split) noexcept
      : AutoPartitionState(source, Split{}),
        head((source.head + source.divisor) % source.slots_count),
        slots_count(source.slots_count), slots(source.slots), delay(Delay::Pass) {}
  AffinityPartitionState(AffinityPartitionState &source,
                         ProportionalSplit split) noexcept
      : AutoPartitionState(0), head(0), slots_count(source.slots_count),
        slots(source.slots) {
    divisor = split.right * factor;
    source.divisor -= divisor;
    head = (source.head + source.divisor) % slots_count;
    max_depth = source.max_depth;
  }
  bool IsDivisible() const noexcept { return divisor > factor; }
  ProportionalSplit GetSplit() const noexcept {
    const auto count = divisor / factor;
    return {count - count / 2, count / 2};
  }
  bool NeedsStealCheck() const noexcept { return divisor < factor; }
  bool CheckStolen(bool stolen, bool peer_active) noexcept {
    if (!NeedsStealCheck())
      return false;
    divisor = 0;
    return AutoPartitionState::CheckStolen(stolen, peer_active);
  }
  bool CheckDemand(bool peer_stolen) noexcept {
    if (delay == Delay::Begin) {
      delay = Delay::Pass;
      return false;
    }
    if (divisor > 1)
      return true;
    if (divisor && max_depth) {
      divisor = 0;
      return true;
    }
    return AutoPartitionState::CheckDemand(peer_stolen);
  }
  std::size_t Hint() const noexcept {
    if (!divisor)
      return no_affinity;
    const auto saved = slots[head].load(std::memory_order_relaxed);
    return saved ? saved : head / factor;
  }
  void NoteExecution(std::size_t worker, std::size_t hint) noexcept {
    if (divisor && hint != no_affinity && worker != no_affinity && worker != hint)
      slots[head].store(worker, std::memory_order_relaxed);
  }

  std::size_t head, slots_count;
  std::atomic<std::size_t> *slots;
  Delay delay = Delay::Begin;
  // Only root-state copies own the history. Child states borrow it while their
  // Region lease is active; cancelled queued children never access the map.
  std::shared_ptr<AffinityHistory> lifetime;
};

// This is an owner-only range buffer, not a concurrent work queue. The range
// policy transfers work by copying a range into an ordinary Eigen task.
template <typename Range, std::size_t Capacity = 8> class RangePool {
  static_assert(Capacity > 1);
  static_assert(std::is_nothrow_copy_constructible_v<Range>);
  static_assert(std::is_nothrow_constructible_v<Range, Range &, Split>);

public:
  explicit constexpr RangePool(const Range &range) noexcept {
    ranges_[0].emplace(range);
  }

  constexpr void Fill(unsigned max_depth) noexcept {
    while (size_ < Capacity && IsDivisible(max_depth)) {
      const std::size_t previous = head_;
      head_ = (head_ + 1) % Capacity;
      ranges_[head_].emplace(*ranges_[previous]);
      ranges_[previous].emplace(*ranges_[head_], Split{});
      depth_[head_] = ++depth_[previous];
      ++size_;
    }
  }

  constexpr bool Empty() const noexcept { return size_ == 0; }
  constexpr std::size_t Size() const noexcept { return size_; }
  constexpr Range &Back() noexcept { assert(size_); return *ranges_[head_]; }
  constexpr Range &Front() noexcept { assert(size_); return *ranges_[tail_]; }
  constexpr unsigned FrontDepth() const noexcept { assert(size_); return depth_[tail_]; }

  constexpr bool IsDivisible(unsigned max_depth) const noexcept {
    return size_ && depth_[head_] < max_depth &&
           ranges_[head_]->IsDivisible();
  }

  constexpr void PopBack() noexcept {
    assert(size_);
    ranges_[head_].reset();
    --size_;
    head_ = (head_ + Capacity - 1) % Capacity;
  }

  constexpr void PopFront() noexcept {
    assert(size_);
    ranges_[tail_].reset();
    --size_;
    tail_ = (tail_ + 1) % Capacity;
  }

private:
  std::array<std::optional<Range>, Capacity> ranges_{};
  std::array<unsigned, Capacity> depth_{};
  std::size_t head_ = 0;
  std::size_t tail_ = 0;
  std::size_t size_ = 1;
};

} // namespace oox::detail::eigen_pool::partitioning

#endif // OOX_EIGEN_TBB_PARTITIONING_H
