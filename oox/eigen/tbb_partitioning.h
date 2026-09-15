// Copyright (c) 2005-2021 Intel Corporation
// SPDX-License-Identifier: Apache-2.0
//
// Adapted from oneTBB 2021.5.0, partitioner.h, revision
// 70bf10c0a9e65e3a954156f801b0a11c96f7f6bd.
// OOX replaces TBB storage/assertions, bounds depth, and exposes the automatic
// partitioner's decisions separately from the TBB task runtime.

#ifndef OOX_EIGEN_TBB_PARTITIONING_H
#define OOX_EIGEN_TBB_PARTITIONING_H

#include <array>
#include <cassert>
#include <cstddef>
#include <limits>
#include <optional>
#include <type_traits>

namespace oox::detail::eigen_pool::partitioning {

struct Split {};

// The split constructor keeps the left half in the source, as TBB ranges do.
struct IndexRange {
  std::size_t begin;
  std::size_t end;

  IndexRange(std::size_t first, std::size_t last) noexcept
      : begin(first), end(last) {}
  IndexRange(IndexRange &source, Split) noexcept
      : begin(source.begin + (source.end - source.begin) / 2), end(source.end) {
    source.end = begin;
  }

  bool IsDivisible() const noexcept { return end - begin > 1; }
};

// This is an owner-only range buffer, not a concurrent work queue. The range
// policy transfers work by copying a range into an ordinary Eigen task.
template <typename Range, std::size_t Capacity = 8> class RangePool {
  static_assert(Capacity > 1);
  static_assert(std::is_nothrow_copy_constructible_v<Range>);
  static_assert(std::is_nothrow_constructible_v<Range, Range &, Split>);

public:
  explicit RangePool(const Range &range) noexcept {
    ranges_[0].emplace(range);
  }

  void Fill(unsigned max_depth) noexcept {
    while (size_ < Capacity && IsDivisible(max_depth)) {
      const std::size_t previous = head_;
      head_ = (head_ + 1) % Capacity;
      ranges_[head_].emplace(*ranges_[previous]);
      ranges_[previous].emplace(*ranges_[head_], Split{});
      depth_[head_] = ++depth_[previous];
      ++size_;
    }
  }

  bool Empty() const noexcept { return size_ == 0; }
  std::size_t Size() const noexcept { return size_; }
  Range &Back() noexcept { assert(size_); return *ranges_[head_]; }
  Range &Front() noexcept { assert(size_); return *ranges_[tail_]; }
  unsigned FrontDepth() const noexcept { assert(size_); return depth_[tail_]; }
  unsigned BackDepth() const noexcept { assert(size_); return depth_[head_]; }

  bool IsDivisible(unsigned max_depth) const noexcept {
    return size_ && depth_[head_] < max_depth &&
           ranges_[head_]->IsDivisible();
  }

  void PopBack() noexcept {
    assert(size_);
    ranges_[head_].reset();
    --size_;
    head_ = (head_ + Capacity - 1) % Capacity;
  }

  void PopFront() noexcept {
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

// The automatic partitioner's budget/depth transitions, with scheduler state
// (remote execution and live sibling) supplied explicitly by the adapter.
class AutoPartition {
public:
  static constexpr unsigned initial_depth = 5;
  static constexpr unsigned demand_depth_add = 1;
  static constexpr unsigned max_depth = std::numeric_limits<std::size_t>::digits;

  explicit AutoPartition(std::size_t concurrency = 1,
                         unsigned depth = initial_depth) noexcept
      : divisor_(concurrency > std::numeric_limits<std::size_t>::max() / 2
                     ? std::numeric_limits<std::size_t>::max()
                     : concurrency * 2),
        depth_(depth > max_depth ? max_depth : depth) {}

  AutoPartition(AutoPartition &source, Split) noexcept
      : divisor_(source.divisor_ /= 2), depth_(source.depth_) {}

  bool IsDivisible() noexcept {
    if (divisor_ > 1)
      return true;
    if (divisor_ && depth_) {
      --depth_;
      divisor_ = 0;
      return true;
    }
    return false;
  }

  bool CheckBeingStolen(bool remote, bool live_peer) noexcept {
    if (divisor_ == 0) {
      divisor_ = 1;
      if (remote && live_peer) {
        if (!depth_)
          ++depth_;
        IncreaseDepth();
        return true;
      }
    }
    return false;
  }

  bool CheckForDemand(bool peer_stolen) noexcept {
    if (!peer_stolen)
      return false;
    IncreaseDepth();
    return true;
  }

  void AlignDepth(unsigned base) noexcept {
    assert(base <= depth_);
    depth_ -= base;
  }

  unsigned MaxDepth() const noexcept { return depth_; }
  std::size_t Divisor() const noexcept { return divisor_; }

private:
  void IncreaseDepth() noexcept {
    if (depth_ < max_depth)
      depth_ += demand_depth_add;
  }

  std::size_t divisor_;
  unsigned depth_;
};

} // namespace oox::detail::eigen_pool::partitioning

#endif // OOX_EIGEN_TBB_PARTITIONING_H
