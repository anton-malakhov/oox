// Copyright (c) 2005-2021 Intel Corporation
// SPDX-License-Identifier: Apache-2.0
//
// Adapted from oneTBB 2021.5.0, partitioner.h, revision
// 70bf10c0a9e65e3a954156f801b0a11c96f7f6bd.
// OOX adapts the owner-private range buffer independently of the TBB runtime.

#ifndef OOX_EIGEN_TBB_PARTITIONING_H
#define OOX_EIGEN_TBB_PARTITIONING_H

#include <array>
#include <cassert>
#include <cstddef>
#include <optional>
#include <type_traits>

namespace oox::detail::eigen_pool::partitioning {

struct Split {};

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

} // namespace oox::detail::eigen_pool::partitioning

#endif // OOX_EIGEN_TBB_PARTITIONING_H
