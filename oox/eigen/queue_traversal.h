// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2016 Dmitry Vyukov <dvyukov@google.com>
#pragma once

#include <cassert>
#include <cstdint>
#include <span>

namespace oox::detail::eigen_pool::internal {

// One round visits every queue offset once using a stride coprime to size.
class QueueTraversal {
public:
  QueueTraversal() = default;
  QueueTraversal(unsigned size, std::span<const unsigned> coprime_steps, uint32_t seed) noexcept
      : size_(size), remaining_(size) {
    assert(size && !coprime_steps.empty() && coprime_steps.size() <= size);
    // Map the same 32-bit draw to the starting offset and the stride index.
    const uint64_t random = seed;
    next_ = static_cast<unsigned>((random * size) >> 32);
    stride_ = coprime_steps[(random * coprime_steps.size()) >> 32];
  }

  bool Empty() const noexcept { return remaining_ == 0; }

  unsigned Next() noexcept {
    assert(!Empty());
    const unsigned current = next_;
    next_ += stride_;
    if (next_ >= size_) next_ -= size_;
    --remaining_;
    return current;
  }

private:
  unsigned size_ = 0;
  unsigned remaining_ = 0;
  unsigned next_ = 0;
  unsigned stride_ = 0;
};

} // namespace oox::detail::eigen_pool::internal
