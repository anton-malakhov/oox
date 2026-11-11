// SPDX-License-Identifier: MPL-2.0
#pragma once

#include "resident_worker_mask.h"
#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace oox::detail::eigen_pool {

struct DomainId {
  unsigned start = 0;
  unsigned limit = 0;

  DomainId LimitedTo(size_t workers) const noexcept {
    return {start, static_cast<unsigned>(std::min<size_t>(limit, workers))};
  }

  size_t Size() const noexcept {
    return limit >= start ? limit - start : 0;
  }
  bool Contains(size_t worker) const noexcept {
    return worker >= start && worker < limit;
  }
};

namespace internal {

// The pool supplies the current eligibility limit. The set owns the ready
// words from which each launch reserves its helpers.
class ResidentWorkerSet {
public:
  ResidentWorkerSet(unsigned pool_size, unsigned first_background)
      : pool_size_(pool_size), first_background_(first_background),
        words_(pool_size ? std::make_unique<ResidentMaskWord[]>(WordCount()) : nullptr) {}

  size_t WordCount() const noexcept { return (size_t{pool_size_} + 63) / 64; }
  unsigned FirstBackgroundWorker() const noexcept { return first_background_; }
  ResidentMaskWord &Word(size_t index) noexcept { return words_[index]; }
  const ResidentMaskWord &Word(size_t index) const noexcept { return words_[index]; }

  size_t Capacity(DomainId domain) const noexcept {
    const unsigned first = std::max(domain.start, first_background_);
    const unsigned last = std::min(domain.limit, pool_size_);
    return last > first ? last - first : 0;
  }

  size_t Count(DomainId domain) const noexcept {
    size_t available = 0;
    for (size_t word = 0; word < WordCount(); ++word)
      available += std::popcount(words_[word].Available(Allowed(word, domain)));
    return available;
  }

  size_t Claim(DomainId domain, unsigned *workers, size_t capacity) noexcept {
    if (!capacity || !Capacity(domain)) return 0;
    const unsigned first_bit = ClaimStartBit(capacity);
    const size_t words = WordCount();
    size_t claimed = 0;
    for (size_t word = 0; word < words && claimed < capacity; ++word) {
      uint64_t selected = words_[word].ClaimResidentWord(
          Allowed(word, domain), first_bit, capacity - claimed, pool_size_);
      while (selected) {
        workers[claimed++] = static_cast<unsigned>(word * 64 + std::countr_zero(selected));
        selected &= selected - 1;
      }
    }
    return claimed;
  }

private:
  uint64_t Allowed(size_t word, DomainId domain) const noexcept {
    return ResidentMaskWord::DomainMask(word, domain.start, std::min(domain.limit, pool_size_));
  }

  unsigned ClaimStartBit(size_t capacity) const noexcept {
    // Small pools can rotate a multi-helper claim using the existing packed clock.
    if (capacity > 1 && ResidentMaskWord::SupportsPackedCompletion(pool_size_)) {
      if (auto version = words_[0].PackedEpoch(pool_size_, std::memory_order_relaxed))
        return first_background_ + static_cast<unsigned>(*version % (pool_size_ - first_background_));
    }
    return 0;
  }

  const unsigned pool_size_;
  // Background worker IDs start at 1 when the pool creator owns slot 0, otherwise at 0.
  // In a single-slot pool, the background worker always gets ID 0.
  const unsigned first_background_;
  std::unique_ptr<ResidentMaskWord[]> words_;
};

} // namespace internal
} // namespace oox::detail::eigen_pool
