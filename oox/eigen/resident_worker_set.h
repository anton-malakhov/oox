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
// words and the rotation cursor used by concurrent group launches.
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

  bool Any(DomainId domain) const noexcept {
    for (size_t word = 0; word < WordCount(); ++word)
      if (words_[word].Available(Allowed(word, domain))) return true;
    return false;
  }

  size_t Claim(DomainId domain, unsigned *workers, size_t capacity) noexcept {
    if (!capacity || !Capacity(domain)) return 0;
    const size_t start = ClaimStart(capacity);
    const size_t words = WordCount();
    size_t claimed = 0;
    for (size_t offset = 0; offset < words && claimed < capacity; ++offset) {
      const size_t word = (start / 64 + offset) % words;
      uint64_t selected = words_[word].ClaimResidentWord(
          Allowed(word, domain), static_cast<unsigned>(start % 64), capacity - claimed, pool_size_);
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

  size_t ClaimStart(size_t capacity) noexcept {
    // A single-helper launch takes the first ready worker. Larger claims rotate.
    if (capacity == 1) return 0;
    if (ResidentMaskWord::SupportsPackedCompletion(pool_size_)) {
      if (auto version = words_[0].PackedEpoch(pool_size_, std::memory_order_relaxed))
        return first_background_ + *version % (pool_size_ - first_background_);
    }
    return claim_cursor_.fetch_add(capacity, std::memory_order_relaxed);
  }

  const unsigned pool_size_;
  const unsigned first_background_;
  std::unique_ptr<ResidentMaskWord[]> words_;
  scheduler_sync::atomic<size_t> claim_cursor_{0};
};

} // namespace internal
} // namespace oox::detail::eigen_pool
