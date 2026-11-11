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

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <optional>
#include "scheduler_sync.h"

namespace oox::detail::eigen_pool {

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

namespace internal {

#if defined(OOX_EIGEN_THREAD_POOL_TESTING) && defined(OOX_EIGEN_TEST_RESIDENT_CLOCK_BITS)
static_assert(OOX_EIGEN_TEST_RESIDENT_CLOCK_BITS > 0 && OOX_EIGEN_TEST_RESIDENT_CLOCK_BITS <= 56);
inline constexpr unsigned resident_packed_clock_bits = OOX_EIGEN_TEST_RESIDENT_CLOCK_BITS;
inline constexpr uint64_t resident_packed_clock_max = (uint64_t{1} << resident_packed_clock_bits) - 1;
#else
inline constexpr unsigned resident_packed_clock_bits = 0;
inline constexpr uint64_t resident_packed_clock_max = 0;
#endif

// Small pools keep a claim clock above their worker bits. Registration and
// withdrawal must preserve those upper bits, including during reconfiguration.
// For <=8 workers: [clock | N sticky shared-cohort done bits | N ready bits].
// Larger pools use ordinary readiness words and separate epoch records.
class ResidentMaskWord {
public:
  static constexpr bool SupportsPackedCompletion(unsigned workers) noexcept { return workers <= 8; }

  static uint64_t DomainMask(size_t word, unsigned begin, unsigned end) noexcept {
    const size_t word_begin = word * 64;
    const size_t first = std::max(word_begin, static_cast<size_t>(begin));
    const size_t last =
        std::min(word_begin + 64, static_cast<size_t>(end));
    if (first >= last) {
      return 0;
    }
    const unsigned offset = static_cast<unsigned>(first - word_begin);
    const unsigned count = static_cast<unsigned>(last - first);
    const uint64_t bits = count == 64 ? ~uint64_t{0}
                                      : (uint64_t{1} << count) - 1;
    return bits << offset;
  }

  uint64_t Available(uint64_t allowed) const noexcept {
    return bits_.load(std::memory_order_acquire) & allowed;
  }

  // On CAS failure, select again from the updated word. A worker might have withdrawn.
  uint64_t ClaimResidentWord(uint64_t allowed, unsigned first_bit, size_t capacity,
                             unsigned workers) noexcept {
    uint64_t observed = bits_.load(std::memory_order_acquire);
    while (observed & allowed) {
      const uint64_t selected = ResidentClaimMask(observed & allowed, first_bit, capacity);
      const uint64_t desired = (observed & ~selected) + ClockIncrement(observed, workers);
      if (bits_.compare_exchange_weak(observed, desired, std::memory_order_acq_rel,
                                      std::memory_order_acquire))
        return selected;
    }
    return 0;
  }

  bool WithdrawResident(unsigned worker) noexcept {
    const uint64_t bit = WorkerBit(worker);
    return (bits_.fetch_and(~bit, std::memory_order_acq_rel) & bit) != 0;
  }

  void Advertise(unsigned worker, unsigned workers, bool packed_done = false) noexcept {
    const uint64_t bit = WorkerBit(worker);
    const uint64_t done = packed_done ? DoneBits(bit, workers) : 0;
    bits_.fetch_or(bit | done, std::memory_order_release);
  }

  // Only a small pool has a packed clock. Saturation switches subsequent joins to counters.
  std::optional<uint64_t> PackedEpoch(unsigned workers, std::memory_order order) const noexcept {
    const uint64_t version = bits_.load(order) >> (2 * workers);
    if (version == ClockMax(workers)) return std::nullopt;
    return version;
  }

  static uint64_t DoneBits(uint64_t members, unsigned workers) noexcept {
    return members << workers;
  }

  bool Completed(uint64_t done) const noexcept { return Available(done) == done; }

  void ClearDone(uint64_t members, unsigned workers) noexcept {
    bits_.fetch_and(~DoneBits(members, workers), std::memory_order_relaxed);
  }

private:
  static uint64_t WorkerBit(unsigned worker) noexcept { return uint64_t{1} << (worker % 64); }

  static uint64_t ClockMax(unsigned workers) noexcept {
    const uint64_t maximum = ~uint64_t{0} >> (2 * workers);
#ifdef OOX_EIGEN_THREAD_POOL_TESTING
    if (resident_packed_clock_bits) return std::min(resident_packed_clock_max, maximum);
#endif
    return maximum;
  }

  static uint64_t ClockIncrement(uint64_t observed, unsigned workers) noexcept {
    if (!SupportsPackedCompletion(workers) || (observed >> (2 * workers)) == ClockMax(workers))
      return 0;
    return uint64_t{1} << (2 * workers);
  }

  scheduler_sync::atomic<uint64_t> bits_{0};
};

} // namespace internal
} // namespace oox::detail::eigen_pool
