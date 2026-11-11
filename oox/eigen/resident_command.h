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

#include <bit>
#include <cstddef>
#include <cstdint>
#include "scheduler_sync.h"

namespace oox::detail::eigen_pool {

class ResidentTask {
public:
  virtual void Run(size_t slot) noexcept = 0;
  virtual ~ResidentTask() = default;
};

namespace internal {

#if defined(OOX_EIGEN_THREAD_POOL_TESTING) && defined(OOX_EIGEN_TEST_RESIDENT_CLOCK_BITS)
static_assert(OOX_EIGEN_TEST_RESIDENT_CLOCK_BITS > 0 && OOX_EIGEN_TEST_RESIDENT_CLOCK_BITS <= 56);
inline constexpr unsigned resident_packed_clock_bits = OOX_EIGEN_TEST_RESIDENT_CLOCK_BITS;
inline constexpr uint64_t resident_packed_clock_max = (uint64_t{1} << resident_packed_clock_bits) - 1;
#else
inline constexpr unsigned resident_packed_clock_bits = 0;
inline constexpr uint64_t resident_packed_clock_max = 0;
#endif
#if defined(OOX_EIGEN_THREAD_POOL_TESTING) && defined(OOX_EIGEN_TEST_GLOBAL_RESIDENT_CLOCK_BITS)
static_assert(OOX_EIGEN_TEST_GLOBAL_RESIDENT_CLOCK_BITS > 0 && OOX_EIGEN_TEST_GLOBAL_RESIDENT_CLOCK_BITS < 64);
inline constexpr uint64_t resident_global_clock_max =
    (uint64_t{1} << OOX_EIGEN_TEST_GLOBAL_RESIDENT_CLOCK_BITS) - 1;
#else
inline constexpr uint64_t resident_global_clock_max = ~uint64_t{0};
#endif
#if defined(OOX_EIGEN_THREAD_POOL_TESTING) && defined(OOX_EIGEN_TEST_RESIDENT_BATCH_GENERATION_BITS)
static_assert(OOX_EIGEN_TEST_RESIDENT_BATCH_GENERATION_BITS > 0 && OOX_EIGEN_TEST_RESIDENT_BATCH_GENERATION_BITS < 64);
inline constexpr uint64_t resident_batch_generation_max =
    ((uint64_t{1} << OOX_EIGEN_TEST_RESIDENT_BATCH_GENERATION_BITS) - 1) & ~uint64_t{1};
#else
inline constexpr uint64_t resident_batch_generation_max = ~uint64_t{1};
#endif

struct ResidentBroadcast;
struct ResidentCommand {
  ResidentTask *task;
  size_t slot;
  scheduler_sync::atomic<size_t> *completion;
  uint64_t epoch;
  ResidentBroadcast *batch = nullptr;
  bool packed_completion = false;
};

struct alignas(128) ResidentBroadcast {
  scheduler_sync::atomic<uint64_t> generation{0};
  alignas(128) scheduler_sync::atomic_flag busy{};
  scheduler_sync::atomic<uint64_t> mask{0};
  ResidentTask *task = nullptr;
  scheduler_sync::atomic<size_t> *completion = nullptr;
  uint64_t epoch = 0;
  bool packed_completion = false;
  size_t first_slot = 1;

  bool Begin([[maybe_unused]] size_t word, uint64_t &previous) noexcept {
    if (busy.test_and_set(std::memory_order_acquire)) return false;
    previous = generation.load(std::memory_order_relaxed);
    if (previous == resident_batch_generation_max) {
      // A nonmember may retain an old generation indefinitely. Stop at the
      // last even value and let the caller publish per-worker commands.
      busy.clear(std::memory_order_release);
      return false;
    }
    generation.store(previous + 1, std::memory_order_release);
#if defined(OOX_EIGEN_THREAD_POOL_TESTING) && defined(OOX_EIGEN_TEST_RESIDENT_BATCH_EDITING)
    OOX_EIGEN_TEST_RESIDENT_BATCH_EDITING(word);
#endif
    return true;
  }

  void Commit([[maybe_unused]] size_t word, uint64_t previous, uint64_t members) noexcept {
    // Seeing this mask must also make the preceding odd generation visible.
    mask.store(members, std::memory_order_release);
#if defined(OOX_EIGEN_THREAD_POOL_TESTING) && defined(OOX_EIGEN_TEST_RESIDENT_BATCH_MASK_READY)
    OOX_EIGEN_TEST_RESIDENT_BATCH_MASK_READY(word);
#endif
    generation.store(previous + 2, std::memory_order_release);
  }

private:
  template <bool Consume>
  uint64_t ReadMembers([[maybe_unused]] unsigned worker, uint64_t &seen) const noexcept {
    const uint64_t version = generation.load(std::memory_order_acquire);
    if ((version & 1) || version == seen) return 0;
#if defined(OOX_EIGEN_THREAD_POOL_TESTING) && defined(OOX_EIGEN_TEST_RESIDENT_BATCH_READER)
    if constexpr (Consume) OOX_EIGEN_TEST_RESIDENT_BATCH_READER(worker, version);
#endif
    const uint64_t members = mask.load(std::memory_order_acquire);
    if (generation.load(std::memory_order_acquire) != version) {
#if defined(OOX_EIGEN_THREAD_POOL_TESTING) && defined(OOX_EIGEN_TEST_RESIDENT_BATCH_REJECTED)
      if constexpr (Consume) OOX_EIGEN_TEST_RESIDENT_BATCH_REJECTED(worker);
#endif
      return 0;
    }
    if constexpr (Consume) seen = version;
    return members;
  }

public:
  bool Contains(unsigned worker, uint64_t seen) const noexcept {
    return ReadMembers<false>(worker, seen) & (uint64_t{1} << (worker % 64));
  }

  ResidentCommand Take(unsigned worker, uint64_t &seen) noexcept {
    const uint64_t members = ReadMembers<true>(worker, seen);
    const uint64_t bit = uint64_t{1} << (worker % 64);
    if (!(members & bit)) return {};
    // Membership holds the descriptor until this worker completes. Only
    // members may read its non-atomic metadata.
    const size_t slot = first_slot + std::popcount(members & (bit - 1));
    return {task, slot, completion, epoch, this, packed_completion};
  }

  void Release([[maybe_unused]] size_t word) noexcept {
    mask.store(0, std::memory_order_release);
    busy.clear(std::memory_order_release);
#if defined(OOX_EIGEN_THREAD_POOL_TESTING) && defined(OOX_EIGEN_TEST_RESIDENT_BATCH_RELEASED)
    OOX_EIGEN_TEST_RESIDENT_BATCH_RELEASED(word);
#endif
  }
};

} // namespace internal
} // namespace oox::detail::eigen_pool
