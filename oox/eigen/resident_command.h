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
#include <cassert>
#include <cstddef>
#include <cstdint>
#include "resident_worker_mask.h"
#include "scheduler_sync.h"
#include "scheduler_trace.h"

namespace oox::detail::eigen_pool {

class ResidentTask {
public:
  virtual void Run(size_t slot) noexcept = 0;
  virtual ~ResidentTask() = default;
};

namespace internal {

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

  void Complete(unsigned worker, unsigned pool_size, ResidentMaskWord &available,
                scheduler_sync::atomic<uint64_t> &finished) const noexcept;
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

  void ReleaseAfterJoin(size_t word, ResidentMaskWord &available, unsigned pool_size) noexcept {
    // The caller has acquired completion. Direct commands can reuse a worker
    // while these done bits still belong to the busy-held descriptor.
    if (packed_completion) available.ClearDone(mask.load(std::memory_order_relaxed), pool_size);
    Release(word);
  }

  void Release([[maybe_unused]] size_t word) noexcept {
    mask.store(0, std::memory_order_release);
    busy.clear(std::memory_order_release);
#if defined(OOX_EIGEN_THREAD_POOL_TESTING) && defined(OOX_EIGEN_TEST_RESIDENT_BATCH_RELEASED)
    OOX_EIGEN_TEST_RESIDENT_BATCH_RELEASED(word);
#endif
  }
};

inline void ResidentCommand::Complete(unsigned worker, unsigned pool_size, ResidentMaskWord &available,
                                      scheduler_sync::atomic<uint64_t> &finished) const noexcept {
  // A worker can accept another command as soon as it advertises readiness.
  // Everything used below belongs to this command snapshot or to the pool.
  available.Advertise(worker, pool_size, packed_completion);
  if (completion) {
    const auto before = completion->fetch_sub(1, std::memory_order_release);
    if (before == 1 && batch) {
      // The caller may already have destroyed its counter. Join the other
      // releases before making the pool-owned broadcast reusable.
      scheduler_sync::atomic_thread_fence(std::memory_order_acquire);
      batch->Release(worker / 64);
    }
  } else if (!packed_completion) {
    finished.store(epoch, std::memory_order_release);
  }
}

// A claimed worker has one direct slot. The word-sized broadcast is the second
// source of commands, and each worker tracks the last generation it consumed.
class ResidentCommandSlot {
public:
  void Publish([[maybe_unused]] unsigned worker, ResidentTask &task,
               scheduler_sync::atomic<size_t> &completion, size_t slot, uint64_t epoch) noexcept {
    assert(!task_.load(std::memory_order_relaxed));
    slot_ = slot;
    completion_ = epoch ? nullptr : &completion;
    epoch_ = epoch;
#ifdef OOX_SCHEDULER_TRACE
    TraceActivation(worker, slot);
#endif
    task_.store(&task, std::memory_order_release);
  }

  bool HasCommand(unsigned worker, const ResidentBroadcast *broadcasts) const noexcept {
    return task_.load(std::memory_order_acquire) != nullptr ||
        (broadcasts && broadcasts[worker / 64].Contains(worker, seen_));
  }

  ResidentCommand Take(unsigned worker, ResidentBroadcast *broadcasts) noexcept {
    if (task_.load(std::memory_order_relaxed)) {
      if (auto *task = task_.load(std::memory_order_acquire)) {
        // The next publisher must claim the worker after it advertises readiness.
        task_.store(nullptr, std::memory_order_relaxed);
        return {task, slot_, completion_, epoch_};
      }
    }
    return broadcasts[worker / 64].Take(worker, seen_);
  }

  void Run(const ResidentCommand &command, [[maybe_unused]] unsigned worker) noexcept {
    assert(command.completion || command.epoch);
    OOX_SCHED_WORKER(worker);
    OOX_SCHED_SCOPE("resident", trace_id_, worker, command.slot);
    command.task->Run(command.slot);
  }

#ifdef OOX_SCHEDULER_TRACE
  void TraceActivation(unsigned worker, size_t slot) noexcept {
    trace_id_ = ::oox::scheduler_trace::NextId();
    OOX_SCHED_EVENT("activate", trace_id_, worker, slot, ::oox::scheduler_trace::Current());
  }
#endif

private:
  scheduler_sync::atomic<ResidentTask *> task_{nullptr};
  uint64_t seen_ = 0;
  size_t slot_ = 0;
  scheduler_sync::atomic<size_t> *completion_ = nullptr;
  uint64_t epoch_ = 0;
#ifdef OOX_SCHEDULER_TRACE
  uint64_t trace_id_ = 0;
#endif
};

// A broadcast derives each slot from its worker's rank in the mask. Other
// orders, including single-worker commands, use direct publication.
inline uint64_t ResidentBroadcastMask(const unsigned *workers, size_t count, size_t word) noexcept {
  if (count < 2) return 0;
  uint64_t members = 0;
  for (size_t i = 0; i < count; ++i) {
    if (workers[i] / 64 != word || (i && workers[i - 1] >= workers[i])) return 0;
    members |= uint64_t{1} << (workers[i] % 64);
  }
  return members;
}

template <class SlotFor>
size_t PublishResidentCommands(ResidentBroadcast *broadcasts, SlotFor slot_for, ResidentTask &task,
                               scheduler_sync::atomic<size_t> &completion, const unsigned *workers,
                               size_t count, size_t first_slot, uint64_t epoch, bool packed) noexcept {
  if (!count) return size_t(-1);
  const size_t word = workers[0] / 64;
  const uint64_t members = ResidentBroadcastMask(workers, count, word);
  auto &batch = broadcasts[word];
  uint64_t generation;
  if (!members || !batch.Begin(word, generation)) {
    for (size_t i = 0; i < count; ++i)
      slot_for(workers[i]).Publish(workers[i], task, completion, first_slot + i, epoch);
    return size_t(-1);
  }
  assert(completion.load(std::memory_order_relaxed) == count);
  batch.task = &task;
  batch.completion = epoch ? nullptr : &completion;
  batch.epoch = epoch;
  batch.packed_completion = packed && epoch;
  batch.first_slot = first_slot;
#ifdef OOX_SCHEDULER_TRACE
  for (size_t i = 0; i < count; ++i)
    slot_for(workers[i]).TraceActivation(workers[i], first_slot + i);
#endif
  batch.Commit(word, generation, members);
  return word;
}

} // namespace internal
} // namespace oox::detail::eigen_pool
