// SPDX-License-Identifier: MPL-2.0 AND Apache-2.0
// Pool record helpers are from nonblocking_thread_pool.h (MPL-2.0).
// The caller-owned ResidentCohort is from rapid_start.h (Apache-2.0).
#pragma once

#include "resident_command.h"
#include "resident_worker_set.h"
#include <cassert>
#include <cstddef>
#include <cstdint>

namespace oox::detail::eigen_pool::internal {

inline constexpr size_t no_resident_batch = size_t(-1);

// The worker records and both clocks outlive each invocation. Allocate the
// epoch after claiming the workers and before publishing the first command.
inline uint64_t AllocateResidentEpoch(unsigned pool_size, const ResidentWorkerSet &workers,
                                      scheduler_sync::atomic<uint64_t> &global_clock) noexcept {
  if (ResidentMaskWord::SupportsPackedCompletion(pool_size))
    return workers.Word(0).PackedEpoch(pool_size, std::memory_order_acquire).value_or(0);
  uint64_t observed = global_clock.load(std::memory_order_relaxed);
  while (observed < resident_global_clock_max) {
    const uint64_t next = observed + 1;
    if (global_clock.compare_exchange_weak(observed, next, std::memory_order_relaxed))
      return next == resident_global_clock_max ? 0 : next;
  }
  return 0;  // Saturation selects counters for future commands.
}

inline uint64_t CohortDoneMask(size_t batch, const unsigned *workers, size_t count,
                               unsigned pool_size) noexcept {
  if (batch == no_resident_batch || !ResidentMaskWord::SupportsPackedCompletion(pool_size)) return 0;
  uint64_t members = 0;
  for (size_t i = 0; i < count; ++i) members |= uint64_t{1} << workers[i];
  return ResidentMaskWord::DoneBits(members, pool_size);
}

template <class WorkerRecords>
bool CohortEpochComplete(const WorkerRecords &records, uint64_t epoch,
                         const unsigned *workers, size_t count) noexcept {
  // A delayed join can see the completion of a newer command on the same worker.
  for (size_t i = 0; i < count; ++i)
    if (records[workers[i]].resident_finished.load(std::memory_order_acquire) < epoch) return false;
  return true;
}

// The caller owns this join. The pool owns each worker's completion record.
template <class Pool>
class ResidentCohort {
public:
  ResidentCohort(Pool &pool, const unsigned *workers, size_t count,
                 scheduler_sync::atomic<size_t> &completion) noexcept
      : pool_(pool), workers_(workers), count_(count), completion_(completion),
        epoch_(count > 1 ? pool.NextResidentEpoch() : 0) {}
  ResidentCohort(const ResidentCohort &) = delete;
  ResidentCohort &operator=(const ResidentCohort &) = delete;

  ~ResidentCohort() {
    if (!finalized_) {
      assert(IsComplete());
      ReleaseBatch();
    }
  }

  void Publish(ResidentTask &task) noexcept {
    const bool packed = epoch_ && pool_.SupportsPackedResidentCompletion();
    const size_t batch = pool_.PublishResidentBatch(task, completion_, workers_, count_, 1, epoch_, packed);
    // With a zero epoch the last helper releases the broadcast.
    if (!epoch_) return;
    batch_ = batch;
    if (packed) done_mask_ = pool_.ResidentBatchDoneMask(batch_, workers_, count_);
  }

  bool IsComplete() const noexcept {
    if (finalized_) return true;
    if (!epoch_) return completion_.load(std::memory_order_acquire) == 0;
    // A shared packed batch holds its done bits until join. Direct commands
    // and larger pools record the completed epoch on each worker.
    if (done_mask_) return pool_.ResidentBatchDone(batch_, done_mask_);
    return pool_.ResidentEpochComplete(epoch_, workers_, count_);
  }

  void Finalize() noexcept {
    assert(IsComplete());
    if (epoch_) completion_.store(0, std::memory_order_relaxed);
    ReleaseBatch();
    finalized_ = true;
  }

private:
  void ReleaseBatch() noexcept {
    if (batch_ != no_resident_batch) {
      pool_.ReleaseResidentEpochBatch(batch_);
      batch_ = no_resident_batch;
    }
  }

  Pool &pool_;
  const unsigned *workers_;
  size_t count_;
  scheduler_sync::atomic<size_t> &completion_;
  uint64_t epoch_;
  uint64_t done_mask_ = 0;
  size_t batch_ = no_resident_batch;
  bool finalized_ = false;
};

} // namespace oox::detail::eigen_pool::internal
