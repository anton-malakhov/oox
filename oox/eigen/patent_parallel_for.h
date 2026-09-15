// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "nonblocking_thread_pool.h"
#include <algorithm>
#include <exception>
#include "tbb_partitioning.h"

namespace oox::detail::eigen_pool {
namespace patent_detail {

inline constexpr unsigned initial_depth = 3;

struct Metrics {
  std::atomic<size_t> owner_ranges{0};
  std::atomic<size_t> range_tasks{0};
  std::atomic<size_t> initial_tasks{0};
  std::atomic<size_t> signal_tasks{0};
  std::atomic<size_t> stolen_signals{0};
  std::atomic<size_t> donated_ranges{0};
  std::atomic<size_t> private_chunks{0};
};

class Region {
public:
  explicit Region(ThreadPool &pool, Metrics *metrics)
      : pool(pool), metrics(metrics) {}
  void AddTask() noexcept { remaining.fetch_add(1, std::memory_order_relaxed); }
  void TaskComplete() noexcept {
    // Copy notification state before publishing completion. The caller may
    // destroy the region as soon as it observes complete=true.
    ThreadPool *saved_pool = &pool;
    if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      complete.store(true, std::memory_order_release);
      saved_pool->NotifyTaskCompletion();
    }
  }
  bool IsComplete() const noexcept {
    return complete.load(std::memory_order_acquire);
  }
  bool IsCancelled() const noexcept {
    return cancelled.load(std::memory_order_relaxed) || pool.IsCancelled();
  }
  void Fail(std::exception_ptr error) noexcept {
    std::lock_guard<std::mutex> lock(error_mutex);
    if (!exception)
      exception = std::move(error);
    cancelled.store(true, std::memory_order_release);
  }
  void Rethrow() {
    if (exception)
      std::rethrow_exception(exception);
  }

  ThreadPool &pool;
  Metrics *const metrics;

private:
  std::atomic<size_t> remaining{1};
  std::atomic<bool> complete{false};
  std::atomic<bool> cancelled{false};
  std::mutex error_mutex;
  std::exception_ptr exception;
};

struct LoopRange {
  size_t begin, end, grain;
  LoopRange(size_t first, size_t last, size_t minimum) noexcept
      : begin(first), end(last), grain(minimum) {}
  LoopRange(LoopRange &source, partitioning::Split) noexcept
      : begin(source.begin + (source.end - source.begin) / 2), end(source.end),
        grain(source.grain) {
    source.end = begin;
  }
  bool IsDivisible() const noexcept { return end - begin > grain; }
};

struct Signal {
  std::atomic<unsigned> references{1};
  std::atomic<bool> demand{false};
  std::atomic<bool> pending{false};
  void AddRef() noexcept { references.fetch_add(1, std::memory_order_relaxed); }
  void Release() noexcept {
    if (references.fetch_sub(1, std::memory_order_acq_rel) == 1)
      delete this;
  }
};

class Probe final : public Task {
public:
  Probe(Region &region, Signal &signal, std::thread::id owner) noexcept
      : region_(region), signal_(signal), owner_(owner) {
    signal_.AddRef();
    region_.AddTask();
    if (region_.metrics)
      region_.metrics->signal_tasks.fetch_add(1, std::memory_order_relaxed);
  }
  ~Probe() override {
    signal_.pending.store(false, std::memory_order_release);
    signal_.Release();
    region_.TaskComplete();
  }
  void operator()() final {
    if (std::this_thread::get_id() != owner_) {
      signal_.demand.store(true, std::memory_order_release);
      if (region_.metrics)
        region_.metrics->stolen_signals.fetch_add(1, std::memory_order_relaxed);
    }
    delete this;
  }
  void Discard() noexcept final { delete this; }

private:
  Region &region_;
  Signal &signal_;
  std::thread::id owner_;
};

inline void Arm(Region &region, Signal &signal) {
  if (signal.pending.load(std::memory_order_acquire) || region.IsCancelled())
    return;
  signal.pending.store(true, std::memory_order_release);
  auto probe =
      std::make_unique<Probe>(region, signal, std::this_thread::get_id());
  region.pool.Schedule(probe.release());
}

template <typename F>
void Process(Region &, F &, LoopRange, size_t) noexcept;

template <typename F>
class Work final : public Task {
public:
  Work(Region &region, F &function, LoopRange range, size_t budget) noexcept
      : region_(region), function_(function), range_(range), budget_(budget) {
    region_.AddTask();
    if (region_.metrics)
      region_.metrics->range_tasks.fetch_add(1, std::memory_order_relaxed);
  }
  ~Work() override { region_.TaskComplete(); }
  void operator()() final {
    Process(region_, function_, range_, budget_);
    delete this;
  }
  void Discard() noexcept final { delete this; }

private:
  Region &region_;
  F &function_;
  LoopRange range_;
  size_t budget_;
};

struct OwnerSignal {
  Signal *value = nullptr;
  ~OwnerSignal() {
    if (value)
      value->Release();
  }
};

// The range pool belongs exclusively to this running invocation. A stolen
// probe requests work; only this owner removes and publishes its FIFO range.
template <typename F>
void Process(Region &region, F &function, LoopRange range,
             size_t budget) noexcept {
  OwnerSignal signal;
  size_t chunks = 0;
  try {
    // Initial subdivision is a tree of ordinary Eigen tasks. The budget is
    // split with the range, so this creates at most P initial owners.
    while (budget > 1 && range.IsDivisible() && !region.IsCancelled()) {
      const size_t left_budget = budget / 2;
      const size_t size = range.end - range.begin;
      const size_t middle = range.begin + (size / budget) * left_budget +
                            std::min(left_budget, size % budget);
      auto child = std::make_unique<Work<F>>(
          region, function, LoopRange{middle, range.end, range.grain},
          budget - left_budget);
      range.end = middle;
      budget = left_budget;
      region.pool.Schedule(child.release());
      if (region.metrics)
        region.metrics->initial_tasks.fetch_add(1, std::memory_order_relaxed);
    }
    if (region.metrics)
      region.metrics->owner_ranges.fetch_add(1, std::memory_order_relaxed);
    partitioning::RangePool<LoopRange> ranges(range);
    unsigned limit = initial_depth;
    if (range.IsDivisible()) {
      signal.value = new Signal;
      Arm(region, *signal.value);
    }
    while (!ranges.Empty() && !region.IsCancelled()) {
      ranges.Fill(limit);
      LoopRange &current = ranges.Back();
      const size_t first = current.begin;
      while (current.begin < current.end) {
        // An indivisible remainder always executes: a stream of stolen
        // probes must not prevent the owner from making progress.
        if (current.IsDivisible() && signal.value &&
            signal.value->demand.load(std::memory_order_relaxed))
          break;
        const size_t index = current.begin++;
        std::invoke(function, index);
      }
      if (current.begin != first)
        ++chunks;
      if (current.begin == current.end)
        ranges.PopBack();
      if (signal.value && !ranges.Empty() && !region.IsCancelled()) {
        if (signal.value->demand.exchange(false, std::memory_order_acq_rel)) {
          if (limit < partitioning::AutoPartition::max_depth)
            ++limit;
          if (ranges.Size() > 1) {
            auto child = std::make_unique<Work<F>>(
                region, function, ranges.Front(), 1);
            region.pool.Schedule(child.release());
            ranges.PopFront();
            if (region.metrics)
              region.metrics->donated_ranges.fetch_add(
                  1, std::memory_order_relaxed);
          }
        }
        // Re-arm also when a nested helper consumed its own probe.
        Arm(region, *signal.value);
      }
    }
  } catch (...) {
    region.Fail(std::current_exception());
  }
  if (region.metrics)
    region.metrics->private_chunks.fetch_add(chunks, std::memory_order_relaxed);
}

} // namespace patent_detail

// Worker-count-based initial subdivision, private range buffering,
// stolen-signal feedback, FIFO donation, and LIFO local processing from
// WO2013021223A1 / US9262230B2. U=1 and initial V=8.
// Only ordinary Eigen task submission, cancellation, and waiting are used.
template <typename F>
void ParallelForPatent(ThreadPool &pool, size_t begin, size_t end,
                       F &&function, size_t grain = 1,
                       patent_detail::Metrics *metrics = nullptr) {
  if (begin >= end || pool.IsCancelled())
    return;
  grain = std::max<size_t>(grain, 1);
  if (pool.NumThreads() == 1 || end - begin <= grain) {
    if (metrics) {
      metrics->owner_ranges.fetch_add(1, std::memory_order_relaxed);
      metrics->private_chunks.fetch_add(1, std::memory_order_relaxed);
    }
    for (size_t i = begin; i < end && !pool.IsCancelled(); ++i)
      std::invoke(function, i);
    return;
  }
  const size_t budget =
      std::min(pool.NumThreads(), (end - begin - 1) / grain + 1);
  patent_detail::Region region(pool, metrics);
  patent_detail::Process(region, function, {begin, end, grain}, budget);
  region.TaskComplete();
  // Wait may return early on pool cancellation. Published tasks still own
  // references to the callback and region, so retain both until all tasks
  // have either completed or been discarded by the pool.
  while (!region.IsComplete())
    pool.Wait([&] { return region.IsComplete(); });
  region.Rethrow();
}
} // namespace oox::detail::eigen_pool
