// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "rapid_start.h"
#include "tbb_partitioning.h"

namespace oox::detail::eigen_pool::rapid {
namespace patent_detail {

struct Metrics {
  std::atomic<size_t> owner_ranges{0};
  std::atomic<size_t> range_tasks{0};
  std::atomic<size_t> signal_tasks{0};
  std::atomic<size_t> stolen_signals{0};
  std::atomic<size_t> donated_ranges{0};
  std::atomic<size_t> private_chunks{0};
};

class Region {
public:
  explicit Region(ThreadPool &pool, Metrics *metrics)
      : pool(pool), worker_waiter(pool.CurrentThreadId() < pool.NumThreads()),
        metrics(metrics) {}
  void AddTask() noexcept { remaining.fetch_add(1, std::memory_order_relaxed); }
  void TaskComplete() noexcept {
    // Copy notification state before publishing completion. The caller may
    // destroy the region as soon as it observes complete=true.
    ThreadPool *saved_pool = &pool;
    const bool saved_worker = worker_waiter;
    if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      complete.store(true, std::memory_order_release);
      saved_pool->NotifyTaskCompletion(saved_worker);
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
  const bool worker_waiter;
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
  region.pool.Schedule(probe.get());
  probe.release();
}

template <bool Feedback, bool Interruptible, typename F>
void Process(Region &, RapidDomainState &, F &, DomainId, LoopRange, unsigned,
             unsigned) noexcept;

template <bool Feedback, bool Interruptible, typename F>
class Work final : public Task {
public:
  Work(Region &region, RapidDomainState &state, F &function, DomainId domain,
       LoopRange range, unsigned depth, unsigned initial_depth) noexcept
      : region_(region), state_(state), function_(function), domain_(domain),
        range_(range), depth_(depth), initial_depth_(initial_depth) {
    region_.AddTask();
    if (region_.metrics)
      region_.metrics->range_tasks.fetch_add(1, std::memory_order_relaxed);
  }
  ~Work() override { region_.TaskComplete(); }
  void operator()() final {
    Process<Feedback, Interruptible>(region_, state_, function_, domain_,
                                     range_, depth_, initial_depth_);
    delete this;
  }
  void Discard() noexcept final { delete this; }

private:
  Region &region_;
  RapidDomainState &state_;
  F &function_;
  DomainId domain_;
  LoopRange range_;
  unsigned depth_, initial_depth_;
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
template <bool Feedback, bool Interruptible, typename F>
void Process(Region &region, RapidDomainState &state, F &function,
             DomainId domain, LoopRange range, unsigned base_depth,
             unsigned initial_depth) noexcept {
  ScopedMailboxDomainContext nested_context({&state, domain});
  OwnerSignal signal;
  size_t chunks = 0;
  try {
    partitioning::RangePool<LoopRange> ranges(range);
    unsigned limit = initial_depth;
    if constexpr (Feedback) {
      if (range.IsDivisible()) {
        signal.value = new Signal;
        Arm(region, *signal.value);
      }
    }
    while (!ranges.Empty() && !region.IsCancelled()) {
      ranges.Fill(limit);
      if constexpr (Feedback && Interruptible) {
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
      } else {
        const LoopRange current = ranges.Back();
        ranges.PopBack();
        for (size_t i = current.begin; i < current.end; ++i)
          std::invoke(function, i);
        ++chunks;
      }
      if constexpr (Feedback) {
        if (signal.value && !ranges.Empty() && !region.IsCancelled()) {
          if (signal.value->demand.exchange(false, std::memory_order_acq_rel)) {
            if (limit < partitioning::AutoPartition::max_depth)
              ++limit;
            if (ranges.Size() > 1) {
              auto child = std::make_unique<Work<Feedback, Interruptible, F>>(
                  region, state, function, domain, ranges.Front(),
                  base_depth + ranges.FrontDepth(), initial_depth);
              region.pool.Schedule(child.get());
              child.release();
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
    }
  } catch (...) {
    region.Fail(std::current_exception());
  }
  if (region.metrics)
    region.metrics->private_chunks.fetch_add(chunks, std::memory_order_relaxed);
}

} // namespace patent_detail

// Initial sharing is proportional to the effective worker domain. Each owner
// then applies private splitting, stolen-signal feedback, FIFO donation, and
// LIFO local processing from WO2013021223A1 / US9262230B2.
// U=1 and V=2^initial_depth describe the initial private split density.
// Early checkpoints stop a divisible remainder when a stolen probe requests
// work.
template <bool Feedback = true, bool Interruptible = true, typename F>
void ParallelForPatent(RapidStartGroup group, size_t begin, size_t end,
                       F &&function, size_t grain = 1,
                       unsigned initial_depth = 3,
                       patent_detail::Metrics *metrics = nullptr) {
  if (group.IsEmpty() || begin >= end)
    return;
  group.Validate();
  ThreadPool &pool = group.state->Pool();
  RegionContext *parent = CompatibleParentContext(pool, *group.state);
  DomainId domain = current_mailbox_context.state == group.state
                        ? current_mailbox_context.domain
                    : parent ? parent->domain
                             : group.domain;
  grain = std::max<size_t>(grain, 1);
  if (domain.Size() == 1 || end - begin <= grain) {
    if (metrics) {
      metrics->owner_ranges.fetch_add(1, std::memory_order_relaxed);
      metrics->private_chunks.fetch_add(1, std::memory_order_relaxed);
    }
    for (size_t i = begin; i < end && !pool.IsCancelled(); ++i)
      std::invoke(function, i);
    return;
  }
  initial_depth =
      std::min(initial_depth, partitioning::AutoPartition::max_depth);
  group.domain = domain;
  const size_t size = end - begin;
  const size_t slots = std::min(domain.Size(), (size - 1) / grain + 1);
  using Function = std::remove_reference_t<F>;
  Function &callable = function;
  patent_detail::Region region(pool, metrics);
  try {
    ParallelForRanges(
        group, 0, slots,
        [&](size_t first, size_t last) {
          for (size_t slot = first; slot < last; ++slot) {
            const size_t low =
                begin + slot * (size / slots) + std::min(slot, size % slots);
            const size_t high = begin + (slot + 1) * (size / slots) +
                                std::min(slot + 1, size % slots);
            RegionContext *active = pool.CurrentRegionContext();
            const DomainId owner_domain = active ? active->domain : domain;
            if (metrics)
              metrics->owner_ranges.fetch_add(1, std::memory_order_relaxed);
            patent_detail::Process<Feedback, Interruptible>(
                region, *group.state, callable, owner_domain,
                {low, high, grain}, 0, initial_depth);
          }
        },
        true);
  } catch (...) {
    region.Fail(std::current_exception());
  }
  region.TaskComplete();
  pool.HelpUntil(region);
  region.Rethrow();
}
} // namespace oox::detail::eigen_pool::rapid
