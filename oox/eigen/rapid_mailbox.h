// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "rapid_start.h"
#include "parallel_for.h"

namespace oox::detail::eigen_pool::rapid {

enum class MailboxHandoff { Immediate, LocalFirst };

namespace mailbox_detail {

inline std::uint64_t AdaptiveBudget(ThreadPool &pool) noexcept {
  using Clock = std::chrono::steady_clock;
  static thread_local uint64_t calibrated_pool = 0;
  static thread_local size_t calibrated_limit = 0;
  static thread_local std::uint64_t check_cost_ns = 0;
  const size_t limit = pool.ResidentLimit();
  if (calibrated_pool != pool.Generation() || calibrated_limit != limit) {
    std::uint64_t minimum = std::numeric_limits<std::uint64_t>::max();
    for (unsigned sample = 0; sample < 8; ++sample) {
      const auto begin = Clock::now();
      for (unsigned probe = 0; probe < 16; ++probe) {
        (void)Clock::now();
        (void)pool.HasIdleWorker();
        (void)Clock::now();
      }
      const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
          Clock::now() - begin).count();
      minimum = std::min(minimum, static_cast<std::uint64_t>(
          std::max<std::int64_t>(1, elapsed / 16)));
    }
    check_cost_ns = minimum;
    calibrated_limit = limit;
    calibrated_pool = pool.Generation();
  }
  const size_t multiplier = pool.CalibrationMultiplier();
  return std::min(check_cost_ns, std::numeric_limits<std::uint64_t>::max() /
      multiplier) * multiplier;
}

inline size_t AdaptiveChunk(size_t previous, size_t executed,
                           std::uint64_t elapsed_ns, std::uint64_t target_ns,
                           size_t grain, size_t remaining) noexcept {
  if (!remaining) return 0;
  const size_t limit = previous > remaining / 4 ? remaining : previous * 4;
  const double estimate = static_cast<double>(executed) * target_ns /
                          std::max<std::uint64_t>(1, elapsed_ns);
  const size_t proposed = estimate >= static_cast<double>(limit)
      ? limit : std::max(grain, static_cast<size_t>(estimate));
  return std::min(remaining, proposed);
}

template <typename F>
void ProcessSeed(partitioner_detail::Region &, F *, partitioner_detail::LoopRange) noexcept;

// A queued seed owns a region reference and transfers it to its adaptive tree
// when executed. Discarding a seed never touches the borrowed user callback.
template <typename F>
class Seed final : public Task, public SmallObjectAllocated<Seed<F>> {
public:
  Seed(partitioner_detail::Region &region, F &function,
       size_t begin, size_t end, size_t grain) noexcept
      : region_(region), function_(&function), range_(begin, end, grain) {
    region_.AddTask();
  }
  void operator()() noexcept final {
    // Keep the borrowed callback opaque until ProcessSeed acquires its lease.
    ProcessSeed(region_, function_, range_);
    // Process may have released the last region reference: do not use it here.
    DeleteSmallObject(this);
  }
  void Discard() noexcept final {
    auto *region = &region_;
    DeleteSmallObject(this);
    region->TaskComplete();
  }
private:
  partitioner_detail::Region &region_;
  F *function_;
  partitioner_detail::LoopRange range_;
};

template <typename F>
void ProcessSeed(partitioner_detail::Region &region, F *function,
                 partitioner_detail::LoopRange range) noexcept {
  {
    partitioner_detail::WorkLease work(region);
    if (work) try {
      const auto target_ns = AdaptiveBudget(region.pool);
      size_t chunk = range.grain;
      while (range.begin < range.end && !region.IsCancelled()) {
        const size_t size = range.end - range.begin;
        if (size > range.grain && size / 2 >= chunk && region.pool.HasIdleWorker()) {
          const size_t middle = range.begin + size / 2;
          auto *child = NewSmallObject<Seed<F>>(
              region, *function, middle, range.end, range.grain);
          range.end = middle;
          region.pool.Schedule(child);
          continue;
        }
        const size_t stop = range.begin + std::min(size, chunk);
        const auto started = std::chrono::steady_clock::now();
        for (size_t i = range.begin; i < stop; ++i)
          std::invoke(*function, i);
        const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - started).count();
        chunk = AdaptiveChunk(chunk, stop - range.begin,
            static_cast<std::uint64_t>(std::max<std::int64_t>(0, elapsed)),
            target_ns, range.grain, range.end - stop);
        range.begin = stop;
      }
    } catch (...) {
      region.Fail(std::current_exception());
    }
  }
  // The lease ends before releasing a potentially final region reference.
  region.TaskComplete();
}

template <typename F> class Launch final : public ResidentTask {
public:
  Launch(RapidDomainState &state, partitioner_detail::Region &region,
         F &function, size_t begin, size_t end, size_t grain,
         const unsigned *workers, size_t helpers, MailboxHandoff policy,
         bool prefer_nonmembers) noexcept
      : state_(state), region_(region), function_(function), begin_(begin),
        size_(end - begin), grain_(grain), slots_(helpers + 1), policy_(policy),
        prefer_nonmembers_(prefer_nonmembers), remaining_(helpers),
        producers_(helpers + 1) {
    for (size_t i = 0; i < helpers; ++i)
      members_[member_count_++] = workers[i];
    const auto caller = state.Pool().CurrentThreadId();
    size_t priority_count = 0;
    if (caller < state.Pool().NumThreads())
      priority_[priority_count++] = static_cast<unsigned>(caller);
    for (size_t i = 0; i < helpers; ++i)
      priority_[priority_count++] = workers[i];
    if (caller < state.Pool().NumThreads())
      members_[member_count_++] = static_cast<unsigned>(caller);
    std::sort(members_.begin(), members_.begin() + member_count_);
    units_ = state.Pool().NumThreads() +
        (prefer_nonmembers_ ? state.Pool().NumThreads() - member_count_ : 0);
  }

  bool IsComplete() const noexcept {
    return remaining_.load(std::memory_order_acquire) == 0;
  }
  std::atomic<size_t> &CompletionCounter() noexcept { return remaining_; }

  void Deregister(size_t slot) noexcept final {
    if (active_[slot]) {
      active_[slot] = false;
      if (current_resident_state == &state_)
        current_resident_state = nullptr;
    }
  }

  void Run(size_t slot) noexcept final {
    ScopedResidentState context(state_);
    active_[slot] = true;
    auto &pool = state_.Pool();
    size_t direct_first = begin_, direct_last = begin_;
    try {
      for (size_t target = slot; target < pool.NumThreads() &&
           !region_.IsCancelled(); target += slots_) {
        const size_t worker = TargetWorker(target);
        const bool captured = target < member_count_;
        const size_t first_unit = target +
            (prefer_nonmembers_ ? target - std::min(target, member_count_) : 0);
        const size_t count = prefer_nonmembers_ && !captured ? 2 : 1;
        for (size_t unit = first_unit; unit < first_unit + count; ++unit) {
          const auto first = Boundary(unit), last = Boundary(unit + 1);
          if (first != last) {
            if (worker == pool.CurrentThreadId()) {
              direct_first = first;
              direct_last = last;
              continue;
            }
            pool.RunOnThread(NewSmallObject<Seed<F>>(
                region_, function_, first, last, grain_), worker);
          }
        }
      }
    } catch (...) {
      region_.Fail(std::current_exception());
    }
    producers_.fetch_sub(1, std::memory_order_release);
    if (policy_ == MailboxHandoff::Immediate)
      pool.DeregisterResident();
    if (direct_first != direct_last) {
      region_.AddTask();
      pool.RunInlineWork([&] {
        ProcessSeed(region_, &function_,
            partitioner_detail::LoopRange(direct_first, direct_last, grain_));
      });
    }
    if (policy_ == MailboxHandoff::LocalFirst) {
      while (active_[slot] && !region_.IsCancelled()) {
        if (pool.TryExecuteLocalTask())
          continue;
        if (producers_.load(std::memory_order_acquire) == 0)
          break;
        ThreadPool::RelaxResidentWait();
      }
    }
    // Saturation or a nested wait may already have ended participation.
    pool.DeregisterResident();
    Deregister(slot);
  }

private:
  size_t TargetWorker(size_t target) const noexcept {
    if (target < member_count_)
      return priority_[target];
    size_t worker = target - member_count_;
    for (size_t i = 0; i < member_count_ && members_[i] <= worker; ++i)
      ++worker;
    return worker;
  }
  size_t Boundary(size_t unit) const noexcept {
    return begin_ + (size_ / units_) * unit + std::min(unit, size_ % units_);
  }
  RapidDomainState &state_;
  partitioner_detail::Region &region_;
  F &function_;
  size_t begin_, size_, grain_, slots_;
  MailboxHandoff policy_;
  bool prefer_nonmembers_;
  std::array<unsigned, 64> members_{};
  std::array<unsigned, 64> priority_{};
  size_t member_count_ = 0, units_ = 0;
  // Each flag is accessed only by its executing participant, including the
  // pool's before-steal hook. Completion pins the descriptor until it returns.
  std::array<bool, 64> active_{};
  std::atomic<size_t> remaining_;
  std::atomic<size_t> producers_;
};

} // namespace mailbox_detail

// The domain limits Rapid producers, not placement: ordinary mailbox work is
// distributed pool-wide and may be stolen. Nonmember preference doubles both
// the seed count and initial work share for each uncaptured worker (opt-in).
template <typename F>
void ParallelForMailbox(RapidStartGroup group, size_t begin, size_t end,
                        F &&function, MailboxHandoff policy,
                        bool prefer_nonmembers = false, size_t grain = 1) {
  if (group.IsEmpty() || begin >= end)
    return;
  group.Validate();
  auto &pool = group.state->Pool();
  if (!pool.UsesResidentBusyWait())
    throw std::invalid_argument("Rapid mailbox handoff requires a resident-busy pool");
  if (pool.IsCancelled())
    return;
  grain = std::max(grain, size_t{1});
  // Task ancestry survives deregistration: recursively activating groups
  // while helping mailbox seeds can otherwise exhaust the worker stack.
  if (current_resident_state == group.state || pool.IsExecutingTask() ||
      pool.ResidentLimit() == 0 || pool.NumThreads() == 1 || end - begin <= grain) {
    return oox::detail::eigen_pool::ParallelFor(
        pool, begin, end, std::forward<F>(function), grain);
  }
  const auto release = [](partitioner_detail::Region *region) {
    region->TaskComplete();
  };
  // Allocate before claiming workers: every captured command must be published.
  std::unique_ptr<partitioner_detail::Region, decltype(release)> region(
      NewSmallObject<partitioner_detail::Region>(pool, nullptr, true), release);
  const size_t slots = std::min({end - begin, group.domain.Size(), size_t{64}});
  std::array<unsigned, 63> workers{};
  const size_t helpers = pool.ClaimResidentWorkers(
      group.domain, workers.data(), slots - 1);
  mailbox_detail::Launch<std::remove_reference_t<F>> launch(
      *group.state, *region, function, begin, end, std::max(grain, size_t{1}),
      workers.data(), helpers, policy, prefer_nonmembers);
  for (size_t i = 0; i < helpers; ++i)
    pool.PublishResident(launch, launch.CompletionCounter(), workers[i], i + 1, true);
  pool.RunResidentProducer(launch, 0);
  pool.HelpResidentUntil(launch);
  pool.Wait([&] { return region->IsComplete(); });
  region->CloseAndWait();
  region->Rethrow();
}

} // namespace oox::detail::eigen_pool::rapid
