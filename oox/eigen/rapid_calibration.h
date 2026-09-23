// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "rapid_mailbox.h"

namespace oox::detail::eigen_pool::rapid {

struct CalibrationOptions {
  std::chrono::nanoseconds probe_budget = std::chrono::milliseconds(50);
  double relative_tolerance = 0.05;
};

struct CalibrationTrial {
  size_t resident_limit = 0;
  size_t multiplier = 50;
  std::array<std::uint64_t, 3> nanoseconds{};
  double score = 0;
};

struct CalibrationResult {
  size_t resident_limit = 0;
  size_t multiplier = 50;
  std::uint64_t elapsed_ns = 0;
  bool budget_exhausted = false;
  bool cancelled = false;
  std::vector<CalibrationTrial> trials;
};

namespace calibration_detail {
using Clock = std::chrono::steady_clock;

// Prefer the smaller spinning cohort within the requested performance band;
// with equal cohorts, prefer a larger budget (less frequent timing/polling).
inline size_t Select(const std::vector<CalibrationTrial> &trials, double tolerance) {
  if (trials.empty()) return 0;
  double best = trials.front().score;
  for (const auto &trial : trials) best = std::min(best, trial.score);
  const double factor = 1 + tolerance;
  const double threshold = best * factor * factor * factor;
  size_t selected = trials.size();
  for (size_t i = 0; i < trials.size(); ++i) {
    if (trials[i].score > threshold) continue;
    if (selected == trials.size() ||
        trials[i].resident_limit < trials[selected].resident_limit ||
        (trials[i].resident_limit == trials[selected].resident_limit &&
         trials[i].multiplier > trials[selected].multiplier))
      selected = i;
  }
  return selected;
}

inline std::uint64_t Probe(RapidStartGroup group, MailboxHandoff policy,
                           std::vector<std::uint64_t> &output, unsigned kind) {
  const auto started = Clock::now();
  ParallelForMailbox(group, 0, output.size(), [&](size_t i) {
    std::uint64_t value = i + 1;
    const unsigned steps = kind == 0 ? 0 : kind == 1 ? 16 :
        (i < output.size() / 16 ? 128 : 4);
    for (unsigned step = 0; step < steps; ++step)
      value = (value ^ (value >> 27)) * 0x3c79ac492ba7b653ULL + step;
    output[i] = value;
  }, policy);
  std::atomic_signal_fence(std::memory_order_seq_cst);
  const auto elapsed = static_cast<std::uint64_t>(std::max<std::int64_t>(1,
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - started).count()));
  std::uint64_t checksum = 0;
  for (auto value : output) checksum ^= value;
  volatile std::uint64_t consumed = checksum;
  (void)consumed;
  return elapsed;
}
} // namespace calibration_detail

// Explicit, serialized startup operation on the full pool. Loop launches never
// invoke this routine. The budget is checked between synchronous probes: an
// in-flight probe is always joined, so OS preemption can extend wall-clock time.
// Unavailable cohorts are skipped; exhaustion keeps the best completed trial,
// or ordinary auto scheduling when no trial completed. No busy-pool exception.
inline CalibrationResult CalibrateMailbox(RapidStartGroup group,
    MailboxHandoff policy = MailboxHandoff::LocalFirst,
    CalibrationOptions options = {}) {
  using namespace calibration_detail;
  CalibrationResult result;
  if (group.IsEmpty()) return result;
  group.Validate();
  auto &pool = group.state->Pool();
  if (!pool.UsesResidentBusyWait() || group.domain.start != 0 ||
      group.domain.limit != pool.NumThreads())
    throw std::invalid_argument("calibration requires a full resident-capable pool");
  if (pool.IsExecutingTask() || current_resident_state)
    throw std::logic_error("calibration must run outside tasks and resident commands");
  if (!(options.relative_tolerance >= 0 && options.relative_tolerance <= 0.25))
    throw std::invalid_argument("calibration tolerance must be between 0 and 0.25");
  struct FallbackOnFailure {
    ThreadPool &pool;
    bool committed = false;
    ~FallbackOnFailure() {
      if (!committed) {
        pool.SetResidentLimit(0);
        pool.SetCalibrationMultiplier(50);
      }
    }
  } fallback{pool};
  const auto started = Clock::now();
  const auto expired = [&] { return Clock::now() - started >= options.probe_budget; };
  pool.SetResidentLimit(0);
  pool.SetCalibrationMultiplier(50);
  if (pool.NumThreads() > 1 && options.probe_budget.count() > 0 && !pool.IsCancelled()) {
    const size_t maximum = std::min(pool.NumThreads(), size_t{64});
    std::vector<size_t> limits{0};
    for (size_t limit = 1; limit < maximum; limit *= 2) limits.push_back(limit);
    limits.push_back(maximum);
    std::vector<std::uint64_t> output(maximum * 128);
    result.trials.reserve(limits.size() + 2);
    const auto evaluate = [&](size_t limit, size_t multiplier) {
      if (expired() || pool.IsCancelled()) return false;
      pool.SetResidentLimit(limit);
      pool.SetCalibrationMultiplier(multiplier);
      const auto ready_started = Clock::now();
      while (pool.ResidentAvailableWorkers(group.domain) < pool.ResidentCapacity(group.domain)) {
        if (expired() || Clock::now() - ready_started >= std::chrono::milliseconds(1) ||
            pool.IsCancelled()) return false;
        std::this_thread::yield();
      }
      CalibrationTrial trial{limit, multiplier};
      for (unsigned kind = 0; kind < 3; ++kind) {
        if (expired() || pool.IsCancelled()) return false;
        (void)Probe(group, policy, output, kind);
        std::array<std::uint64_t, 3> samples{};
        for (auto &sample : samples) {
          if (expired() || pool.IsCancelled()) return false;
          sample = Probe(group, policy, output, kind);
          if (pool.IsCancelled()) return false;
        }
        std::sort(samples.begin(), samples.end());
        trial.nanoseconds[kind] = samples[1];
      }
      trial.score = static_cast<double>(trial.nanoseconds[0]) *
          trial.nanoseconds[1] * trial.nanoseconds[2];
      result.trials.push_back(trial);
      return true;
    };
    for (size_t limit : limits) {
      (void)evaluate(limit, 50);
      if (expired() || pool.IsCancelled()) break;
    }
    if (!result.trials.empty()) {
      const size_t limit = result.trials[Select(result.trials, options.relative_tolerance)].resident_limit;
      if (limit) {
        (void)evaluate(limit, 20);
        (void)evaluate(limit, 100);
      }
      const auto &selected = result.trials[Select(result.trials, options.relative_tolerance)];
      result.resident_limit = selected.resident_limit;
      result.multiplier = selected.multiplier;
    }
  }
  result.cancelled = pool.IsCancelled();
  if (result.cancelled) result.resident_limit = 0;
  pool.SetResidentLimit(result.resident_limit);
  pool.SetCalibrationMultiplier(result.multiplier);
  const auto finished = Clock::now();
  result.budget_exhausted = finished - started >= options.probe_budget;
  result.elapsed_ns = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(finished - started).count());
  fallback.committed = true;
  return result;
}

} // namespace oox::detail::eigen_pool::rapid
