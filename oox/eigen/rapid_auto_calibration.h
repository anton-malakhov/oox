// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "rapid_auto.h"
#include <chrono>

namespace oox::detail::eigen_pool::rapid {

struct AutoCalibrationOptions {
  std::chrono::nanoseconds probe_budget = std::chrono::milliseconds(50);
  double relative_tolerance = 0.05;
};
struct AutoCalibrationTrial {
  size_t resident_limit = 0;
  std::array<std::uint64_t, 3> nanoseconds{};
};
struct AutoCalibrationResult {
  size_t resident_limit = 0;
  std::uint64_t elapsed_ns = 0;
  bool budget_exhausted = false, cancelled = false;
  std::vector<AutoCalibrationTrial> trials;
};

namespace auto_calibration_detail {
using Clock = std::chrono::steady_clock;

inline size_t Select(const std::vector<AutoCalibrationTrial> &trials, double tolerance) {
  if (trials.empty()) return 0;
  const auto score = [](const auto &trial) {
    return static_cast<double>(trial.nanoseconds[0]) * trial.nanoseconds[1] * trial.nanoseconds[2];
  };
  double best = score(trials.front());
  for (const auto &trial : trials) best = std::min(best, score(trial));
  const double factor = 1 + tolerance;
  size_t selected = trials.size();
  for (size_t i = 0; i < trials.size(); ++i)
    if (score(trials[i]) <= best * factor * factor * factor &&
        (selected == trials.size() || trials[i].resident_limit < trials[selected].resident_limit))
      selected = i;
  return selected;
}

inline std::uint64_t Probe(RapidStartGroup group,
                          std::vector<std::uint64_t> &output, unsigned kind) {
  const auto started = Clock::now();
  ParallelForAuto(group, 0, output.size(), [&](size_t i) {
    std::uint64_t value = i + 1;
    const bool edge = i < output.size() / 32 || i >= output.size() - output.size() / 32;
    const unsigned steps = kind == 0 ? 0 : kind == 1 ? 16 : edge ? 256 : 4;
    for (unsigned step = 0; step < steps; ++step)
      value = (value ^ (value >> 27)) * 0x3c79ac492ba7b653ULL + step;
    output[i] = value;
  });
  std::atomic_signal_fence(std::memory_order_seq_cst);
  const auto elapsed = static_cast<std::uint64_t>(std::max<std::int64_t>(1,
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - started).count()));
  std::uint64_t checksum = 0;
  for (auto value : output) checksum ^= value;
  volatile std::uint64_t consumed = checksum;
  (void)consumed;
  return elapsed;
}
} // namespace auto_calibration_detail

// Explicit, serialized startup operation, never called by loop launches.
// It measures this Auto kernel, not the timed-chunk mailbox controller. The
// soft budget is checked between joined probes; scheduling delays can extend it.
inline AutoCalibrationResult CalibrateAutoGroup(RapidStartGroup group,
    AutoCalibrationOptions options = {}) {
  using namespace auto_calibration_detail;
  AutoCalibrationResult result;
  if (group.IsEmpty()) return result;
  group.Validate();
  auto &pool = group.state->Pool();
  if (!pool.UsesResidentBusyWait() || group.domain.start != 0 || group.domain.limit != pool.NumThreads())
    throw std::invalid_argument("Auto calibration requires the full resident-capable pool");
  if (pool.IsExecutingTask() || current_resident_state)
    throw std::logic_error("Auto calibration requires a quiet startup/control context");
  if (!(options.relative_tolerance >= 0 && options.relative_tolerance <= 0.25))
    throw std::invalid_argument("Auto calibration tolerance must be between 0 and 0.25");
  struct Fallback {
    ThreadPool &pool;
    bool committed = false;
    ~Fallback() { if (!committed) pool.SetResidentLimit(0); }
  } fallback{pool};
  const auto started = Clock::now();
  const auto expired = [&] { return Clock::now() - started >= options.probe_budget; };
  pool.SetResidentLimit(0);
  if (pool.NumThreads() > 1 && options.probe_budget.count() > 0 && !pool.IsCancelled()) {
    const size_t maximum = std::min(pool.NumThreads(), size_t{64});
    std::vector<size_t> limits;
    // Intermediate sizes matter on heterogeneous/loaded CPUs; powers of two
    // alone can miss a useful cohort between half and all of the workers.
    for (size_t limit = 0; limit <= std::min(maximum, size_t{16}); ++limit) limits.push_back(limit);
    for (size_t limit = 32; limit <= maximum; limit *= 2) limits.push_back(limit);
    if (limits.back() != maximum) limits.push_back(maximum);
    std::vector<std::uint64_t> output(std::max(size_t{512}, maximum * 128));
    result.trials.reserve(limits.size());
    for (size_t limit : limits) {
      if (expired() || pool.IsCancelled()) break;
      pool.SetResidentLimit(limit);
      const auto ready_started = Clock::now();
      while (pool.ResidentAvailableWorkers(group.domain) < pool.ResidentCapacity(group.domain)) {
        if (expired() || pool.IsCancelled() || Clock::now() - ready_started >= std::chrono::milliseconds(1)) break;
        std::this_thread::yield();
      }
      if (pool.ResidentAvailableWorkers(group.domain) < pool.ResidentCapacity(group.domain)) continue;
      AutoCalibrationTrial trial{limit};
      bool complete = true;
      for (unsigned kind = 0; kind < 3 && complete; ++kind) {
        if (expired() || pool.IsCancelled()) { complete = false; break; }
        (void)Probe(group, output, kind);
        std::array<std::uint64_t, 3> samples{};
        for (auto &sample : samples) {
          if (expired() || pool.IsCancelled()) { complete = false; break; }
          sample = Probe(group, output, kind);
          if (pool.IsCancelled()) { complete = false; break; }
        }
        std::sort(samples.begin(), samples.end());
        trial.nanoseconds[kind] = samples[1];
      }
      if (complete) result.trials.push_back(trial);
    }
    if (!result.trials.empty())
      result.resident_limit = result.trials[Select(result.trials, options.relative_tolerance)].resident_limit;
  }
  result.cancelled = pool.IsCancelled();
  if (result.cancelled) result.resident_limit = 0;
  pool.SetResidentLimit(result.resident_limit);
  const auto elapsed = Clock::now() - started;
  result.elapsed_ns = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
  result.budget_exhausted = elapsed >= options.probe_budget;
  fallback.committed = true;
  return result;
}

} // namespace oox::detail::eigen_pool::rapid
