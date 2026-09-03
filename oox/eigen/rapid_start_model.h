// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace oox::detail::eigen_pool::rapid {

namespace model_detail {

inline size_t CeilDivide(size_t numerator, size_t denominator) noexcept {
  denominator = std::max<size_t>(denominator, 1);
  return numerator / denominator + (numerator % denominator != 0);
}

inline size_t SaturatingMultiply(size_t lhs, size_t rhs) noexcept {
  if (lhs != 0 && rhs > std::numeric_limits<size_t>::max() / lhs) {
    return std::numeric_limits<size_t>::max();
  }
  return lhs * rhs;
}

inline size_t RoundNonnegativeToSize(long double value) noexcept {
  const long double maximum =
      static_cast<long double>(std::numeric_limits<size_t>::max());
  if (!(value > 0.0L)) {
    return 0;
  }
  if (value >= maximum - 0.5L) {
    return std::numeric_limits<size_t>::max();
  }
  return static_cast<size_t>(value + 0.5L);
}

} // namespace model_detail

// ---------------------------------------------------------------------------
// Static block sizing (unchanged from the fixed-lazy policy).
// ---------------------------------------------------------------------------

inline size_t HybridBlockSize(size_t work, size_t workers, size_t grain,
                              size_t blocks_per_worker_divisor = 1) noexcept {
  workers = std::max<size_t>(workers, 1);
  blocks_per_worker_divisor =
      std::max<size_t>(blocks_per_worker_divisor, 1);
  const size_t work_per_worker = model_detail::CeilDivide(work, workers);
  // Bound scheduler metadata for short loops, then expose finer blocks only
  // when each worker has enough useful work to amortize them.
  const size_t blocks_per_worker = work_per_worker <= 8      ? 2
                                   : work_per_worker <= 64   ? 8
                                   : work_per_worker <= 4096 ? 32
                                                             : 64;
  const size_t blocks = model_detail::SaturatingMultiply(
      workers,
      std::max<size_t>(blocks_per_worker / blocks_per_worker_divisor, 1));
  const size_t target = model_detail::CeilDivide(work, blocks);
  return std::max<size_t>({target, grain, 1});
}

// ---------------------------------------------------------------------------
// Platform overhead calibration (unchanged).
// ---------------------------------------------------------------------------

inline size_t CalibratedTimespanSchedulingOverheadNs() noexcept {
  static const size_t overhead_ns = [] {
    constexpr size_t kSamples = 128;
    constexpr size_t kRounds = 5;
    std::atomic<size_t> cursor{0};
    std::atomic<size_t> published_block{1};
    size_t best = std::numeric_limits<size_t>::max();
    // Use the best batch average so preemption changes neither the estimate nor
    // every later block decision. This calibration runs once per process.
    for (size_t round = 0; round < kRounds; ++round) {
      size_t total = 0;
      for (size_t sample = 0; sample < kSamples; ++sample) {
        const auto origin = std::chrono::steady_clock::now();
        const size_t block = published_block.load(std::memory_order_relaxed);
        cursor.fetch_add(block, std::memory_order_relaxed);
        cursor.load(std::memory_order_relaxed);
        published_block.store(block, std::memory_order_relaxed);
        const auto elapsed =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - origin)
                .count();
        total += static_cast<size_t>(
            std::max<decltype(elapsed)>(elapsed, 1));
      }
      best = std::min(best,
                      model_detail::CeilDivide(total, kSamples));
    }
    return std::max<size_t>(best, 1);
  }();
  return overhead_ns;
}

inline size_t TimespanDomainSchedulingOverheadNs(size_t local_overhead_ns,
                                                 size_t workers) noexcept {
  // A stolen block can be inspected by every worker in the domain, so account
  // for the domain-wide cache/coherence opportunity rather than one claim.
  return model_detail::SaturatingMultiply(local_overhead_ns,
                                          std::max<size_t>(workers, 1));
}

// ---------------------------------------------------------------------------
// Online per-range statistics. Only the proportional owner writes; thieves
// never touch these, so no synchronization is required beyond publication of
// the derived block size.
//
// Samples are per-block mean item times. The per-item standard deviation is
// approximated as sigma_block_mean * sqrt(mean block size), which assumes
// independent items within a block. For spatially ordered work (sorted rows)
// the trend across blocks dominates that term, which is the desired signal.
// ---------------------------------------------------------------------------

struct RunningStats {
  size_t count = 0;
  long double mean = 0.0L;
  long double m2 = 0.0L;
  long double mean_weight = 0.0L; // running mean of block sizes

  void Add(long double sample, long double weight = 1.0L) noexcept {
    ++count;
    const long double delta = sample - mean;
    mean += delta / static_cast<long double>(count);
    m2 += delta * (sample - mean);
    mean_weight += (weight - mean_weight) / static_cast<long double>(count);
  }
  long double Variance() const noexcept {
    return count > 1 ? m2 / static_cast<long double>(count - 1) : 0.0L;
  }
  long double StdDev() const noexcept { return std::sqrt(Variance()); }
  // Coefficient of variation of the per-item time, see the note above.
  long double ItemCv() const noexcept {
    if (count < 2 || !(mean > 0.0L)) {
      return 0.0L;
    }
    return StdDev() * std::sqrt(std::max(mean_weight, 1.0L)) / mean;
  }
  long double ItemSigmaNs() const noexcept { return ItemCv() * mean; }
};

// ---------------------------------------------------------------------------
// Grain laws.
//
//   Fixed          HybridBlockSize static block; no timing (RAPID_LAZY_STEALING).
//   Sqrt           OOX rule tau = sqrt((P h) R / (1 + s/P)).
//   SqrtCv         Sqrt divided by sqrt(1 + cv^2): variance inflates the
//                  exposed-tail term, so blocks shrink with irregularity.
//                  Heuristic; the cv term is what FAC and TAP have and Sqrt
//                  lacks.
//   Heartbeat      tau = k * h_domain with k = 20 (Acar, Charguéraud, Rainey
//                  et al.: ~5% overhead when overhead is 1/k of work). Linear
//                  in overhead, independent of remaining work.
//   FixedSizeChunk Kruskal-Weiss FSC: K = ((sqrt2 N h)/(sigma P sqrt(ln P)))^(2/3)
//                  items. Needs sigma > 0 and P > 1, otherwise falls back to
//                  Sqrt. A 2/3-power law in (N h); the exponent the OOX rule
//                  should be tested against.
//   Factoring      FAC2 (Hummel-Schonberg-Flynn): chunk = ceil(R / (2P)).
//                  Item-based, no timing. Parameter-free.
//   Guided         GSS (Polychronopoulos-Kuck): chunk = ceil(R / P).
//                  Item-based, no timing.
// ---------------------------------------------------------------------------

enum class GrainLaw : unsigned char {
  Fixed,
  Sqrt,
  SqrtCv,
  Heartbeat,
  FixedSizeChunk,
  Factoring,
  Guided
};

constexpr bool IsTimedLaw(GrainLaw law) noexcept {
  return law == GrainLaw::Sqrt || law == GrainLaw::SqrtCv ||
         law == GrainLaw::Heartbeat || law == GrainLaw::FixedSizeChunk;
}

constexpr bool IsItemLaw(GrainLaw law) noexcept {
  return law == GrainLaw::Factoring || law == GrainLaw::Guided;
}

constexpr size_t kHeartbeatOverheadRatio = 20;

inline size_t TimespanTargetNanoseconds(size_t scheduling_overhead_ns,
                                        size_t completed, size_t elapsed_ns,
                                        size_t range_work,
                                        size_t stealing_workers,
                                        size_t workers) noexcept {
  scheduling_overhead_ns = std::max<size_t>(scheduling_overhead_ns, 1);
  if (range_work == 0 || completed == 0) {
    return scheduling_overhead_ns;
  }
  const long double projected_range_ns =
      static_cast<long double>(elapsed_ns) *
      static_cast<long double>(range_work) /
      static_cast<long double>(completed);
  const long double steal_pressure =
      1.0L + static_cast<long double>(stealing_workers) /
                   static_cast<long double>(std::max<size_t>(workers, 1));
  // Minimize H*T/tau scheduling work plus pressure*tau tail exposure.
  const long double target = std::sqrt(
      static_cast<long double>(scheduling_overhead_ns) * projected_range_ns /
      steal_pressure);
  return std::max<size_t>(model_detail::RoundNonnegativeToSize(target),
                          scheduling_overhead_ns);
}

inline size_t SqrtCvTargetNanoseconds(size_t scheduling_overhead_ns,
                                      size_t completed, size_t elapsed_ns,
                                      size_t range_work,
                                      size_t stealing_workers, size_t workers,
                                      long double item_cv) noexcept {
  const size_t base = TimespanTargetNanoseconds(
      scheduling_overhead_ns, completed, elapsed_ns, range_work,
      stealing_workers, workers);
  const long double inflation = std::sqrt(1.0L + item_cv * item_cv);
  return std::max<size_t>(
      model_detail::RoundNonnegativeToSize(
          static_cast<long double>(base) / inflation),
      std::max<size_t>(scheduling_overhead_ns, 1));
}

inline size_t HeartbeatTargetNanoseconds(
    size_t scheduling_overhead_ns,
    size_t ratio = kHeartbeatOverheadRatio) noexcept {
  return std::max<size_t>(
      model_detail::SaturatingMultiply(
          std::max<size_t>(scheduling_overhead_ns, 1),
          std::max<size_t>(ratio, 1)),
      1);
}

// Returns the FSC chunk in items, or 0 when the formula's preconditions
// (P > 1, sigma > 0, N > 0) do not hold.
inline size_t FixedSizeChunkItems(size_t work, size_t scheduling_overhead_ns,
                                  long double item_sigma_ns,
                                  size_t workers) noexcept {
  if (work == 0 || workers < 2 || !(item_sigma_ns > 0.0L)) {
    return 0;
  }
  const long double n = static_cast<long double>(work);
  const long double h =
      static_cast<long double>(std::max<size_t>(scheduling_overhead_ns, 1));
  const long double p = static_cast<long double>(workers);
  const long double numerator = std::sqrt(2.0L) * n * h;
  const long double denominator = item_sigma_ns * p * std::sqrt(std::log(p));
  if (!(denominator > 0.0L)) {
    return 0;
  }
  const long double k = std::pow(numerator / denominator, 2.0L / 3.0L);
  return std::max<size_t>(model_detail::RoundNonnegativeToSize(k), 1);
}

inline size_t FactoringChunk(size_t remaining, size_t workers,
                             size_t grain) noexcept {
  const size_t chunk =
      model_detail::CeilDivide(remaining, 2 * std::max<size_t>(workers, 1));
  return std::max<size_t>({chunk, grain, 1});
}

inline size_t GuidedChunk(size_t remaining, size_t workers,
                          size_t grain) noexcept {
  const size_t chunk =
      model_detail::CeilDivide(remaining, std::max<size_t>(workers, 1));
  return std::max<size_t>({chunk, grain, 1});
}

inline size_t TimespanBlockSize(size_t current, size_t completed,
                                size_t elapsed_ns, size_t remaining,
                                size_t grain, size_t target_ns) noexcept {
  if (remaining == 0) {
    return grain;
  }
  const long double ratio = std::clamp(
      static_cast<long double>(target_ns) /
          static_cast<long double>(std::max<size_t>(elapsed_ns, 1)),
      0.25L, 8.0L);
  const long double estimate = static_cast<long double>(completed) * ratio;
  const size_t scaled =
      std::max<size_t>(model_detail::RoundNonnegativeToSize(estimate), grain);
  const size_t growth_limit =
      model_detail::SaturatingMultiply(current, size_t{8});
  const size_t balance_limit =
      std::max(grain, model_detail::CeilDivide(remaining, size_t{4}));
  const size_t upper = std::max(grain, std::min(growth_limit, balance_limit));
  const size_t lower = std::min(
      upper,
      std::max(grain, model_detail::CeilDivide(current, size_t{4})));
  return std::clamp(scaled, lower, upper);
}

// Convert a target duration to an initial item count from a known per-item
// time. Used to skip cold-start calibration when a loop profile exists.
inline size_t ItemsForDuration(size_t target_ns, size_t item_ns,
                               size_t grain, size_t work) noexcept {
  if (item_ns == 0 || work == 0) {
    return std::max<size_t>(grain, 1);
  }
  const size_t items = model_detail::CeilDivide(target_ns, item_ns);
  return std::clamp<size_t>(std::max(items, grain), 1, work);
}

// ---------------------------------------------------------------------------
// Victim (peer range) selection for the lazy coordinator.
//
//   Linear         own+1, own+2, ... (the original behavior).
//   Hierarchical   nearest-first ring: own+1, own-1, own+2, own-2, ...
//                  Approximates hierarchical victim selection (HotSLAW HVS)
//                  because the activation tree hands out contiguous domains,
//                  so index distance is topology distance.
//   MostRemaining  pressure-aware: choose the range with the most unclaimed
//                  work. One relaxed load per range; those cursors are already
//                  scanned by HasUnclaimedWork.
// ---------------------------------------------------------------------------

enum class VictimPolicy : unsigned char { Linear, Hierarchical, MostRemaining };

// Returns the k-th candidate slot (k >= 1) for the given policy, or the total
// slot count when exhausted. Only Linear and Hierarchical are enumerable;
// MostRemaining is resolved by the caller from live cursors.
inline size_t VictimCandidate(VictimPolicy policy, size_t own, size_t k,
                              size_t slots) noexcept {
  if (slots <= 1 || k == 0 || k >= slots) {
    return slots;
  }
  if (policy == VictimPolicy::Hierarchical) {
    // k = 1 -> +1, k = 2 -> -1, k = 3 -> +2, k = 4 -> -2, ...
    const size_t distance = (k + 1) / 2;
    const bool forward = (k & 1) != 0;
    const size_t offset = forward ? distance : slots - distance;
    return (own + offset) % slots;
  }
  return (own + k) % slots;
}

// ---------------------------------------------------------------------------
// Cross-call loop profile (analogous to TBB affinity_partitioner state and
// BinLPT multiloop reuse). A caller that repeats the same loop shape passes the
// same profile object; the coordinator warms its initial block from the
// previous call's measured per-item time and cv, and writes the new estimate
// back after completion. All fields are relaxed atomics: the object is a hint,
// and a torn read only costs one suboptimal first block.
// ---------------------------------------------------------------------------

struct LoopProfile {
  std::atomic<size_t> item_ns{0};
  std::atomic<size_t> item_cv_milli{0}; // cv * 1000
  std::atomic<size_t> last_block{0};
  std::atomic<size_t> calls{0};

  bool IsWarm() const noexcept {
    return item_ns.load(std::memory_order_relaxed) != 0;
  }
  void Record(size_t new_item_ns, long double cv, size_t block) noexcept {
    if (new_item_ns == 0) {
      return;
    }
    // Exponential smoothing with 1/4 weight, like the in-loop block update.
    const size_t previous = item_ns.load(std::memory_order_relaxed);
    size_t smoothed = new_item_ns;
    if (previous != 0) {
      smoothed = new_item_ns > previous
                     ? previous + (new_item_ns - previous) / 4
                     : previous - (previous - new_item_ns) / 4;
    }
    item_ns.store(std::max<size_t>(smoothed, 1), std::memory_order_relaxed);
    item_cv_milli.store(
        model_detail::RoundNonnegativeToSize(cv * 1000.0L),
        std::memory_order_relaxed);
    last_block.store(block, std::memory_order_relaxed);
    calls.fetch_add(1, std::memory_order_relaxed);
  }
  long double Cv() const noexcept {
    return static_cast<long double>(
               item_cv_milli.load(std::memory_order_relaxed)) /
           1000.0L;
  }
};

// A small open-addressed table keyed by an opaque loop-site hash. Useful when
// the caller cannot keep a profile object per site (e.g. a benchmark adapter).
template <size_t Capacity> class LoopProfileTable {
public:
  static_assert(Capacity != 0 && (Capacity & (Capacity - 1)) == 0,
                "capacity must be a power of two");

  LoopProfile *Get(size_t key) noexcept {
    key = key == 0 ? 1 : key;
    size_t index = Mix(key) & (Capacity - 1);
    for (size_t probe = 0; probe < Capacity; ++probe) {
      std::atomic<size_t> &slot_key = keys_[index];
      const size_t current = slot_key.load(std::memory_order_acquire);
      if (current == key) {
        return &profiles_[index];
      }
      if (current == 0) {
        size_t expected = 0;
        if (slot_key.compare_exchange_strong(expected, key,
                                             std::memory_order_acq_rel,
                                             std::memory_order_acquire) ||
            expected == key) {
          return &profiles_[index];
        }
      }
      index = (index + 1) & (Capacity - 1);
    }
    return nullptr;
  }

private:
  static size_t Mix(size_t value) noexcept {
    uint64_t x = static_cast<uint64_t>(value);
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return static_cast<size_t>(x);
  }
  std::atomic<size_t> keys_[Capacity] = {};
  LoopProfile profiles_[Capacity];
};

} // namespace oox::detail::eigen_pool::rapid
