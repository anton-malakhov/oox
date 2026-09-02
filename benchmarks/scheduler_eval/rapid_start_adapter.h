// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "benchmarks/eigen/eigen_pool.h"
#include "benchmarks/eigen/thread_index.h"
#include "benchmarks/eigen/util.h"
#include "oox/eigen/rapid_start.h"

#include <cstddef>
#include <stdexcept>
#include <typeinfo>
#include <utility>

#ifndef OOX_RAPID_TIMESPAN_TARGET_NS
#define OOX_RAPID_TIMESPAN_TARGET_NS 0
#endif

namespace rapid_start_eval {

namespace rapid = oox::detail::eigen_pool::rapid;

// Compile-time policy selection. Every lazy-stealing variant shares one entry
// point; the mode only chooses the grain law, the victim order, and whether a
// cross-call loop profile is consulted.
struct Policy {
  rapid::GrainLaw law;
  rapid::VictimPolicy victims;
  bool profiled;
};

constexpr Policy SelectPolicy() {
#if EIGEN_MODE == EIGEN_RAPID_LAZY_STEALING
  return {rapid::GrainLaw::Fixed, rapid::VictimPolicy::Linear, false};
#elif EIGEN_MODE == EIGEN_RAPID_TIMESPAN_LAZY_STEALING
  return {rapid::GrainLaw::Sqrt, rapid::VictimPolicy::Linear, false};
#elif EIGEN_MODE == EIGEN_RAPID_SQRTCV_LAZY
  return {rapid::GrainLaw::SqrtCv, rapid::VictimPolicy::Linear, false};
#elif EIGEN_MODE == EIGEN_RAPID_HEARTBEAT_LAZY
  return {rapid::GrainLaw::Heartbeat, rapid::VictimPolicy::Linear, false};
#elif EIGEN_MODE == EIGEN_RAPID_FSC_LAZY
  return {rapid::GrainLaw::FixedSizeChunk, rapid::VictimPolicy::Linear, false};
#elif EIGEN_MODE == EIGEN_RAPID_FACTORING_LAZY
  return {rapid::GrainLaw::Factoring, rapid::VictimPolicy::Linear, false};
#elif EIGEN_MODE == EIGEN_RAPID_GUIDED_LAZY
  return {rapid::GrainLaw::Guided, rapid::VictimPolicy::Linear, false};
#elif EIGEN_MODE == EIGEN_RAPID_TIMESPAN_LAZY_PROFILED
  return {rapid::GrainLaw::Sqrt, rapid::VictimPolicy::Linear, true};
#elif EIGEN_MODE == EIGEN_RAPID_LAZY_HIERARCHICAL
  return {rapid::GrainLaw::Fixed, rapid::VictimPolicy::Hierarchical, false};
#elif EIGEN_MODE == EIGEN_RAPID_LAZY_PRESSURE
  return {rapid::GrainLaw::Fixed, rapid::VictimPolicy::MostRemaining, false};
#else
  return {rapid::GrainLaw::Fixed, rapid::VictimPolicy::Linear, false};
#endif
}

constexpr Policy kPolicy = SelectPolicy();

class Runtime {
public:
  explicit Runtime(std::size_t threads)
      : threads_(Validate(threads)), state_(EigenPool()),
        group_{&state_, {0, static_cast<unsigned>(threads_)}} {
    Run(0, threads_, [](std::size_t) {});
  }

  template <typename F>
  void Run(std::size_t from, std::size_t to, F &&func,
           std::size_t grain = 1) {
    if (from >= to) {
      return;
    }
#if EIGEN_MODE == EIGEN_RAPID
    rapid::ParallelFor(group_, from, to, std::forward<F>(func));
#elif EIGEN_MODE == EIGEN_RAPID_MAILBOX
    rapid::ParallelForMailbox(group_, from, to, std::forward<F>(func), grain);
#elif EIGEN_MODE == EIGEN_RAPID_LAZY_STEALING ||                               \
    EIGEN_MODE == EIGEN_RAPID_TIMESPAN_LAZY_STEALING ||                        \
    EIGEN_MODE == EIGEN_RAPID_SQRTCV_LAZY ||                                   \
    EIGEN_MODE == EIGEN_RAPID_HEARTBEAT_LAZY ||                                \
    EIGEN_MODE == EIGEN_RAPID_FSC_LAZY ||                                      \
    EIGEN_MODE == EIGEN_RAPID_FACTORING_LAZY ||                                \
    EIGEN_MODE == EIGEN_RAPID_GUIDED_LAZY ||                                   \
    EIGEN_MODE == EIGEN_RAPID_TIMESPAN_LAZY_PROFILED ||                        \
    EIGEN_MODE == EIGEN_RAPID_LAZY_HIERARCHICAL ||                             \
    EIGEN_MODE == EIGEN_RAPID_LAZY_PRESSURE
    rapid::LoopProfile *profile = nullptr;
    if constexpr (kPolicy.profiled) {
      // Key by callable type and trip count: a stand-in for a call site.
      const std::size_t trip_key =
          (to - from) * static_cast<std::size_t>(0x9e3779b97f4a7c15ULL);
      profile = profiles_.Get(typeid(F).hash_code() ^ trip_key);
    }
    rapid::ParallelForLazyStealingPolicy<kPolicy.law, kPolicy.victims>(
        group_, from, to, std::forward<F>(func), grain,
        OOX_RAPID_TIMESPAN_TARGET_NS, profile);
#else
#error "Unsupported Rapid Start policy"
#endif
  }

private:
  static std::size_t Validate(std::size_t threads) {
    if (threads == 0 || threads >= (std::size_t{1} << 16))
      throw std::invalid_argument(
          "Rapid Start requires between 1 and 65535 workers");
    return threads;
  }

  std::size_t threads_;
  rapid::RapidDomainState state_;
  rapid::RapidStartGroup group_;
  rapid::LoopProfileTable<256> profiles_;
};

inline Runtime &GetRuntime() {
  static Runtime runtime(static_cast<std::size_t>(GetNumThreads()));
  return runtime;
}

} // namespace rapid_start_eval

template <typename F>
void ParallelFor(std::size_t from, std::size_t to, F &&func,
                 std::size_t grain = 1) {
  rapid_start_eval::GetRuntime().Run(from, to, std::forward<F>(func), grain);
}

inline void InitParallel(std::size_t threads) {
  if (threads != static_cast<std::size_t>(GetNumThreads()))
    throw std::invalid_argument("Rapid Start worker-count mismatch");
  rapid_start_eval::GetRuntime();
}
