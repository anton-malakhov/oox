// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "test_support/eigen_resident.h"
#include "benchmarks/eigen/eigen_pool.h"
#include "benchmarks/eigen/thread_index.h"
#include "benchmarks/eigen/util.h"
#include "oox/eigen/rapid_parallel_for.h"

// Opt-in paired probes: identical resident-capable pool, workload and policy.
// Only the initial activation mechanism differs. No automatic calibration.
namespace rapid_policy_eval {
namespace eigen = oox::detail::eigen_pool;
class Runtime {
public:
  Runtime() : state_(EigenPool()),
      group_{&state_, {0, static_cast<unsigned>(GetNumThreads())}} {
    if (const char *value = std::getenv("OOX_RAPID_RESIDENT_LIMIT"))
      EigenPool().SetResidentLimit(benchmark_arguments::Number<unsigned>(value,
          "OOX_RAPID_RESIDENT_LIMIT", 0, GetNumThreads()));
    eigen_test_support::WaitForResidentWorkers(group_, std::chrono::seconds(5));
  }
  template <typename F>
  void Run(std::size_t first, std::size_t last, F &&function, std::size_t grain) {
#if OOX_RAPID_POLICY_ACTIVATE
    eigen::rapid::ParallelFor(group_, first, last, std::forward<F>(function), policy_, grain);
#else
    eigen::ParallelFor(EigenPool(), first, last, std::forward<F>(function), policy_, grain);
#endif
  }
private:
  eigen::rapid::RapidDomainState state_;
  eigen::rapid::RapidStartGroup group_;
  eigen::OOX_RAPID_POLICY policy_;
};
inline Runtime &GetRuntime() { static Runtime runtime; return runtime; }
} // namespace rapid_policy_eval

template <typename F>
void ParallelFor(std::size_t first, std::size_t last, F &&function, std::size_t grain = 1) {
  rapid_policy_eval::GetRuntime().Run(first, last, std::forward<F>(function), grain);
}
inline void InitParallel(std::size_t threads) {
  if (threads != static_cast<std::size_t>(GetNumThreads()))
    throw std::invalid_argument("Rapid policy worker-count mismatch");
  rapid_policy_eval::GetRuntime();
}
