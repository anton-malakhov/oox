// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "benchmarks/eigen/resident_test_support.h"
#include "benchmarks/eigen/eigen_pool.h"
#include "benchmarks/eigen/thread_index.h"
#include "benchmarks/eigen/util.h"
#include "oox/eigen/rapid_auto.h"
#include "oox/eigen/rapid_auto_calibration.h"

namespace rapid_auto_eval {
namespace rapid = oox::detail::eigen_pool::rapid;

class Runtime {
public:
  Runtime() : state_(EigenPool()),
      group_{&state_, {0, static_cast<unsigned>(GetNumThreads())}} {
    bool automatic = EIGEN_MODE == EIGEN_RAPID_AUTO;
    if (const char *value = std::getenv("OOX_RAPID_AUTOCALIBRATE"))
      automatic = automatic && benchmark_arguments::Number<unsigned>(value,
          "OOX_RAPID_AUTOCALIBRATE", 0, 1) != 0;
    if (const char *value = std::getenv("OOX_RAPID_RESIDENT_LIMIT")) {
      automatic = false;
      EigenPool().SetResidentLimit(benchmark_arguments::Number<unsigned>(value,
          "OOX_RAPID_RESIDENT_LIMIT", 0, GetNumThreads()));
    }
    if (automatic)
      calibration_ = rapid::CalibrateAutoGroup(group_);
    else {
      eigen_test_support::WaitForResidentWorkers(group_, std::chrono::seconds(5));
      calibration_.resident_limit = EigenPool().ResidentLimit();
    }
  }
  const rapid::AutoCalibrationResult &Calibration() const noexcept { return calibration_; }
  template <typename F>
  void Run(std::size_t first, std::size_t last, F &&function, std::size_t grain) {
#if EIGEN_MODE == EIGEN_AUTO_RESIDENT
    oox::detail::eigen_pool::ParallelFor(
        EigenPool(), first, last, std::forward<F>(function), grain);
#else
    rapid::ParallelForAuto(group_, first, last, std::forward<F>(function), grain);
#endif
  }
private:
  rapid::RapidDomainState state_;
  rapid::RapidStartGroup group_;
  rapid::AutoCalibrationResult calibration_;
};

inline Runtime &GetRuntime() {
  static Runtime runtime;
  return runtime;
}
} // namespace rapid_auto_eval

template <typename F>
void ParallelFor(std::size_t first, std::size_t last, F &&function,
                 std::size_t grain = 1) {
  rapid_auto_eval::GetRuntime().Run(first, last, std::forward<F>(function), grain);
}

inline void InitParallel(std::size_t threads) {
  if (threads != static_cast<std::size_t>(GetNumThreads()))
    throw std::invalid_argument("Rapid auto worker-count mismatch");
  rapid_auto_eval::GetRuntime();
}
