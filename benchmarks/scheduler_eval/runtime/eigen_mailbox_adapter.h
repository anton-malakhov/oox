// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "benchmarks/eigen/resident_test_support.h"
#include "benchmarks/eigen/eigen_pool.h"
#include "benchmarks/eigen/thread_index.h"
#include "benchmarks/eigen/util.h"
#include "oox/eigen/rapid_calibration.h"

namespace rapid_mailbox_eval {
namespace rapid = oox::detail::eigen_pool::rapid;

class Runtime {
public:
  Runtime() : state_(EigenPool()),
      group_{&state_, {0, static_cast<unsigned>(GetNumThreads())}} {
    if (const char *value = std::getenv("OOX_RAPID_MAILBOX_PREFER_NONMEMBERS"))
      prefer_nonmembers_ = benchmark_arguments::Number<unsigned>(
          value, "OOX_RAPID_MAILBOX_PREFER_NONMEMBERS", 0, 1) != 0;
    bool automatic = true;
    if (const char *value = std::getenv("OOX_RAPID_AUTOCALIBRATE"))
      automatic = benchmark_arguments::Number<unsigned>(value,
          "OOX_RAPID_AUTOCALIBRATE", 0, 1) != 0;
    if (const char *value = std::getenv("OOX_RAPID_RESIDENT_LIMIT")) {
      automatic = false;
      EigenPool().SetResidentLimit(benchmark_arguments::Number<unsigned>(value,
          "OOX_RAPID_RESIDENT_LIMIT", 0, GetNumThreads()));
    }
    if (automatic)
      calibration_ = rapid::CalibrateMailbox(group_, policy);
    else {
      eigen_test_support::WaitForResidentWorkers(group_, std::chrono::seconds(5));
      calibration_.resident_limit = EigenPool().ResidentLimit();
      calibration_.multiplier = EigenPool().CalibrationMultiplier();
    }
    Run(0, GetNumThreads(), [](std::size_t) {}, 1);
  }

  const rapid::CalibrationResult &Calibration() const noexcept { return calibration_; }

  template <typename F>
  void Run(std::size_t first, std::size_t last, F &&function, std::size_t grain) {
    rapid::ParallelForMailbox(group_, first, last, std::forward<F>(function),
                             policy, prefer_nonmembers_, grain);
  }
private:
#if EIGEN_MODE == EIGEN_RAPID_MAILBOX_EAGER
  static constexpr auto policy = rapid::MailboxHandoff::Immediate;
#else
  static constexpr auto policy = rapid::MailboxHandoff::LocalFirst;
#endif
  rapid::RapidDomainState state_;
  rapid::RapidStartGroup group_;
  rapid::CalibrationResult calibration_;
  bool prefer_nonmembers_ = false;
};

inline Runtime &GetRuntime() {
  static Runtime runtime;
  return runtime;
}
} // namespace rapid_mailbox_eval

template <typename F>
void ParallelFor(std::size_t first, std::size_t last, F &&function,
                 std::size_t grain = 1) {
  rapid_mailbox_eval::GetRuntime().Run(first, last, std::forward<F>(function), grain);
}

inline void InitParallel(std::size_t threads) {
  if (threads != static_cast<std::size_t>(GetNumThreads()))
    throw std::invalid_argument("Rapid mailbox worker-count mismatch");
  rapid_mailbox_eval::GetRuntime();
}
