// SPDX-License-Identifier: MIT
// Adapted for OOX from Deepsea SPTL's spestimator.hpp.

#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>

namespace scheduler_eval {

class GranularityEstimator {
public:
  explicit GranularityEstimator(std::chrono::nanoseconds kappa,
                                double alpha = 1.8);

  bool IsSmall(std::size_t complexity) const;
  void Report(std::size_t complexity, std::chrono::nanoseconds elapsed);
  std::size_t SequentialComplexityLimit() const;

private:
  std::chrono::nanoseconds kappa_;
  double alpha_;
  std::atomic<std::size_t> maximum_sequential_complexity_{1};
};

} // namespace scheduler_eval
