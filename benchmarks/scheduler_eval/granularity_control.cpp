// SPDX-License-Identifier: MIT
// Adapted for OOX from Deepsea SPTL's spestimator.hpp.

#include "granularity_control.h"

namespace scheduler_eval {

GranularityEstimator::GranularityEstimator(std::chrono::nanoseconds kappa,
                                           double alpha)
    : kappa_(kappa), alpha_(alpha) {}

bool GranularityEstimator::IsSmall(std::size_t complexity) const {
  return static_cast<double>(complexity) <=
         alpha_ *
             maximum_sequential_complexity_.load(std::memory_order_relaxed);
}

void GranularityEstimator::Report(std::size_t complexity,
                                  std::chrono::nanoseconds elapsed) {
  if (elapsed > kappa_)
    return;
  auto current = maximum_sequential_complexity_.load(std::memory_order_relaxed);
  while (current < complexity &&
         !maximum_sequential_complexity_.compare_exchange_weak(
             current, complexity, std::memory_order_relaxed)) {
  }
}

std::size_t GranularityEstimator::SequentialComplexityLimit() const {
  return maximum_sequential_complexity_.load(std::memory_order_relaxed);
}

} // namespace scheduler_eval
