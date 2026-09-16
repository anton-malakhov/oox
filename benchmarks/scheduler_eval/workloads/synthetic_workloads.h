// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace scheduler_eval {

enum class CostKind {
  Constant,
  Uniform,
  Exponential,
  Pareto,
  Linear,
  Clustered,
  Periodic,
  Shuffled,
  PhaseChanging
};

std::vector<std::uint32_t> MakeIterationCosts(CostKind kind, std::size_t size,
                                              std::uint64_t seed = 1);
std::uint64_t RunCostLoop(const std::vector<std::uint32_t> &costs);
std::uint64_t RunCostLoopSerial(const std::vector<std::uint32_t> &costs);

} // namespace scheduler_eval
