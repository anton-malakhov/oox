// SPDX-License-Identifier: Apache-2.0

#include "synthetic_workloads.h"
#include "common.h"

#include <algorithm>
#include <bit>
#include <numeric>
#include <random>

namespace scheduler_eval {
namespace {

std::uint64_t Work(std::size_t index, std::uint32_t cost) {
  std::uint64_t value = index + 0x9e3779b97f4a7c15ULL;
  for (std::uint32_t i = 0; i < cost; ++i)
    value = (value ^ (value >> 27)) * 0x3c79ac492ba7b653ULL + i;
  return value;
}

} // namespace

std::vector<std::uint32_t> MakeIterationCosts(CostKind kind, std::size_t size,
                                              std::uint64_t seed) {
  std::vector<std::uint32_t> costs(size);
  std::mt19937_64 random(seed);
  for (std::size_t i = 0; i < size; ++i) {
    const auto bits = random();
    switch (kind) {
    case CostKind::Constant:
      costs[i] = 64;
      break;
    case CostKind::Uniform:
      costs[i] = 1 + bits % 128;
      break;
    case CostKind::Exponential:
      costs[i] = 1u << std::min(10, std::countr_zero(bits));
      break;
    case CostKind::Pareto:
      costs[i] = 1 + 1024 / (1 + bits % 1024);
      break;
    case CostKind::Linear:
      costs[i] = 1 + 127 * i / std::max<std::size_t>(1, size - 1);
      break;
    case CostKind::Clustered:
      costs[i] = (i / 256) % 4 == 0 ? 512 : 8;
      break;
    case CostKind::Periodic:
      costs[i] = i % 64 == 0 ? 1024 : 4;
      break;
    case CostKind::Shuffled:
      costs[i] = 1 + 127 * i / std::max<std::size_t>(1, size - 1);
      break;
    case CostKind::PhaseChanging:
      costs[i] = i < size / 2 ? 8 : 256;
      break;
    }
  }
  if (kind == CostKind::Shuffled)
    std::shuffle(costs.begin(), costs.end(), std::mt19937_64(seed));
  return costs;
}

std::uint64_t RunCostLoop(const std::vector<std::uint32_t> &costs) {
  std::vector<std::uint64_t> values(costs.size());
  ParallelFor(0, costs.size(),
              [&](std::size_t i) { values[i] = Work(i, costs[i]); });
  return std::accumulate(values.begin(), values.end(), std::uint64_t{0});
}

std::uint64_t RunCostLoopSerial(const std::vector<std::uint32_t> &costs) {
  std::uint64_t result = 0;
  for (std::size_t i = 0; i < costs.size(); ++i)
    result += Work(i, costs[i]);
  return result;
}

} // namespace scheduler_eval
