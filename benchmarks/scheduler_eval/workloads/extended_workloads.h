// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "primary_workloads.h"

#include <string>

namespace scheduler_eval {

struct QuickHullMetrics {
  std::size_t depth{};
  std::size_t partitions{};
};

std::vector<Point> QuickHullParallel(const std::vector<Point> &points,
                                     QuickHullMetrics *metrics = nullptr);
std::vector<std::string> MakeTrigrams(std::size_t size, std::uint64_t seed = 1);
std::vector<std::string>
RemoveDuplicateStrings(const std::vector<std::string> &keys,
                       DedupMetrics *metrics = nullptr);
bool CheckExtendedWorkloads();

} // namespace scheduler_eval
