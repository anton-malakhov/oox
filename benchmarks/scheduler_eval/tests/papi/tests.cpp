// SPDX-License-Identifier: Apache-2.0
#include "papi_metrics.h"
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <thread>

template <typename F>
void ParallelFor(std::size_t first, std::size_t last, F &&body, std::size_t) {
  for (auto i = first; i < last; ++i)
    body(i);
}
#include "eval_parallel.h"

int main() {
  using namespace scheduler_eval;
  setenv("OOX_EVAL_PAPI_EVENTS", "PAPI_TOT_CYC,PAPI_TOT_INS", 1);
  BeginPapiMeasurement();
  {
    PapiRegion outer;
    {
      PapiRegion inner;
    }
  }
  std::thread a([] { PapiRegion region; });
  std::thread b([] { PapiRegion region; });
  a.join();
  b.join();
  auto result = EndPapiMeasurement();
  if (!result.error.empty() || result.regions != 3 ||
      result.values != std::vector<long long>{21, 42}) {
    std::cerr << "PAPI nested/multi-thread aggregation failed\n";
    return 1;
  }
  BeginPapiMeasurement();
  int visits = 0;
  EvalParallelFor(0, 3, [&](std::size_t) {
    EvalParallelFor(0, 2, [&](std::size_t) { ++visits; });
  });
  result = EndPapiMeasurement();
  if (visits != 6 || result.regions != 3 ||
      result.values != std::vector<long long>{21, 42})
    return 6;
  for (const auto *event : {"BAD", "FAIL_START", "FAIL_STOP"}) {
    setenv("OOX_EVAL_PAPI_EVENTS", event, 1);
    BeginPapiMeasurement();
    {
      PapiRegion region;
    }
    result = EndPapiMeasurement();
    if (result.error.empty() || result.regions != 0)
      return 2;
  }
  setenv("OOX_EVAL_PAPI_EVENTS", "PAPI_TOT_CYC", 1);
  BeginPapiMeasurement();
  {
    PapiRegion unfinished;
    result = EndPapiMeasurement();
    if (result.error.empty())
      return 3;
  }
  for (const auto *invalid : {"A,A", "A,", "A, B"}) {
    setenv("OOX_EVAL_PAPI_EVENTS", invalid, 1);
    try {
      BeginPapiMeasurement();
      return 4;
    } catch (const std::invalid_argument &) {
    }
  }
  unsetenv("OOX_EVAL_PAPI_EVENTS");
  BeginPapiMeasurement();
  {
    PapiRegion disabled;
  }
  if (!EndPapiMeasurement().events.empty())
    return 5;
  std::cout << "PAPI lifecycle, nested accounting, worker ownership and "
               "failures verified\n";
}
