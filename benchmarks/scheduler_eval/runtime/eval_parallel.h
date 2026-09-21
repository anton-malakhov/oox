// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "papi_metrics.h"

// The provider's ParallelFor is defined before this adapter is included.
template <typename F>
void EvalParallelFor(std::size_t first, std::size_t last, F &&body,
                     std::size_t grain = 0) {
#ifdef OOX_EVAL_HAVE_PAPI
  if (!scheduler_eval::PapiMeasurementActive()) {
    ::ParallelFor(first, last, std::forward<F>(body), grain);
    return;
  }
  ::ParallelFor(
      first, last,
      [&](std::size_t i) {
        scheduler_eval::PapiRegion region;
        body(i);
      },
      grain);
#else
  ::ParallelFor(first, last, std::forward<F>(body), grain);
#endif
}
