// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "oox/eigen/scheduler_trace.h"
#include <cstddef>
#include <iosfwd>
#include <string_view>

namespace oox::scheduler_trace {
// One recording per process. Buffers are preallocated before enabling events.
// Dump closes scope admission, drains accepted scopes for at most 250ms, then
// disables recording and joins writers. Unfinished/dropped ends stay explicit.
void Start(std::size_t lanes, std::size_t events_per_lane);
void Dump(std::ostream &, std::string_view mode, std::string_view workload,
          std::size_t workers, std::size_t tasks, std::size_t iterations,
          std::size_t work, std::size_t grain, bool oracle_ok);
#ifdef OOX_TRACE_RECORDER_TESTING
// Set before Dump, on its calling thread. Invoked after scope admission closes.
void SetClosingHook(void (*hook)()) noexcept;
#endif
}
