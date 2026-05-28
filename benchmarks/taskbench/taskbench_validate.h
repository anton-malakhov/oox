#ifndef OOX_BENCH_TASKBENCH_VALIDATE_H
#define OOX_BENCH_TASKBENCH_VALIDATE_H

#include "taskbench_graph.h"

#include <cstddef>

namespace oox_bench::taskbench {

inline bool token_payload_matches(const Token& token, std::size_t expected_bytes) {
  return payload_matches_checksum(token, expected_bytes);
}

} // namespace oox_bench::taskbench

#endif // OOX_BENCH_TASKBENCH_VALIDATE_H
