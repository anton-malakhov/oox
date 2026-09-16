#pragma once

#include "../eigen/poor_barrier.h"

namespace parlay::internal {

struct InitOnce {
    // NOLINTNEXTLINE(bugprone-forwarding-reference-overload)
    template <typename F> InitOnce(F &&f) { f(); }
};

using SpinBarrier = SpinBarrier;
}
