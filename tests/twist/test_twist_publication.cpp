#include <oox/oox.h>

#include "oox_twist_harness.h"

#include <twist/assist/assert.hpp>
#include <twist/assist/preempt.hpp>

namespace {

oox::var<int> PublicationTree(int depth) {
    if (depth == 0) {
        return oox::run([] { return 1; });
    }

    auto left = PublicationTree(depth - 1);
    auto right = PublicationTree(depth - 1);
    return oox::run([](int a, int b) {
        twist::assist::PreemptionPoint();
        return a + b + 1;
    }, left, right);
}

void DependencyPublicationTree() {
#if __TWIST_SIM__
    constexpr int depth = 3;
    constexpr int expected = 15;
#else
    constexpr int depth = 6;
    constexpr int expected = 127;
#endif

    auto result = PublicationTree(depth);
    TWIST_ASSERT_M(oox::wait_and_get(result) == expected, "publication tree result");
}

} // namespace

int main() {
    oox::twist_tests::RunRandomSeeds("DependencyPublicationTree", DependencyPublicationTree);

    return 0;
}
