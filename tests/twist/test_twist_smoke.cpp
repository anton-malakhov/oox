#include <oox/oox.h>

#include "oox_twist_harness.h"

#include <twist/assist/assert.hpp>

namespace {

void SimpleChain() {
    auto first = oox::run([] { return 1; });
    auto second = oox::run([](int value) { return value + 1; }, first);
    auto third = oox::run([](int value) { return value + 1; }, second);

    TWIST_ASSERT_M(oox::wait_and_get(third) == 3, "simple chain result");
}

void DeferredDiamond() {
    oox::var<int> source(oox::deferred);

    auto left = oox::run([](int value) { return value + 1; }, source);
    auto right = oox::run([](int value) { return value + 2; }, source);
    auto joined = oox::run([](int a, int b) { return a + b; }, left, right);

    oox::run([](int& value) { value = 5; }, source);

    TWIST_ASSERT_M(oox::wait_and_get(source) == 5, "deferred source value");
    TWIST_ASSERT_M(oox::wait_and_get(left) == 6, "deferred left value");
    TWIST_ASSERT_M(oox::wait_and_get(right) == 7, "deferred right value");
    TWIST_ASSERT_M(oox::wait_and_get(joined) == 13, "deferred diamond join");
}

} // namespace

int main() {
    oox::twist_tests::RunRandomSeeds("SimpleChain", SimpleChain);
    oox::twist_tests::RunRandomSeeds("DeferredDiamond", DeferredDiamond);

    return 0;
}
