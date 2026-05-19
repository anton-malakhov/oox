#include <oox/oox.h>

#include "oox_twist_harness.h"

#include <twist/assist/assert.hpp>
#include <twist/assist/preempt.hpp>
#include <twist/test/body/wg.hpp>

namespace {

void DeferredPublicationRace() {
    oox::var<int> source(oox::deferred);
    oox::var<int> result;

    twist::test::body::WaitGroup wg;

    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        result = oox::run([](int value) {
            twist::assist::PreemptionPoint();
            return value + 1;
        }, source);
    });

    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        oox::run([](int& value) {
            twist::assist::PreemptionPoint();
            value = 41;
        }, source);
    });

    wg.Join();

    TWIST_ASSERT_M(oox::wait_and_get(result) == 42, "deferred publication result");
}

} // namespace

int main() {
    oox::twist_tests::RunRandomSeeds("DeferredPublicationRace", DeferredPublicationRace);
    return 0;
}
