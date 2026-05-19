#include <oox/oox.h>

#include "oox_twist_harness.h"

#include <twist/assist/assert.hpp>
#include <twist/assist/preempt.hpp>
#include <twist/test/body/wg.hpp>

namespace {

void ConsumerAddedWhileProducerCompletes() {
    auto producer = oox::run([] {
        twist::assist::PreemptionPoint();
        return 41;
    });

    oox::var<int> consumer;
    twist::test::body::WaitGroup wg;

    wg.Add(1, [&] {
        consumer = oox::run([](int value) { return value + 1; }, producer);
    });

    wg.Add(1, [&] {
        TWIST_ASSERT_M(oox::wait_and_get(producer) == 41, "producer value");
    });

    wg.Join();

    TWIST_ASSERT_M(oox::wait_and_get(consumer) == 42, "consumer value");
}

} // namespace

int main() {
    oox::twist_tests::RunRandomSeeds("ConsumerAddedWhileProducerCompletes",
                                     ConsumerAddedWhileProducerCompletes);

    return 0;
}
