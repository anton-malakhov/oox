#include <oox/oox.h>

#include "oox_twist_harness.h"

#include <twist/assist/assert.hpp>
#include <twist/assist/preempt.hpp>
#include <twist/test/body/wg.hpp>

namespace {

oox::var<int> PlusOneAsync(int value) {
    return oox::run([](int x) {
        twist::assist::PreemptionPoint();
        return x + 1;
    }, value);
}

void NestedForwardingChain() {
    auto inner = [](int value) -> oox::var<int> {
        return PlusOneAsync(value);
    };

    auto middle = [inner](int value) -> oox::var<int> {
        return oox::run(inner, value);
    };

    auto outer = [middle](int value) -> oox::var<int> {
        return oox::run(middle, value);
    };

    auto result = oox::run(outer, 39);
    TWIST_ASSERT_M(oox::wait_and_get(result) == 40, "nested forwarding result");
}

void ForwardedFanOut() {
    auto make_forwarded = [](int value) -> oox::var<int> {
        return oox::run([](int x) {
            twist::assist::PreemptionPoint();
            return x * 2;
        }, value);
    };

    auto forwarded = oox::run(make_forwarded, 21);
    auto c1 = oox::run([](int value) { return value + 1; }, forwarded);
    auto c2 = oox::run([](int value) { return value + 2; }, forwarded);
    auto c3 = oox::run([](int value) { return value + 3; }, forwarded);
    auto joined = oox::run([](int a, int b, int c) {
        return a + b + c;
    }, c1, c2, c3);

    TWIST_ASSERT_M(oox::wait_and_get(forwarded) == 42, "forwarded fanout base");
    TWIST_ASSERT_M(oox::wait_and_get(joined) == 132, "forwarded fanout join");
}

void ForwardedConsumerRegistrationRace() {
    auto make_forwarded = []() -> oox::var<int> {
        return oox::run([] {
            twist::assist::PreemptionPoint();
            return 7;
        });
    };

    auto forwarded = oox::run(make_forwarded);
    oox::var<int> c1;
    oox::var<int> c2;

    twist::test::body::WaitGroup wg;
    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        c1 = oox::run([](int value) { return value + 10; }, forwarded);
    });
    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        c2 = oox::run([](int value) { return value + 20; }, forwarded);
    });
    wg.Join();

    TWIST_ASSERT_M(oox::wait_and_get(c1) == 17, "forwarded concurrent consumer c1");
    TWIST_ASSERT_M(oox::wait_and_get(c2) == 27, "forwarded concurrent consumer c2");
}

void ForwardingThroughDeferredInput() {
    oox::var<int> source(oox::deferred);

    auto inner = [](oox::var<int> input) -> oox::var<int> {
        return oox::run([](int value) {
            twist::assist::PreemptionPoint();
            return value + 1;
        }, input);
    };

    auto forwarded = oox::run(inner, source);
    auto consumer = oox::run([](int value) { return value * 2; }, forwarded);

    oox::run([](int& value) {
        twist::assist::PreemptionPoint();
        value = 20;
    }, source);

    TWIST_ASSERT_M(oox::wait_and_get(forwarded) == 21, "forwarded deferred result");
    TWIST_ASSERT_M(oox::wait_and_get(consumer) == 42, "forwarded deferred consumer");
}

} // namespace

int main() {
    oox::twist_tests::RunRandomSeeds("NestedForwardingChain", NestedForwardingChain);
    oox::twist_tests::RunRandomSeeds("ForwardedFanOut", ForwardedFanOut);
    oox::twist_tests::RunRandomSeeds("ForwardedConsumerRegistrationRace", ForwardedConsumerRegistrationRace);
    oox::twist_tests::RunRandomSeeds("ForwardingThroughDeferredInput", ForwardingThroughDeferredInput);

    return 0;
}
