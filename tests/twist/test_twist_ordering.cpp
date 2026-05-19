#include <oox/oox.h>

#include "oox_twist_harness.h"

#include <twist/assist/assert.hpp>
#include <twist/assist/preempt.hpp>

namespace {

void ReaderBetweenTwoWriters() {
    oox::var<int> value = 0;

    auto r0 = oox::run([](const int& v) {
        twist::assist::PreemptionPoint();
        return v;
    }, value);

    oox::run([](int& v) {
        twist::assist::PreemptionPoint();
        v = 1;
    }, value);

    auto r1 = oox::run([](const int& v) {
        twist::assist::PreemptionPoint();
        return v;
    }, value);

    oox::run([](int& v) {
        twist::assist::PreemptionPoint();
        v = 2;
    }, value);

    auto r2 = oox::run([](const int& v) {
        twist::assist::PreemptionPoint();
        return v;
    }, value);

    TWIST_ASSERT_M(oox::wait_and_get(r0) == 0, "first reader sees initial value");
    TWIST_ASSERT_M(oox::wait_and_get(r1) == 1, "middle reader sees first write");
    TWIST_ASSERT_M(oox::wait_and_get(r2) == 2, "last reader sees second write");
    TWIST_ASSERT_M(oox::wait_and_get(value) == 2, "final value after two writers");
}

void ManyReadersBeforeWriter() {
    oox::var<int> value = 3;

    auto r1 = oox::run([](const int& v) { return v + 1; }, value);
    auto r2 = oox::run([](const int& v) { return v + 2; }, value);
    auto r3 = oox::run([](const int& v) { return v + 3; }, value);

    oox::run([](int& v) {
        twist::assist::PreemptionPoint();
        v = 10;
    }, value);

    TWIST_ASSERT_M(oox::wait_and_get(r1) == 4, "reader 1");
    TWIST_ASSERT_M(oox::wait_and_get(r2) == 5, "reader 2");
    TWIST_ASSERT_M(oox::wait_and_get(r3) == 6, "reader 3");
    TWIST_ASSERT_M(oox::wait_and_get(value) == 10, "writer after readers");
}

void MultipleWritersSerialize() {
    oox::var<int> value = 0;

    oox::run([](int& v) {
        twist::assist::PreemptionPoint();
        v = 1;
    }, value);
    oox::run([](int& v) {
        twist::assist::PreemptionPoint();
        v = 2;
    }, value);
    oox::run([](int& v) {
        twist::assist::PreemptionPoint();
        v = 3;
    }, value);

    TWIST_ASSERT_M(oox::wait_and_get(value) == 3, "multiple writers final value");
}

void MixedFanInReadyAndPending() {
    oox::var<int> deferred(oox::deferred);
    oox::var<int> ready = 5;

    auto sum = oox::run([](int a, int b) {
        twist::assist::PreemptionPoint();
        return a + b;
    }, deferred, ready);

    oox::run([](int& v) {
        twist::assist::PreemptionPoint();
        v = 37;
    }, deferred);

    TWIST_ASSERT_M(oox::wait_and_get(sum) == 42, "mixed ready and pending fan-in");
}

} // namespace

int main() {
    oox::twist_tests::RunRandomSeeds("ReaderBetweenTwoWriters", ReaderBetweenTwoWriters);
    oox::twist_tests::RunRandomSeeds("ManyReadersBeforeWriter", ManyReadersBeforeWriter);
    oox::twist_tests::RunRandomSeeds("MultipleWritersSerialize", MultipleWritersSerialize);
    oox::twist_tests::RunRandomSeeds("MixedFanInReadyAndPending", MixedFanInReadyAndPending);

    return 0;
}
