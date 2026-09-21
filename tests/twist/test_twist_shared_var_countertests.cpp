#include <oox/shared_var.h>

#include "oox_twist_harness.h"

#include <twist/assist/assert.hpp>
#include <twist/assist/preempt.hpp>
#include <twist/ed/std/atomic.hpp>
#include <twist/test/body/wg.hpp>

#include <atomic>
#include <cstdint>
#include <string_view>
#include <utility>

namespace {

void AdoptForwardedVar() {
    constexpr std::uint64_t expected = 0x1122334455667788ULL;
    auto forwarded = oox::run([] {
        return oox::run([] { return expected; });
    });

    oox::shared_var<std::uint64_t> value(std::move(forwarded));
    TWIST_ASSERT_M(value.get() == expected, "shared_var preserves a forwarded var value");
}

void RegisterAdoptedForwardedVarBeforeProducerRuns() {
    oox::shared_var<int> gate(oox::deferred);
    auto forwarded = oox::run([](int input) {
        return oox::run([input] { return input + 1; });
    }, gate);
    oox::shared_var<int> value(std::move(forwarded));
    auto result = oox::run([](int input) { return input + 1; }, value);
    gate = 40;
    TWIST_ASSERT_M(oox::wait_and_get(result) == 42,
                   "registration resolves the adopted var only after its producer runs");
}

void ForwardedWaitDoesNotBlockWriterRegistration() {
    oox::shared_var<int> gate(oox::deferred);
    auto forwarded = oox::run([](int input) {
        return oox::run([input] { return input; });
    }, gate);
    oox::shared_var<int> value(std::move(forwarded));
    twist::ed::std::atomic<bool> getter_started{false};
    twist::ed::std::atomic<bool> writer_registered{false};
    twist::ed::std::atomic<int> observed{-1};
    twist::test::body::WaitGroup wg;

    wg.Add(1, [&] {
        getter_started.store(true, std::memory_order_release);
        observed.store(value.get(), std::memory_order_relaxed);
    });
    wg.Add(1, [&] {
        while (!getter_started.load(std::memory_order_acquire)) {
            twist::assist::PreemptionPoint();
        }
        oox::run([](int& input) { ++input; }, value);
        writer_registered.store(true, std::memory_order_release);
    });
    wg.Add(1, [&] {
        while (!writer_registered.load(std::memory_order_acquire)) {
            twist::assist::PreemptionPoint();
        }
        gate = 41;
    });
    wg.Join();
    TWIST_ASSERT_M(observed.load(std::memory_order_relaxed) == 42,
                   "forwarded wait releases the state mutex before blocking");
}

void WaitOnDeferredDoesNotBlockPublication() {
    oox::shared_var<int> value(oox::deferred);
    twist::test::body::WaitGroup wg;

    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        value.wait();
    });

    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        oox::run([](int& v) {
            twist::assist::PreemptionPoint();
            v = 41;
        }, value);
    });

    wg.Join();
    TWIST_ASSERT_M(value.get() == 41, "deferred publication completes while another thread waits");
}

void ConcurrentOppositeOrderMultiVarWritersComplete() {
    oox::shared_var<int> first(0);
    oox::shared_var<int> second(0);
    twist::test::body::WaitGroup wg;

    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        oox::run([](int& a, int& b) {
            ++a;
            ++b;
        }, first, second);
    });

    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        oox::run([](int& b, int& a) {
            ++b;
            ++a;
        }, second, first);
    });

    wg.Join();
    TWIST_ASSERT_M(first.get() == 2, "both writers update the first value");
    TWIST_ASSERT_M(second.get() == 2, "both writers update the second value");
}

} // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        return 2;
    }

    const std::string_view scenario = argv[1];
    if (scenario == "AdoptForwardedVar") {
        oox::twist_tests::RunRandomSeeds("AdoptForwardedVar", AdoptForwardedVar, 1);
    } else if (scenario == "RegisterAdoptedForwardedVarBeforeProducerRuns") {
        oox::twist_tests::RunRandomSeeds("RegisterAdoptedForwardedVarBeforeProducerRuns",
                                         RegisterAdoptedForwardedVarBeforeProducerRuns);
    } else if (scenario == "ForwardedWaitDoesNotBlockWriterRegistration") {
        oox::twist_tests::RunRandomSeeds("ForwardedWaitDoesNotBlockWriterRegistration",
                                         ForwardedWaitDoesNotBlockWriterRegistration);
    } else if (scenario == "WaitOnDeferredDoesNotBlockPublication") {
        oox::twist_tests::RunRandomSeeds("WaitOnDeferredDoesNotBlockPublication",
                                         WaitOnDeferredDoesNotBlockPublication);
    } else if (scenario == "ConcurrentOppositeOrderMultiVarWritersComplete") {
        oox::twist_tests::RunRandomSeeds("ConcurrentOppositeOrderMultiVarWritersComplete",
                                         ConcurrentOppositeOrderMultiVarWritersComplete);
    } else {
        return 2;
    }
    return 0;
}
