#include <oox/shared_var.h>

#include "oox_twist_harness.h"

#include <twist/assist/assert.hpp>
#include <twist/assist/preempt.hpp>
#include <twist/ed/std/atomic.hpp>
#include <twist/ed/std/thread.hpp>
#include <twist/test/body/wg.hpp>

#include <atomic>
#include <cstdint>
#include <string_view>
#include <utility>

namespace {

twist::ed::std::atomic<int>* writer_registration_participants = nullptr;

struct gated_value {
    int value = 0;

    gated_value() {
        auto* participants = writer_registration_participants;
        TWIST_ASSERT_M(participants != nullptr, "registration gate is installed");
        const int arrived = participants->fetch_add(1, std::memory_order_acq_rel) + 1;
        TWIST_ASSERT_M(arrived <= 2, "only the first materialization of each state reaches the gate");
        while (participants->load(std::memory_order_acquire) < 2) {
            twist::ed::std::this_thread::yield();
        }
    }
};

void AdoptForwardedVar() {
    constexpr std::uint64_t expected = 0x1122334455667788ULL;
    auto forwarded = oox::run([] {
        return oox::run([] { return expected; });
    });

    oox::shared_var<std::uint64_t> value(std::move(forwarded));
    TWIST_ASSERT_M(value.get() == expected, "shared_var preserves a forwarded var value");
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
    twist::ed::std::atomic<int> participants{0};
    writer_registration_participants = &participants;

    oox::shared_var<gated_value> first;
    oox::shared_var<gated_value> second;
    twist::test::body::WaitGroup wg;

    wg.Add(1, [&] {
        oox::run([](gated_value& a, gated_value& b) {
            ++a.value;
            ++b.value;
        }, first, second);
    });

    wg.Add(1, [&] {
        oox::run([](gated_value& b, gated_value& a) {
            ++b.value;
            ++a.value;
        }, second, first);
    });

    wg.Join();
    TWIST_ASSERT_M(first.get().value == 2, "both writers update the first value");
    TWIST_ASSERT_M(second.get().value == 2, "both writers update the second value");
    writer_registration_participants = nullptr;
}

} // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        return 2;
    }

    const std::string_view scenario = argv[1];
    if (scenario == "AdoptForwardedVar") {
        oox::twist_tests::RunRandomSeeds("AdoptForwardedVar", AdoptForwardedVar, 1);
    } else if (scenario == "WaitOnDeferredDoesNotBlockPublication") {
        oox::twist_tests::RunDfs("WaitOnDeferredDoesNotBlockPublication", WaitOnDeferredDoesNotBlockPublication);
    } else if (scenario == "ConcurrentOppositeOrderMultiVarWritersComplete") {
        oox::twist_tests::RunDfs("ConcurrentOppositeOrderMultiVarWritersComplete",
                                ConcurrentOppositeOrderMultiVarWritersComplete);
    } else {
        return 2;
    }
    return 0;
}
