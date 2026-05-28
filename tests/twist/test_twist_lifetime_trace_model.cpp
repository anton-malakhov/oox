#include <oox/oox.h>

#include "oox_twist_harness.h"

#include <twist/assist/assert.hpp>
#include <twist/assist/preempt.hpp>
#include <twist/ed/std/atomic.hpp>
#include <twist/ed/std/thread.hpp>

#include <atomic>
#include <cstdint>

namespace {

void wait_until_non_zero(const twist::ed::std::atomic<int>& flag) {
    while (flag.load(std::memory_order_acquire) == 0) {
        twist::ed::std::this_thread::yield();
    }
}

void DroppedForwardingTreeStillCompletes() {
    twist::ed::std::atomic<int> terminal_done{0};

    {
        auto dropped = oox::run([&terminal_done]() -> oox::var<int> {
            auto seed = oox::run([] {
                twist::assist::PreemptionPoint();
                return 1;
            });

            auto left = oox::run([](int value) {
                twist::assist::PreemptionPoint();
                return value + 10;
            }, seed);

            auto right = oox::run([](int value) {
                twist::assist::PreemptionPoint();
                return value + 20;
            }, seed);

            return oox::run([&terminal_done](int a, int b) {
                twist::assist::PreemptionPoint();
                terminal_done.store(1, std::memory_order_release);
                return a + b;
            }, left, right);
        });

        twist::assist::PreemptionPoint();
    }

    wait_until_non_zero(terminal_done);
    TWIST_ASSERT_M(terminal_done.load(std::memory_order_acquire) == 1,
                   "terminal node must execute even if outer var is destroyed");
}

} // namespace

int main() {
    oox::twist_tests::RunRandomSeeds("DroppedForwardingTreeStillCompletes",
                                     DroppedForwardingTreeStillCompletes);
    return 0;
}
