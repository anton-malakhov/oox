#include <oox/oox.h>

#include "oox_twist_harness.h"

#include <twist/assist/assert.hpp>
#include <twist/assist/preempt.hpp>
#include <twist/ed/std/atomic.hpp>
#include <twist/ed/std/thread.hpp>

#include <atomic>

namespace {

void ReturnedVarDestroyedWhileWorkerPending() {
    twist::ed::std::atomic<int> completed{0};

    {
        auto dropped = oox::run([&completed] {
            twist::assist::PreemptionPoint();
            completed.store(1, std::memory_order_release);
        });
        twist::assist::PreemptionPoint();
    }

    while (completed.load(std::memory_order_acquire) == 0) {
        twist::ed::std::this_thread::yield();
    }
    TWIST_ASSERT_M(completed.load(std::memory_order_acquire) == 1,
                   "detached worker must finish after returned var is destroyed");
}

void ChainedResultDestroyedWhileChildPending() {
    twist::ed::std::atomic<int> completed{0};

    {
        auto dropped = oox::run([&completed]() -> oox::var<int> {
            return oox::run([&completed] {
                twist::assist::PreemptionPoint();
                completed.store(1, std::memory_order_release);
                return 7;
            });
        });
        twist::assist::PreemptionPoint();
    }

    while (completed.load(std::memory_order_acquire) == 0) {
        twist::ed::std::this_thread::yield();
    }
    TWIST_ASSERT_M(completed.load(std::memory_order_acquire) == 1,
                   "forwarded child worker must finish after outer var is destroyed");
}

} // namespace

int main() {
    oox::twist_tests::RunRandomSeeds("ReturnedVarDestroyedWhileWorkerPending",
                                     ReturnedVarDestroyedWhileWorkerPending);
    oox::twist_tests::RunRandomSeeds("ChainedResultDestroyedWhileChildPending",
                                     ChainedResultDestroyedWhileChildPending);

    return 0;
}
