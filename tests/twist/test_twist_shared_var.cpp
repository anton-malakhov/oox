#include <oox/shared_var.h>

#include "oox_twist_harness.h"

#include <twist/assist/assert.hpp>
#include <twist/assist/preempt.hpp>
#include <twist/ed/std/atomic.hpp>
#include <twist/test/body/wg.hpp>

namespace {

// Mirror of test_twist_known_deferred_race: two threads race to register a
// reader and a writer on the same deferred shared_var. The state mutex
// linearizes registration, so the result is deterministic (no handle race).
void DeferredSharedPublication() {
    oox::shared_var<int> source(oox::deferred);
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

    TWIST_ASSERT_M(oox::wait_and_get(result) == 42, "deferred shared publication result");
    TWIST_ASSERT_M(oox::wait_and_get(source) == 41, "deferred shared source value");
}

// A ready shared_var with concurrent reader and writer registration: the
// reader must observe a value consistent with the registration order.
void ConcurrentReaderAndWriterRegistration() {
    oox::shared_var<int> value(1);
    twist::ed::std::atomic<int> seen{0};
    twist::test::body::WaitGroup wg;

    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        auto doubled = oox::run([](int v) {
            twist::assist::PreemptionPoint();
            return v * 2;
        }, value);
        seen.store(oox::wait_and_get(doubled), std::memory_order_relaxed);
    });

    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        oox::run([](int& v) {
            twist::assist::PreemptionPoint();
            v = 10;
        }, value);
    });

    wg.Join();

    const int v = seen.load(std::memory_order_relaxed);
    TWIST_ASSERT_M(v == 2 || v == 20, "reader observes a consistent value");
    // Wait for the writer chain before the scenario returns (twist requires
    // all threads to finish); the final value must be the writer's.
    TWIST_ASSERT_M(oox::wait_and_get(value) == 10, "writer applied last");
}

// N threads register writers on one shared_var: writer serialization on the
// state mutex must apply every increment exactly once.
void ConcurrentWriterRegistration() {
    oox::shared_var<int> value(0);
    constexpr int N = 4;
    twist::test::body::WaitGroup wg;
    for (int i = 0; i < N; ++i) {
        wg.Add(1, [&, i] {
            twist::assist::PreemptionPoint();
            oox::run([i](int& v) {
                twist::assist::PreemptionPoint();
                v += i;
            }, value);
        });
    }
    wg.Join();
    TWIST_ASSERT_M(oox::wait_and_get(value) == N * (N - 1) / 2,
                   "all writer increments applied exactly once");
}

// Many threads call get() on the same shared_var concurrently.
void ConcurrentGet() {
    oox::shared_var<int> value(42);
    constexpr int N = 4;
    twist::ed::std::atomic<int> ok{0};
    twist::test::body::WaitGroup wg;
    for (int i = 0; i < N; ++i) {
        wg.Add(1, [&] {
            twist::assist::PreemptionPoint();
            if (value.get() == 42) {
                ok.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    wg.Join();
    TWIST_ASSERT_M(ok.load(std::memory_order_relaxed) == N,
                   "all concurrent gets see the value");
}

// A get() snapshots a slot that a concurrent registration may switch away
// from; the retained slot reference must prevent a use-after-free, and the
// observed value must be a consistent slot value (0 or 7, never garbage).
void WriterSwitchWhileGetPending() {
    oox::shared_var<int> value(0);
    twist::ed::std::atomic<int> seen{-1};
    twist::test::body::WaitGroup wg;

    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        seen.store(value.get(), std::memory_order_relaxed);
    });

    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        oox::run([](int& v) {
            twist::assist::PreemptionPoint();
            v = 7;
        }, value);
    });

    wg.Join();

    const int v = seen.load(std::memory_order_relaxed);
    TWIST_ASSERT_M(v == 0 || v == 7, "get observes a consistent slot value across a writer switch");
    TWIST_ASSERT_M(oox::wait_and_get(value) == 7, "writer applied");
}

// Handles are reference-counted: a copy used and destroyed in another thread
// must not interfere with a concurrent write through the original handle.
void HandleCopyLifecycle() {
    oox::shared_var<int> value(5);
    twist::ed::std::atomic<int> got{0};
    twist::test::body::WaitGroup wg;

    wg.Add(1, [&] {
        oox::shared_var<int> copy = value; // copy the handle in another thread
        twist::assist::PreemptionPoint();
        got.store(copy.get(), std::memory_order_relaxed);
        // copy destroyed here, in this thread
    });

    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        value = 9; // write through the shared state while the copy is in flight
    });

    wg.Join();

    const int v = got.load(std::memory_order_relaxed);
    TWIST_ASSERT_M(v == 5 || v == 9, "copy observes a consistent value");
    TWIST_ASSERT_M(oox::wait_and_get(value) == 9, "last write wins");
}

// A single task may mix plain oox::var and oox::shared_var arguments: the
// per-argument machinery handles each kind independently, and a concurrent
// writer on the shared_var must still serialize with the mixed task's writer.
void MixedVarAndSharedVarArgs() {
    oox::var<int> plain(0);
    oox::shared_var<int> shared(10);
    twist::test::body::WaitGroup wg;

    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        auto r = oox::run([](int& p, int& s) {
            twist::assist::PreemptionPoint();
            p = 1;
            s = 2;
        }, plain, shared);
        oox::wait_for_all(r);
    });

    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        oox::run([](int& s) {
            twist::assist::PreemptionPoint();
            s = 100;
        }, shared);
    });

    wg.Join();

    TWIST_ASSERT_M(oox::wait_and_get(plain) == 1, "plain var written by the mixed task");
    const int sv = oox::wait_and_get(shared);
    TWIST_ASSERT_M(sv == 2 || sv == 100, "shared var written by one serialized writer");
}

// Regression: a reader's get() racing a fast writer chain. On async backends
// the next writer is chained before the current task completes, and the chain
// consumes the current task's slot hold at its completion — the get() used to
// wait on the freed task (task::wait: life_get_count assertion). The wait now
// runs without the state mutex and re-validates the current slot.
void GetWhileFastWriters() {
    oox::shared_var<int> value(0);
    constexpr int kRegistrations = 64;
    constexpr int kGets = 32;
    twist::test::body::WaitGroup wg;

    wg.Add(1, [&] {
        for (int i = 0; i < kRegistrations; ++i) {
            twist::assist::PreemptionPoint();
            oox::run([i](int& v) {
                int acc = i;
                for (int j = 0; j < 100; ++j) {
                    acc = acc * 31 + j;
                }
                v = acc;
            }, value);
        }
    });

    wg.Add(1, [&] {
        for (int i = 0; i < kGets; ++i) {
            twist::assist::PreemptionPoint();
            (void)value.get(); // must not crash
        }
    });

    wg.Join();

    int expected = kRegistrations - 1;
    for (int j = 0; j < 100; ++j) {
        expected = expected * 31 + j;
    }
    TWIST_ASSERT_M(oox::wait_and_get(value) == expected, "last writer applied");
}

} // namespace

int main() {
    oox::twist_tests::RunRandomSeeds("DeferredSharedPublication", DeferredSharedPublication);
    oox::twist_tests::RunRandomSeeds("ConcurrentReaderAndWriterRegistration", ConcurrentReaderAndWriterRegistration);
    oox::twist_tests::RunRandomSeeds("ConcurrentWriterRegistration", ConcurrentWriterRegistration);
    oox::twist_tests::RunRandomSeeds("ConcurrentGet", ConcurrentGet);
    oox::twist_tests::RunRandomSeeds("WriterSwitchWhileGetPending", WriterSwitchWhileGetPending);
    oox::twist_tests::RunRandomSeeds("HandleCopyLifecycle", HandleCopyLifecycle);
    oox::twist_tests::RunRandomSeeds("MixedVarAndSharedVarArgs", MixedVarAndSharedVarArgs);
    oox::twist_tests::RunRandomSeeds("GetWhileFastWriters", GetWhileFastWriters);
    return 0;
}
