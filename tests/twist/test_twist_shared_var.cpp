#include <oox/shared_var.h>

#include "oox_twist_harness.h"

#include <twist/assist/assert.hpp>
#include <twist/assist/preempt.hpp>
#include <twist/ed/std/atomic.hpp>
#include <twist/test/body/wg.hpp>

#include <cstdint>

namespace {

twist::ed::std::atomic<int>* materialization_gate = nullptr;
oox::shared_var<int>* materialization_side_effect = nullptr;
oox::shared_var<int>* plain_constructor_side_effect = nullptr;
twist::ed::std::atomic<bool>* publication_fallback_started = nullptr;
twist::ed::std::atomic<bool>* publication_fallback_release = nullptr;

struct gated_default_value {
    int value = 0;

    gated_default_value() noexcept {
        materialization_gate->fetch_add(1, std::memory_order_relaxed);
        twist::assist::PreemptionPoint();
        oox::run([](int& side_effect) { ++side_effect; }, *materialization_side_effect);
    }
};

struct publication_fallback_value {
    int value = 42;

    publication_fallback_value() noexcept {
        publication_fallback_started->store(true, std::memory_order_release);
        while (!publication_fallback_release->load(std::memory_order_acquire)) {
            twist::ed::std::this_thread::yield();
        }
    }
};

struct copy_only_value {
    int value = 0;

    copy_only_value() = default;
    explicit copy_only_value(int v) : value(v) {}
    copy_only_value(const copy_only_value&) = default;
    copy_only_value(copy_only_value&&) = delete;
    copy_only_value& operator=(const copy_only_value&) = default;
    copy_only_value& operator=(copy_only_value&&) = delete;
};

struct copy_only_value_initialized {
    int value;

    copy_only_value_initialized() = default;
    copy_only_value_initialized(const copy_only_value_initialized&) noexcept = default;
    copy_only_value_initialized(copy_only_value_initialized&&) = delete;
};

struct throwing_move_copyable_value {
    int value = 0;

    throwing_move_copyable_value() noexcept = default;
    throwing_move_copyable_value(const throwing_move_copyable_value&) noexcept = default;
    throwing_move_copyable_value(throwing_move_copyable_value&&) { throw 1; }
};

struct asymmetric_assignment_value {
    int value = 0;

    asymmetric_assignment_value() = default;
    explicit asymmetric_assignment_value(int v) : value(v) {}
    asymmetric_assignment_value(const asymmetric_assignment_value&) = default;
    asymmetric_assignment_value(asymmetric_assignment_value&&) = default;
    asymmetric_assignment_value& operator=(const asymmetric_assignment_value& other) noexcept {
        value = other.value;
        return *this;
    }
    asymmetric_assignment_value& operator=(asymmetric_assignment_value&) { throw 1; }
};

struct immovable_default_value {
    immovable_default_value() = default;
    immovable_default_value(const immovable_default_value&) = delete;
    immovable_default_value(immovable_default_value&&) = delete;
};

static_assert(!oox::internal::value_materializable<immovable_default_value>);

struct reentrant_plain_default_value {
    reentrant_plain_default_value() noexcept {
        oox::run([](int& side_effect) { ++side_effect; }, *plain_constructor_side_effect);
    }
};

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

void DeferredAssignmentWakesWaitingReader() {
    oox::shared_var<int> value(oox::deferred);
    twist::ed::std::atomic<int> observed{-1};
    twist::test::body::WaitGroup wg;

    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        observed.store(value.get(), std::memory_order_relaxed);
    });
    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        value = 42;
    });

    wg.Join();
    TWIST_ASSERT_M(observed.load(std::memory_order_relaxed) == 42,
                   "assignment publishes a deferred shared_var to a waiting reader");
}

void CopyOnlyAssignmentUsesTheCopyOverload() {
    copy_only_value initial(1);
    copy_only_value replacement(42);
    oox::shared_var<copy_only_value> value(initial);
    value = replacement;
    TWIST_ASSERT_M(value.get().value == 42, "copy-only shared_var assignment");
}

void MaterializationAndConstCopyAssignmentContracts() {
    oox::shared_var<copy_only_value_initialized, false> initialized;
    auto initialized_done = oox::run<false>([](copy_only_value_initialized& value) noexcept {
        ++value.value;
    }, initialized);
    oox::wait_for_all(initialized_done);
    TWIST_ASSERT_M(initialized.get().value == 1,
                   "shared copy-only fallback must value-initialize");

    oox::shared_var<throwing_move_copyable_value, false> copied;
    auto copied_done = oox::run<false>([](throwing_move_copyable_value& value) noexcept {
        value.value = 42;
    }, copied);
    oox::wait_for_all(copied_done);
    TWIST_ASSERT_M(copied.get().value == 42,
                   "shared materialization must prefer the nothrow copy");

    asymmetric_assignment_value initial(1);
    const asymmetric_assignment_value replacement(42);
    oox::shared_var<asymmetric_assignment_value, false> assigned(initial);
    assigned = replacement;
    TWIST_ASSERT_M(assigned.get().value == 42,
                   "shared assignment must execute the const-qualified overload");
}

void AssignmentPublishesAfterReleasingStateLock() {
    oox::shared_var<int> value(oox::deferred);
    oox::shared_var<int> alias = value;
    auto reader = oox::run([alias](int) { return alias.get(); }, value);
    value = 42;
    TWIST_ASSERT_M(oox::wait_and_get(reader) == 42,
                   "assignment continuation runs after the shared state unlock");
}

void ConcurrentLazyMaterializationRunsInGraphTasks() {
    twist::ed::std::atomic<int> gate{0};
    oox::shared_var<int> side_effect(0);
    materialization_gate = &gate;
    materialization_side_effect = &side_effect;
    oox::shared_var<gated_default_value> first;
    oox::shared_var<gated_default_value> second;
    twist::test::body::WaitGroup wg;

    wg.Add(1, [&] {
        oox::run([](gated_default_value& a, gated_default_value& b) {
            ++a.value;
            ++b.value;
        }, first, second);
    });
    wg.Add(1, [&] {
        oox::run([](gated_default_value& b, gated_default_value& a) {
            ++b.value;
            ++a.value;
        }, second, first);
    });
    wg.Join();
    TWIST_ASSERT_M(first.get().value == 2 && second.get().value == 2,
                   "both writers survive concurrent lazy materialization");
    TWIST_ASSERT_M(gate.load(std::memory_order_relaxed) == 2,
                   "one materializer was published for each shared state");
    TWIST_ASSERT_M(side_effect.get() == 2, "default constructors ran exactly once per state");
    materialization_gate = nullptr;
    materialization_side_effect = nullptr;
}

void ConcurrentGetPublishesOneLazyMaterializer() {
    twist::ed::std::atomic<int> constructors{0};
    oox::shared_var<int> side_effect(0);
    materialization_gate = &constructors;
    materialization_side_effect = &side_effect;
    oox::shared_var<gated_default_value> value;
    twist::test::body::WaitGroup wg;

    for (int i = 0; i < 4; ++i) {
        wg.Add(1, [&] {
            twist::assist::PreemptionPoint();
            TWIST_ASSERT_M(value.get().value == 0, "lazy value materialized");
        });
    }
    wg.Join();

    TWIST_ASSERT_M(constructors.load(std::memory_order_relaxed) == 1,
                   "concurrent gets publish one lazy materializer");
    TWIST_ASSERT_M(side_effect.get() == 1, "only the installed materializer entered user code");
    materialization_gate = nullptr;
    materialization_side_effect = nullptr;
}

void PublicationFailureExecutesInstalledMaterializerInline() {
    twist::ed::std::atomic<bool> fallback_started{false};
    twist::ed::std::atomic<bool> release_fallback{false};
    twist::ed::std::atomic<bool> waiter_entered{false};
    twist::ed::std::atomic<bool> publisher_caught{false};
    twist::ed::std::atomic<bool> spawn_failure{false};
    publication_fallback_started = &fallback_started;
    publication_fallback_release = &release_fallback;
    oox::shared_var<publication_fallback_value> value;
    twist::test::body::WaitGroup wg;

    oox::internal::inject_next_task_spawn_failure(spawn_failure);
    wg.Add(1, [&] {
        try {
            (void)value.get();
        } catch (const oox::internal::injected_task_spawn_failure&) {
            publisher_caught.store(true, std::memory_order_release);
        }
    });
    wg.Add(1, [&] {
        while (!fallback_started.load(std::memory_order_acquire)) {
            twist::ed::std::this_thread::yield();
        }
        waiter_entered.store(true, std::memory_order_release);
        TWIST_ASSERT_M(value.get().value == 42, "concurrent waiter completes");
    });
    wg.Add(1, [&] {
        while (!waiter_entered.load(std::memory_order_acquire)) {
            twist::ed::std::this_thread::yield();
        }
        twist::assist::PreemptionPoint();
        release_fallback.store(true, std::memory_order_release);
    });
    wg.Join();

    TWIST_ASSERT_M(publisher_caught.load(std::memory_order_acquire),
                   "first get observes the backend publication failure");
    TWIST_ASSERT_M(value.get().value == 42, "retry observes the inline materialized value");
    oox::internal::clear_task_spawn_failure_injection();
    publication_fallback_started = nullptr;
    publication_fallback_release = nullptr;
}

void ReentrantPlainVarSetupUsesAnIndependentRegistrationBatch() {
    oox::shared_var<int> value(0);
    oox::shared_var<int> side_effect(0);
    oox::var<reentrant_plain_default_value> plain;
    plain_constructor_side_effect = &side_effect;
    auto done = oox::run([](int& target, reentrant_plain_default_value&) {
        ++target;
    }, value, plain);
    oox::wait_for_all(done);
    TWIST_ASSERT_M(value.get() == 1 && side_effect.get() == 1,
                   "reentrant run uses a separate shared registration batch");
    plain_constructor_side_effect = nullptr;
}

void MutableAliasesShareOneWriterRegistration() {
    oox::shared_var<int> value(0);
    oox::shared_var<int> alias = value;

    auto first = oox::run([](int& a, int& b) {
        ++a;
        ++b;
    }, value, value);
    oox::wait_for_all(first);
    TWIST_ASSERT_M(oox::internal::details::is_next_writer_no_owner_marker(
                       first.current_task->out(2).next_writer.load(std::memory_order_acquire)),
                   "discarded aliased writer port has no owner hold");

    auto second = oox::run([](int& a, int& b) {
        ++a;
        ++b;
    }, value, alias);
    oox::wait_for_all(second);
    TWIST_ASSERT_M(oox::internal::details::is_next_writer_no_owner_marker(
                       second.current_task->out(2).next_writer.load(std::memory_order_acquire)),
                   "copied alias writer port has no owner hold");
    TWIST_ASSERT_M(value.get() == 4, "aliased mutable arguments execute without self-dependency");

    auto mixed = oox::run([](int& target, int snapshot) {
        target += snapshot;
    }, value, value);
    oox::wait_for_all(mixed);
    TWIST_ASSERT_M(value.get() == 8,
                   "reader/writer aliases share the writer registration and storage");
}

void DeferredMixedAliasesMaterializeBeforeEveryArgument() {
    oox::shared_var<int> read_first(oox::deferred);
    auto first = oox::run([](int old, int& out) noexcept {
        twist::assist::PreemptionPoint();
        out = old + 1;
    }, read_first, read_first);
    oox::wait_for_all(first);
    TWIST_ASSERT_M(read_first.get() == 1, "read-first alias observes materialized writer storage");

    oox::shared_var<int> write_first(oox::deferred);
    auto second = oox::run([](int& out, int old) noexcept {
        twist::assist::PreemptionPoint();
        out = old + 1;
    }, write_first, write_first);
    oox::wait_for_all(second);
    TWIST_ASSERT_M(write_first.get() == 1, "write-first alias preserves the same semantics");
}

void FailedWaiterSubscriptionReleasesArc() {
    oox::var<int> ready(1);
    auto* waiter = oox::internal::task::allocate<oox::internal::shared_var_waiter>();
    waiter->life_set_count(1);
    TWIST_ASSERT_M(!waiter->subscribe(ready.current_task),
                   "completed producer rejects a late waiter arc");
    waiter->release();
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
    oox::shared_var<std::uint32_t> value(0);
    constexpr int kRegistrations = 8;
    constexpr int kGets = 8;
    twist::test::body::WaitGroup wg;

    wg.Add(1, [&] {
        for (int i = 0; i < kRegistrations; ++i) {
            twist::assist::PreemptionPoint();
            oox::run([i](std::uint32_t& v) {
                std::uint32_t acc = static_cast<std::uint32_t>(i);
                for (int j = 0; j < 100; ++j) {
                    acc = acc * 31 + static_cast<std::uint32_t>(j);
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

    std::uint32_t expected = kRegistrations - 1;
    for (int j = 0; j < 100; ++j) {
        expected = expected * 31 + static_cast<std::uint32_t>(j);
    }
    TWIST_ASSERT_M(oox::wait_and_get(value) == expected, "last writer applied");
}

} // namespace

int main() {
    oox::twist_tests::RunRandomSeeds("DeferredSharedPublication", DeferredSharedPublication);
    oox::twist_tests::RunRandomSeeds("DeferredAssignmentWakesWaitingReader", DeferredAssignmentWakesWaitingReader);
    oox::twist_tests::RunRandomSeeds("CopyOnlyAssignmentUsesTheCopyOverload",
                                     CopyOnlyAssignmentUsesTheCopyOverload);
    oox::twist_tests::RunRandomSeeds("MaterializationAndConstCopyAssignmentContracts",
                                     MaterializationAndConstCopyAssignmentContracts);
    oox::twist_tests::RunRandomSeeds("AssignmentPublishesAfterReleasingStateLock",
                                     AssignmentPublishesAfterReleasingStateLock);
    oox::twist_tests::RunRandomSeeds("ConcurrentLazyMaterializationRunsInGraphTasks",
                                     ConcurrentLazyMaterializationRunsInGraphTasks);
    oox::twist_tests::RunRandomSeeds("ConcurrentGetPublishesOneLazyMaterializer",
                                     ConcurrentGetPublishesOneLazyMaterializer);
    oox::twist_tests::RunRandomSeeds("PublicationFailureExecutesInstalledMaterializerInline",
                                     PublicationFailureExecutesInstalledMaterializerInline);
    oox::twist_tests::RunRandomSeeds("ReentrantPlainVarSetupUsesAnIndependentRegistrationBatch",
                                     ReentrantPlainVarSetupUsesAnIndependentRegistrationBatch);
    oox::twist_tests::RunRandomSeeds("MutableAliasesShareOneWriterRegistration",
                                     MutableAliasesShareOneWriterRegistration);
    oox::twist_tests::RunRandomSeeds("DeferredMixedAliasesMaterializeBeforeEveryArgument",
                                     DeferredMixedAliasesMaterializeBeforeEveryArgument);
    oox::twist_tests::RunRandomSeeds("FailedWaiterSubscriptionReleasesArc",
                                     FailedWaiterSubscriptionReleasesArc);
    oox::twist_tests::RunRandomSeeds("ConcurrentReaderAndWriterRegistration", ConcurrentReaderAndWriterRegistration);
    oox::twist_tests::RunRandomSeeds("ConcurrentWriterRegistration", ConcurrentWriterRegistration);
    oox::twist_tests::RunRandomSeeds("ConcurrentGet", ConcurrentGet);
    oox::twist_tests::RunRandomSeeds("WriterSwitchWhileGetPending", WriterSwitchWhileGetPending);
    oox::twist_tests::RunRandomSeeds("HandleCopyLifecycle", HandleCopyLifecycle);
    oox::twist_tests::RunRandomSeeds("MixedVarAndSharedVarArgs", MixedVarAndSharedVarArgs);
    oox::twist_tests::RunRandomSeeds("GetWhileFastWriters", GetWhileFastWriters);
    return 0;
}
