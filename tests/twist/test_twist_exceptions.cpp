#include <oox/shared_var.h>

#include "oox_twist_harness.h"

#include <twist/assist/assert.hpp>
#include <twist/assist/preempt.hpp>
#include <twist/ed/std/atomic.hpp>
#include <twist/test/body/wg.hpp>

#include <exception>
#include <type_traits>

namespace {

struct DummyException final : std::exception {
    [[nodiscard]] const char* what() const noexcept override {
        return "dummy twist exception";
    }
};

struct throwing_assignment_value {
    int value = 0;

    throwing_assignment_value() = default;
    explicit throwing_assignment_value(int v) : value(v) {}
    throwing_assignment_value(const throwing_assignment_value&) = default;
    throwing_assignment_value(throwing_assignment_value&&) = default;
    throwing_assignment_value& operator=(const throwing_assignment_value&) {
        throw DummyException{};
    }
    throwing_assignment_value& operator=(throwing_assignment_value&&) {
        throw DummyException{};
    }
};

template <typename Var, typename Value>
concept supports_value_assignment = requires(Var& var, Value&& value) {
    var = std::forward<Value>(value);
};

static_assert(!supports_value_assignment<oox::var<throwing_assignment_value, false>,
                                         throwing_assignment_value>);
static_assert(!supports_value_assignment<oox::shared_var<throwing_assignment_value, false>,
                                         throwing_assignment_value>);
static_assert(supports_value_assignment<oox::var<throwing_assignment_value, true>,
                                        throwing_assignment_value>);
static_assert(supports_value_assignment<oox::shared_var<throwing_assignment_value, true>,
                                        throwing_assignment_value>);

template <typename Exception, typename F>
void ExpectThrows(F&& f, const char* message) {
    bool seen = false;
    try {
        std::forward<F>(f)();
    } catch (const Exception&) {
        seen = true;
    } catch (...) {
        TWIST_ASSERT_M(false, message);
    }
    TWIST_ASSERT_M(seen, message);
}

void ThrowingProducerRethrowsOriginal() {
    auto value = oox::run([]() -> int {
        throw DummyException{};
    });

    ExpectThrows<DummyException>([&] {
        oox::wait_for_all(value);
    }, "throwing producer must rethrow original exception");
}

void ExceptionSkipsDependentBodies() {
    auto bad = oox::run([]() -> int {
        throw DummyException{};
    });

    twist::ed::std::atomic<int> ran_first{0};
    twist::ed::std::atomic<int> ran_second{0};

    auto first = oox::run([&](int value) -> int {
        ran_first.fetch_add(1, std::memory_order_relaxed);
        return value + 1;
    }, bad);

    auto second = oox::run([&](int value) -> int {
        ran_second.fetch_add(1, std::memory_order_relaxed);
        return value + 1;
    }, first);

    ExpectThrows<DummyException>([&] {
        oox::wait_for_all(second);
    }, "exception must propagate through chain");
    TWIST_ASSERT_M(ran_first.load(std::memory_order_relaxed) == 0, "first dependent body must be skipped");
    TWIST_ASSERT_M(ran_second.load(std::memory_order_relaxed) == 0, "second dependent body must be skipped");
}

void DeferredWriterPoisonCancelsReaders() {
    oox::var<int> value(oox::deferred);
    auto reader = oox::run([](int input) {
        return input + 1;
    }, value);

    oox::run([](int& output) {
        output = 1;
        throw DummyException{};
    }, value);

    ExpectThrows<oox::cancelled_by_exception>([&] {
        oox::wait_for_all(value);
    }, "deferred writer exception must cancel written value");
    ExpectThrows<oox::cancelled_by_exception>([&] {
        oox::wait_for_all(reader);
    }, "deferred writer exception must cancel reader");
}

void LateConsumerAfterFailureSeesFailure() {
    auto bad = oox::run([]() -> int {
        throw DummyException{};
    });

    ExpectThrows<DummyException>([&] {
        oox::wait_for_all(bad);
    }, "producer wait must observe original exception");

    auto late = oox::run([](int value) {
        return value + 1;
    }, bad);

    ExpectThrows<DummyException>([&] {
        oox::wait_for_all(late);
    }, "late consumer must observe producer exception");
}

void ExplicitCancelIsBestEffort() {
    oox::var<int> gate(oox::deferred);
    twist::ed::std::atomic<int> ran_first{0};
    twist::ed::std::atomic<int> ran_second{0};

    auto first = oox::run([&](int value) -> int {
        ran_first.fetch_add(1, std::memory_order_relaxed);
        return value + 1;
    }, gate);
    auto second = oox::run([&](int value) -> int {
        ran_second.fetch_add(1, std::memory_order_relaxed);
        return value + 1;
    }, first);

    first.cancel();
    oox::run([](int& output) {
        output = 1;
    }, gate);

    bool first_cancelled = false;
    try {
        oox::wait_for_all(first);
    } catch (const oox::cancelled_by_user&) {
        first_cancelled = true;
    } catch (...) {
        TWIST_ASSERT_M(false, "first must either succeed or be cancelled_by_user");
    }

    bool second_cancelled = false;
    try {
        oox::wait_for_all(second);
    } catch (const oox::cancelled_by_user&) {
        second_cancelled = true;
    } catch (...) {
        TWIST_ASSERT_M(false, "second must either succeed or be cancelled_by_user");
    }

    TWIST_ASSERT_M(ran_first.load(std::memory_order_relaxed) <= 1, "first body must run at most once");
    TWIST_ASSERT_M(ran_second.load(std::memory_order_relaxed) <= 1, "second body must run at most once");
    TWIST_ASSERT_M(first_cancelled || second_cancelled || ran_first.load(std::memory_order_relaxed) == 1,
                   "explicit cancel should be best-effort: may cancel or may allow normal execution");
}

void ForwardingThrowsBeforeReturningVar() {
    oox::var<int> input = 1;

    auto bad_forward = [](oox::var<int>) -> oox::var<int> {
        throw DummyException{};
    };

    auto result = oox::run(bad_forward, input);

    ExpectThrows<DummyException>([&] {
        oox::wait_for_all(result);
    }, "forwarding task exception must propagate");
}

void ForwardedInnerExceptionPropagates() {
    auto result = oox::run([]() -> oox::var<int> {
        return oox::run([]() -> int {
            throw DummyException{};
        });
    });

    ExpectThrows<DummyException>([&] {
        oox::wait_for_all(result);
    }, "forwarded inner task exception must propagate");
}

void SharedVarExceptionWakesWaiter() {
    oox::shared_var<int, true> gate(oox::deferred);
    auto producer = oox::run<true>([](int) -> int {
        throw DummyException{};
    }, gate);
    oox::shared_var<int, true> value(std::move(producer));
    twist::test::body::WaitGroup wg;

    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        ExpectThrows<DummyException>([&] { (void)value.get(); },
                                     "shared_var waiter must wake and rethrow producer exception");
    });
    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        gate = 1;
    });
    wg.Join();
}

void SharedVarForwardedProducerExceptionPropagates() {
    oox::shared_var<int, true> gate(oox::deferred);
    auto producer = oox::run<true>([](int) -> oox::var<int, true> {
        throw DummyException{};
    }, gate);
    oox::shared_var<int, true> value(std::move(producer));
    twist::ed::std::atomic<bool> dependent_ran{false};
    auto dependent = oox::run<true>([&](int input) {
        dependent_ran.store(true, std::memory_order_relaxed);
        return input;
    }, value);
    gate = 1;
    ExpectThrows<DummyException>([&] { (void)value.get(); },
                                 "forwarded shared_var producer must rethrow before resolving storage");
    ExpectThrows<DummyException>([&] { (void)oox::wait_and_get(dependent); },
                                 "forwarded producer failure propagates to a registered dependent");
    TWIST_ASSERT_M(!dependent_ran.load(std::memory_order_relaxed),
                   "dependent body must not run after forwarded producer failure");
}

void ForwardedProducerFailureCanBeRecovered() {
    auto plain = oox::run<true>([]() -> oox::var<int, true> {
        throw DummyException{};
    });
    oox::run<true>([](int& value) { value = 41; }, plain);
    TWIST_ASSERT_M(oox::wait_and_get(plain) == 41, "plain var writer recovers failed forwarding");

    auto failed_shared = oox::run<true>([]() -> oox::var<int, true> {
        throw DummyException{};
    });
    oox::shared_var<int, true> shared(std::move(failed_shared));
    shared = 42;
    TWIST_ASSERT_M(shared.get() == 42, "shared_var assignment recovers failed forwarding");
}

void ThrowingValueAssignmentPropagates() {
    oox::var<throwing_assignment_value, true> plain(throwing_assignment_value{1});
    plain = throwing_assignment_value{2};
    ExpectThrows<oox::cancelled_by_exception>([&] { (void)oox::wait_and_get(plain); },
                                              "throwing var assignment must cancel its output");

    oox::shared_var<throwing_assignment_value, true> shared(throwing_assignment_value{1});
    const throwing_assignment_value replacement(2);
    shared = replacement;
    ExpectThrows<oox::cancelled_by_exception>([&] { (void)shared.get(); },
                                              "throwing shared_var assignment must cancel its output");
}

void SharedVarCancellationPropagates() {
    oox::shared_var<int, true> gate(oox::deferred);
    auto producer = oox::run<true>([](int input) { return input + 1; }, gate);
    oox::shared_var<int, true> value(std::move(producer));
    value.cancel();
    twist::test::body::WaitGroup wg;
    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        ExpectThrows<oox::cancelled_by_user>([&] { (void)value.get(); },
                                            "shared_var must preserve explicit cancellation");
    });
    wg.Add(1, [&] {
        twist::assist::PreemptionPoint();
        gate = 1;
    });
    wg.Join();
}

void WaitStatusNoThrowRethrowsOriginalForProducer() {
    auto bad = oox::run([]() -> int {
        throw DummyException{};
    });

    ExpectThrows<DummyException>([&] {
        (void)oox::wait_for_all_status<false>(bad);
    }, "wait_for_all_status<false> on producer must rethrow original exception");
}

void WaitStatusNoThrowDependentReportsFailure() {
    auto bad = oox::run([]() -> int {
        throw DummyException{};
    });
    auto dependent = oox::run([](int value) -> int {
        return value + 1;
    }, bad);

    bool observed_failure = false;
    try {
        const auto dependent_status = oox::wait_for_all_status<false>(dependent);
        observed_failure = (dependent_status == oox::wait_status::dependency_cancelled);
    } catch (const DummyException&) {
        observed_failure = true;
    } catch (...) {
        TWIST_ASSERT_M(false, "dependent failure must be either dependency_cancelled or original exception");
    }
    TWIST_ASSERT_M(observed_failure, "dependent failure must be observable");
}

void WaitStatusBestEffortCancelIsStableAcrossRepeatedWaits() {
    oox::var<int> gate(oox::deferred);
    auto task = oox::run([](int value) -> int {
        return value + 1;
    }, gate);

    task.cancel();
    oox::run([](int& output) {
        output = 1;
    }, gate);

    const auto first = oox::wait_for_all_status<false>(task);
    const auto second = oox::wait_for_all_status<false>(task);
    TWIST_ASSERT_M(first == second, "repeated wait status must be stable after completion");
    TWIST_ASSERT_M(first == oox::wait_status::ready || first == oox::wait_status::user_cancelled,
                   "best-effort cancel must resolve to either ready or user_cancelled");
}

} // namespace

int main() {
    static_assert(OOX_EXCEPTIONS_ENABLED, "exception Twist tests must compile with OOX_EXCEPTIONS_ENABLED=1");

    oox::twist_tests::RunRandomSeeds("ThrowingProducerRethrowsOriginal", ThrowingProducerRethrowsOriginal);
    oox::twist_tests::RunRandomSeeds("ExceptionSkipsDependentBodies", ExceptionSkipsDependentBodies);
    oox::twist_tests::RunRandomSeeds("DeferredWriterPoisonCancelsReaders", DeferredWriterPoisonCancelsReaders);
    oox::twist_tests::RunRandomSeeds("LateConsumerAfterFailureSeesFailure", LateConsumerAfterFailureSeesFailure);
    oox::twist_tests::RunRandomSeeds("ExplicitCancelIsBestEffort", ExplicitCancelIsBestEffort);
    oox::twist_tests::RunRandomSeeds("ForwardingThrowsBeforeReturningVar", ForwardingThrowsBeforeReturningVar);
    oox::twist_tests::RunRandomSeeds("ForwardedInnerExceptionPropagates", ForwardedInnerExceptionPropagates);
    oox::twist_tests::RunRandomSeeds("SharedVarExceptionWakesWaiter", SharedVarExceptionWakesWaiter);
    oox::twist_tests::RunRandomSeeds("SharedVarForwardedProducerExceptionPropagates",
                                     SharedVarForwardedProducerExceptionPropagates);
    oox::twist_tests::RunRandomSeeds("ForwardedProducerFailureCanBeRecovered",
                                     ForwardedProducerFailureCanBeRecovered);
    oox::twist_tests::RunRandomSeeds("ThrowingValueAssignmentPropagates",
                                     ThrowingValueAssignmentPropagates);
    oox::twist_tests::RunRandomSeeds("SharedVarCancellationPropagates", SharedVarCancellationPropagates);
    oox::twist_tests::RunRandomSeeds("WaitStatusNoThrowRethrowsOriginalForProducer",
                                     WaitStatusNoThrowRethrowsOriginalForProducer);
    oox::twist_tests::RunRandomSeeds("WaitStatusNoThrowDependentReportsFailure",
                                     WaitStatusNoThrowDependentReportsFailure);
    oox::twist_tests::RunRandomSeeds("WaitStatusBestEffortCancelIsStableAcrossRepeatedWaits",
                                     WaitStatusBestEffortCancelIsStableAcrossRepeatedWaits);

    return 0;
}
