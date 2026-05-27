#include <oox/oox.h>

#include "oox_twist_harness.h"

#include <twist/assist/assert.hpp>
#include <twist/ed/std/atomic.hpp>

#include <exception>
#include <type_traits>

namespace {

struct DummyException final : std::exception {
    [[nodiscard]] const char* what() const noexcept override {
        return "dummy twist exception";
    }
};

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
    oox::twist_tests::RunRandomSeeds("WaitStatusNoThrowRethrowsOriginalForProducer",
                                     WaitStatusNoThrowRethrowsOriginalForProducer);
    oox::twist_tests::RunRandomSeeds("WaitStatusNoThrowDependentReportsFailure",
                                     WaitStatusNoThrowDependentReportsFailure);
    oox::twist_tests::RunRandomSeeds("WaitStatusBestEffortCancelIsStableAcrossRepeatedWaits",
                                     WaitStatusBestEffortCancelIsStableAcrossRepeatedWaits);

    return 0;
}
