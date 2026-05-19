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
        (void)oox::wait_and_get(value);
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
        (void)oox::wait_and_get(second);
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
        (void)oox::wait_and_get(value);
    }, "deferred writer exception must cancel written value");
    ExpectThrows<oox::cancelled_by_exception>([&] {
        (void)oox::wait_and_get(reader);
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
        (void)oox::wait_and_get(late);
    }, "late consumer must observe producer exception");
}

void ExplicitCancelSkipsBodies() {
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

    ExpectThrows<oox::cancelled_by_user>([&] {
        (void)oox::wait_and_get(first);
    }, "explicit cancel must cancel target");
    ExpectThrows<oox::cancelled_by_user>([&] {
        (void)oox::wait_and_get(second);
    }, "explicit cancel must propagate");
    TWIST_ASSERT_M(ran_first.load(std::memory_order_relaxed) == 0, "cancelled body must be skipped");
    TWIST_ASSERT_M(ran_second.load(std::memory_order_relaxed) == 0, "dependent cancelled body must be skipped");
}

void ForwardingThrowsBeforeReturningVar() {
    oox::var<int> input = 1;

    auto bad_forward = [](oox::var<int>) -> oox::var<int> {
        throw DummyException{};
    };

    auto result = oox::run(bad_forward, input);

    ExpectThrows<DummyException>([&] {
        (void)oox::wait_and_get(result);
    }, "forwarding task exception must propagate");
}

void ForwardedInnerExceptionPropagates() {
    auto result = oox::run([]() -> oox::var<int> {
        return oox::run([]() -> int {
            throw DummyException{};
        });
    });

    ExpectThrows<DummyException>([&] {
        (void)oox::wait_and_get(result);
    }, "forwarded inner task exception must propagate");
}

} // namespace

int main() {
    static_assert(OOX_ENABLE_EXCEPTIONS, "exception Twist tests must compile with OOX_ENABLE_EXCEPTIONS=1");

    oox::twist_tests::RunRandomSeeds("ThrowingProducerRethrowsOriginal", ThrowingProducerRethrowsOriginal);
    oox::twist_tests::RunRandomSeeds("ExceptionSkipsDependentBodies", ExceptionSkipsDependentBodies);
    oox::twist_tests::RunRandomSeeds("DeferredWriterPoisonCancelsReaders", DeferredWriterPoisonCancelsReaders);
    oox::twist_tests::RunRandomSeeds("LateConsumerAfterFailureSeesFailure", LateConsumerAfterFailureSeesFailure);
    oox::twist_tests::RunRandomSeeds("ExplicitCancelSkipsBodies", ExplicitCancelSkipsBodies);
    oox::twist_tests::RunRandomSeeds("ForwardingThrowsBeforeReturningVar", ForwardingThrowsBeforeReturningVar);
    oox::twist_tests::RunRandomSeeds("ForwardedInnerExceptionPropagates", ForwardedInnerExceptionPropagates);

    return 0;
}
