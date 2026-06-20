#include <oox/oox.h>

#include "oox_twist_harness.h"
#include "oox_internal_test_hooks.h"

#include <twist/assist/assert.hpp>
#include <twist/assist/preempt.hpp>
#include <twist/ed/std/atomic.hpp>

#include <cstring>
#include <exception>

namespace {

struct DummyException final : std::exception {
    [[nodiscard]] const char* what() const noexcept override { return "dummy twist exception"; }
};

void InternalFailurePaths() {
    TWIST_ASSERT_M(oox::internal::test_hooks::hit_publish_failure_from_branches(),
                   "publish_failure_from must cover null/port/user/dependency/exception branches");
    TWIST_ASSERT_M(oox::internal::test_hooks::hit_failure_accessors_clean(),
                   "pristine task must report no failure through all accessors");
    TWIST_ASSERT_M(oox::internal::test_hooks::hit_failure_wait_status_branches(),
                   "failure_wait_status must cover ready/port/user/dependency/exception returns");
    TWIST_ASSERT_M(oox::internal::test_hooks::hit_exception_control_lifecycle(),
                   "exception_control retain/release must cover null and delete branches");
    TWIST_ASSERT_M(oox::internal::test_hooks::hit_failure_on_completed_task(),
                   "mark_failure/store_exception_control must no-op on completed tasks");
    TWIST_ASSERT_M(oox::internal::test_hooks::hit_do_notify_arcs_failed_producer(),
                   "failed producer must forward failure to flow-only successors");
    TWIST_ASSERT_M(oox::internal::test_hooks::hit_publish_failure_add_arc_failure(),
                   "publish_failure_from must release the signal when the consumer is complete");
}

void CancellationExceptionMessages() {
    const oox::cancelled_by_exception by_exc;
    const oox::cancelled_by_user by_user;
    TWIST_ASSERT_M(std::strstr(by_exc.what(), "cancelled_by_exception") != nullptr,
                   "cancelled_by_exception::what()");
    TWIST_ASSERT_M(std::strstr(by_user.what(), "cancelled_by_user") != nullptr,
                   "cancelled_by_user::what()");
}

void WaitOnErasedVarBase() {
    oox::var<int> value = 7;
    oox::internal::oox_var_base& base = value;
    const auto status = oox::wait_for_all_status(base);
    TWIST_ASSERT_M(status == oox::wait_status::ready, "erased wait_for_all_status on ready var");
    oox::wait_for_all(base);
    TWIST_ASSERT_M(oox::wait_and_get(value) == 7, "erased base wait must not disturb value");
}

void ThrowingProducerSkipsAllDependentShapes() {
    auto bad = oox::run([]() -> int {
        twist::assist::PreemptionPoint();
        throw DummyException{};
    });

    twist::ed::std::atomic<int> ran{0};

    auto value_dep = oox::run([&](int v) -> int {
        ran.fetch_add(1, std::memory_order_relaxed);
        return v + 1;
    }, bad);

    auto void_dep = oox::run([&]([[maybe_unused]] int v) {
        ran.fetch_add(1, std::memory_order_relaxed);
    }, bad);

    auto var_dep = oox::run([&](int v) -> oox::var<int> {
        ran.fetch_add(1, std::memory_order_relaxed);
        return oox::run([](int x) { return x; }, oox::var<int>(v));
    }, bad);

    try { oox::wait_for_all(value_dep); TWIST_ASSERT_M(false, "value dependent must fail"); }
    catch (const DummyException&) {} catch (const oox::cancelled_by_exception&) {}
    try { oox::wait_for_all(void_dep); TWIST_ASSERT_M(false, "void dependent must fail"); }
    catch (const DummyException&) {} catch (const oox::cancelled_by_exception&) {}
    try { oox::wait_for_all(var_dep); TWIST_ASSERT_M(false, "var dependent must fail"); }
    catch (const DummyException&) {} catch (const oox::cancelled_by_exception&) {}

    TWIST_ASSERT_M(ran.load(std::memory_order_relaxed) == 0, "no dependent body may run");
}

void ForwardedWaitAndGetFollowsChain() {
    oox::var<int> src(oox::deferred);
    auto fwd = oox::run([](oox::var<int> in) -> oox::var<int> {
        return oox::run([](int v) {
            twist::assist::PreemptionPoint();
            return v + 1;
        }, in);
    }, src);
    oox::run([](int& o) { o = 41; }, src);
    TWIST_ASSERT_M(oox::wait_and_get(fwd) == 42, "forwarded wait_and_get must follow the chain");
}

}  // namespace

int main() {
    static_assert(OOX_EXCEPTIONS_ENABLED, "exception Twist tests must compile with OOX_EXCEPTIONS_ENABLED=1");

    oox::twist_tests::RunRandomSeeds("InternalFailurePaths", InternalFailurePaths);
    oox::twist_tests::RunRandomSeeds("CancellationExceptionMessages", CancellationExceptionMessages);
    oox::twist_tests::RunRandomSeeds("WaitOnErasedVarBase", WaitOnErasedVarBase);
    oox::twist_tests::RunRandomSeeds("ThrowingProducerSkipsAllDependentShapes",
                                     ThrowingProducerSkipsAllDependentShapes);
    oox::twist_tests::RunRandomSeeds("ForwardedWaitAndGetFollowsChain", ForwardedWaitAndGetFollowsChain);
    return 0;
}
