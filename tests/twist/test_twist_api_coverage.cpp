#include <oox/oox.h>

#include "oox_twist_harness.h"
#include "oox_internal_test_hooks.h"

#include <twist/assist/assert.hpp>
#include <twist/assist/preempt.hpp>

namespace {

void DeferredTwoInputs() {
    oox::var<int> a(oox::deferred);
    oox::var<int> b(oox::deferred);

    auto c = oox::run([](int x, int y) {
        twist::assist::PreemptionPoint();
        return x + y;
    }, a, b);

    auto write_b = oox::run([](int& value) { value = 3; }, b);
    auto write_a = oox::run([](int& value) { value = 2; }, a);

    TWIST_ASSERT_M(oox::wait_and_get(a) == 2, "deferred input a");
    TWIST_ASSERT_M(oox::wait_and_get(b) == 3, "deferred input b");
    TWIST_ASSERT_M(oox::wait_and_get(c) == 5, "deferred two-input join");
    oox::wait_for_all(write_a);
    oox::wait_for_all(write_b);
}

void DeferredChainWithMultipleWriters() {
    oox::var<int> a(oox::deferred);
    auto b = oox::run([](int value) { return value + 1; }, a);

    auto write1 = oox::run([](int& value) { value = 1; }, a);
    auto write10 = oox::run([](int& value) { value = 10; }, a);

    int aval = oox::wait_and_get(a);
    int bval = oox::wait_and_get(b);

    TWIST_ASSERT_M(aval == 10, "last writer must win for source");
    // Depending on scheduling, b can consume either before or after overwrite.
    TWIST_ASSERT_M(bval == 2 || bval == 11, "multiple-writers chain result");
    oox::wait_for_all(write1);
    oox::wait_for_all(write10);
}

void DeferredForwardingLayer() {
    oox::var<int> source(oox::deferred);

    auto inner = [](int input) -> oox::var<int> {
        return oox::run([](int v) {
            twist::assist::PreemptionPoint();
            return v + 1;
        }, input);
    };

    auto outer = [inner](int input) -> oox::var<int> {
        return oox::run(inner, input);  // forwarding task
    };

    auto result = oox::run(outer, source);
    auto writer = oox::run([](int& value) { value = 41; }, source);

    TWIST_ASSERT_M(oox::wait_and_get(source) == 41, "forwarding source");
    TWIST_ASSERT_M(oox::wait_and_get(result) == 42, "forwarding layer result");
    oox::wait_for_all(writer);
}

void DeferredArrayLayered() {
    oox::var<int> nodes[3] = {
        oox::var<int>(oox::deferred),
        oox::var<int>(oox::deferred),
        oox::var<int>(oox::deferred)
    };

    nodes[1] = oox::run([](int x) { return x + 1; }, nodes[0]);
    nodes[2] = oox::run([](int x) { return x + 1; }, nodes[1]);

    auto writer = oox::run([](int& x) { x = 100; }, nodes[0]);

    TWIST_ASSERT_M(oox::wait_and_get(nodes[0]) == 100, "array layer source");
    TWIST_ASSERT_M(oox::wait_and_get(nodes[1]) == 101, "array layer mid");
    TWIST_ASSERT_M(oox::wait_and_get(nodes[2]) == 102, "array layer sink");
    oox::wait_for_all(writer);
}

void ImmediateValueConsistency() {
    const oox::var<int> tmp = 1;
    TWIST_ASSERT_M(oox::wait_and_get(tmp) == 1, "immediate var consistency");
}

void DeferredRedirectLateConsumer() {
    oox::var<int> source(oox::deferred);

    auto early = oox::run([](int value) {
        twist::assist::PreemptionPoint();
        return value + 1;
    }, source);

    auto writer = oox::run([](int& value) {
        twist::assist::PreemptionPoint();
        value = 10;
    }, source);

    twist::assist::PreemptionPoint();

    auto late = oox::run([](int value) {
        twist::assist::PreemptionPoint();
        return value + 2;
    }, source);

    TWIST_ASSERT_M(oox::wait_and_get(source) == 10, "redirect source value");
    TWIST_ASSERT_M(oox::wait_and_get(early) == 11, "early consumer through deferred");
    oox::wait_for_all(writer);
    TWIST_ASSERT_M(oox::wait_and_get(late) == 12, "late consumer through deferred redirect");
}

void WriterPipelineOnSingleVar() {
    oox::var<int> value(oox::deferred);

    auto w1 = oox::run([](int& x) {
        twist::assist::PreemptionPoint();
        x = 1;
    }, value);

    auto w2 = oox::run([](int& x) {
        twist::assist::PreemptionPoint();
        x += 2;
    }, value);

    auto read = oox::run([](int x) {
        twist::assist::PreemptionPoint();
        return x;
    }, value);

    oox::wait_for_all(w1);
    oox::wait_for_all(w2);

    int observed = oox::wait_and_get(read);
    TWIST_ASSERT_M(oox::wait_and_get(value) == 3, "writer pipeline final value");
    TWIST_ASSERT_M(observed == 1 || observed == 3,
                   "reader sees either first or final writer value");
}

void InternalHooksRareBranches() {
    TWIST_ASSERT_M(oox::internal::test_hooks::hit_deferred_redirect_assign_prerequisite(),
                   "deferred redirect branch in assign_prerequisite");
    TWIST_ASSERT_M(oox::internal::test_hooks::hit_do_notify_out_with_non_tagged_writer(),
                   "do_notify_out branch with non-tagged next_writer");
    TWIST_ASSERT_M(oox::internal::test_hooks::hit_set_next_writer_no_owner_branch(),
                   "set_next_writer branch for no-owner markers");
}

void ForwardingImmediateVarPath() {
    auto forwarded = oox::run([]() -> oox::var<int> {
        // Return an already-ready storage var: forwarding add_arc should fail,
        // forcing execute() fallback path in forwarding functional_task.
        return oox::var<int>(5);
    });

    TWIST_ASSERT_M(oox::wait_and_get(forwarded) == 5, "forwarding immediate var");
}

void EmptyVarReaderWriterForms() {
    oox::var<int> writer_target;
    auto writer = oox::run([](int& x) {
        twist::assist::PreemptionPoint();
        x = 9;
    }, writer_target);
    oox::wait_for_all(writer);
    TWIST_ASSERT_M(oox::wait_and_get(writer_target) == 9, "writer form on empty var");

    oox::var<int> reader_target;
    const oox::var<int>& const_reader_target = reader_target;
    auto r1 = oox::run([](int x) { return x + 1; }, reader_target);
    auto r2 = oox::run([](int x) { return x + 2; }, const_reader_target);
    TWIST_ASSERT_M(oox::wait_and_get(r1) == 1, "reader form on empty var");
    TWIST_ASSERT_M(oox::wait_and_get(r2) == 2, "const-reader form on empty var");
}

void RuntimeDeferredTagAndLifeCount() {
    [[maybe_unused]] oox::deferred_t tag_runtime{1};
    oox::internal::storage_task<1, int> t;
    TWIST_ASSERT_M(t.life_get_count() == 0, "life_get_count baseline");
}

}  // namespace

int main() {
    oox::twist_tests::RunRandomSeeds("DeferredTwoInputs", DeferredTwoInputs);
    oox::twist_tests::RunRandomSeeds("DeferredChainWithMultipleWriters", DeferredChainWithMultipleWriters);
    oox::twist_tests::RunRandomSeeds("DeferredForwardingLayer", DeferredForwardingLayer);
    oox::twist_tests::RunRandomSeeds("DeferredArrayLayered", DeferredArrayLayered);
    oox::twist_tests::RunRandomSeeds("ImmediateValueConsistency", ImmediateValueConsistency);
    oox::twist_tests::RunRandomSeeds("DeferredRedirectLateConsumer", DeferredRedirectLateConsumer);
    oox::twist_tests::RunRandomSeeds("WriterPipelineOnSingleVar", WriterPipelineOnSingleVar);
    oox::twist_tests::RunRandomSeeds("InternalHooksRareBranches", InternalHooksRareBranches);
    oox::twist_tests::RunRandomSeeds("ForwardingImmediateVarPath", ForwardingImmediateVarPath);
    oox::twist_tests::RunRandomSeeds("EmptyVarReaderWriterForms", EmptyVarReaderWriterForms);
    oox::twist_tests::RunRandomSeeds("RuntimeDeferredTagAndLifeCount", RuntimeDeferredTagAndLifeCount);
    return 0;
}
