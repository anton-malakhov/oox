#include <oox/oox.h>

#include "oox_twist_harness.h"
#include "oox_internal_test_hooks.h"

#include <twist/assist/assert.hpp>
#include <twist/assist/preempt.hpp>

namespace {

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

struct non_default_forwarded_value {
    int value;

    explicit non_default_forwarded_value(int v) : value(v) {}
    non_default_forwarded_value(const non_default_forwarded_value&) = default;
    non_default_forwarded_value(non_default_forwarded_value&&) = default;
};

void DeferredTwoInputs() {
    oox::var<int> a(oox::deferred);
    oox::var<int> b(oox::deferred);

    auto c = oox::run([](int x, int y) {
        twist::assist::PreemptionPoint();
        return x + y;
    }, a, b);

    oox::run([](int& value) { value = 3; }, b);
    oox::run([](int& value) { value = 2; }, a);

    TWIST_ASSERT_M(oox::wait_and_get(a) == 2, "deferred input a");
    TWIST_ASSERT_M(oox::wait_and_get(b) == 3, "deferred input b");
    TWIST_ASSERT_M(oox::wait_and_get(c) == 5, "deferred two-input join");
}

void DeferredChainWithMultipleWriters() {
    oox::var<int> a(oox::deferred);
    auto b = oox::run([](int value) { return value + 1; }, a);

    oox::run([](int& value) { value = 1; }, a);
    oox::run([](int& value) { value = 10; }, a);

    int aval = oox::wait_and_get(a);
    int bval = oox::wait_and_get(b);

    TWIST_ASSERT_M(aval == 10, "last writer must win for source");
    // Depending on scheduling, b can consume either before or after overwrite.
    TWIST_ASSERT_M(bval == 2 || bval == 11, "multiple-writers chain result");
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
    oox::run([](int& value) { value = 41; }, source);

    TWIST_ASSERT_M(oox::wait_and_get(source) == 41, "forwarding source");
    TWIST_ASSERT_M(oox::wait_and_get(result) == 42, "forwarding layer result");
}

void DeferredArrayLayered() {
    oox::var<int> nodes[3] = {
        oox::var<int>(oox::deferred),
        oox::var<int>(oox::deferred),
        oox::var<int>(oox::deferred)
    };

    nodes[1] = oox::run([](int x) { return x + 1; }, nodes[0]);
    nodes[2] = oox::run([](int x) { return x + 1; }, nodes[1]);

    oox::run([](int& x) { x = 100; }, nodes[0]);

    TWIST_ASSERT_M(oox::wait_and_get(nodes[0]) == 100, "array layer source");
    TWIST_ASSERT_M(oox::wait_and_get(nodes[1]) == 101, "array layer mid");
    TWIST_ASSERT_M(oox::wait_and_get(nodes[2]) == 102, "array layer sink");
}

void ImmediateValueConsistency() {
    const oox::var<int> tmp = 1;
    TWIST_ASSERT_M(oox::wait_and_get(tmp) == 1, "immediate var consistency");
}

void VarStorageKeepsFlagsInPointerTags() {
    static_assert(sizeof(oox::internal::var_storage) == sizeof(void*));
    static_assert(std::is_trivially_copyable_v<oox::internal::var_storage>);
    static_assert(alignof(oox::internal::result_state<unsigned char>) >=
                  oox::internal::var_storage_pointer_alignment);

    alignas(4) unsigned char slot[4]{};
    oox::internal::var_storage direct(slot, false, false);
    oox::internal::var_storage both(slot, true, true);
    TWIST_ASSERT_M(direct.tagged_ptr == reinterpret_cast<std::uintptr_t>(slot),
                   "untagged storage pointer must keep its fast representation");
    TWIST_ASSERT_M(both.ptr() == slot, "tagged storage pointer must round-trip");
    TWIST_ASSERT_M(both.forwarded(), "forwarded flag must use a pointer tag");
    TWIST_ASSERT_M(both.initialize_if_empty(), "initialize flag must use a pointer tag");
}

void ValueAssignmentPublishesAndSerializes() {
    oox::var<int> deferred(oox::deferred);
    auto deferred_reader = oox::run([](int value) { return value + 1; }, deferred);
    deferred = 41;
    TWIST_ASSERT_M(oox::wait_and_get(deferred) == 41, "value assignment publishes deferred var");
    TWIST_ASSERT_M(oox::wait_and_get(deferred_reader) == 42, "deferred reader observes assignment");

    oox::var<int> ready(1);
    auto ready_reader = oox::run([](int value) { return value; }, ready);
    ready = 7;
    TWIST_ASSERT_M(oox::wait_and_get(ready_reader) == 1, "existing reader precedes assignment writer");
    TWIST_ASSERT_M(oox::wait_and_get(ready) == 7, "assignment becomes current writer");
}

void CopyOnlyAssignmentUsesTheCopyOverload() {
    copy_only_value initial(1);
    copy_only_value replacement(42);
    oox::var<copy_only_value> value(initial);
    value = replacement;
    TWIST_ASSERT_M(oox::wait_and_get(value).value == 42, "copy-only var assignment");
}

void MaterializationAssignmentDefaultsAndForwardingContracts() {
    oox::var<copy_only_value_initialized, false> initialized;
    oox::run<false>([](copy_only_value_initialized& value) noexcept {
        ++value.value;
    }, initialized);
    TWIST_ASSERT_M(oox::wait_and_get(initialized).value == 1,
                   "copy-only fallback must value-initialize");

    oox::var<throwing_move_copyable_value, false> copied;
    oox::run<false>([](throwing_move_copyable_value& value) noexcept {
        value.value = 42;
    }, copied);
    TWIST_ASSERT_M(oox::wait_and_get(copied).value == 42,
                   "lazy materialization must prefer the nothrow copy");

    asymmetric_assignment_value initial(1);
    const asymmetric_assignment_value replacement(42);
    oox::var<asymmetric_assignment_value, false> assigned(initial);
    assigned = replacement;
    TWIST_ASSERT_M(oox::wait_and_get(assigned).value == 42,
                   "copy assignment must execute the const-qualified overload");

    auto omitted = oox::run<false>([](int value = 42) noexcept { return value; });
    auto partial = oox::run<false>([](int first, int second = 2) noexcept {
        return first + second;
    }, 40);
    TWIST_ASSERT_M(oox::wait_and_get(omitted) == 42, "fully omitted default argument");
    TWIST_ASSERT_M(oox::wait_and_get(partial) == 42, "partially omitted default argument");

    auto forwarded = oox::run<false>([]() noexcept -> oox::var<non_default_forwarded_value, false> {
        non_default_forwarded_value value(42);
        return oox::var<non_default_forwarded_value, false>(value);
    });
    TWIST_ASSERT_M(oox::wait_and_get(forwarded).value == 42,
                   "populated non-default-constructible var must remain forwardable");
}

void DeferredRedirectLateConsumer() {
    oox::var<int> source(oox::deferred);

    auto early = oox::run([](int value) {
        twist::assist::PreemptionPoint();
        return value + 1;
    }, source);

    auto late = oox::run([](int value) {
        twist::assist::PreemptionPoint();
        return value + 2;
    }, source);

    oox::run([](int& value) {
        twist::assist::PreemptionPoint();
        value = 10;
    }, source);

    TWIST_ASSERT_M(oox::wait_and_get(source) == 10, "redirect source value");
    TWIST_ASSERT_M(oox::wait_and_get(early) == 11, "early consumer through deferred");
    TWIST_ASSERT_M(oox::wait_and_get(late) == 12, "second consumer through deferred redirect");
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

void WaitStatusReadyOnSuccess() {
    auto t = oox::run([] { return 5; });
    TWIST_ASSERT_M(oox::wait_for_all_status(t) == oox::wait_status::ready,
                   "wait_for_all_status must report ready on success");
}

}  // namespace

int main() {
    oox::twist_tests::RunRandomSeeds("DeferredTwoInputs", DeferredTwoInputs);
    oox::twist_tests::RunRandomSeeds("DeferredChainWithMultipleWriters", DeferredChainWithMultipleWriters);
    oox::twist_tests::RunRandomSeeds("DeferredForwardingLayer", DeferredForwardingLayer);
    oox::twist_tests::RunRandomSeeds("DeferredArrayLayered", DeferredArrayLayered);
    oox::twist_tests::RunRandomSeeds("ImmediateValueConsistency", ImmediateValueConsistency);
    oox::twist_tests::RunRandomSeeds("VarStorageKeepsFlagsInPointerTags",
                                     VarStorageKeepsFlagsInPointerTags);
    oox::twist_tests::RunRandomSeeds("ValueAssignmentPublishesAndSerializes",
                                     ValueAssignmentPublishesAndSerializes);
    oox::twist_tests::RunRandomSeeds("CopyOnlyAssignmentUsesTheCopyOverload",
                                     CopyOnlyAssignmentUsesTheCopyOverload);
    oox::twist_tests::RunRandomSeeds("MaterializationAssignmentDefaultsAndForwardingContracts",
                                     MaterializationAssignmentDefaultsAndForwardingContracts);
    oox::twist_tests::RunRandomSeeds("DeferredRedirectLateConsumer", DeferredRedirectLateConsumer);
    oox::twist_tests::RunRandomSeeds("WriterPipelineOnSingleVar", WriterPipelineOnSingleVar);
    oox::twist_tests::RunRandomSeeds("InternalHooksRareBranches", InternalHooksRareBranches);
    oox::twist_tests::RunRandomSeeds("ForwardingImmediateVarPath", ForwardingImmediateVarPath);
    oox::twist_tests::RunRandomSeeds("EmptyVarReaderWriterForms", EmptyVarReaderWriterForms);
    oox::twist_tests::RunRandomSeeds("RuntimeDeferredTagAndLifeCount", RuntimeDeferredTagAndLifeCount);
    oox::twist_tests::RunRandomSeeds("WaitStatusReadyOnSuccess", WaitStatusReadyOnSuccess);
    return 0;
}
