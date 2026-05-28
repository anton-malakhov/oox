#pragma once

#include <oox/oox.h>

#include <exception>

#if OOX_TWIST_TEST
namespace oox::internal {

struct test_hooks {
    // Force execution of assign_prerequisite deferred-redirect branch.
    static bool hit_deferred_redirect_assign_prerequisite() {
        storage_task<1, int> producer;
        storage_task<1, int> redirected_target;
        storage_task<1, int> consumer;

        arc* forwarding = new arc(&redirected_target, 0, arc::flow_only);
        producer.head.store(details::encode_deferred_redirect_arc(forwarding), std::memory_order_release);

        int refs = consumer.assign_prerequisite(&producer, 0);

        int moved_arcs = 0;
        arc* list = redirected_target.head.exchange(reinterpret_cast<arc*>(k_task_done_tag), std::memory_order_acq_rel);
        while (list && !details::is_arc_list_tagged(list)) {
            arc* next = list->next;
            delete list;
            list = next;
            ++moved_arcs;
        }

        return refs == 1 && moved_arcs >= 1;
    }

    // Force do_notify_out branch with an already installed non-tagged next_writer.
    static bool hit_do_notify_out_with_non_tagged_writer() {
        storage_task<1, int> producer;
        storage_task<1, int> next_writer;

        producer.out(0).next_writer.store(&next_writer, std::memory_order_release);
        producer.out(0).countdown.store(6, std::memory_order_release);

        int refs = producer.do_notify_out(0, 5);
        return refs == 0;
    }

    // Force set_next_writer branch where old and new values are no-owner markers.
    static bool hit_set_next_writer_no_owner_branch() {
        storage_task<1, int> producer;
        producer.out(0).next_writer.store(details::next_writer_no_owner_marker(), std::memory_order_release);
        producer.set_next_writer(0, details::next_writer_no_owner_marker());
        return details::is_next_writer_no_owner_marker(
            producer.out(0).next_writer.load(std::memory_order_acquire));
    }

#if OOX_EXCEPTIONS_ENABLED
    // Drive every branch of publish_failure_from() deterministically. These cancellation
    // transitions only touch start_count/failure-bit atomics, so they are safe to invoke
    // on standalone task nodes without a live scheduler.
    static bool hit_publish_failure_from_branches() {
        // No source: treated as an explicit user cancellation.
        storage_task<1, int> c_null;
        c_null.publish_failure_from(nullptr, 0);
        const bool null_user =
            (c_null.failure_bits_relaxed() & task_node::start_user_cancelled_bit) != 0;

        // Failure observed on a non-zero consumer port collapses to dependency cancellation.
        storage_task<1, int> src_port, c_port;
        c_port.publish_failure_from(&src_port, 1);
        const bool port_dep =
            (c_port.failure_bits_relaxed() & task_node::start_dependency_cancelled_bit) != 0;

        // A user-cancelled source propagates as user cancellation.
        storage_task<1, int> src_user, c_user;
        src_user.cancel();
        c_user.publish_failure_from(&src_user, 0);
        const bool prop_user =
            (c_user.failure_bits_relaxed() & task_node::start_user_cancelled_bit) != 0;

        // A dependency-cancelled source (set via a null exception) propagates as dependency
        // cancellation; this also exercises the null-eptr branch of try_set_exception().
        storage_task<1, int> src_dep, c_dep;
        src_dep.try_set_exception(std::exception_ptr{});
        const bool src_dep_set =
            (src_dep.failure_bits_relaxed() & task_node::start_dependency_cancelled_bit) != 0;
        c_dep.publish_failure_from(&src_dep, 0);
        const bool prop_dep =
            (c_dep.failure_bits_relaxed() & task_node::start_dependency_cancelled_bit) != 0;

        // Exception bit set but no exception control available: must degrade to dependency
        // cancellation rather than dereference a missing control.
        storage_task<1, int> src_exc, c_exc;
        src_exc.visible_failure_bits.fetch_or(task_node::start_exception_bit, std::memory_order_release);
        c_exc.publish_failure_from(&src_exc, 0);
        const bool prop_exc =
            (c_exc.failure_bits_relaxed() & task_node::start_dependency_cancelled_bit) != 0;

        return null_user && port_dep && prop_user && src_dep_set && prop_dep && prop_exc;
    }

    // A pristine task reports no failure and no exception through every accessor.
    static bool hit_failure_accessors_clean() {
        storage_task<1, int> t;
        const bool no_failure =
            !t.has_start_failure() && !t.has_failure() && t.failure_bits() == 0u;
        const bool no_exception =
            t.local_exception() == nullptr &&
            t.local_exception_control_handle() == nullptr &&
            t.incoming_exception_control_handle() == nullptr;
        return no_failure && no_exception;
    }

    // Cover every return of internal::failure_wait_status() without a live wait.
    static bool hit_failure_wait_status_branches() {
        using internal::failure_wait_status;

        storage_task<1, int> ok;
        const bool ready =
            failure_wait_status<false>(nullptr, 0) == wait_status::ready &&
            failure_wait_status<false>(&ok, 0) == wait_status::ready;

        storage_task<1, int> port_failed;
        port_failed.cancel();
        const bool port_dep =
            failure_wait_status<false>(&port_failed, 1) == wait_status::dependency_cancelled;

        storage_task<1, int> user_cancelled;
        user_cancelled.cancel();
        const bool user =
            failure_wait_status<false>(&user_cancelled, 0) == wait_status::user_cancelled;

        storage_task<1, int> dep_cancelled;
        dep_cancelled.try_set_exception(std::exception_ptr{});
        const bool dep =
            failure_wait_status<false>(&dep_cancelled, 0) == wait_status::dependency_cancelled;

        storage_task<1, int> exc_no_control;
        exc_no_control.visible_failure_bits.fetch_or(task_node::start_exception_bit, std::memory_order_release);
        const bool exc =
            failure_wait_status<false>(&exc_no_control, 0) == wait_status::dependency_cancelled;

        return ready && port_dep && user && dep && exc;
    }

    // Reference-count both branches of the exception_control helpers, including the
    // null no-ops and the final delete.
    static bool hit_exception_control_lifecycle() {
        const bool retain_null = retain_exception_control(nullptr) == nullptr;
        release_exception_control(nullptr);  // null no-op branch

        auto* control = new exception_control_struct(std::make_exception_ptr(int{0}));
        const bool retained = retain_exception_control(control) == control;  // ref 1 -> 2
        release_exception_control(control);  // 2 -> 1
        release_exception_control(control);  // 1 -> 0, deletes
        return retain_null && retained;
    }

    // mark_failure() and store_exception_control() must no-op once a task is completed
    // (its head is the done marker), since there are no successors left to notify.
    static bool hit_failure_on_completed_task() {
        storage_task<1, int> done_marked;
        done_marked.head.store(reinterpret_cast<arc*>(k_task_done_tag), std::memory_order_release);
        const bool mark_rejected =
            !done_marked.mark_failure(task_node::start_user_cancelled_bit) &&
            done_marked.failure_bits_relaxed() == 0u;

        storage_task<1, int> done_store;
        done_store.head.store(reinterpret_cast<arc*>(k_task_done_tag), std::memory_order_release);
        const bool store_rejected =
            !done_store.store_exception_control(new exception_control_struct(std::make_exception_ptr(int{0})));

        return mark_rejected && store_rejected;
    }

    // Drive do_notify_arcs_impl<MayFail=true> for a failed producer notifying a flow-only
    // successor: exercises the producer_failed computation and the failure forwarding to "n".
    static bool hit_do_notify_arcs_failed_producer() {
        storage_task<1, int> producer;
        storage_task<1, int> consumer;
        // Keep the consumer from actually spawning when its prerequisite is removed.
        consumer.start_count.store(task_node::start_count_mask, std::memory_order_release);
        producer.cancel();  // sets start_failure_flag -> producer_failed == true

        int count[1] = {1 << 20};
        arc* a = new arc(&consumer, 0, arc::flow_only);
        a->next = nullptr;
        producer.template do_notify_arcs<true>(a, count);

        return (consumer.failure_bits_relaxed() & task_node::start_user_cancelled_bit) != 0;
    }

    // publish_failure_from on a completed consumer: the exception signal cannot be installed,
    // so the freshly retained control must be released again (the delete-signal branch).
    static bool hit_publish_failure_add_arc_failure() {
        storage_task<1, int> source;
        source.try_set_exception(std::make_exception_ptr(int{0}));  // real exception + control on source
        const bool source_has_exception =
            (source.failure_bits_relaxed() & task_node::start_exception_bit) != 0;

        storage_task<1, int> done_consumer;
        done_consumer.head.store(reinterpret_cast<arc*>(k_task_done_tag), std::memory_order_release);
        done_consumer.publish_failure_from(&source, 0);  // add_arc on done consumer fails -> delete signal
        // mark_failure is rejected on the completed consumer, so its bits stay clear.
        const bool consumer_clear = done_consumer.failure_bits_relaxed() == 0u;

        // Drain the source's pending exception signal so it can be destroyed cleanly.
        arc* h = source.head.exchange(reinterpret_cast<arc*>(k_task_done_tag), std::memory_order_acq_rel);
        while (h && !details::is_arc_list_tagged(h)) {
            arc* next = h->next;
            delete h;
            h = next;
        }
        return source_has_exception && consumer_clear;
    }
#endif  // OOX_EXCEPTIONS_ENABLED
};

}  // namespace oox::internal
#endif
