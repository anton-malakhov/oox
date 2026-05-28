#pragma once

#include <oox/oox.h>

#if OOX_TWIST_TEST
namespace oox::internal {

struct test_hooks {
    // Force execution of assign_prerequisite deferred-redirect branch.
    static bool hit_deferred_redirect_assign_prerequisite() {
        storage_task<1, int> producer;
        storage_task<1, int> redirected_target;
        storage_task<1, int> consumer;

        arc* forwarding = new arc(&redirected_target, 0, arc::flow_only);
        producer.head.store(encode_deferred_redirect_arc(forwarding), std::memory_order_release);

        int refs = consumer.assign_prerequisite(&producer, 0);

        int moved_arcs = 0;
        arc* list = redirected_target.head.exchange(reinterpret_cast<arc*>(k_task_done_tag), std::memory_order_acq_rel);
        while (list && !is_arc_list_tagged(list)) {
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
};

}  // namespace oox::internal
#endif
