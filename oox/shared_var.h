// Copyright (C) 2026
//
// SPDX-License-Identifier: Apache-2.0

#ifndef __OOX_SHARED_VAR_H__
#define __OOX_SHARED_VAR_H__

#include <oox/oox.h>

#include <memory>
#include <mutex>

// oox::shared_var<T> — thread-safe, copyable counterpart of oox::var<T>.
//
// Multiple threads may concurrently:
//   - register readers/writers through oox::run(f, sv, ...);
//   - call get()/wait()/cancel();
//   - copy the handle (reference-counted) and assign new values.
//
// Writer serialization across threads is guaranteed: writers chained onto the
// same shared_var are linearized by the internal state mutex, so their order
// is a total order (same model as a single-threaded var, now multi-threaded).
//
// Design (variant 1, "thick handle"): see docs/design-shared-var.md.
// v1 limitations:
//   - get() returns a copy (T must be copyable);
//   - forwarding (producers returning var) is not supported;
//   - Folly backend: get()/wait() from multiple fibers is not supported
//     (single-waiter Baton);
//   - moving a shared_var from two threads at once is a data race on the
//     user side (same contract as std::shared_ptr).

namespace oox {

template <typename T, bool CanThrow = default_exception_policy>
class shared_var;

namespace internal {

// Mutex abstraction for the shared state. oox.h's sync namespace provides a
// mutex only under HAVE_TWIST; shared_var needs one in every build, so the
// alias lives here (keeps oox.h untouched).
#if HAVE_TWIST
using shared_var_mutex = sync::mutex;
#else
using shared_var_mutex = std::mutex;
#endif

// ---------------------------------------------------------------------------
// shared_var_args — mirrors oox_var_args for shared_var arguments of oox::run
// ---------------------------------------------------------------------------

template <typename Types, bool SelfCanThrow, typename C, bool VarCanThrow, typename... Args>
struct shared_var_args;

template <typename T, typename... Types, typename C, bool VarCanThrow, typename... Args, bool SelfCanThrow>
struct shared_var_args<types<T, Types...>, SelfCanThrow, C, VarCanThrow, Args...>
    : base_args<types<Types...>, SelfCanThrow, Args...> {
    using base_type = base_args<types<Types...>, SelfCanThrow, Args...>;
    using ooxed_type = std::decay_t<C>;
    using var_type = var<ooxed_type, VarCanThrow>;
    using shared_type = shared_var<ooxed_type, VarCanThrow>;

    uintptr_t my_ptr;

    shared_var_args(const shared_type& cov, Args&&... args)
        : base_type(std::forward<Args>(args)...) {}

    static constexpr int is_writer = (std::is_rvalue_reference_v<C>
        || (std::is_lvalue_reference_v<T> && !std::is_const_v<std::remove_reference_t<T>>)) ? 1 : 0;
    static constexpr int write_nodes_count = base_type::write_nodes_count + is_writer;

    // Registration runs under the shared state mutex. The lock is scoped to
    // THIS argument only: the recursive base_type::setup() for the following
    // arguments runs unlocked, so passing the same shared_var twice (e.g. a
    // reader functor with two parameters fed from one handle) cannot
    // self-deadlock on a non-recursive mutex.
    int setup(int port, task_node* self, const shared_type& cov, Args&&... args) {
        int count = is_writer;
        __OOX_TRACE("%p arg: %s=%p as %s: is_writer=%d", self, get_type<C>("shared_var<A>").c_str(),
                    cov.state_.get(), get_type<T>("T").c_str(), count);
        {
            // Copy the shared_ptr: the caller's handle may be released while
            // we hold the lock, but the state must stay alive.
            auto state = cov.state_;
            std::unique_lock<shared_var_mutex> lock(state->mtx);
            if (!state->inner.current_task) {
                state->inner = var_type(ooxed_type()); // lazy default value, like var
            }
            if constexpr (is_writer) {
                static_assert(VarCanThrow || !SelfCanThrow,
                              "throwing task cannot write to non-throwing shared_var");
                state->inner.set_next_writer(port, self);
            } else {
                static_assert(SelfCanThrow || !VarCanThrow,
                              "non-throwing task cannot depend on throwing shared_var");
                count = self->assign_prerequisite(state->inner.current_task,
                                                  state->inner.current_port());
            }
            if (state->inner.current_port_and_flags.is_forwarded) {
                // Kept for symmetry with oox_var_args; never reachable in v1
                // (shared_var never binds a forwarded producer).
                oox_var_base& next = *(oox_var_base*)state->inner.storage_ptr;
                my_ptr = details::encode_forwarded_storage_ptr(&next.storage_ptr);
            } else {
                my_ptr = (uintptr_t)state->inner.storage_ptr;
            }
        }
        return count + base_type::setup(port + is_writer, self, std::forward<Args>(args)...);
    }

    // Runs inside the worker task. No lock required: the graph orders the
    // producer's write before this read, and the storage slot is kept alive by
    // the owning shared state.
    C&& consume() {
        void* state_ptr = nullptr;
        if (details::is_forwarded_storage_ptr(my_ptr)) {
            state_ptr = *details::decode_forwarded_storage_ptr(my_ptr);
        } else {
            state_ptr = reinterpret_cast<void*>(my_ptr);
        }
        __OOX_ASSERT_EX(state_ptr, "null result_state storage");

        auto* state = static_cast<internal::result_state<ooxed_type, VarCanThrow>*>(state_ptr);
        if constexpr (std::is_lvalue_reference_v<T> && !std::is_const_v<std::remove_reference_t<T>>) {
            if (!state->has_value()) {
                state->emplace(); // requires default-constructible T
            }
        }
        __OOX_ASSERT_EX(state->has_value(), "read from empty result_state");
        return static_cast<C&&>(state->value());
    }
};

// base_args partial specializations mapping shared_var argument kinds to
// the same access categories as var (writer/reader/copy/final).
template <typename T, typename... Types, typename A, bool VarCanThrow, typename... Args, bool SelfCanThrow>
struct base_args<types<T, Types...>, SelfCanThrow, shared_var<A, VarCanThrow>&, Args...>
    : shared_var_args<types<T, Types...>, SelfCanThrow, A&, VarCanThrow, Args...> {
    using shared_var_args<types<T, Types...>, SelfCanThrow, A&, VarCanThrow, Args...>::shared_var_args;
};

template <typename T, typename... Types, typename A, bool VarCanThrow, typename... Args, bool SelfCanThrow>
struct base_args<types<T, Types...>, SelfCanThrow, const shared_var<A, VarCanThrow>&, Args...>
    : shared_var_args<types<T, Types...>, SelfCanThrow, const A&, VarCanThrow, Args...> {
    using shared_var_args<types<T, Types...>, SelfCanThrow, const A&, VarCanThrow, Args...>::shared_var_args;
};

template <typename T, typename... Types, typename A, bool VarCanThrow, typename... Args, bool SelfCanThrow>
struct base_args<types<T, Types...>, SelfCanThrow, shared_var<A, VarCanThrow>&&, Args...>
    : shared_var_args<types<T, Types...>, SelfCanThrow, A&&, VarCanThrow, Args...> {
    using shared_var_args<types<T, Types...>, SelfCanThrow, A&&, VarCanThrow, Args...>::shared_var_args;
};

} // namespace internal

// ---------------------------------------------------------------------------
// shared_var<T, CanThrow>
// ---------------------------------------------------------------------------

template <typename T, bool CanThrow>
class shared_var {
    static_assert(std::is_same_v<T, std::decay_t<T>>,
                  "Specialize oox::shared_var only by plain types and pointers."
                  "For references, use reference_wrapper,"
                  "for const types use shared_ptr<T>.");
    static_assert(OOX_EXCEPTIONS_ENABLED || !CanThrow,
                  "oox::shared_var<T, true> requires OOX_EXCEPTIONS_ENABLED=1");

    template <typename, bool, typename, bool, typename...>
    friend struct internal::shared_var_args;

    struct shared_state {
        internal::shared_var_mutex mtx;   // serializes every mutation of the handle state
        var<T, CanThrow> inner;           // the "eternal owner": keeps value slots alive

        shared_state() = default;
        explicit shared_state(deferred_t d) : inner(d) {}
        shared_state(const T& t) : inner(t) {}
        shared_state(T&& t) : inner(std::move(t)) {}
        explicit shared_state(var<T, CanThrow>&& v) : inner(std::move(v)) {}
    };

    std::shared_ptr<shared_state> state_;

    // Snapshot the current slot under the lock, wait for its completion, then
    // invoke fn(task, storage, port). The state mutex is held for the whole
    // operation: the current slot is kept alive by the inner var's countdown
    // hold, and all slot transitions (writer switches, value assignments)
    // happen under the same mutex — so the slot cannot be freed while we wait
    // or read. (Retaining via task_life::life_count does NOT work: the graph's
    // release(n) paths decrement raw refs and would consume the retained one,
    // corrupting the lifetime accounting.)
    template <typename F>
    auto with_ready_slot(F&& fn) const
        -> decltype(std::forward<F>(fn)(static_cast<internal::task_node*>(nullptr),
                                        static_cast<void*>(nullptr), 0)) {
        std::unique_lock<internal::shared_var_mutex> lock(state_->mtx);
        if (!state_->inner.current_task) {
            state_->inner = var<T, CanThrow>(T{}); // lazy var: materialize default value
        }
        internal::task_node* task = state_->inner.current_task;
        void* storage = state_->inner.storage_ptr;
        const int port = state_->inner.current_port();
        // Early-out for slots that are already produced (e.g. a constant value
        // storage task that never executed): var::wait() mirrors this check.
        // Otherwise wait() would block forever on their never-set promise.
        if (!internal::details::is_task_done_marker(task->head.load(std::memory_order_acquire))) {
            task->wait(); // under the state mutex; task execution never takes it
        }
        return std::forward<F>(fn)(task, storage, port);
    }

public:
    shared_var() : state_(std::make_shared<shared_state>()) {}                 // lazy default value
    explicit shared_var(deferred_t d) : state_(std::make_shared<shared_state>(d)) {} // deferred publication
    shared_var(const T& t) : state_(std::make_shared<shared_state>(t)) {}
    shared_var(T&& t) : state_(std::make_shared<shared_state>(std::move(t))) {}
    shared_var(var<T, CanThrow>&& v) : state_(std::make_shared<shared_state>(std::move(v))) {}

    shared_var(const shared_var&) = default;
    shared_var& operator=(const shared_var&) = default;
    shared_var(shared_var&&) noexcept = default;
    shared_var& operator=(shared_var&&) noexcept = default;

    // Write a new value into the shared state. Concurrent assignments
    // serialize on the state mutex; the last one wins.
    shared_var& operator=(const T& t) {
        std::unique_lock<internal::shared_var_mutex> lock(state_->mtx);
        state_->inner = var<T, CanThrow>(t);
        return *this;
    }
    shared_var& operator=(T&& t) {
        std::unique_lock<internal::shared_var_mutex> lock(state_->mtx);
        state_->inner = var<T, CanThrow>(std::move(t));
        return *this;
    }

    // Wait for the current slot and return a copy of its value.
    // Safe to call from any number of threads concurrently.
    [[nodiscard]] T get() const {
        return with_ready_slot([](internal::task_node* task, void* storage, int port) -> T {
#if OOX_EXCEPTIONS_ENABLED
            if constexpr (CanThrow) {
                if (task->has_failure()) {
                    task->throw_failure_for_port(port);
                }
            }
#endif
            return static_cast<internal::result_state<T, CanThrow>*>(storage)->value();
        });
    }

    // Wait until the current slot is produced. Safe to call concurrently.
    void wait() const {
        with_ready_slot([](internal::task_node*, void*, int) {});
    }

    // Wait and report the completion status (failure-aware overloads use this).
    template <bool ThrowOnCancellation = true>
    wait_status wait_for_all_status() const {
        return with_ready_slot([](internal::task_node* task, void*, int port) -> wait_status {
#if OOX_EXCEPTIONS_ENABLED
            return internal::failure_wait_status<ThrowOnCancellation>(task, port);
#else
            (void)task;
            (void)port;
            return wait_status::ready;
#endif
        });
    }

    void cancel() noexcept {
#if OOX_EXCEPTIONS_ENABLED
        if constexpr (CanThrow) {
            std::unique_lock<internal::shared_var_mutex> lock(state_->mtx);
            state_->inner.cancel();
        }
#endif
    }
};

// ---------------------------------------------------------------------------
// Free helpers, mirroring the var overloads
// ---------------------------------------------------------------------------

template <bool ThrowOnCancellation = true, typename T, bool CanThrow>
wait_status wait_for_all_status(const shared_var<T, CanThrow>& on) {
    return on.template wait_for_all_status<ThrowOnCancellation>();
}

template <typename T, bool CanThrow>
void wait_for_all(const shared_var<T, CanThrow>& on) {
    wait_for_all_status<true>(on);
}

template <typename T, bool CanThrow>
[[nodiscard]] T wait_and_get(const shared_var<T, CanThrow>& sv) {
    return sv.get();
}

} // namespace oox

#endif // __OOX_SHARED_VAR_H__
