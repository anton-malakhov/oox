// Copyright (C) 2026
//
// SPDX-License-Identifier: Apache-2.0

#ifndef __OOX_SHARED_VAR_H__
#define __OOX_SHARED_VAR_H__

#include <oox/oox.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

// Twist's simulation runtime models each simulated thread as a coroutine on
// one OS thread, so the standard thread_local storage would be shared across
// simulated threads. The header below must be included at global scope (its
// code resolves unqualified names against the global namespace); the
// TWISTED_STATIC_THREAD_LOCAL_PTR declaration itself is placed next to the
// context type it refers to.
#if HAVE_TWIST && defined(__TWIST_SIM__)
#include <twist/ed/static/thread_local/ptr.hpp>
#endif

// oox::shared_var<T> — thread-safe, copyable counterpart of oox::var<T>.
//
// Multiple threads may concurrently:
//   - register readers/writers through oox::run(f, sv, ...);
//   - call get()/wait()/cancel();
//   - copy the handle (reference-counted) and assign T values.
// Copy/move assignment from another shared_var rebinds the handle object and,
// like assigning one std::shared_ptr object, requires external synchronization.
//
// Writer serialization across threads is guaranteed: writers chained onto the
// same shared_var are linearized by the internal state mutex, so their order
// is a total order (same model as a single-threaded var, now multi-threaded).
// All shared_var arguments of one oox::run() are registered atomically: their
// states are locked in a canonical (address-sorted) order, which prevents
// writer-chain cycles when two threads register writers on the same vars in
// opposite orders.
//
// Design (variant 1, "thick handle"): see docs/design-shared-var.md.
// v1 limitations:
//   - get() returns a copy made under the state lock (T must be copyable);
//   - T must satisfy the selected policy's value-materialization requirements;
//   - T cannot itself be a shared_var specialization;
//   - racing first materializations may construct multiple T{} candidates,
//     but exactly one candidate is installed;
//   - an adopted forwarded var is resolved after its producer completes;
//   - moving from or rebinding one handle concurrently with another access to
//     that handle is a user-side data race (same contract as std::shared_ptr).

namespace oox {

template <typename T, bool CanThrow = default_exception_policy>
class shared_var;

namespace internal {

template <typename>
struct is_shared_var_specialization : std::false_type {};

template <typename T, bool CanThrow>
struct is_shared_var_specialization<shared_var<T, CanThrow>> : std::true_type {};

template <typename T>
concept shareable_value = !is_shared_var_specialization<std::remove_cvref_t<T>>::value;

// Mutex abstraction for the shared state. oox.h's sync namespace provides
// it only under HAVE_TWIST; shared_var needs it in every build, so the alias
// lives here (keeps oox.h untouched).
#if HAVE_TWIST
using shared_var_mutex = sync::mutex;
#else
using shared_var_mutex = std::mutex;
#endif

struct shared_var_storage {
    void* ptr = nullptr;
    bool forwarded = false;
    bool initialize_if_empty = false;
};

// ---------------------------------------------------------------------------
// shared_state_base: type-erased state for atomic multi-state registration
// ---------------------------------------------------------------------------

struct shared_state_base {
    shared_var_mutex mtx;   // serializes every mutation of the handle state

    virtual ~shared_state_base() = default;

    // Lazy materialization of the default value (if the var is still empty).
    virtual void materialize() = 0;
    // Chain `self` as the next writer of this state at `port`.
    virtual void chain_writer(int port, task_node* self) = 0;
    // Register `self` as a reader of the current slot; returns the
    // prerequisite count for run()'s start_count accounting.
    virtual int preregister_reader(task_node* self, int port) = 0;
    // Capture the storage pointer or a descriptor for an adopted forwarded
    // var. The descriptor is resolved in consume(), after its producer ran.
    virtual shared_var_storage capture_storage() = 0;
};

// One pending registration contributed by a single shared_var argument of one
// oox::run() call. Applied at commit() while all involved states are locked.
struct shared_var_registration {
    std::shared_ptr<shared_state_base> state;
    task_node* self;
    int port;
    bool is_writer;
    shared_var_storage* my_storage;
    int count = 0;

    void apply() {
        if (is_writer) {
            state->chain_writer(port, self);
            count = 1;
        } else {
            count = state->preregister_reader(self, port);
        }
        *my_storage = state->capture_storage();
        my_storage->initialize_if_empty = is_writer;
    }

    void discard_alias(const shared_var_storage& primary_storage) {
        if (is_writer) {
            __OOX_ASSERT(self->out(port).next_writer.load(std::memory_order_acquire) == nullptr,
                         "aliased writer output was already registered");
            self->out(port).next_writer.store(details::next_writer_no_owner_marker(),
                                              std::memory_order_release);
        }
        count = 0;
        *my_storage = primary_storage;
    }
};

// Thread-local context for the registration of one oox::run() call. The
// outermost shared_var argument creates it; the setup recursion records every
// shared_var argument; at the end the outermost locks ALL involved states in
// a canonical (address-sorted) order and applies every registration as one
// atomic unit. This is what prevents writer-chain cycles: with atomic
// registration, the second run() always chains onto the complete result of
// the first, no matter the argument order.
struct shared_var_setup_context {
    task_node* registration_task;
    std::vector<shared_var_registration> ops;

    explicit shared_var_setup_context(task_node* self) : registration_task(self) {}

    void add(shared_var_registration op) {
        ops.push_back(std::move(op));
    }

    void commit() {
        if (ops.empty()) {
            return;
        }
        // Canonical lock order: unique states sorted by address.
        std::vector<shared_state_base*> states;
        states.reserve(ops.size());
        for (const auto& op : ops) {
            states.push_back(op.state.get());
        }
        std::sort(states.begin(), states.end());
        states.erase(std::unique(states.begin(), states.end()), states.end());

        // T{} is user code. Materialize each state before taking the atomic
        // multi-state registration lock set.
        for (auto* state : states) {
            state->materialize();
        }

        std::vector<std::unique_lock<shared_var_mutex>> locks;
        locks.reserve(states.size());
        for (auto* s : states) {
            locks.emplace_back(s->mtx);
        }
        for (auto* state : states) {
            auto first = std::find_if(ops.begin(), ops.end(), [state](const auto& op) {
                return op.state.get() == state;
            });
            auto writer = std::find_if(ops.begin(), ops.end(), [state](const auto& op) {
                return op.state.get() == state && op.is_writer;
            });
            auto& primary = writer != ops.end() ? *writer : *first;
            primary.apply();
            for (auto& op : ops) {
                if (op.state.get() == state && &op != &primary) {
                    op.discard_alias(*primary.my_storage);
                }
            }
        }
        locks.clear();
    }

    int total_count() const {
        int total = 0;
        for (const auto& op : ops) {
            total += op.count;
        }
        return total;
    }
};

// Setup context for one oox::run() call, reachable from every shared_var
// argument of that call through the current thread's TLS. Twist's simulation
// runtime models each simulated thread as a coroutine on one OS thread, so the
// standard thread_local storage would be shared across simulated threads;
// under the simulator, use twist's per-simulated-thread TLS instead.
#if HAVE_TWIST && defined(__TWIST_SIM__)
TWISTED_STATIC_THREAD_LOCAL_PTR(shared_var_setup_context, g_shared_var_setup_context);
#else
inline thread_local shared_var_setup_context* g_shared_var_setup_context = nullptr;
#endif

// RAII: creates the setup context at the outermost shared_var argument of a
// run() and destroys it when that setup returns. Inner shared_var arguments
// reuse the outer's context.
struct shared_var_setup_guard {
    shared_var_setup_context* ctx;
    shared_var_setup_context* previous;
    bool outermost;

    explicit shared_var_setup_guard(task_node* self)
        : ctx(g_shared_var_setup_context), previous(nullptr), outermost(false) {
        if (!ctx || ctx->registration_task != self) {
            previous = ctx;
            ctx = new shared_var_setup_context(self);
            g_shared_var_setup_context = ctx;
            outermost = true;
        }
    }

    ~shared_var_setup_guard() {
        if (outermost) {
            g_shared_var_setup_context = previous;
            delete ctx;
        }
    }

    shared_var_setup_context* context() const {
        return ctx;
    }

    bool is_outermost() const {
        return outermost;
    }

    int commit_and_count() {
        __OOX_ASSERT(outermost, "only the outermost shared_var setup can commit");
        g_shared_var_setup_context = previous;
        outermost = false;
        std::unique_ptr<shared_var_setup_context> owned(ctx);
        ctx = nullptr;
        owned->commit();
        return owned->total_count();
    }
};

// oox.h #undef's TASK_EXECUTE_METHOD and the lifetime macros at its end;
// define a local backend-conditional execute signature for the waiter node.
// (The execute_lifetime_guard struct itself stays visible; only its macro
// aliases are gone.)
#if defined(OOX_USING_TBB)
#define OOX_SHARED_VAR_EXECUTE_METHOD tbb_task* execute(execution_data&) override
#else
#define OOX_SHARED_VAR_EXECUTE_METHOD void* execute() override
#endif

// A graph node used by get()/wait() to block until the current slot is
// produced. The getter registers the waiter as a successor of the current
// task (a flow arc — the graph's own lock-free wait-queue) and blocks on the
// waiter's completion via the backend's own task wait (TBB wait_context,
// std promise, ...) — the pool's native mechanism. The graph notifies the
// waiter at the current task's completion (do_notify_arcs ->
// remove_prerequisite -> spawn -> execute -> wakeup). The getter never
// touches the current task after the registration, so the task can be freed
// at its completion safely. The waiter is kept alive by the getter's life
// hold until the wait returns. This also covers the deferred placeholder:
// the first writer's deferred redirect forwards the waiter's arc to the
// writer task.
struct shared_var_waiter : task_node {
    bool subscribe(task_node* producer) {
        auto* successor = new arc(this, 0, arc::flow_only);
        if (producer->add_arc(successor)) {
            return true;
        }
        delete successor;
        return false;
    }

    OOX_SHARED_VAR_EXECUTE_METHOD {
        execute_lifetime_guard oox_waiter_lifetime_guard{this};
        wakeup(); // the backend's own waiter-release (pool-native)
        return nullptr;
    }
#if OOX_EXCEPTIONS_ENABLED
    void notify_successors_virtual() override {
        int unused_count = 0;
        const int refs = task_node::notify_successors<true>(0, &unused_count);
        wakeup();
        release(refs);
    }
#endif
};

#undef OOX_SHARED_VAR_EXECUTE_METHOD

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

    shared_var_storage my_storage;

    shared_var_args(const shared_type& cov, Args&&... args)
        : base_type(std::forward<Args>(args)...) {}

    static constexpr int is_writer = (std::is_rvalue_reference_v<C>
        || (std::is_lvalue_reference_v<T> && !std::is_const_v<std::remove_reference_t<T>>)) ? 1 : 0;
    static constexpr int write_nodes_count = base_type::write_nodes_count + is_writer;
    static constexpr bool consume_is_nothrow = base_type::consume_is_nothrow
        && (!is_writer || policy_value_materializable<ooxed_type, SelfCanThrow>);
    using consumed_args_type = typename prepend_type<
        C&&, typename base_type::consumed_args_type>::type;

    // Registration is deferred to the outermost argument of this run(): the
    // setup context collects every shared_var argument, and the outermost
    // commits them all under one sorted multi-state lock (see
    // shared_var_setup_context). The count contributed by this argument is
    // computed at commit time (readers return the assign_prerequisite result),
    // so inner levels only pass through their recursion result and the
    // outermost returns rest + total_count().
    int setup(int port, task_node* self, const shared_type& cov, Args&&... args) {
        __OOX_TRACE("%p arg: %s=%p as %s: is_writer=%d", self, get_type<C>("shared_var<A>").c_str(),
                    cov.state_.get(), get_type<T>("T").c_str(), is_writer);
        if constexpr (is_writer) {
            static_assert(VarCanThrow || !SelfCanThrow,
                          "throwing task cannot write to non-throwing shared_var");
        } else {
            static_assert(SelfCanThrow || !VarCanThrow,
                          "non-throwing task cannot depend on throwing shared_var");
        }
        shared_var_setup_guard guard(self);
        guard.context()->add(shared_var_registration{
            cov.state_, self, port, is_writer != 0, &my_storage});
        const int rest = base_type::setup(port + is_writer, self, std::forward<Args>(args)...);
        if (guard.is_outermost()) {
            return rest + guard.commit_and_count();
        }
        return rest;
    }

    // Runs inside the worker task. No lock required: the graph orders the
    // producer's write before this read, and the storage slot is kept alive by
    // the owning shared state.
    C&& consume() {
        const internal::var_storage storage{
            my_storage.ptr, my_storage.forwarded, my_storage.initialize_if_empty};
        void* state_ptr = resolve_var_storage<ooxed_type, VarCanThrow>(storage);
        __OOX_ASSERT_EX(state_ptr, "null result_state storage");

        auto* state = static_cast<internal::result_state<ooxed_type, VarCanThrow>*>(state_ptr);
        if (my_storage.initialize_if_empty && !state->has_value()) {
            state->emplace(); // requires default-constructible T
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
    static_assert(internal::policy_value_materializable<T, CanThrow>,
                  "oox::shared_var<T> requires a default-constructible and move- or "
                  "copy-constructible value; non-throwing policy requires nothrow materialization");
    static_assert(internal::shareable_value<T>,
                  "oox::shared_var<T> cannot store another shared_var specialization");

    template <typename, bool, typename, bool, typename...>
    friend struct internal::shared_var_args;

    struct shared_state : internal::shared_state_base {
        var<T, CanThrow> inner; // the "eternal owner": keeps value slots alive

        shared_state() = default;
        explicit shared_state(deferred_t d) : inner(d) {}
        shared_state(const T& t) : inner(t) {}
        shared_state(T&& t) : inner(std::move(t)) {}
        explicit shared_state(var<T, CanThrow>&& v) : inner(std::move(v)) {}

        void materialize() override {
            {
                std::unique_lock<internal::shared_var_mutex> lock(mtx);
                if (inner.current_task) {
                    return;
                }
            }
            var<T, CanThrow> candidate = [&] {
                if constexpr (std::is_move_constructible_v<T>
                              && (CanThrow || std::is_nothrow_move_constructible_v<T>)) {
                    return var<T, CanThrow>(T{});
                } else {
                    T value;
                    return var<T, CanThrow>(value);
                }
            }();
            std::unique_lock<internal::shared_var_mutex> lock(mtx);
            if (!inner.current_task) {
                inner = std::move(candidate);
            }
        }

        void chain_writer(int port, internal::task_node* self) override {
            inner.set_next_writer(port, self);
        }

        int preregister_reader(internal::task_node* self, int port) override {
            return self->assign_prerequisite(inner.current_task, inner.current_port());
        }

        internal::shared_var_storage capture_storage() override {
            if (inner.current_port_and_flags.is_forwarded) {
                return {inner.storage_ptr, true};
            }
            return {inner.storage_ptr, false};
        }
    };

    std::shared_ptr<shared_state> state_;

    // Follow the forwarding chain of an adopted forwarded var (a producer that
    // returned a var): storage_ptr of a forwarded var points at the next var
    // object. The chain is fixed after adoption and alive while inner lives,
    // so it is safe to walk under the state mutex.
    internal::oox_var_base* resolve_current_locked() const {
        internal::oox_var_base* base = &state_->inner;
        while (base->current_port_and_flags.is_forwarded) {
            base = reinterpret_cast<internal::oox_var_base*>(base->storage_ptr);
        }
        return base;
    }

    // Snapshot the current slot under the lock, wait for its completion, then
    // invoke fn(task, storage, port). Every blocking wait releases the state
    // mutex and revalidates the slot after re-locking.
    //
    // Adopted forwarded vars are resolved to the final var after their
    // producer completes. Deferred placeholders use the same waiter-node path
    // as ordinary pending tasks: the first writer redirects the waiter arc.
    template <typename F>
    auto with_ready_slot(F&& fn) const
        -> decltype(std::forward<F>(fn)(static_cast<internal::task_node*>(nullptr),
                                        static_cast<void*>(nullptr), 0)) {
        state_->materialize();
        std::unique_lock<internal::shared_var_mutex> lock(state_->mtx);
        auto wait_for_task = [&](internal::task_node* pending) {
            auto* waiter = internal::task::allocate<internal::shared_var_waiter>();
            waiter->life_set_count(2); // the execute guard (1) + the getter's hold (1)
            waiter->start_count.store(1, std::memory_order_release);
            if (waiter->subscribe(pending)) {
                lock.unlock();
                waiter->wait();
                waiter->release(1);
                lock.lock();
            } else {
                waiter->release(2);
            }
        };
        // A forwarded var's chain target is materialized inside the producer
        // task's result storage during its execution: on async backends (TBB)
        // the chain is not walkable until the producer completes, so wait for
        // it first. (The deferred placeholder never completes on its own and
        // is handled by the deferred branch below instead.)
        while (state_->inner.current_port_and_flags.is_forwarded) {
            internal::task_node* producer = state_->inner.current_task;
            if (!internal::details::is_task_done_marker(producer->head.load(std::memory_order_acquire))) {
                wait_for_task(producer);
            }
            if (state_->inner.current_task != producer) {
                continue;
            }
            if (!internal::details::is_task_done_marker(producer->head.load(std::memory_order_acquire))) {
                continue;
            }
#if OOX_EXCEPTIONS_ENABLED
            if constexpr (CanThrow) {
                if (producer->has_failure()) {
                    return std::forward<F>(fn)(producer, nullptr, state_->inner.current_port());
                }
            }
#endif
            break;
        }
        internal::task_node* task = nullptr;
        void* storage = nullptr;
        int port = 0;
        auto snapshot = [&]() {
            internal::oox_var_base* base = resolve_current_locked();
            task = base->current_task;
            storage = base->storage_ptr;
            port = base->current_port();
        };
        snapshot();
        // Wait until the current slot is produced. The getter registers a
        // waiter node as a successor of the current task (a flow arc — the
        // graph's own lock-free wait-queue) and blocks on the waiter's OWN
        // cv; the graph notifies the waiter at the task's completion
        // (do_notify_arcs -> remove_prerequisite -> spawn -> execute ->
        // cv notify). The getter never touches the current task after the
        // registration, so the task can be freed at its completion safely.
        // This also covers the deferred placeholder: the first writer's
        // deferred redirect forwards the waiter's arc to the writer task, so
        // there is no separate deferred branch and no condition variable on
        // the state. The waiter is allocated per wait and kept alive by the
        // getter's life hold until the wait returns.
        while (!internal::details::is_task_done_marker(task->head.load(std::memory_order_acquire))) {
            wait_for_task(task);
            if (state_->inner.current_task == task
                && internal::details::is_task_done_marker(task->head.load(std::memory_order_acquire))) {
                break; // still the current slot and complete — safe to read
            }
            snapshot(); // the slot was switched while we waited — retry
        }
        return std::forward<F>(fn)(task, storage, port);
    }

public:
    shared_var() : state_(std::make_shared<shared_state>()) {}                 // lazy default value
    explicit shared_var(deferred_t d) : state_(std::make_shared<shared_state>(d)) {} // deferred publication
    shared_var(const T& t) : state_(std::make_shared<shared_state>(t)) {}
    shared_var(T&& t) : state_(std::make_shared<shared_state>(std::move(t))) {}
    shared_var(var<T, CanThrow>&& v) : state_(std::make_shared<shared_state>(std::move(v))) {}

    // These overloads rebind this handle object. As with assigning one
    // std::shared_ptr object, concurrent access to the same object requires
    // external synchronization. Assignment from T below is a graph write.
    shared_var(const shared_var&) = default;
    shared_var& operator=(const shared_var&) = default;
    shared_var(shared_var&&) noexcept = default;
    shared_var& operator=(shared_var&&) noexcept = default;

    // Write through the same shared registration path as every other writer.
    shared_var& operator=(const T& t) requires internal::policy_copy_value_assignable<T, CanThrow> {
        auto value = std::make_shared<T>(t);
        run<CanThrow>([value = std::move(value)](T& target) noexcept(std::is_nothrow_copy_assignable_v<T>) {
            target = *value;
        }, *this);
        return *this;
    }
    shared_var& operator=(const T&) requires (!internal::policy_copy_value_assignable<T, CanThrow>) = delete;
    shared_var& operator=(T&& t) requires internal::policy_move_value_assignable<T, CanThrow> {
        run<CanThrow>([value = std::move(t)](T& target) mutable noexcept(std::is_nothrow_move_assignable_v<T>) {
            target = std::move(value);
        }, *this);
        return *this;
    }
    shared_var& operator=(T&&) requires (!internal::policy_move_value_assignable<T, CanThrow>) = delete;

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
