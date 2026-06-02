// Copyright (C) 2021 Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

#ifndef __OOX_H__
#define __OOX_H__

#include <utility>
#include <type_traits>
#include <limits>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#ifndef OOX_EXCEPTIONS_ENABLED
#define OOX_EXCEPTIONS_ENABLED 0
#endif

#if HAVE_TWIST
#include <mutex>
#include <twist/assist/assert.hpp>
#include <twist/assist/preempt.hpp>
#include <twist/ed/std/atomic.hpp>
#include <twist/ed/std/condition_variable.hpp>
#include <twist/ed/std/mutex.hpp>
#include <twist/ed/std/thread.hpp>
#endif
#if OOX_EXCEPTIONS_ENABLED
#include <exception>
#endif

#if HAVE_OMP
#include <omp.h>
#include <setjmp.h>
#elif HAVE_TBB
#define TBB_USE_ASSERT 0
#include <oneapi/tbb/detail/_task.h>
#include <oneapi/tbb/task_group.h>
#elif HAVE_TF
#include <taskflow/taskflow.hpp>
#elif HAVE_FOLLY
#include <folly/fibers/Baton.h>
#include <folly/fibers/FiberManager.h>
#include <folly/fibers/FiberManagerMap.h>
#include <folly/fibers/FiberManagerInternal.h>
#include <folly/fibers/SimpleLoopController.h>
#else
#include <future>
#endif

#if HAVE_TWIST && OOX_TWIST_TEST
#ifndef __OOX_ASSERT
#define __OOX_ASSERT(cond, msg) TWIST_ASSERT_M((cond), (msg))
#define __OOX_ASSERT_EX(cond, msg) TWIST_ASSERT_M((cond), (msg))
#endif
#endif
#ifndef __OOX_TRACE
#define __OOX_TRACE(...)
#endif
#ifndef __OOX_ASSERT
#include <cassert>
#define __OOX_ASSERT(a, b) assert(a), b
#define __OOX_ASSERT_EX(a, b) __OOX_ASSERT(a, b)
#endif
#if !defined(NDEBUG) || (HAVE_TWIST && OOX_TWIST_TEST)
#define OOX_DEBUG_ONLY(...) do { __VA_ARGS__; } while (false)
#else
#define OOX_DEBUG_ONLY(...) do { } while (false)
#endif

namespace oox {

struct deferred_t { explicit constexpr deferred_t(int = 0) {} };
inline constexpr deferred_t deferred{};

namespace internal {

inline constexpr std::uintptr_t k_task_done_tag = 0x1;
inline constexpr std::uintptr_t k_task_deferred_redirect_tag = 0x2;
inline constexpr std::uintptr_t k_task_tag_mask = k_task_done_tag | k_task_deferred_redirect_tag;
inline constexpr unsigned char k_result_state_empty = 0;
inline constexpr unsigned char k_result_state_cancelled = 1;
inline constexpr unsigned char k_result_state_value = 2;
inline constexpr unsigned char k_result_state_exception = 3;

namespace sync {

#if HAVE_TWIST

template <typename T>
using atomic = twist::ed::std::atomic<T>;

using mutex = twist::ed::std::mutex;
using condition_variable = twist::ed::std::condition_variable;
using thread = twist::ed::std::thread;

inline void preemption_point() {
    twist::assist::PreemptionPoint();
}

#else

template <typename T>
using atomic = std::atomic<T>;

inline void preemption_point() {}

#endif

} // namespace sync

struct task_life {
    // Pointers to this structure and live output nodes
    sync::atomic<int> life_count{0};
    virtual ~task_life() = default;

    void life_add_count(int lifetime) {
        life_count.fetch_add(lifetime, std::memory_order_release);
    }

    void life_set_count(int lifetime) {
        life_count.store(lifetime, std::memory_order_release);
    }

    int  life_get_count() {
        return life_count.load(std::memory_order_acquire);
    }

    bool life_release( int n ) {
        if(life_count.load(std::memory_order_acquire) == n) {
            __OOX_TRACE("%p release all: %d", this, n);
            return true;
        }
        else {
            int k = life_count-=n;
            __OOX_TRACE("%p release: %d", this, k);
            __OOX_ASSERT(k >= 0, "invalid life_count detected while removing prerequisite");
            return (k == 0);          // double-check after atomic
        }
    }
};

template<typename T, bool CanThrow>
struct result_state;

template <typename T>
struct result_state<T, false> {
    using value_type = T;
    static constexpr unsigned char state_unset = 0;
    static constexpr unsigned char state_set = 1;

    result_state() : state_bits_field(state_unset) {}
    result_state(const result_state&) = delete;
    result_state& operator=(const result_state&) = delete;
    result_state(result_state&&) = delete;
    result_state& operator=(result_state&&) = delete;
    ~result_state() = default;

    template <typename... Args>
    void emplace(Args&&... args) {
        const auto previous = static_cast<unsigned char>(state_bits_field);
        __OOX_ASSERT(previous != state_set, "never changing value inside storage with emplace");
        construct_value(std::forward<Args>(args)...);
        state_bits_field = state_set;
    }
    bool has_value() const noexcept { return static_cast<unsigned char>(state_bits_field) == state_set; }
    T& value() { return *ptr(); }
    const T& value() const { return *ptr(); }
    void reset() {
        const auto previous = static_cast<unsigned char>(state_bits_field);
        if (previous == state_set) {
            ptr()->~T();
        }
        state_bits_field = state_unset;
    }

  private:
    struct storage_t {
        alignas(alignof(T)) std::byte data[sizeof(T)];
    };
    storage_t storage{};
    bool state_bits_field : 1;

    template <typename... Args>
    void construct_value(Args&&... args) {
        if constexpr (sizeof...(Args) == 0) {
            ::new (static_cast<void*>(storage.data)) T;
        } else {
            ::new (static_cast<void*>(storage.data)) T(std::forward<Args>(args)...);
        }
    }
    T* ptr() noexcept { return std::launder(reinterpret_cast<T*>(storage.data)); }
    const T* ptr() const noexcept { return std::launder(reinterpret_cast<const T*>(storage.data)); }
};

template <bool CanThrow>
struct result_state<void, CanThrow>;


template <> struct result_state<void, false> {
};

#if OOX_SERIAL_DEBUG  ////////////////////// Serial backend //////////////////////////////////

#define OOX_USING_SERIAL
#define TASK_EXECUTE_METHOD void* execute() override

    struct task : task_life {

        virtual ~task() {}
        virtual void* execute() = 0;

        void release(int n = 1) {
            if (life_release(n)) {
                delete this;
            }
        }

        template<typename T, typename... Args>
        static T* allocate(Args&&... args) {
            return new T(std::forward<Args>(args)...);
        }

        // SERIAL: run synchronously in the current thread
        void spawn() {
            this->execute();
        }

        // SERIAL: nothing to wait for, execute() already ran in spawn()
        void wait() {

        }

        void wakeup() {

        }
    };

///////////////////////////////// Parallel execution  ///////////////////////////////////
#elif HAVE_OMP ///////////////////////// OpenMP ///////////////////////////////////////////
#define OOX_USING_OMP
#define TASK_EXECUTE_METHOD void* execute() override
jmp_buf __openmp_ctx;
struct __openmp_initializer_t {
    __openmp_initializer_t() {
        if(setjmp(__openmp_ctx)) {
            #pragma omp parallel
            #pragma omp masked
            longjmp(__openmp_ctx, 1);
        }
    }
} __openmp_initializer_t;

struct task : task_life {

    virtual ~task() = default;
    virtual void* execute() = 0;

    void release( int n = 1 ) {
        if(life_release(n))
            delete this;
    }
    template<typename T, typename... Args>
    static T* allocate(Args && ... args) {
        return new T(std::forward<Args>(args)...);
    }
    void spawn() {
        auto t = this;
        #pragma omp task firstprivate(t)
        t->execute();
    }
    void wait() {
        #pragma omp taskwait
    }
    void wakeup() {
    }
};
#elif HAVE_TBB ///////////////////////// TBB ///////////////////////////////////////////
#define OOX_USING_TBB
using tbb::detail::d1::execution_data;
using tbb_task = tbb::detail::d1::task;
using tbb::detail::d1::small_object_allocator;
static tbb::task_group_context tbb_context;
#define TASK_EXECUTE_METHOD tbb_task* execute(execution_data&) override

struct task : public tbb_task, task_life {
    tbb::detail::d1::wait_context waiter{1};
#ifndef OOX_USE_STDMALLOC
    small_object_allocator alloc{};
#endif
#if TBB_USE_ASSERT
    std::atomic<bool> is_spawned{false};
    virtual ~task() {
        if(!is_spawned.load(std::memory_order_acquire);)
            waiter.release();
    }
#else
    virtual ~task() = default;
#endif

    TASK_EXECUTE_METHOD {
        __OOX_ASSERT(false, "");
        return nullptr;
    }
    virtual tbb_task* cancel(execution_data& ed) override {
        __OOX_ASSERT(false, "");
        return nullptr;
    }
    void release( int n = 1 ) {
        if(life_release(n)) {
#if OOX_USE_STDMALLOC
            delete this;
#else
            this->~task();
            alloc.deallocate(this);
#endif
        }
    }
    template<typename T, typename... Args>
    static T* allocate(Args && ... args) {
#if OOX_USE_STDMALLOC
        return new T(std::forward<Args>(args)...);
#else
        small_object_allocator a{};
        auto *t = a.new_object<T>(std::forward<Args>(args)...);
        t->alloc = a; // store deallocation info
        return t;
#endif
    }
    void spawn() {
#if TBB_USE_ASSERT
        is_spawned.store(true, std::memory_order_release);
#endif
        tbb::detail::d1::spawn(*this, tbb_context);
    }
    void wait() {
        __OOX_ASSERT(life_get_count(), "");
        tbb::detail::d1::wait(waiter, tbb_context);
    }
    void wakeup() {
        waiter.release();
    }
};
#elif HAVE_TF /////////////////////// Taskflow ///////////////////////////////////////
#include <mutex>
#define OOX_USING_TF
#define TASK_EXECUTE_METHOD void* execute() override

tf::Executor& get_tf_pool() {
    static tf::Executor* tf_pool = new tf::Executor();
    return *tf_pool;
}

struct task : task_life {

    std::promise<void> waiter;
    std::shared_future<void> waiter_future;
    std::once_flag wakeup_once;

    task() : waiter_future(waiter.get_future().share()) {}
    virtual ~task() = default;
    virtual void* execute() = 0;

    void release( int n = 1 ) {
        if(life_release(n))
            delete this;
    }
    template<typename T, typename... Args>
    static T* allocate(Args && ... args) {
        return new T(std::forward<Args>(args)...);
    }
    void spawn() {
        get_tf_pool().silent_async([this]{
            this->execute(); // releases execute lifetime ref via the in-execute guard
        });
    }
    void wait() {
        waiter_future.wait();
    }
    void wakeup() {
      std::call_once(wakeup_once, [this] {
        waiter.set_value();
      });
    }
};
#elif HAVE_TWIST /////////////////////// Twist ///////////////////////////////////////
#define OOX_USING_TWIST
#define TASK_EXECUTE_METHOD void* execute() override

struct task : task_life {
    sync::mutex waiter_mutex;
    sync::condition_variable waiter_cv;
    bool completed = false;

    virtual ~task() = default;
    virtual void* execute() = 0;

    void release( int n = 1 ) {
        if(life_release(n))
            delete this;
    }
    template<typename T, typename... Args>
    static T* allocate(Args && ... args) {
        return new T(std::forward<Args>(args)...);
    }
    void spawn() {
        sync::thread([this] {
            sync::preemption_point();
            this->execute(); // releases execute lifetime ref via the in-execute guard
        }).detach();
    }
    void wait() {
        std::unique_lock<sync::mutex> lock(waiter_mutex);
        waiter_cv.wait(lock, [this] { return completed; });
    }
    void wakeup() {
        {
            std::lock_guard<sync::mutex> lock(waiter_mutex);
            completed = true;
        }
        waiter_cv.notify_all();
    }
};
#elif HAVE_FOLLY /////////////////////// Folly ///////////////////////////////////////
#define OOX_USING_FOLLY
#define TASK_EXECUTE_METHOD void* execute() override

folly::fibers::FiberManager& get_fiber_manager() {
    static folly::fibers::FiberManager* fiber_manager = nullptr;
    static std::once_flag once;
    std::call_once(once, [] {
        auto evb = std::make_unique<folly::EventBase>();
        auto loopController = std::make_unique<folly::fibers::EventBaseLoopController>();
        loopController->attachEventBase(*evb);
        fiber_manager = new folly::fibers::FiberManager(std::move(loopController));

        // Запускаем цикл обработки в отдельном потоке
        std::thread([evb = std::move(evb)]() {
            evb->loopForever();
        }).detach();
    });
    return *fiber_manager;
}

struct task : task_life {

    folly::fibers::Baton baton;

    virtual ~task() = default;
    virtual void* execute() = 0;

    void release( int n = 1 ) {
        if(life_release(n))
            delete this;
    }
    template<typename T, typename... Args>
    static T* allocate(Args && ... args) {
        return new T(std::forward<Args>(args)...);
    }
    void spawn() {
         get_fiber_manager().add([this] {
            this->execute();
        });
    }
    void wait() {
       baton.wait();
    }
    void wakeup() {
        baton.post();
    }
};
#else /////////////////////////////// plain STD impl /////////////////////////////////
#define OOX_USING_STD
#define TASK_EXECUTE_METHOD void* execute() override

struct task : task_life {
    std::promise<void> waiter;

    virtual ~task() = default;
    virtual void* execute() = 0;

    void release( int n = 1 ) {
        if(life_release(n))
            delete this;
    }
    template<typename T, typename... Args>
    static T* allocate(Args && ... args) {
        return new T(std::forward<Args>(args)...);
    }
    void spawn() {
        std::async(std::launch::async, &task::execute, this);
    }
    void wait() {
        waiter.get_future().wait();
    }
    void wakeup() {
        waiter.set_value();
    }
};
#endif // HAVE_TBB,TF ////////////////////////////////////////////////////////////////

inline constexpr int k_execute_lifetime_ref = 1;
inline constexpr int k_embedded_arc_lifetime_ref = 1;

// Each execute() invocation owns one k_execute_lifetime_ref. Forwarding
// continuations add the next execute ref before publishing their wait arc.
struct execute_lifetime_guard {
    task* self;
    ~execute_lifetime_guard() { self->release(k_execute_lifetime_ref); }
};
#define OOX_TASK_EXECUTE_LIFETIME_GUARD ::oox::internal::execute_lifetime_guard oox_execute_lifetime_guard{this}

struct task_node;
struct oox_var_base;
struct arc;

namespace details {

inline constexpr std::uintptr_t k_next_writer_ready_tag = 0x1;
inline constexpr std::uintptr_t k_next_writer_no_owner_tag = 0x3;
inline constexpr std::uintptr_t k_forwarded_storage_ptr_tag = 0x1;
inline constexpr std::uintptr_t k_resolved_storage_ptr_tag = 0x1;

inline task_node* next_writer_ready_marker() {
    return reinterpret_cast<task_node*>(k_next_writer_ready_tag);
}

inline task_node* next_writer_no_owner_marker() {
    return reinterpret_cast<task_node*>(k_next_writer_no_owner_tag);
}

inline bool is_tagged_next_writer(task_node* p) {
    return uintptr_t(p)&k_next_writer_ready_tag;
}

inline bool is_next_writer_ready_marker(task_node* p) {
    return uintptr_t(p) == k_next_writer_ready_tag;
}

inline bool is_next_writer_no_owner_marker(task_node* p) {
    return uintptr_t(p) == k_next_writer_no_owner_tag;
}

inline task_node* encode_next_writer_owner(task_node* owner) {
    __OOX_ASSERT((uintptr_t(owner)&k_next_writer_ready_tag) == 0, "next-writer owner pointer is not aligned");
    return reinterpret_cast<task_node*>(uintptr_t(owner)|k_next_writer_ready_tag);
}

inline task_node* decode_next_writer_owner(task_node* owner) {
    return reinterpret_cast<task_node*>(uintptr_t(owner)&~k_next_writer_ready_tag);
}

inline uintptr_t encode_forwarded_storage_ptr(void* ptr) {
    __OOX_ASSERT((uintptr_t(ptr)&k_forwarded_storage_ptr_tag) == 0, "forwarded storage pointer is not aligned");
    return uintptr_t(ptr)|k_forwarded_storage_ptr_tag;
}

inline bool is_forwarded_storage_ptr(uintptr_t ptr) {
    return ptr&k_forwarded_storage_ptr_tag;
}

inline void* decode_forwarded_storage_ptr(uintptr_t ptr) {
    return reinterpret_cast<void*>(ptr&~k_forwarded_storage_ptr_tag);
}

inline arc* encode_resolved_storage_ptr(void* ptr) {
    __OOX_ASSERT((uintptr_t(ptr)&k_resolved_storage_ptr_tag) == 0, "resolved storage pointer is not aligned");
    return reinterpret_cast<arc*>(uintptr_t(ptr)|k_resolved_storage_ptr_tag);
}

inline void* decode_resolved_storage_ptr(arc* ptr) {
    __OOX_ASSERT((uintptr_t(ptr)&k_resolved_storage_ptr_tag) != 0, "resolved storage pointer is not cached");
    return reinterpret_cast<void*>(uintptr_t(ptr)&~k_resolved_storage_ptr_tag);
}

} // namespace details

struct output_node {
    // 0 if next writer is not known yet.
    // details::next_writer_ready_marker() if value is available and countdown includes extra one.
    // details::next_writer_no_owner_marker() if next writer is end without var ownership.
    // details::encode_next_writer_owner(ptr) if next writer is end with var ownership.
    // Otherwise points to next node that overwrites the value written by this node.
    sync::atomic<task_node*> next_writer{nullptr};
    sync::atomic<int> countdown{1};
    output_node() = default;
};

struct arc {
    // types of task relations beside output dependence
    enum kinds : char {
        flow_only,    //< notify consumer when producer is completed
        flow_only_embedded, //< flow_only arc owned by its task object
        back_only,    //< notify producer when consumer is completed TODO: unnecessary when stored in task directly
        flow_back,    //< flow_only then back_only
        flow_copy,    //< call consumer to copy its value when producer is completed
        forward_copy  //< copy a pointer to the var storage found by producer to consumer
    };
    using port_int = short int;
    arc*       next;
    task_node* node;
    port_int   port;
    kinds      kind;
    arc( task_node* n, int p, kinds k = flow_back ) : node(n), port(port_int(p)), kind(k) {}
};

inline bool is_arc_list_tagged(arc* p) {
    return uintptr_t(p)&k_task_tag_mask;
}

inline arc* encode_deferred_redirect_arc(arc* p) {
    __OOX_ASSERT((uintptr_t(p)&k_task_tag_mask) == 0, "deferred redirect descriptor is not aligned");
    return reinterpret_cast<arc*>(uintptr_t(p)|k_task_deferred_redirect_tag);
}

inline arc* decode_deferred_redirect_arc(arc* p) {
    __OOX_ASSERT((uintptr_t(p)&k_task_deferred_redirect_tag) != 0, "not a deferred redirect descriptor");
    return reinterpret_cast<arc*>(uintptr_t(p)&~k_task_tag_mask);
}

struct arc_list {
    // Root of list of nodes that are waiting for this node's value to be produced.
    // A node can be waiting for *this to produce a value OR waiting for *this to consume its value.
    // Special value 1 means no need to wait (e.g. value has been produced).
    sync::atomic<arc*> head{nullptr};
    // Add i to arc_list.
    // Return true if success, false otherwise.
    bool add_arc( arc* i );
    arc_list() = default;
    ~arc_list() {
        arc* h = head.load(std::memory_order_relaxed);
        __OOX_ASSERT(!h || h == (arc*)k_task_done_tag || (uintptr_t(h)&k_task_deferred_redirect_tag),
                     "destroying task with pending successor arcs");
        if (h && (uintptr_t(h)&k_task_deferred_redirect_tag)) {
            delete decode_deferred_redirect_arc(h);
        }
    }
};

struct task_node : public task, arc_list {
    // Prerequisites to start the task
    sync::atomic<int> start_count{0};
    // TODO: exception storage here?

    task_node() { } // prepare the task for waiting on it directly
    virtual ~task_node() = default;

    // Result output node
    inline output_node& out(int n) const;
    // Add a prerequisite
    int  assign_prerequisite( task_node *n, int req_port );
    // Process flow- and anti-dependence arcs
    void do_notify_arcs( arc* r, int *count );
    // Process output dependence
    int  do_notify_out( int port, int count );
    // Process flow and output arcs. Returns number of finished output nodes
    int  notify_successors( int output_slots, int *counters );
    // Process flow- and anti-dependence arcs. Returns number of finished output nodes
    int  forward_successors( int output_slots, int *counters, oox_var_base& );
    // Account for completion of n prerequisites
    void remove_prerequisite( int n=1 );
    // Process next writer notification
    int  notify_next_writer( task_node* d );
    // Account for removal of a back_arc
    int  remove_back_arc( int output_port, int n=1 );
    // Set new output dependence
    void set_next_writer( int output_port, task_node* n );
    // Call base notify successors
    template<int slots>
    void notify_successors();
    // Call base forward successors
    template<int slots>
    void forward_successors( oox_var_base& );

    // It is called when producer is done and notifies consumers to copy the value
    virtual void on_ready(int) { __OOX_ASSERT(false, "not implemented"); }

    // Forwarding tasks override this to expose the final resolved result storage pointer.
    // Must be valid once task completion is published to consumers.
    virtual void* resolved_storage_ptr() const { __OOX_ASSERT(false, "not a forwarding task"); return nullptr; }
};

bool arc_list::add_arc( arc* i ) {
    __OOX_ASSERT( uintptr_t(i->node)>2, "" );
    for(;;) {
        sync::preemption_point();
        arc* j = head.load(std::memory_order_acquire);
        if( is_arc_list_tagged(j) )
            return false;
        i->next = j;
        sync::preemption_point();
        if( head.compare_exchange_weak( j, i ) ) // TODO: weak or strong? what's perf?
            return true;
        sync::preemption_point();
    }
}

int task_node::assign_prerequisite( task_node *n, int req_port ) {
    arc* j = new arc( this, req_port ); // TODO: embed into the task
    __OOX_ASSERT_EX(j && n, "");
    if( n->add_arc(j) ) {
        __OOX_TRACE("%p assign_prerequisite: assigned to %p, %d",this,n,req_port);
        return 1; // Prerequisite n will decrement start_count when it produces a value
    }

    arc* h = n->head.load(std::memory_order_acquire);
    if (h && (uintptr_t(h)&k_task_deferred_redirect_tag)) {
        // A consumer can race with the first real writer of a deferred var.
        // In that case the deferred placeholder redirects new consumers to
        // the writer task that inherited its waiting arcs.
        arc* forwarded = decode_deferred_redirect_arc(h);
        task_node* d = forwarded->node;
        int port = forwarded->port;
        __OOX_ASSERT(d && port >= 0, "deferred forwarding target is not published");
        n = d;
        req_port = port;
        j->port = arc::port_int(req_port);
        if( n->add_arc(j) ) {
            __OOX_TRACE("%p assign_prerequisite: assigned to forwarded %p, %d",this,n,req_port);
            return 1;
        }
    }

    // Prerequisite n already produced a value. Add this as a consumer of n.
    sync::preemption_point();
    int k = ++n->out(req_port).countdown;
    __OOX_TRACE("%p assign_prerequisite: preventing %p, port %d, count %d",this,n,req_port,k);
    __OOX_ASSERT_EX(k>1,"risk that a prerequisite might be prematurely destroyed");
    j->node = n;
    j->kind = arc::back_only;
    bool success = add_arc(j); //TODO: add_arc_unsafe?
    __OOX_ASSERT_EX(success, "");
    return 0;
}

void task_node::do_notify_arcs( arc* r, int *count ) {
    // Notify successors that value is available
    do {
        arc* j = r;
        r = j->next;
        task_node* n = j->node;
        // Leak trace that motivated explicit ownership here:
        // - test_twist_forwarding / NestedForwardingChain: 72 byte(s), 3 allocation(s) of arc
        // - test_twist_lifetime / ChainedResultDestroyedWhileChildPending: 24 byte(s), 1 allocation(s) of arc
        // Arc is deleted in this loop unless ownership is transferred via n->add_arc(j).
        bool delete_arc = true;
        bool release_embedded_arc_ref = false;

        if( j->kind == arc::back_only ) {
            // Notify producer that this task has finished consuming its value
            __OOX_TRACE("%p notify: %p->remove_back_arc(%d)",this,n,j->port);
            if( int k = n->remove_back_arc( j->port ) )
                n->release( k );
        } else {
            if (j->kind == arc::flow_only_embedded) {
                delete_arc = false;
                release_embedded_arc_ref = true;
            }
            if( j->kind == arc::flow_back ) {
                // "n" is task that consumes value that this task produced.
                // Add back arc so that "n" can notify this when it is done consuming the value.
                j->node = this;
                j->kind = arc::back_only;
                if( out(j->port).next_writer.load(std::memory_order_acquire) != (task_node*)uintptr_t(3) ) {
                    bool b = n->add_arc( j );
                    __OOX_ASSERT_EX(b, "corrupted?");
                    --count[j->port];
                    delete_arc = false; // ownership transferred to n
                }
            } else if( j->kind == arc::flow_copy ) {
                n->on_ready( j->port );
            } else if( j->kind == arc::forward_copy ) {
                __OOX_ASSERT(false, "incorrect forwarding"); // has to be processed by forward_successors only
            }
            // Let "n" know that prerequisite "this" is ready.
            __OOX_TRACE("%p notify: %p->remove_prequisite()",this,n);
            n->remove_prerequisite();
        }
        if (delete_arc) {
            delete j;
        }
        if (release_embedded_arc_ref) {
            n->release(k_embedded_arc_lifetime_ref);
        }
    } while( r );
}

int task_node::do_notify_out( int port, int count ) {
    task_node* null = nullptr;
    if( out(port).next_writer.load(std::memory_order_acquire)==nullptr
        && out(port).next_writer.compare_exchange_strong( null, details::next_writer_ready_marker()) ) {
        // The thread that installs the non-nullptr next_writer will see the ready marker and do the decrement.
        --count;
        __OOX_TRACE("%p notify out %d: next_writer went from 0 to ready",this,port);
    } else if( !details::is_tagged_next_writer(out(port).next_writer.load(std::memory_order_acquire)) ) {
#if OOX_AFFINITY
        task_node* d = out(port).next_writer;
        d->affinity = a;
#endif /* OOX_AFFINITY */
        __OOX_TRACE("%p notify out %d: next_writer is %p\n",this,port,out(port).next_writer.load(std::memory_order_acquire));
    } else {
        __OOX_TRACE("%p notify out %d: next_writer is final: %p\n",this,port,out(port).next_writer.load(std::memory_order_acquire));
    }
    return remove_back_arc( port, count );
}

int task_node::notify_successors( int output_slots, int *count ) {
    for( int i = 0; i <  output_slots; i++ ) {
        // it should be safe to assign countdowns here because no successors were notified yet
        out(i).countdown.store( count[i] = std::numeric_limits<int>::max()/2, std::memory_order_release );
    }
    __OOX_TRACE("%p notify successors",this);
    // Grab list of successors and mark as competed.
    // Note that countdowns can change asynchronously after this point

   if( arc* r = head.exchange( (arc*)k_task_done_tag ) )
        do_notify_arcs( r, count );
    int refs = 0;
    for( int i = 0; i <  output_slots; i++ )
        refs += do_notify_out( i, count[i] );
    __OOX_ASSERT(refs>=0, "");
    return refs;
}

void task_node::remove_prerequisite( int n ) {
    int k = start_count-=n;
    __OOX_ASSERT(k>=0,"invalid start_count detected while removing prerequisite");
    if( k==0 ) {
        __OOX_TRACE("%p remove_prerequisite: spawning",this);
        sync::preemption_point();
        spawn();
    }
}

int task_node::notify_next_writer( task_node* d ) {
    if( details::is_tagged_next_writer(d) ) {
        if( details::is_next_writer_no_owner_marker(d) )
            return 1;
        d = details::decode_next_writer_owner(d);
        if( d == this )
            return 2;
        d->release();
    } else {
        __OOX_ASSERT( d!=nullptr, "remove_back_arc called on output node with next_writer==0" );
        d->remove_prerequisite();
    }
    return 1; // the last, release the node
}

int task_node::remove_back_arc( int output_port, int n ) {
    int k = out(output_port).countdown -= n;
    __OOX_ASSERT(k>=0,"invalid countdown detected while removing back_arc");
    __OOX_TRACE("%p remove_back_arc port %d: %d (next_writer is %p)",this,output_port,k,out(output_port).next_writer.load(std::memory_order_acquire));
    if( k==0 ) {
        // Next writer was waiting on all consumers of me to finish.
        return notify_next_writer( out(output_port).next_writer.load(std::memory_order_acquire) );
    }
    return 0;
}

void task_node::set_next_writer( int output_port, task_node* d ) {
    __OOX_ASSERT( !details::is_next_writer_ready_marker(d), "" );
    task_node* o = out(output_port).next_writer.exchange(d);
    __OOX_TRACE("%p set_next_writer(%d, %p): next_writer was %p",this,output_port,d,o);
    if( o ) {
        if( details::is_next_writer_ready_marker(o) ) {
            // this has value and conceptual back_arc from its owning oox that was removed.
            if( int k = remove_back_arc( output_port ) ) // TODO: optimize it for set_next_writer without contention
                release( k );
        } else {
            __OOX_ASSERT( details::is_next_writer_no_owner_marker(o), "" );
            __OOX_ASSERT( details::is_next_writer_no_owner_marker(d), "TODO forward_successors" ); // TODO
        }
    }
}

template<int slots>
void task_node::notify_successors() {
    int counters[slots];
    int n = notify_successors( slots, counters );
    wakeup();
    release(n);
}

template<int N>
struct output_slots_storage {
    output_node output_nodes[N];
};

template<int slots>
struct task_node_slots : task_node, output_slots_storage<slots> {
    TASK_EXECUTE_METHOD { __OOX_ASSERT(false, "not runnable"); return nullptr; }
};

#if defined(__clang__)
__attribute__((no_sanitize("undefined")))
#endif
output_node& task_node::out(int n) const {
    using self_t = task_node_slots<1024>;
    auto self = const_cast<self_t*>(reinterpret_cast<const self_t*>(this));
    return self->output_nodes[n];
}

template<int slots, typename T>
struct alignas(64) storage_task : task_node_slots<slots>, result_state<T, false> {
    TASK_EXECUTE_METHOD { __OOX_ASSERT(false, "not runnable"); return nullptr; }
    storage_task() = default;
    storage_task(T&& t) { this->emplace(std::move(t)); }
    storage_task(const T& t) { this->emplace(t); }
    ~storage_task() { this->reset(); }
};

struct oox_var_base {
    //TODO: make it a class with private members
    oox_var_base &operator=(const oox_var_base &) = delete;
    static constexpr int k_port_bits = 14;
    static constexpr int k_max_port = (1 << k_port_bits) - 1;

    struct current_port_and_flags_t {
        std::uint16_t port : k_port_bits;
        bool is_forwarded : 1;
        bool is_deferred : 1;
    };
    static_assert(sizeof(current_port_and_flags_t) == sizeof(std::uint16_t),
                  "oox::var packed port/flags must stay 16-bit");

    template< typename T > friend struct gen_oox;
    task_node*  current_task = nullptr;
    void*       storage_ptr;
    int         storage_offset; // task_node* original = ptr - offset
    current_port_and_flags_t current_port_and_flags{}; // port plus var-local forward/deferred flags

    int current_port() const noexcept {
        return static_cast<int>(current_port_and_flags.port);
    }
    void set_current_port(int port) noexcept {
        __OOX_ASSERT_EX(port >= 0 && port <= k_max_port, "oox::var port does not fit packed field");
        current_port_and_flags.port = static_cast<std::uint16_t>(port);
    }

    void set_next_writer( int output_port, task_node* d ) {
        __OOX_ASSERT(current_task, "empty oox::var");

        // If this var was created as deferred, tasks may already be waiting on the
        // deferred storage node (current_task/current_port). The first real writer
        // must inherit those waiting arcs, otherwise readers would never be notified.
        //
        // Also, we must retarget arc->port to the writer's output port, so that
        // back-arcs/countdown protect the correct output slot (the var slot), not slot 0.
        if (current_port_and_flags.is_deferred) {
            arc* forwarding = new arc(d, output_port, arc::flow_only);
            arc* r = current_task->head.exchange(encode_deferred_redirect_arc(forwarding), std::memory_order_acq_rel);
            while(r && !is_arc_list_tagged(r)) {
                sync::preemption_point();
                arc* j = r;
                r = j->next;
                j->port = arc::port_int(output_port);
                bool ok = d->add_arc(j);
                __OOX_ASSERT_EX(ok, "unexpected: writer task already completed while forwarding deferred arcs");
            }
            current_port_and_flags.is_deferred = false;
        }
        current_task->set_next_writer( current_port(), d );
        current_task = d;
        set_current_port(output_port);
    }
    void bind_to( task_node * t, void* ptr, int lifetime, bool fwd = false, bool deferred = false ) {
        current_task = t, storage_ptr = ptr, current_port_and_flags = {};
        current_port_and_flags.is_forwarded = fwd;
        current_port_and_flags.is_deferred = deferred;
        storage_offset = uintptr_t(storage_ptr) - uintptr_t(current_task);
        t->life_set_count(lifetime);
        __OOX_TRACE("%p bind: store=%p life=%d fwd=%d deferred=%d",t,ptr,lifetime,fwd,deferred);
    }
    void* resolved_storage_ptr() const {
        if (current_port_and_flags.is_forwarded) {
            __OOX_ASSERT(current_task, "forwarded var has null current_task");
            return current_task->resolved_storage_ptr();
        }
        __OOX_ASSERT(storage_ptr, "var has null storage_ptr");
        return storage_ptr;
    }
    void wait() {
        __OOX_ASSERT_EX(current_task, "wait for empty oox::var");
        // if head is done marker, the producer is already done:
        // - either a constant storage_task, or
        // - a completed functional_task.
        arc* h = current_task->head.load(std::memory_order_acquire);
        if (k_task_done_tag == (uintptr_t)h) {
            return;
        }
        current_task->wait();
        OOX_DEBUG_ONLY(
            h = current_task->head.load(std::memory_order_acquire);
            __OOX_ASSERT(k_task_done_tag == (uintptr_t)h, "wait returned before task completed");
        );
    }
    void release() {
        if( current_task ) {
            task_node* owner = storage_offset
                ? details::encode_next_writer_owner(reinterpret_cast<task_node*>(uintptr_t(storage_ptr)-storage_offset))
                : details::next_writer_no_owner_marker();
            current_task->set_next_writer( current_port(), owner );
            current_task = nullptr;
        }
    }
    ~oox_var_base() { release(); }
};

#if 0
int task_node::forward_successors( int output_slots, int *count, oox_var_base& n ) {
    for( int i = 0; i <  output_slots; i++ ) {
        // it is safe to assign countdowns here because no successors were notified yet
        out(i).countdown.store(count[i] = std::numeric_limits<int>::max()/2, std::memory_order_release);
    }
    arc* r = head.exchange( (arc*)k_task_done_tag ); // mark it completed
    task_node* d = out(0).next_writer.exchange( details::next_writer_no_owner_marker() ); // finish this node
    int refs = 1;
    __OOX_TRACE("%p forward_successors(%p, %d): arcs=%p next_writer=%p",this,n.current_task,n.current_port,r,d);
    if( r ) {
        arc* l = n.current_task->head.exchange( r ); // forward dependencies
        if( l ) {
            __OOX_TRACE("%p forward_successors(%p, %d): notify arcs myself %p",this,n.current_task,n.current_port,l);
            __OOX_ASSERT( k_task_done_tag == uintptr_t(l), "arc lists merge is not implemented" ); // TODO
            __OOX_ASSERT(!n.is_forward, "not implemented"); // TODO
            do_notify_arcs( r, count );
        }
    }
    if( d ) { // TODO: can be converted as another arc type instead of working with outputs?
        task_node* o = n.current_task->out(n.current_port).next_writer.exchange( d );
        if( o ) { // next node is ready already
            __OOX_TRACE("%p forward_successors(%p, %d): removing back arc myself %p",this,n.current_task,n.current_port,o);
            __OOX_ASSERT( details::is_next_writer_ready_marker(o), "" );
            __OOX_ASSERT(!n.is_forward, "not implemented"); // TODO
            __OOX_ASSERT(out(0).countdown == count[0], "not implemented"); // TODO?
            notify_next_writer( d );
        }
    }
    //n.current_task = nullptr;
    // now we have next writer to be processed here
    for( int i = 1; i <  output_slots; i++ )
        refs += do_notify_out( i, count[i] );
    __OOX_ASSERT(refs>=0, "");
    return refs;
}

template<int slots>
tbb::task* task_node::forward_successors( oox_var_base& m ) {
    int counters[slots];
    int k, n = forward_successors( slots, counters, m );
    if( life_count.load(std::memory_order_aquire) != n && (k = (life_count -= n)) > 0 ) {
        __OOX_ASSERT(k>=0,"invalid life_count detected while forwarding prerequisites");
        recycle_as_safe_continuation(); // do not destroy the task after execution and decrement parent().ref_count()
        set_parent(this);   // and decrement this->ref_count() after completion to enable direct waits on this task
    } else set_ref_count(0);
    return nullptr;
}
#endif

template< typename T > struct gen_oox;

} // namespace internal


template< typename T >
class var : public internal::oox_var_base {
    static_assert(std::is_same_v<T, std::decay_t<T>>,
                  "Specialize oox::var only by plain types and pointers."
                  "For references, use reference_wrapper,"
                  "for const types use shared_ptr<T>.");

    void* allocate_new() noexcept {
        auto *v = internal::task::allocate<internal::storage_task<1, T>>();
        __OOX_TRACE("%p oox::var",v);
        v->out(0).next_writer.store(internal::details::next_writer_ready_marker(), std::memory_order_release);
        v->head.store((internal::arc*)internal::k_task_done_tag, std::memory_order_release);
        // nobody wait on this task
        this->bind_to( v, static_cast<internal::result_state<T, false>*>(v), 2 );
        return storage_ptr;
    }

    void* allocate_deferred() noexcept {
        auto *v = internal::task::allocate<internal::storage_task<1, T>>();
        __OOX_TRACE("%p oox::var(deferred)", v);
        // Make writers behave like for a normal initial value (next_writer ready),
        // BUT do NOT mark the node completed (head stays nullptr), so readers block.
        v->out(0).next_writer.store(internal::details::next_writer_ready_marker(), std::memory_order_release);
        // v->head is intentionally left as nullptr (not ready)
        this->bind_to(v, static_cast<internal::result_state<T, false>*>(v), 2, false, true);
        return storage_ptr;
    }

public:
    var()                    { } // allocates default value lazily for sake of optimization
    var(deferred_t) {
        allocate_deferred(); // storage exists, but value is not ready
    }
    var(const T& t) noexcept {
        auto* state = static_cast<internal::result_state<T, false>*>(allocate_new());
        state->emplace(t); // TODO: add exception-safe
    }
    var(T&& t)      noexcept {
        auto* state = static_cast<internal::result_state<T, false>*>(allocate_new());
        state->emplace(std::move(t));
    }
    var(var<T>&& t) : internal::oox_var_base(std::move(t)) { t.current_task = nullptr; }
    var& operator=(var<T>&& t) {
        release();
        new(this) internal::oox_var_base(std::move(t));
        __OOX_ASSERT_EX(current_task, "");
        t.current_task = nullptr;
        return *this;
    }
    ~var() { release(); }
    [[nodiscard]] T get() {
        wait();
        return static_cast<internal::result_state<T, false>*>(storage_ptr)->value();
    }
};

template<>
class var<void> : public internal::oox_var_base {
    template< typename T > friend struct gen_oox;
public:
    var() {}
    template<typename D>
    var(var<D>&& src) : internal::oox_var_base(src) {
        ((internal::task_node*)(uintptr_t(src.storage_ptr)-src.storage_offset))->release();
        src.current_task = nullptr;
    }
};

using node = var<void>;

namespace internal {
template< typename T >
std::string get_type(const char *m = "T") {
    std::string s;
    if constexpr (std::is_const_v<std::remove_reference_t<T>> || std::is_const_v<T>) {
        s += "const ";
    }
    s += m;
    if constexpr (std::is_lvalue_reference_v<T>) s += "&";
    if constexpr (std::is_rvalue_reference_v<T>) s += "&&";
    return s;
}

template< typename... Args > struct types {};

// Types is types<list> of user functor argument types
// Args is variadic list of run argument types
template< typename Types, typename... Args > struct base_args;
// User functor might have default arguments which are not specified thus ignoring them
template< typename IgnoredTypes > struct base_args<IgnoredTypes> {
    static constexpr int write_nodes_count = 1; // for resulting node
    int setup(int, internal::task_node *) { return 0 /* resulting node is ready initially*/; }
};

template< typename T, typename... Types, typename A, typename... Args >
struct base_args<types<T, Types...>, A, Args...> : base_args<types<Types...>, Args...> {
    using base_type = base_args<types<Types...>, Args...>;

    std::decay_t<A> my_value;

    base_args( A&& a, Args&&... args ) : base_type( std::forward<Args>(args)... ), my_value(std::forward<A>(a)) {}
    std::decay_t<A>&& consume() { return std::move(my_value); }
    static constexpr int write_nodes_count = base_type::write_nodes_count;
    int setup( int port, internal::task_node *self, A&& a, Args&&... args ) {
        //__OOX_ASSERT(my_value == a, "");
        return base_type::setup( port, self, std::forward<Args>(args)...);
    }
};

template< typename Types, typename... Args > struct oox_var_args;
template< typename T, typename... Types, typename C, typename... Args >
struct oox_var_args<types<T, Types...>, C, Args...> : base_args<types<Types...>, Args...> {
    using base_type = base_args<types<Types...>, Args...>;
    using ooxed_type = std::decay_t<C>;
    using var_type = var<ooxed_type>;

    uintptr_t my_ptr;
    // TODO: copy-based optimizations
    oox_var_args( const var_type& cov, Args&&... args ) : base_type( std::forward<Args>(args)... ) {}
    static constexpr int is_writer = (std::is_rvalue_reference_v<C>
        || (std::is_lvalue_reference_v<T> && !std::is_const_v<std::remove_reference_t<T>>))? 1 : 0;
    static constexpr int write_nodes_count = base_type::write_nodes_count + is_writer;

    int setup( int port, internal::task_node *self, const var_type& cov, Args&&... args ) {
        int count = is_writer;
        __OOX_TRACE("%p arg: %s=%p as %s: is_writer=%d", self, get_type<C>("oox::var<A>").c_str(), cov.current_task, get_type<T>("T").c_str(), count);
        if( !cov.current_task )
            new( &const_cast<var_type&>(cov) ) var_type(ooxed_type()); // allocate oox container with default value
        if( count ) {
            auto &ov = const_cast<var_type&>(cov); // actual type is non-const due to is_writer
            ov.set_next_writer( port, self );// TODO: add 'count =' because no need in sync here
        } else
            count = self->assign_prerequisite( cov.current_task, cov.current_port() );
        if( cov.current_port_and_flags.is_forwarded ) {
            my_ptr = details::encode_forwarded_storage_ptr(cov.storage_ptr);
        } else
            my_ptr = (uintptr_t)cov.storage_ptr;
        //TODO: broken? if( !std::is_lvalue_reference_v<C> ) // consume oox::var
        //    ov.~var(); // TODO: no need in sync for not yet published task
        return count + base_type::setup( port+is_writer, self, std::forward<Args>(args)...);
    }
    C&& consume() {
        internal::result_state<ooxed_type, false>* state = nullptr;
        if( details::is_forwarded_storage_ptr(my_ptr) ) {
            auto* base = reinterpret_cast<oox_var_base*>(details::decode_forwarded_storage_ptr(my_ptr));
            state = reinterpret_cast<internal::result_state<ooxed_type, false>*>(base->resolved_storage_ptr());
        } else {
            state = reinterpret_cast<internal::result_state<ooxed_type, false>*>(my_ptr);
        }
        __OOX_ASSERT_EX(state, "null result_state storage");

        if constexpr (std::is_lvalue_reference_v<T> && !std::is_const_v<std::remove_reference_t<T>>) {
            if(!state->has_value()) {
                state->emplace(); // requires default-constructible T
            }
        }
        __OOX_ASSERT_EX(state->has_value(), "read from empty result_state");
        return static_cast<C&&>(state->value());
    }
};
template< typename T, typename... Types, typename A, typename... Args >
struct base_args<types<T, Types...>, var<A>&, Args...> : oox_var_args<types<T, Types...>, A&, Args...> {
    using oox_var_args<types<T, Types...>, A&, Args...>::oox_var_args;
};
template< typename T, typename... Types, typename A, typename... Args >
struct base_args<types<T, Types...>, const var<A>&, Args...> : oox_var_args<types<T, Types...>, const A&, Args...> {
    using oox_var_args<types<T, Types...>, const A&, Args...>::oox_var_args;
};
template< typename T, typename... Types, typename A, typename... Args >
struct base_args<types<T, Types...>, var<A>&&, Args...> : oox_var_args<types<T, Types...>, A&&, Args...> {
    using oox_var_args<types<T, Types...>, A&&, Args...>::oox_var_args;
};

template< typename F, typename... Preceding, typename Args >
auto apply_args( F&& f, Args&& pack, Preceding&&... params ) {
    return apply_args(std::forward<F>(f),
                      std::forward<typename Args::base_type>(pack),
                      std::forward<Preceding>(params)...,
                      pack.consume());
}

template< typename F, typename... Preceding, typename Last >
auto apply_args( F&& f, base_args<Last>&& /*pack*/, Preceding&&... params ) {
    return std::forward<F>(f)(std::forward<Preceding>(params)...);
}

template< typename F, typename Args >
struct oox_bind {
    F my_func;
    Args my_args;
    oox_bind(F&& f, Args&& a) : my_func(std::forward<F>(f)), my_args(std::move(a)) {}
    auto operator()() { return apply_args(std::move(my_func), std::move(my_args)); }
};

template<int slots, typename F, typename R>
struct alignas(64) functional_task : storage_task<slots, F>, result_state<R, false> {
    using functor_base = storage_task<slots, F>;
    using result_base = result_state<R, false>;
    using functor_base::functor_base;
    TASK_EXECUTE_METHOD {
        OOX_TASK_EXECUTE_LIFETIME_GUARD;
        __OOX_TRACE("%p do_run: start",this);
        result_base::emplace(functor_base::value()());
        task_node::notify_successors<slots>();
        return nullptr;
    }
    ~functional_task() {
        result_base::reset();
    }
};

template<int slots, typename F>
struct functional_task<slots, F, void> : storage_task<slots, F> {
    using storage_task<slots, F>::storage_task;
    TASK_EXECUTE_METHOD {
        OOX_TASK_EXECUTE_LIFETIME_GUARD;
        __OOX_TRACE("%p do_run: start",this);
        this->value()();
        task_node::notify_successors<slots>();
        return nullptr;
    }
};

template<int slots, typename F, typename VT> // forwarding task
struct functional_task<slots, F, var<VT> > : storage_task<slots, F> {
    // TODO: NRVO optimized forwarding
    using storage_task<slots, F>::storage_task;
    std::aligned_storage_t<sizeof(var<VT>), alignof(var<VT>)> my_result;
    arc forwarding_wait_arc{nullptr, 0, arc::flow_only_embedded};
    bool is_executed : 1 = false;
    void* resolved_storage_ptr() const override { return details::decode_resolved_storage_ptr(forwarding_wait_arc.next); }
    TASK_EXECUTE_METHOD {
        OOX_TASK_EXECUTE_LIFETIME_GUARD;
#if 0
        __OOX_TRACE("%p do_run: start forward",this);
        new(my_result.begin()) var<VT>( this->value()() );
        return task_node::forward_successors<slots>( *my_result.begin() );
#else
        if( !is_executed ) {
            __OOX_TRACE("%p do_run: start forward",this);
            new(&my_result) var<VT>( this->value()() );
            is_executed = true;
            this->start_count.store(1, std::memory_order_release);
            arc* j = &forwarding_wait_arc;
            j->next = nullptr;
            j->node = this;
            j->port = arc::port_int(0);
            j->kind = arc::flow_only_embedded;
            this->life_add_count(k_execute_lifetime_ref + k_embedded_arc_lifetime_ref);
            if( reinterpret_cast<var<VT>*>(&my_result)->current_task->add_arc(j) ) {
                __OOX_TRACE("%p do_run: add_arc", this); // recycle_as_continuation was here
                return nullptr;
            }
            else {
                this->release(k_execute_lifetime_ref + k_embedded_arc_lifetime_ref);
            }
        }
        __OOX_TRACE("%p do_run: notify forward",this);
        forwarding_wait_arc.next = details::encode_resolved_storage_ptr(reinterpret_cast<var<VT>*>(&my_result)->resolved_storage_ptr());
        task_node::notify_successors<slots>();
        return nullptr;
#endif
    }
    ~functional_task() {
        reinterpret_cast<var<VT>*>(&my_result)->~var<VT>(); // current_task is finished in forward_successors
    }
};

template< typename T >
struct gen_oox {
    using type = var<T>;
    template< int slots, typename F >
    static type bind_to(internal::functional_task<slots, F, T> * t) {
        type oox; oox.bind_to(t, static_cast<internal::result_state<T, false>*>(t), k_execute_lifetime_ref + slots + 1); return oox;
    }
};
template<>
struct gen_oox<void> {
    using type = var<void>;
    template< int slots, typename F >
    static type bind_to(internal::functional_task<slots, F, void> * t) {
        type oox; oox.bind_to( t, t, k_execute_lifetime_ref + slots ); return oox;
    }
};
template< typename VT >
struct gen_oox<var<VT> > {
    using type = var<VT>;
    template< int slots, typename F >
    static type bind_to(internal::functional_task<slots, F, var<VT> > * t) {
        type oox; oox.bind_to( t, &t->my_result, k_execute_lifetime_ref + slots + 1, true ); return oox;
    }
};
template< typename T>
using var_type = typename gen_oox<T>::type;

template< typename R, typename... Types >
struct functor_info {
    using result_type = R;
    using args_list_type = types<Types...>;
};
template< typename R, typename... Args >
functor_info<R, Args...> get_functor_info(R (&)(Args...)) { return functor_info<R, Args...>(); }
template< typename R, typename C, typename... Args >
functor_info<R, Args...> get_functor_info(R (C::*)(Args...)) { return functor_info<R, Args...>(); }
template< typename R, typename C, typename... Args >
functor_info<R, Args...> get_functor_info(R (C::*)(Args...) const) { return functor_info<R, Args...>(); }
template< typename F >
auto get_functor_info(F&&) { return get_functor_info( &std::remove_reference_t<F>::operator() ); }
template< typename F >
using result_type_of = typename decltype( get_functor_info(std::declval<F>()) )::result_type;
template< typename F >
using args_list_of = typename decltype( get_functor_info(std::declval<F>()) )::args_list_type;

} //namespace internal

template< typename F, typename... Args > // ->...decltype(f(internal::unoox(args)...))
auto run(F&& f, Args&&... args)->internal::var_type<internal::result_type_of<F> >
{
    using r_type = internal::result_type_of<F>;
    using call_args_type = internal::args_list_of<F>;
    using args_type = internal::base_args<call_args_type, Args&&...>;
    using functor_type = internal::oox_bind<F, args_type>;
    using task_type = internal::functional_task<args_type::write_nodes_count, functor_type, r_type>;

    task_type *t = internal::task::allocate<task_type>( functor_type(std::forward<F>(f), args_type(std::forward<Args>(args)...)) );
    __OOX_TRACE("%p oox::run: write ports %d",t,args_type::write_nodes_count);
    int protect_count = std::numeric_limits<int>::max();
    t->start_count.store(protect_count, std::memory_order_release);
    // process functor types
    protect_count -= static_cast<internal::storage_task<args_type::write_nodes_count, functor_type>*>(t)
                         ->value()
                         .my_args.setup(1, t, std::forward<Args>(args)...);
    auto r = internal::gen_oox<r_type>::bind_to( t );
    t->remove_prerequisite( protect_count ); // publish it
    return r;
}

void wait_for_all(internal::oox_var_base& on ) {
    on.wait();
}

template<typename T>
[[nodiscard]] T wait_and_get(const var<T> &ov) {
    auto &v = const_cast<var<T>&>(ov);
    wait_for_all(v);

    // Forwarding tasks cache the final result storage before publishing readiness.
    return static_cast<internal::result_state<T, false>*>(v.resolved_storage_ptr())->value();
}

template<typename T>
[[nodiscard]] T wait_and_get(var<T> &ov) { return wait_and_get(static_cast<const var<T>&>(ov)); }
template<typename T>
[[nodiscard]] T wait_and_get(var<T> &&ov) { return wait_and_get(static_cast<const var<T>&>(ov)); }

#undef TASK_EXECUTE_METHOD
#undef OOX_DEBUG_ONLY
#undef OOX_TASK_EXECUTE_LIFETIME_GUARD

} // namespace oox
#endif // __OOX_H__
