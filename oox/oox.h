// Copyright (C) 2021 Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

#ifndef __OOX_H__
#define __OOX_H__

#include <utility>
#include <functional>
#include <type_traits>
#include <limits>
#include <atomic>
#include <exception>
#include <thread>
#include <variant>

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

#ifndef __OOX_TRACE
#define __OOX_TRACE(...)
#endif
#ifndef __OOX_ASSERT
#include <cassert>
#define __OOX_ASSERT(a, b) assert(a), b
#define __OOX_ASSERT_EX(a, b) __OOX_ASSERT(a, b)
#endif

namespace oox {

struct deferred_t { explicit constexpr deferred_t(int = 0) {} };
inline constexpr deferred_t deferred{};

namespace internal {

struct task_life {
    // Pointers to this structure and live output nodes
    std::atomic<int> life_count;
    virtual ~task_life() = default;

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

struct result_state_base {
    virtual ~result_state_base() = default;
    virtual void set_exception(std::exception_ptr ep) noexcept = 0;
    virtual bool has_exception() const noexcept = 0;
    virtual std::exception_ptr get_exception() const noexcept = 0;
};

template<typename T>
struct result_state : result_state_base {
    using value_type = T;
    using variant_type = std::variant<std::monostate, T, std::exception_ptr>;

    variant_type state;

    void set_exception(std::exception_ptr ep) noexcept override {
        state.template emplace<std::exception_ptr>(std::move(ep));
    }
    bool has_exception() const noexcept override {
        return std::holds_alternative<std::exception_ptr>(state);
    }
    std::exception_ptr get_exception() const noexcept override {
        if (std::holds_alternative<std::exception_ptr>(state)) {
            return std::get<std::exception_ptr>(state);
        }
        return std::exception_ptr{};
    }

    template<typename... Args>
    void emplace(Args&&... args) {
        state.template emplace<T>(std::forward<Args>(args)...);
    }
    bool has_value() const noexcept {
        return std::holds_alternative<T>(state);
    }
    T& value() {
        __OOX_ASSERT_EX(has_value(), "read from empty result_state");
        return std::get<T>(state);
    }
    const T& value() const {
        __OOX_ASSERT_EX(has_value(), "read from empty result_state");
        return std::get<T>(state);
    }
};

template<>
struct result_state<void> : result_state_base {
    using variant_type = std::variant<std::monostate, std::exception_ptr>;

    variant_type state;

    void set_value() noexcept {}
    void set_exception(std::exception_ptr ep) noexcept override {
        state.template emplace<std::exception_ptr>(std::move(ep));
    }
    bool has_exception() const noexcept override {
        return std::holds_alternative<std::exception_ptr>(state);
    }
    std::exception_ptr get_exception() const noexcept override {
        if (std::holds_alternative<std::exception_ptr>(state)) {
            return std::get<std::exception_ptr>(state);
        }
        return std::exception_ptr{};
    }
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
#define OOX_USING_TF
#define TASK_EXECUTE_METHOD void* execute() override

tf::Executor tf_pool; // TODO :)

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
        tf_pool.silent_async([this]{this->execute();});
    }
    void wait() {
        waiter.get_future().wait();
    }
    void wakeup() {
        waiter.set_value();
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

    ~task() override = default;
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

struct task_node;
struct oox_var_base;

struct output_node {
    // 0 if next writer is not known yet.
    // 1 if next writer is not known yet, but value is available and countdown includes extra one
    // 3 if next writer is end without var ownership
    // ptr|1 if next writer is end with var ownership, ptr points to var storage.
    // Otherwise points to next node that overwrites the value written by this node.
    std::atomic<task_node*> next_writer;
    std::atomic<int> countdown;
    output_node() {
        next_writer.store(nullptr, std::memory_order_relaxed);
        countdown.store(1, std::memory_order_relaxed);
    }
};

struct arc {
    // types of task relations beside output dependence
    enum kinds : char {
        flow_only,    //< notify consumer when producer is completed
        back_only,    //< notify producer when consumer is completed TODO: unnecessary when stored in task directly
        flow_back,    //< flow_only then back_only
        flow_copy,    //< call consumer to copy its value when producer is completed
        forward_copy  //< copy a pointer to the var storage found by producer to consumer
    };
    using port_int = short int;
    arc*       next{};
    task_node* node;
    port_int   port;
    kinds      kind;
    arc( task_node* n, int p, kinds k = flow_back ) : node(n), port(port_int(p)), kind(k) {}
};

struct arc_list {
    // Root of list of nodes that are waiting for this node's value to be produced.
    // A node can be waiting for *this to produce a value OR waiting for *this to consume its value.
    // Special value 1 means no need to wait (e.g. value has been produced).
    std::atomic<arc*> head;
    // Add i to arc_list.
    // Return true if success, false otherwise.
    bool add_arc( arc* i );
    arc_list() { head.store(nullptr, std::memory_order_relaxed); }
};

struct task_node : public task, arc_list {
    // Prerequisites to start the task
    std::atomic<int> start_count;

    std::atomic<uintptr_t> result_ptr_tagged{0};
    static constexpr uintptr_t exception_tag_mask = 0x3;
    static constexpr uintptr_t exception_tag_setting = 0x1;
    static constexpr uintptr_t exception_tag_set = 0x2;

    static uintptr_t strip_exception_tag(uintptr_t value) noexcept {
        return value & ~exception_tag_mask;
    }

    template<typename ResultState>
    void bind_result_state(ResultState* state) noexcept {
        result_state_base* base = state;
        uintptr_t raw = reinterpret_cast<uintptr_t>(base);
        __OOX_ASSERT_EX((raw & exception_tag_mask) == 0, "result_state pointer not aligned for tagging");
        result_ptr_tagged.store(raw, std::memory_order_release);
    }

    void set_exception(const std::exception_ptr& ep) noexcept {
        uintptr_t current = result_ptr_tagged.load(std::memory_order_acquire);
        if (strip_exception_tag(current) == 0) {
            return;
        }
        while ((current & exception_tag_mask) == 0) {
            uintptr_t desired = current | exception_tag_setting;
            if (result_ptr_tagged.compare_exchange_strong(current, desired, std::memory_order_acq_rel)) {
                uintptr_t raw = strip_exception_tag(desired);
                auto* base = reinterpret_cast<result_state_base*>(raw);
                base->set_exception(ep);
                result_ptr_tagged.store(raw | exception_tag_set, std::memory_order_release);
                return;
            }
        }
    }

    bool has_exception() const noexcept {
        if (strip_exception_tag(result_ptr_tagged.load(std::memory_order_acquire)) == 0) {
            return false;
        }
        return (result_ptr_tagged.load(std::memory_order_acquire) & exception_tag_mask) != 0;
    }

    std::exception_ptr get_exception() const noexcept {
        uintptr_t current = result_ptr_tagged.load(std::memory_order_acquire);
        if (strip_exception_tag(current) == 0) {
            return std::exception_ptr{};
        }
        while ((current & exception_tag_mask) == exception_tag_setting) {
            std::this_thread::yield();
            current = result_ptr_tagged.load(std::memory_order_acquire);
        }
        if ((current & exception_tag_mask) == exception_tag_set) {
            auto* base = reinterpret_cast<result_state_base*>(strip_exception_tag(current));
            return base->get_exception();
        }
        return std::exception_ptr{};
    }

    task_node() = default; // prepare the task for waiting on it directly
    ~task_node() override = default;

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
};

inline bool arc_list::add_arc( arc* i ) {
    __OOX_ASSERT( uintptr_t(i->node)>2, "" );
    for(;;) {
        arc* j = head.load(std::memory_order_acquire);
        if( j==(arc*)uintptr_t(1) )
            return false;
        i->next = j;
        if( head.compare_exchange_weak( j, i ) ) // TODO: weak or strong? what's perf?
            return true;
    }
}

inline int task_node::assign_prerequisite( task_node *n, int req_port ) {
    arc* j = new arc( this, req_port ); // TODO: embed into the task
    __OOX_ASSERT_EX(j && n, "");
    if( n->add_arc(j) ) {
        __OOX_TRACE("%p assign_prerequisite: assigned to %p, %d",this,n,req_port);
        return 1; // Prerequisite n will decrement start_count when it produces a value
    } else {
        // Prerequisite n already produced a value. Add this as a consumer of n.
        if (n->has_exception()) {
            this->set_exception(n->get_exception());
        }
        int k = ++n->out(req_port).countdown;
        __OOX_TRACE("%p assign_prerequisite: preventing %p, port %d, count %d",this,n,req_port,k);
        __OOX_ASSERT_EX(k>1,"risk that a prerequisite might be prematurely destroyed");
        j->node = n;
        j->kind = arc::back_only;
        bool success = add_arc(j); //TODO: add_arc_unsafe?
        __OOX_ASSERT_EX(success, "");
    }
    return 0;
}

inline void task_node::do_notify_arcs( arc* r, int *count ) {
    // Notify successors that value is available
    do {
        arc* j = r;
        r = j->next;
        task_node* n = j->node;
        if( j->kind == arc::back_only ) {
            // Notify producer that this task has finished consuming its value
            __OOX_TRACE("%p notify: %p->remove_back_arc(%d)",this,n,j->port);
            if( int k = n->remove_back_arc( j->port ) )
                n->release( k );
            delete j;
        } else {
            if( j->kind == arc::flow_back ) {
                // "n" is task that consumes value that this task produced.
                // Add back arc so that "n" can notify this when it is done consuming the value.
                j->node = this;
                j->kind = arc::back_only;
                if( out(j->port).next_writer.load(std::memory_order_acquire) != (task_node*)uintptr_t(3) ) {
                    bool b = n->add_arc( j );
                    __OOX_ASSERT_EX(b, "corrupted?");
                    --count[j->port];
                } else delete j; // very unlikely?
            } else if( j->kind == arc::flow_copy )
                n->on_ready( j->port );
            else if( j->kind == arc::forward_copy )
                __OOX_ASSERT(false, "incorrect forwarding"); // has to be processed by forward_successors only
            // Let "n" know that prerequisite "this" is ready.
            __OOX_TRACE("%p notify: %p->remove_prequisite()",this,n);
            if (this->has_exception()) {
                n->set_exception(this->get_exception());
            }
            n->remove_prerequisite();
        }
    } while( r );
}

inline int task_node::do_notify_out( int port, int count ) {
    task_node* null = nullptr;
    if( out(port).next_writer.load(std::memory_order_acquire)==nullptr
        && out(port).next_writer.compare_exchange_strong( null, (task_node*)uintptr_t(1)) ) {
        // The thread that installs the non-nullptr "next_writer" will see the 1 and do the decrement.
        --count;
        __OOX_TRACE("%p notify out %d: next_writer went from 0 to 1",this,port);
    } else if( !(uintptr_t((void*)out(port).next_writer.load(std::memory_order_acquire))&1) ) {
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

inline int task_node::notify_successors( int output_slots, int *count ) {
    for( int i = 0; i <  output_slots; i++ ) {
        // it should be safe to assign countdowns here because no successors were notified yet
        out(i).countdown.store( count[i] = std::numeric_limits<int>::max()/2, std::memory_order_release );
    }
    __OOX_TRACE("%p notify successors",this);
    // Grab list of successors and mark as competed.
    // Note that countdowns can change asynchronously after this point
    if( arc* r = head.exchange( (arc*)uintptr_t(1) ) )
        do_notify_arcs( r, count );
    int refs = 0;
    for( int i = 0; i <  output_slots; i++ )
        refs += do_notify_out( i, count[i] );
    __OOX_ASSERT(refs>=0, "");
    return refs;
}

inline void task_node::remove_prerequisite( int n ) {
    int k = start_count-=n;
    __OOX_ASSERT(k>=0,"invalid start_count detected while removing prerequisite");
    if( k==0 ) {
        __OOX_TRACE("%p remove_prerequisite: spawning",this);
        spawn();
    }
}

inline int task_node::notify_next_writer( task_node* d ) {
    auto i = reinterpret_cast<uintptr_t>(d);
    if( i&1 ) {
        if( i == 3 )
            return 1;
        __OOX_ASSERT( i!=1, "remove_back_arc called on output node with next_writer=1" );
        d = (task_node*)(i&~1);
        if( d == this )
            return 2;
        d->release();
    } else {
        __OOX_ASSERT( d!=nullptr, "remove_back_arc called on output node with next_writer==0" );
        d->remove_prerequisite();
    }
    return 1; // the last, release the node
}

inline int task_node::remove_back_arc( int output_port, int n ) {
    int k = out(output_port).countdown -= n;
    __OOX_ASSERT(k>=0,"invalid countdown detected while removing back_arc");
    __OOX_TRACE("%p remove_back_arc port %d: %d (next_writer is %p)",this,output_port,k,out(output_port).next_writer.load(std::memory_order_acquire));
    if( k==0 ) {
        // Next writer was waiting on all consumers of me to finish.
        return notify_next_writer( out(output_port).next_writer.load(std::memory_order_acquire) );
    }
    return 0;
}

inline void task_node::set_next_writer( int output_port, task_node* d ) {
    __OOX_ASSERT( uintptr_t(d)!=1, "" );
    task_node* o = out(output_port).next_writer.exchange(d);
    __OOX_TRACE("%p set_next_writer(%d, %p): next_writer was %p",this,output_port,d,o);
    if( o ) {
        if( uintptr_t(o)==1 ) {
            // this has value and conceptual back_arc from its owning oox that was removed.
            if( int k = remove_back_arc( output_port ) ) // TODO: optimize it for set_next_writer without contention
                release( k );
        } else {
            __OOX_ASSERT( uintptr_t(o)==3, "" );
            __OOX_ASSERT( uintptr_t(d)==3, "TODO forward_successors" ); // TODO
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

template<int slots>
struct task_node_slots : task_node {
    output_node output_nodes[slots];
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
struct alignas(64) storage_task : task_node_slots<slots> {
    result_state<T> my_precious;
    TASK_EXECUTE_METHOD { __OOX_ASSERT(false, "not runnable"); return nullptr; }
    storage_task() {
        this->bind_result_state(&my_precious);
    }
    explicit storage_task(T&& t) {
        this->bind_result_state(&my_precious);
        my_precious.emplace(std::move(t));
    }
    explicit storage_task(const T& t) {
        this->bind_result_state(&my_precious);
        my_precious.emplace(t);
    }
};

struct oox_var_base {
    //TODO: make it a class with private members
    oox_var_base &operator=(const oox_var_base &) = delete;

    template< typename T > friend struct gen_oox;
    task_node*  current_task = nullptr;
    void*       storage_ptr{};
    int         storage_offset{}; // task_node* original = ptr - offset
    short int   current_port = 0; // the problem can arise from concurrent accesses to oox::var, TODO: check
    bool        is_forward : 1 = false; // indicate if it refers to another oox::var recursively
    bool        is_deferred: 1 = false; // created via oox::deferred and not yet “bound” to a writer

    void set_next_writer( int output_port, task_node* d ) {
        __OOX_ASSERT(current_task, "empty oox::var");

        // If this var was created as deferred, tasks may already be waiting on the
        // deferred storage node (current_task/current_port). The first real writer
        // must inherit those waiting arcs, otherwise readers would never be notified.
        //
        // Also, we must retarget arc->port to the writer's output port, so that
        // back-arcs/countdown protect the correct output slot (the var slot), not slot 0.
        if (is_deferred) {
            arc* r = current_task->head.exchange(nullptr, std::memory_order_acq_rel);
            while(r > (arc*)uintptr_t(1)) {
                arc* j = r;
                r = j->next;
                j->port = static_cast<arc::port_int>(output_port);
                bool ok = d->add_arc(j);
                __OOX_ASSERT_EX(ok, "unexpected: writer task already completed while forwarding deferred arcs");
            }
            is_deferred = false;
        }
        current_task->set_next_writer( current_port, d );
        current_task = d, current_port = output_port;
    }
    void bind_to( task_node * t, void* ptr, int lifetime, bool fwd = false ) {
        current_task = t, current_port = 0, storage_ptr = ptr, is_forward = fwd;
        storage_offset = uintptr_t(storage_ptr) - uintptr_t(current_task);
        t->life_set_count(lifetime);
        __OOX_TRACE("%p bind: store=%p life=%d fwd=%d",t,ptr,lifetime,fwd);
    }
    void wait() const {
        __OOX_ASSERT_EX(current_task, "wait for empty oox::var");
        // if head == 1, the producer is already "done":
        // - either a constant storage_task, or
        // - a completed functional_task.
        arc* h = current_task->head.load(std::memory_order_acquire);
        if (h == (arc*)uintptr_t(1)) {
            return;
        }
        current_task->wait();
    }
    void release() {
        if( current_task ) {
            current_task->set_next_writer( current_port, (task_node*)uintptr_t(
                    storage_offset? ((uintptr_t(storage_ptr)-storage_offset)|1) : 3/*var<void>*/) );
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
    arc* r = head.exchange( (arc*)uintptr_t(1) ); // mark it completed
    task_node* d = out(0).next_writer.exchange( (internal::task_node*)uintptr_t(3) ); // finish this node
    int refs = 1;
    __OOX_TRACE("%p forward_successors(%p, %d): arcs=%p next_writer=%p",this,n.current_task,n.current_port,r,d);
    if( r ) {
        arc* l = n.current_task->head.exchange( r ); // forward dependencies
        if( l ) {
            __OOX_TRACE("%p forward_successors(%p, %d): notify arcs myself %p",this,n.current_task,n.current_port,l);
            __OOX_ASSERT( uintptr_t(l)==1, "arc lists merge is not implemented" ); // TODO
            __OOX_ASSERT(!n.is_forward, "not implemented"); // TODO
            do_notify_arcs( r, count );
        }
    }
    if( d ) { // TODO: can be converted as another arc type instead of working with outputs?
        task_node* o = n.current_task->out(n.current_port).next_writer.exchange( d );
        if( o ) { // next node is ready already
            __OOX_TRACE("%p forward_successors(%p, %d): removing back arc myself %p",this,n.current_task,n.current_port,o);
            __OOX_ASSERT( uintptr_t(o)==1, "" );
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
        v->out(0).next_writer.store((internal::task_node*)uintptr_t(1), std::memory_order_release);
        v->head.store((internal::arc*)uintptr_t(1), std::memory_order_release);
        // nobody wait on this task
        this->bind_to( v, &v->my_precious, 2 );
        return storage_ptr;
    }

    void* allocate_deferred() noexcept {
        auto *v = internal::task::allocate<internal::storage_task<1, T>>();
        __OOX_TRACE("%p oox::var(deferred)", v);
        // Make writers behave like for a normal initial value (next_writer=1),
        // BUT do NOT mark the node completed (head stays nullptr), so readers block.
        v->out(0).next_writer.store((internal::task_node*)uintptr_t(1), std::memory_order_release);
        // v->head is intentionally left as nullptr (not ready)
        this->bind_to(v, &v->my_precious, 2);
        this->is_deferred = true;
        return storage_ptr;
    }

public:
    var()                    = default; // allocates default value lazily for sake of optimization
    var(deferred_t) {
        allocate_deferred(); // storage exists, but value is not ready
    }
    var(const T& t) noexcept {
        auto* state = static_cast<internal::result_state<T>*>(allocate_new());
        state->emplace(t); // TODO: add exception-safe
    }
    var(T&& t)      noexcept {
        auto* state = static_cast<internal::result_state<T>*>(allocate_new());
        state->emplace(std::move(t));
    }
    var(var<T>&& t)  noexcept : internal::oox_var_base(std::move(t)) { t.current_task = nullptr; }
    var& operator=(var<T>&& t)  noexcept {
        release();
        new(this) internal::oox_var_base(std::move(t));
        __OOX_ASSERT_EX(current_task, "");
        t.current_task = nullptr;
        return *this;
    }
    ~var() { release(); }
    [[nodiscard]] T get() {
        wait();
        return static_cast<internal::result_state<T>*>(storage_ptr)->value();
    }
};

template<>
class var<void> : public internal::oox_var_base {
    template< typename T > friend struct gen_oox;
public:
    var() = default;
    template<typename D>
    var(var<D>&& src) : internal::oox_var_base(src) {
        static_cast<internal::task_node *>(static_cast<uintptr_t>(src.storage_ptr) - src.storage_offset)->release();
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

    explicit base_args( A&& a, Args&&... args ) : base_type( std::forward<Args>(args)... ), my_value(std::forward<A>(a)) {}
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

    uintptr_t my_ptr{};
    // TODO: copy-based optimizations
    explicit oox_var_args( const var_type& cov, Args&&... args ) : base_type( std::forward<Args>(args)... ) {}
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
            count = self->assign_prerequisite( cov.current_task, cov.current_port );
        if( cov.is_forward ) {
            my_ptr = 1 | (uintptr_t)cov.storage_ptr;
        } else
            my_ptr = (uintptr_t)cov.storage_ptr;
        //TODO: broken? if( !std::is_lvalue_reference_v<C> ) // consume oox::var
        //    ov.~var(); // TODO: no need in sync for not yet published task
        return count + base_type::setup( port+is_writer, self, std::forward<Args>(args)...);
    }
    C&& consume() {
        internal::result_state<ooxed_type>* state = nullptr;
        if( my_ptr & 1 ) {
            auto* forward_state = reinterpret_cast<internal::result_state<var_type>*>(my_ptr ^ 1);
            oox_var_base* next = static_cast<oox_var_base*>(&forward_state->value());
            while(next->is_forward) {
                auto* next_state = static_cast<internal::result_state<var_type>*>(next->storage_ptr);
                next = static_cast<oox_var_base*>(&next_state->value());
            }
            state = static_cast<internal::result_state<ooxed_type>*>(next->storage_ptr);
        } else {
            state = reinterpret_cast<internal::result_state<ooxed_type>*>(my_ptr);
        }
        __OOX_ASSERT_EX(state, "null result_state storage");

        if constexpr (
            std::is_lvalue_reference_v<T> && !std::is_const_v<std::remove_reference_t<T>>) {
            if(!state->has_value()) {
                state->emplace(); // requires default-constructible T
            }
        } else {
            __OOX_ASSERT_EX(state->has_value(), "read from empty result_state");
        }
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
struct alignas(64) functional_task : storage_task<slots, F> {
    result_state<R> my_result;
    template<typename... Args>
    explicit functional_task(Args&&... args) : storage_task<slots, F>(std::forward<Args>(args)...) {
        this->bind_result_state(&my_result);
    }
    TASK_EXECUTE_METHOD {
        if (!this->has_exception()) {
            try {
                my_result.emplace(this->my_precious.value()());
            } catch(...) {
                this->set_exception(std::current_exception());
            }
        }
        task_node::notify_successors<slots>();
        return nullptr;
    }
    ~functional_task() override = default;
};

template<int slots, typename F>
struct functional_task<slots, F, void> : storage_task<slots, F> {
    result_state<void> my_result;
    template<typename... Args>
    explicit functional_task(Args&&... args) : storage_task<slots, F>(std::forward<Args>(args)...) {
        this->bind_result_state(&my_result);
    }
    TASK_EXECUTE_METHOD {
        __OOX_TRACE("%p do_run: start",this);
        if (!this->has_exception()) {
            try {
                this->my_precious.value()();
                my_result.set_value();
            } catch(...) {
                this->set_exception(std::current_exception());
            }
        }
        task_node::notify_successors<slots>();
        return nullptr;
    }
};

template<int slots, typename F, typename VT>
struct functional_task<slots, F, var<VT>> : storage_task<slots, F> {
    result_state<var<VT>> my_result;
    bool is_executed = false;
    template<typename... Args>
    explicit functional_task(Args&&... args) : storage_task<slots, F>(std::forward<Args>(args)...) {
        this->bind_result_state(&my_result);
    }
    TASK_EXECUTE_METHOD {
    if(!this->has_exception()) {
        try {
            if ( !is_executed ) {
                __OOX_TRACE("%p do_run: start forward",this);
                my_result.emplace(this->my_precious.value()());
                is_executed = true;
                this->start_count.store(1, std::memory_order_release);
                arc* j = new arc(this, 0, arc::flow_only);  // TODO: embed into the task
                auto& result = my_result.value();
                __OOX_ASSERT_EX(result.current_task, "forwarding functor returned empty var");
                if(result.current_task->add_arc(j)) {
                    __OOX_TRACE("%p do_run: add_arc", this); // recycle_as_continuation was here
                    return nullptr;
                } else {
                    delete j;
                }
            }
        } catch(...) {
            this->set_exception(std::current_exception());
        }
    }
    __OOX_TRACE("%p do_run: notify forward",this);
    task_node::notify_successors<slots>();
        return nullptr;
    }

    ~functional_task() override = default;
};

template< typename T >
struct gen_oox {
    using type = var<T>;
    template< int slots, typename F >
    static type bind_to(internal::functional_task<slots, F, T> * t) {
        type oox; oox.bind_to( t, &t->my_result, slots+1 ); return oox;
    }
};
template<>
struct gen_oox<void> {
    using type = var<void>;
    template< int slots, typename F >
    static type bind_to(internal::functional_task<slots, F, void> * t) {
        type oox; oox.bind_to( t, t, slots ); return oox;
    }
};
template< typename VT >
struct gen_oox<var<VT> > {
    using type = var<VT>;
    template< int slots, typename F >
    static type bind_to(internal::functional_task<slots, F, var<VT> > * t) {
        type oox; oox.bind_to( t, &t->my_result, slots+1, true ); return oox;
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
    protect_count -= t->my_precious.value().my_args.setup( 1, t, std::forward<Args>(args)...);
    auto r = internal::gen_oox<r_type>::bind_to( t );
    t->remove_prerequisite( protect_count ); // publish it
    return r;
}

inline void wait_for_all(const internal::oox_var_base& on) {
    on.wait();
    if (on.current_task && on.current_task->has_exception()) {
        std::rethrow_exception(on.current_task->get_exception());
    }
}

template<typename T>
[[nodiscard]] T wait_and_get(const var<T>& ov) {
    auto& v = const_cast<var<T>&>(ov);
    wait_for_all(v);

    internal::oox_var_base* base = &v;
    while(base->is_forward) {
        if (base->current_task && base->current_task->has_exception()) {
            std::rethrow_exception(base->current_task->get_exception());
        }
        auto* forward_state = static_cast<internal::result_state<var<T>>*>(base->storage_ptr);
        base = static_cast<internal::oox_var_base*>(&forward_state->value());
    }

    // have to rethrow before trying to access result_state value, so we have actual error and not bad access
    if (base->current_task && base->current_task->has_exception()) {
        std::rethrow_exception(base->current_task->get_exception());
    }
    return static_cast<internal::result_state<T>*>(base->storage_ptr)->value();
}

template<typename T>
[[nodiscard]] T wait_and_get(var<T> &ov) { return wait_and_get(static_cast<const var<T>&>(ov)); }
template<typename T>
[[nodiscard]] T wait_and_get(var<T> &&ov) { return wait_and_get(static_cast<const var<T>&>(ov)); }

#undef TASK_EXECUTE_METHOD

} // namespace oox
#endif // __OOX_H__
