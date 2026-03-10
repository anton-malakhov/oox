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
#include <cstddef>
#include <cstdint>
#include <new>
#include <thread>
#if defined(OOX_ENABLE_EXCEPTIONS)
#define OOX_EXCEPTIONS_ENABLED (OOX_ENABLE_EXCEPTIONS)
#elif defined(__cpp_exceptions)
#define OOX_EXCEPTIONS_ENABLED 1
#else
#define OOX_EXCEPTIONS_ENABLED 0
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
struct arc;

inline constexpr std::uintptr_t k_task_done_tag = 0x1;
inline constexpr std::uintptr_t k_result_state_tag_mask = 0x6;
inline constexpr std::uintptr_t k_task_tag_mask = k_task_done_tag | k_result_state_tag_mask;
inline constexpr unsigned char k_result_state_empty = 0;
inline constexpr unsigned char k_result_state_lock = 1;
inline constexpr unsigned char k_result_state_value = 2;
inline constexpr unsigned char k_result_state_exception = 3;

// types arc* and uintptr_r explicitly say how do we treat the pointer, just
// as a pointer or as pointer to arc
inline std::uintptr_t head_bits(arc* p) noexcept { return reinterpret_cast<std::uintptr_t>(p); }

inline arc* head_from_bits(std::uintptr_t bits) noexcept { return reinterpret_cast<arc*>(bits); }

inline unsigned char decode_result_state(arc* raw_head) noexcept {
    return static_cast<unsigned char>((head_bits(raw_head) & k_result_state_tag_mask) >> 1);
}

inline arc* encode_result_state(arc* raw_head, unsigned char state) noexcept {
    return head_from_bits((head_bits(raw_head) & ~k_result_state_tag_mask) |
                          ((static_cast<std::uintptr_t>(state) << 1) & k_result_state_tag_mask));
}

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

template<typename T, bool CanThrow>
struct result_state;

struct result_state_base {
    static constexpr unsigned char state_empty = k_result_state_empty;
    static constexpr unsigned char state_lock = k_result_state_lock;
    static constexpr unsigned char state_value = k_result_state_value;
    static constexpr unsigned char state_exception = k_result_state_exception;

  protected:
    unsigned char read_state(const std::atomic<arc*>& owner_head) const noexcept {
        return decode_result_state(owner_head.load(std::memory_order_acquire));
    }
    void store_state(std::atomic<arc*>& owner_head, unsigned char state) noexcept {
        arc* current = owner_head.load(std::memory_order_acquire);
        for (;;) {
            arc* desired = encode_result_state(current, state);
            if (owner_head.compare_exchange_weak(current, desired, std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
                return;
            }
        }
    }
};

#if OOX_EXCEPTIONS_ENABLED
template <typename Derived> struct result_state_throw_base : result_state_base {
  protected:
    Derived& derived() noexcept { return *static_cast<Derived*>(this); }
    const Derived& derived() const noexcept { return *static_cast<const Derived*>(this); }

    unsigned char read_stable_state(const std::atomic<arc*>& owner_head) const noexcept {
        arc* current = owner_head.load(std::memory_order_acquire);
        while (decode_result_state(current) == state_lock) {
            std::this_thread::yield();
            current = owner_head.load(std::memory_order_acquire);
        }
        return decode_result_state(current);
    }
    unsigned char lock_for_transition(std::atomic<arc*>& owner_head) noexcept {
        arc* current = owner_head.load(std::memory_order_acquire);
        for (;;) {
            const auto current_state = decode_result_state(current);
            if (current_state == state_lock) {
                std::this_thread::yield();
                current = owner_head.load(std::memory_order_acquire);
                continue;
            }
            arc* desired = encode_result_state(current, state_lock);
            if (owner_head.compare_exchange_weak(current, desired, std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
                return current_state;
            }
        }
    }
    unsigned char lock_for_value_transition(std::atomic<arc*>& owner_head,
                                            unsigned char blocked_state = 0xFF) noexcept {
        arc* current = owner_head.load(std::memory_order_acquire);
        for (;;) {
            const auto current_state = decode_result_state(current);
            if (current_state == blocked_state) {
                return current_state;
            }
            if (current_state == state_lock) {
                std::this_thread::yield();
                current = owner_head.load(std::memory_order_acquire);
                continue;
            }
            arc* desired = encode_result_state(current, state_lock);
            if (owner_head.compare_exchange_weak(current, desired, std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
                return current_state;
            }
        }
    }

  public:
    std::exception_ptr* exception_slot_ptr(const std::atomic<arc*>& owner_head) noexcept {
        if (read_stable_state(owner_head) != state_exception) {
            return nullptr;
        }
        return derived().exception_ptr();
    }
    int try_set_exception(std::atomic<arc*>& owner_head, std::exception_ptr* eptr) noexcept {
        arc* current = owner_head.load(std::memory_order_acquire);
        for (;;) {
            const auto current_state = decode_result_state(current);
            if (current_state == state_lock) {
                return 2;
            }
            if (current_state == state_value) {
                return 0;
            }

            arc* desired = encode_result_state(current, state_lock);
            if (!owner_head.compare_exchange_weak(current, desired, std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
                continue;
            }

            auto* slot = derived().exception_ptr();
            if (current_state == state_exception) {
                if (*slot || !eptr || !*eptr) {
                    store_state(owner_head, state_exception);
                    return 0;
                }
                *slot = std::move(*eptr);
                store_state(owner_head, state_exception);
                return 1;
            }

            ::new (static_cast<void*>(slot)) std::exception_ptr(eptr ? std::move(*eptr) : std::exception_ptr{});
            store_state(owner_head, state_exception);
            return 1;
        }
    }
    bool has_exception(const std::atomic<arc*>& owner_head) const noexcept {
        return read_stable_state(owner_head) == state_exception;
    }
    std::exception_ptr get_exception(const std::atomic<arc*>& owner_head) const noexcept {
        if (read_stable_state(owner_head) != state_exception) {
            return std::exception_ptr{};
        }
        return *derived().exception_ptr();
    }
};
#endif

template <typename T> struct result_state<T, false> : private result_state_base {
    using base_type = result_state_base;
    using value_type = T;
    static constexpr unsigned char state_unset = base_type::state_empty;
    static constexpr unsigned char state_set = base_type::state_value;

    result_state() = default;
    result_state(const result_state&) = delete;
    result_state& operator=(const result_state&) = delete;
    result_state(result_state&&) = delete;
    result_state& operator=(result_state&&) = delete;
    ~result_state() = default;

    template <typename... Args> void emplace(std::atomic<arc*>& owner_head, Args&&... args) {
        const auto previous = this->read_state(owner_head);
        if (previous == state_set) {
            ptr()->~T();
        }
#if OOX_EXCEPTIONS_ENABLED
        try {
            construct_value(std::forward<Args>(args)...);
            this->store_state(owner_head, state_set);
        } catch (...) {
            this->store_state(owner_head, state_unset);
            throw;
        }
#else
        construct_value(std::forward<Args>(args)...);
        this->store_state(owner_head, state_set);
#endif
    }
    bool has_value(const std::atomic<arc*>& owner_head) const noexcept {
        return this->read_state(owner_head) == state_set;
    }
    T& value() { return *ptr(); }
    const T& value() const { return *ptr(); }
#if OOX_EXCEPTIONS_ENABLED
    std::exception_ptr* exception_slot_ptr() noexcept { return nullptr; }
#endif

    void reset(std::atomic<arc*>& owner_head) {
        const auto previous = this->read_state(owner_head);
        if (previous == state_set) {
            ptr()->~T();
        }
        this->store_state(owner_head, state_unset);
    }

  private:
    struct storage_t {
        alignas(alignof(T)) std::byte data[sizeof(T)];
    };
    storage_t storage{};

    template <typename... Args> void construct_value(Args&&... args) {
        if constexpr (sizeof...(Args) == 0) {
            ::new (static_cast<void*>(storage.data)) T;
        } else {
            ::new (static_cast<void*>(storage.data)) T(std::forward<Args>(args)...);
        }
    }
    T* ptr() noexcept { return std::launder(reinterpret_cast<T*>(storage.data)); }
    const T* ptr() const noexcept { return std::launder(reinterpret_cast<const T*>(storage.data)); }
};

template <bool CanThrow> struct result_state<void, CanThrow>;

#if OOX_EXCEPTIONS_ENABLED
template <typename T> struct result_state<T, true> : private result_state_throw_base<result_state<T, true>> {
    using base_type = result_state_throw_base<result_state<T, true>>;
    using value_type = T;
    static constexpr unsigned char state_empty = base_type::state_empty;
    static constexpr unsigned char state_lock = base_type::state_lock;
    static constexpr unsigned char state_value = base_type::state_value;
    static constexpr unsigned char state_exception = base_type::state_exception;

    result_state() = default;
    result_state(const result_state&) = delete;
    result_state& operator=(const result_state&) = delete;
    result_state(result_state&&) = delete;
    result_state& operator=(result_state&&) = delete;
    ~result_state() = default;

    template <typename... Args> void emplace(std::atomic<arc*>& owner_head, Args&&... args) {
        const auto previous = this->lock_for_value_transition(owner_head, state_exception);
        if (previous == state_exception) {
            return;
        }

        if (previous == state_value) {
            value_ptr()->~T();
        }
#if OOX_EXCEPTIONS_ENABLED
        try {
            construct_value(std::forward<Args>(args)...);
            this->store_state(owner_head, state_value);
        } catch (...) {
            this->store_state(owner_head, state_empty);
            throw;
        }
#else
        construct_value(std::forward<Args>(args)...);
        this->store_state(owner_head, state_value);
#endif
    }
    bool has_value(const std::atomic<arc*>& owner_head) const noexcept {
        return this->read_stable_state(owner_head) == state_value;
    }
    T& value(const std::atomic<arc*>& owner_head) {
        __OOX_ASSERT_EX(has_value(owner_head), "read from empty result_state");
        return *value_ptr();
    }
    const T& value(const std::atomic<arc*>& owner_head) const {
        __OOX_ASSERT_EX(has_value(owner_head), "read from empty result_state");
        return *value_ptr();
    }
    T& value() { return *value_ptr(); }
    const T& value() const { return *value_ptr(); }
    using base_type::exception_slot_ptr;
    using base_type::get_exception;
    using base_type::has_exception;
    using base_type::try_set_exception;
    void reset(std::atomic<arc*>& owner_head) {
        const auto previous = this->lock_for_transition(owner_head);
        if (previous == state_value) {
            value_ptr()->~T();
        } else if (previous == state_exception) {
            exception_ptr()->~exception_ptr();
        }
        this->store_state(owner_head, state_empty);
    }

  private:
    static constexpr std::size_t storage_size =
        (sizeof(T) > sizeof(std::exception_ptr)) ? sizeof(T) : sizeof(std::exception_ptr);
    static constexpr std::size_t storage_align =
        (alignof(T) > alignof(std::exception_ptr)) ? alignof(T) : alignof(std::exception_ptr);
    struct storage_t {
        alignas(storage_align) std::byte data[storage_size];
    };
    template <typename> friend struct result_state_throw_base;
    storage_t storage{};
    template <typename... Args> void construct_value(Args&&... args) {
        if constexpr (sizeof...(Args) == 0) {
            ::new (static_cast<void*>(storage.data)) T;
        } else {
            ::new (static_cast<void*>(storage.data)) T(std::forward<Args>(args)...);
        }
    }
    T* value_ptr() noexcept { return std::launder(reinterpret_cast<T*>(storage.data)); }
    const T* value_ptr() const noexcept { return std::launder(reinterpret_cast<const T*>(storage.data)); }
    std::exception_ptr* exception_ptr() noexcept {
        return std::launder(reinterpret_cast<std::exception_ptr*>(storage.data));
    }
    const std::exception_ptr* exception_ptr() const noexcept {
        return std::launder(reinterpret_cast<const std::exception_ptr*>(storage.data));
    }
};

template <> struct result_state<void, true> : private result_state_throw_base<result_state<void, true>> {
    using base_type = result_state_throw_base<result_state<void, true>>;
    static constexpr unsigned char state_empty = base_type::state_empty;
    static constexpr unsigned char state_lock = base_type::state_lock;
    static constexpr unsigned char state_value = base_type::state_value;
    static constexpr unsigned char state_exception = base_type::state_exception;

    result_state() = default;
    result_state(const result_state&) = delete;
    result_state& operator=(const result_state&) = delete;
    result_state(result_state&&) = delete;
    result_state& operator=(result_state&&) = delete;
    ~result_state() = default;

    using base_type::exception_slot_ptr;
    using base_type::get_exception;
    using base_type::has_exception;
    using base_type::try_set_exception;
    void reset(std::atomic<arc*>& owner_head) {
        const auto previous = this->lock_for_transition(owner_head);
        if (previous == state_exception) {
            exception_ptr()->~exception_ptr();
        }
        this->store_state(owner_head, state_empty);
    }

  private:
    static constexpr std::size_t storage_size = sizeof(std::exception_ptr);
    struct storage_t {
        alignas(alignof(std::exception_ptr)) std::byte data[storage_size];
    };
    template <typename> friend struct result_state_throw_base;
    storage_t storage{};
    std::exception_ptr* exception_ptr() noexcept {
        return std::launder(reinterpret_cast<std::exception_ptr*>(storage.data));
    }
    const std::exception_ptr* exception_ptr() const noexcept {
        return std::launder(reinterpret_cast<const std::exception_ptr*>(storage.data));
    }
};
#endif

template <> struct result_state<void, false> {
#if OOX_EXCEPTIONS_ENABLED
    std::exception_ptr* exception_slot_ptr() noexcept { return nullptr; }
#endif
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
        // Without this guard, concurrent release() on dependency edges can reclaim
        // the task object before execute() reaches its own release path.
        life_count.fetch_add(1, std::memory_order_acq_rel);
        get_tf_pool().silent_async([this]{
            this->execute();
            this->release(1);
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
    arc*       next;
    task_node* node;
    port_int   port;
    kinds      kind;
    arc( task_node* n, int p, kinds k = flow_back ) : node(n), port(port_int(p)), kind(k) {}
};

static_assert(alignof(arc) >= 8, "arc alignment must provide three low tag bits");

inline arc* head_ptr(arc* raw) noexcept { return head_from_bits(head_bits(raw) & ~k_task_tag_mask); }

inline arc* with_head_ptr(arc* raw, arc* ptr) noexcept {
    return head_from_bits((head_bits(raw) & k_task_tag_mask) | (head_bits(ptr) & ~k_task_tag_mask));
}

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
};

bool arc_list::add_arc( arc* i ) {
    __OOX_ASSERT( uintptr_t(i->node)>2, "" );
    __OOX_ASSERT_EX((reinterpret_cast<std::uintptr_t>(i) & k_task_tag_mask) == 0,
                    "arc pointer is not aligned enough for tag bits");
    for(;;) {
        arc* raw_j = head.load(std::memory_order_acquire);
        if( k_task_done_tag == (head_bits(raw_j) & k_task_done_tag) )
            return false;
        auto j = head_ptr(raw_j);
        i->next = j;
        arc* desired = with_head_ptr(raw_j, i);
        if( head.compare_exchange_weak( raw_j, desired ) ) // TODO: weak or strong? what's perf?
            return true;
    }
}

int task_node::assign_prerequisite( task_node *n, int req_port ) {
    arc* j = new arc( this, req_port ); // TODO: embed into the task
    __OOX_ASSERT_EX(j && n, "");
    if( n->add_arc(j) ) {
        __OOX_TRACE("%p assign_prerequisite: assigned to %p, %d",this,n,req_port);
        return 1; // Prerequisite n will decrement start_count when it produces a value
    } else {
        // Prerequisite n already produced a value. Add this as a consumer of n.
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

void task_node::do_notify_arcs( arc* r, int *count ) {
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
            n->remove_prerequisite();
        }
    } while( r );
}

int task_node::do_notify_out( int port, int count ) {
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

int task_node::notify_successors( int output_slots, int *count ) {
    for( int i = 0; i <  output_slots; i++ ) {
        // it should be safe to assign countdowns here because no successors were notified yet
        out(i).countdown.store( count[i] = std::numeric_limits<int>::max()/2, std::memory_order_release );
    }
    __OOX_TRACE("%p notify successors",this);
    // Grab list of successors and mark as competed.
    // Note that countdowns can change asynchronously after this point
    auto raw_head = head.load(std::memory_order_acquire);
    /*
    Optimized path if we have invariant
    dependency graph is fully built before any task can complete,
    so no concurrent head writers (add_arc/set_next_writer) are possible.
    arc* desired = head_from_bits((head_bits(raw_head) & k_result_state_tag_mask) | k_task_done_tag);
    head.store(desired, std::memory_order_release);
    */
    for (;;) {
        arc* desired = head_from_bits((head_bits(raw_head) & k_result_state_tag_mask) | k_task_done_tag);
        if (head.compare_exchange_weak(raw_head, desired, std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
            break;
        }
    }

    if( arc* r = head_ptr(raw_head) )
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
        spawn();
    }
}

int task_node::notify_next_writer( task_node* d ) {
    uintptr_t i = (uintptr_t)d;
    if( i&1 ) {
        if( i == 3 )
            return 1;
        if( i == 1 )
            return 0;
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
    if(n > 0) {
        release(n);
    }
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
    result_state<T, false> my_precious;
    TASK_EXECUTE_METHOD { __OOX_ASSERT(false, "not runnable"); return nullptr; }
    storage_task() = default;
    storage_task(T&& t) { my_precious.emplace(this->head, std::move(t)); }
    storage_task(const T& t) { my_precious.emplace(this->head, t); }
    ~storage_task() { my_precious.reset(this->head); }
};

struct oox_var_base {
    //TODO: make it a class with private members
    oox_var_base &operator=(const oox_var_base &) = delete;
    static constexpr std::uint16_t k_port_mask = 0x3FFFu;
    static constexpr std::uint16_t k_forward_flag = 0x4000u;
    static constexpr std::uint16_t k_deferred_flag = 0x8000u;

    template< typename T > friend struct gen_oox;
    task_node*  current_task = nullptr;
    void*       storage_ptr;
    int         storage_offset; // task_node* original = ptr - offset
    std::uint16_t current_port_and_flags = 0; // port plus var-local forward/deferred flags

    int current_port() const noexcept {
        return static_cast<int>(current_port_and_flags & k_port_mask);
    }
    void set_current_port(int port) noexcept {
        __OOX_ASSERT_EX(port >= 0 && port <= static_cast<int>(k_port_mask), "oox::var port does not fit packed field");
        current_port_and_flags = static_cast<std::uint16_t>((current_port_and_flags & ~k_port_mask) |
                                                            static_cast<std::uint16_t>(port));
    }
    bool is_forward() const noexcept {
        return (current_port_and_flags & k_forward_flag) != 0;
    }
    void set_forward(bool value) noexcept {
        if (value)
            current_port_and_flags |= k_forward_flag;
        else
            current_port_and_flags &= static_cast<std::uint16_t>(~k_forward_flag);
    }
    bool is_deferred() const noexcept {
        return (current_port_and_flags & k_deferred_flag) != 0;
    }
    void set_deferred(bool value) noexcept {
        if (value)
            current_port_and_flags |= k_deferred_flag;
        else
            current_port_and_flags &= static_cast<std::uint16_t>(~k_deferred_flag);
    }

    void set_next_writer( int output_port, task_node* d ) {
        __OOX_ASSERT(current_task, "empty oox::var");

        // If this var was created as deferred, tasks may already be waiting on the
        // deferred storage node (current_task/current_port). The first real writer
        // must inherit those waiting arcs, otherwise readers would never be notified.
        //
        // Also, we must retarget arc->port to the writer's output port, so that
        // back-arcs/countdown protect the correct output slot (the var slot), not slot 0.
        if (is_deferred()) {
            arc* raw_head = current_task->head.load(std::memory_order_acquire);
            // just like notify_successor an optimization might be useful if we restrict some operations from user
            /*
            arc* desired = head_from_bits(head_bits(raw_head) & k_result_state_tag_mask);
            current_task->head.store(desired, std::memory_order_release);
            */
            for (;;) {
                arc* desired = head_from_bits(head_bits(raw_head) & k_result_state_tag_mask);
                if (current_task->head.compare_exchange_weak(raw_head, desired, std::memory_order_acq_rel,
                                                             std::memory_order_acquire)) {
                    break;
                }
            }
            arc* r = head_ptr(raw_head);
            while(r) {
                arc* j = r;
                r = j->next;
                j->port = arc::port_int(output_port);
                bool ok = d->add_arc(j);
                __OOX_ASSERT_EX(ok, "unexpected: writer task already completed while forwarding deferred arcs");
            }
            set_deferred(false);
        }
        current_task->set_next_writer( current_port(), d );
        current_task = d;
        set_current_port(output_port);
    }
    void bind_to( task_node * t, void* ptr, int lifetime, bool fwd = false, bool deferred = false ) {
        current_task = t, storage_ptr = ptr, current_port_and_flags = 0;
        set_forward(fwd);
        set_deferred(deferred);
        storage_offset = uintptr_t(storage_ptr) - uintptr_t(current_task);
        t->life_set_count(lifetime);
        __OOX_TRACE("%p bind: store=%p life=%d fwd=%d deferred=%d",t,ptr,lifetime,fwd,deferred);
    }
    void wait() {
        __OOX_ASSERT_EX(current_task, "wait for empty oox::var");
        // if head == 1, the producer is already "done":
        // - either a constant storage_task, or
        // - a completed functional_task.
        arc* h = current_task->head.load(std::memory_order_acquire);
        if (k_task_done_tag == ((uintptr_t)h & k_task_done_tag)) {
            return;
        }
        current_task->wait();
    }
    void release() {
        if( current_task ) {
            current_task->set_next_writer( current_port(), (task_node*)uintptr_t(
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
        v->head.store((internal::arc*)internal::k_task_done_tag, std::memory_order_release);
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
        this->bind_to(v, &v->my_precious, 2, false, true);
        return storage_ptr;
    }

public:
    var()                    { } // allocates default value lazily for sake of optimization
    var(deferred_t) {
        allocate_deferred(); // storage exists, but value is not ready
    }
    var(const T& t) noexcept {
        auto* state = static_cast<internal::result_state<T, false>*>(allocate_new());
        state->emplace(current_task->head, t); // TODO: add exception-safe
    }
    var(T&& t)      noexcept {
        auto* state = static_cast<internal::result_state<T, false>*>(allocate_new());
        state->emplace(current_task->head, std::move(t));
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
        if( cov.is_forward() ) {
            oox_var_base& next = *(oox_var_base*)cov.storage_ptr;
            my_ptr = 1|(uintptr_t)&next.storage_ptr;
        } else
            my_ptr = (uintptr_t)cov.storage_ptr;
        //TODO: broken? if( !std::is_lvalue_reference_v<C> ) // consume oox::var
        //    ov.~var(); // TODO: no need in sync for not yet published task
        return count + base_type::setup( port+is_writer, self, std::forward<Args>(args)...);
    }
    C&& consume() {
        internal::result_state<ooxed_type, false>* state = nullptr;
        if( my_ptr & 1 ) {
            void* p = *reinterpret_cast<void**>(my_ptr ^ 1);
            state = static_cast<internal::result_state<ooxed_type, false>*>(p);
            if constexpr (std::is_lvalue_reference_v<T> && !std::is_const_v<std::remove_reference_t<T>>) {
                constexpr std::ptrdiff_t offset_delta =
                    offsetof(internal::oox_var_base, storage_offset) - offsetof(internal::oox_var_base, storage_ptr);
                const auto* storage_offset_ptr = reinterpret_cast<const int*>(
                    reinterpret_cast<const char*>(reinterpret_cast<void**>(my_ptr ^ 1)) + offset_delta);
                auto* owner_task = reinterpret_cast<internal::task_node*>(
                    reinterpret_cast<std::uintptr_t>(p) -
                    static_cast<std::uintptr_t>(*storage_offset_ptr));
                __OOX_ASSERT_EX(owner_task, "null owner task");
                if(!state->has_value(owner_task->head)) {
                    state->emplace(owner_task->head); // requires default-constructible T
                }
            }
        } else {
            state = reinterpret_cast<internal::result_state<ooxed_type, false>*>(my_ptr);
        }
        __OOX_ASSERT_EX(state, "null result_state storage");
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
    using storage_task<slots, F>::storage_task;
    result_state<R, false> my_result;
    TASK_EXECUTE_METHOD {
        __OOX_TRACE("%p do_run: start",this);
        my_result.emplace(this->head, this->my_precious.value()());
        task_node::notify_successors<slots>();
        return nullptr;
    }
    ~functional_task() {
        my_result.reset(this->head);
    }
};

template<int slots, typename F>
struct functional_task<slots, F, void> : storage_task<slots, F> {
    using storage_task<slots, F>::storage_task;
    TASK_EXECUTE_METHOD {
        __OOX_TRACE("%p do_run: start",this);
        this->my_precious.value()();
        task_node::notify_successors<slots>();
        return nullptr;
    }
};

template<int slots, typename F, typename VT> // forwarding task
struct functional_task<slots, F, var<VT> > : storage_task<slots, F> {
    // TODO: NRVO optimized forwarding
    using storage_task<slots, F>::storage_task;
    std::aligned_storage_t<sizeof(var<VT>), alignof(var<VT>)> my_result;
    bool is_executed = false;
    TASK_EXECUTE_METHOD {
#if 0
        __OOX_TRACE("%p do_run: start forward",this);
        new(my_result.begin()) var<VT>( this->my_precious() );
        return task_node::forward_successors<slots>( *my_result.begin() );
#else
        if( !is_executed ) {
            __OOX_TRACE("%p do_run: start forward",this);
            new(&my_result) var<VT>( this->my_precious.value()() );
            is_executed = true;
            this->start_count.store(1, std::memory_order_release);
            arc* j = new arc( this, 0, arc::flow_only ); // TODO: embed into the task
            if( reinterpret_cast<var<VT>*>(&my_result)->current_task->add_arc(j) ) {
                __OOX_TRACE("%p do_run: add_arc", this); // recycle_as_continuation was here
                return nullptr;
            }
            else delete j;
        }
        __OOX_TRACE("%p do_run: notify forward",this);
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
[[nodiscard]] auto run(F&& f, Args&&... args)->internal::var_type<internal::result_type_of<F> >
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

void wait_for_all(internal::oox_var_base& on ) {
    on.wait();
}

template<typename T>
[[nodiscard]] T wait_and_get(const var<T> &ov) {
    auto &v = const_cast<var<T>&>(ov);
    wait_for_all(v);

    // Follow forwarding chain until we reach a non-forward var
    internal::oox_var_base* base = &v;
    while (base->is_forward()) {
        __OOX_ASSERT_EX(base->storage_ptr, "forwarded var has null storage_ptr in wait_and_get");
        base = reinterpret_cast<internal::oox_var_base*>(base->storage_ptr);
    }

    __OOX_ASSERT_EX(base->storage_ptr, "var has null storage_ptr in wait_and_get");
    return static_cast<internal::result_state<T, false>*>(base->storage_ptr)->value();
}

template<typename T>
[[nodiscard]] T wait_and_get(var<T> &ov) { return wait_and_get(static_cast<const var<T>&>(ov)); }
template<typename T>
[[nodiscard]] T wait_and_get(var<T> &&ov) { return wait_and_get(static_cast<const var<T>&>(ov)); }

#undef TASK_EXECUTE_METHOD
#undef OOX_EXCEPTIONS_ENABLED

} // namespace oox
#endif // __OOX_H__
