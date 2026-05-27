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

#if OOX_EXCEPTIONS_ENABLED
struct cancelled_by_exception final : std::exception {
    [[nodiscard]] const char* what() const noexcept override {
        return "oox::cancelled_by_exception";
    }
};

struct cancelled_by_user final : std::exception {
    [[nodiscard]] const char* what() const noexcept override {
        return "oox::cancelled_by_user";
    }
};
#endif

#if defined(OOX_DEFAULT_EXCEPTION_POLICY)
inline constexpr bool default_exception_policy =
    OOX_EXCEPTIONS_ENABLED && (OOX_DEFAULT_EXCEPTION_POLICY != 0);
#elif OOX_EXCEPTIONS_ENABLED
inline constexpr bool default_exception_policy = true;
#else
inline constexpr bool default_exception_policy = false;
#endif

enum class wait_status {
    ready,
    user_cancelled,
    dependency_cancelled
};

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

#if OOX_EXCEPTIONS_ENABLED
template <typename Derived>
struct result_state_throw_base {
    static constexpr unsigned char state_empty = k_result_state_empty;
    static constexpr unsigned char state_cancelled = k_result_state_cancelled;
    static constexpr unsigned char state_value = k_result_state_value;
    static constexpr unsigned char state_exception = k_result_state_exception;

  protected:
    Derived& derived() noexcept { return *static_cast<Derived*>(this); }
    const Derived& derived() const noexcept { return *static_cast<const Derived*>(this); }
    unsigned char state_bits() const noexcept { return derived().state_bits(); }
    void set_state_bits(unsigned char state) noexcept { derived().set_state_bits(state); }

  public:
    void try_set_exception(std::exception_ptr eptr) noexcept {
        const bool is_cancelled = !eptr;
        const auto current = state_bits();
        if (current == state_value || current == state_exception) {
            return;
        }

        auto* slot = derived().exception_ptr();
        if (is_cancelled) {
            if (current != state_empty) {
                return;
            }
            ::new (static_cast<void*>(slot)) std::exception_ptr();
            set_state_bits(state_cancelled);
            return;
        }
        if (current == state_cancelled) {
            *slot = std::move(eptr);
            set_state_bits(state_exception);
            return;
        }
        __OOX_ASSERT_EX(current == state_empty, "unexpected result_state while storing exception");
        ::new (static_cast<void*>(slot)) std::exception_ptr(std::move(eptr));
        set_state_bits(state_exception);
    }
    bool has_exception() const noexcept {
        const auto state = state_bits();
        return state == state_exception || state == state_cancelled;
    }
    std::exception_ptr get_exception() const noexcept {
        const auto state = state_bits();
        if (state != state_exception && state != state_cancelled) {
            return std::exception_ptr{};
        }
        return *derived().exception_ptr();
    }
};
#endif

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

    T* ptr() noexcept { return std::launder(reinterpret_cast<T*>(storage.data)); }
    const T* ptr() const noexcept { return std::launder(reinterpret_cast<const T*>(storage.data)); }

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
};

template <bool CanThrow>
struct result_state<void, CanThrow>;

#if OOX_EXCEPTIONS_ENABLED
template <typename T>
struct result_state<T, true> : private result_state_throw_base<result_state<T, true>> {
    using base_type = result_state_throw_base<result_state<T, true>>;
    using value_type = T;
    static constexpr unsigned char state_empty = base_type::state_empty;
    static constexpr unsigned char state_cancelled = base_type::state_cancelled;
    static constexpr unsigned char state_value = base_type::state_value;
    static constexpr unsigned char state_exception = base_type::state_exception;

    result_state() : state_bits_field(state_empty) {}
    result_state(const result_state&) = delete;
    result_state& operator=(const result_state&) = delete;
    result_state(result_state&&) = delete;
    result_state& operator=(result_state&&) = delete;
    ~result_state() = default;

    template <typename... Args>
    void emplace(Args&&... args) {
        const auto previous = state_bits_field;
        if (previous == state_empty) [[likely]] {
            construct_value(std::forward<Args>(args)...);
            state_bits_field = state_value;
            return;
        }
        if (previous == state_value || previous == state_exception) {
            return;
        }
        if (previous == state_cancelled) {
            exception_ptr()->~exception_ptr();
        }
        __OOX_ASSERT_EX(previous == state_cancelled, "unexpected result_state while storing value");
        construct_value(std::forward<Args>(args)...);
        state_bits_field = state_value;
    }
    bool has_value() const noexcept {
        return state_bits_field == state_value;
    }
    T& value() { return *ptr(); }
    const T& value() const { return *ptr(); }
    using base_type::get_exception;
    using base_type::has_exception;
    using base_type::try_set_exception;
    void reset() {
        const auto previous = state_bits_field;
        if (previous == state_empty) {
            return;
        }
        state_bits_field = state_empty;
        if (previous == state_value) {
            ptr()->~T();
        } else {
            exception_ptr()->~exception_ptr();
        }
    }

    T* ptr() noexcept { return std::launder(reinterpret_cast<T*>(storage.data)); }
    const T* ptr() const noexcept { return std::launder(reinterpret_cast<const T*>(storage.data)); }

  private:
    static constexpr std::size_t storage_size =
        (sizeof(T) > sizeof(std::exception_ptr)) ? sizeof(T) : sizeof(std::exception_ptr);
    static constexpr std::size_t storage_align =
        (alignof(T) > alignof(std::exception_ptr)) ? alignof(T) : alignof(std::exception_ptr);
    struct storage_t {
        alignas(storage_align) std::byte data[storage_size];
    };
    template <typename Derived>
    friend struct result_state_throw_base;
    unsigned char state_bits() const noexcept { return state_bits_field; }
    void set_state_bits(unsigned char state) noexcept { state_bits_field = state; }
    storage_t storage{};
    unsigned char state_bits_field : 2;
    template <typename... Args>
    void construct_value(Args&&... args) {
        if constexpr (sizeof...(Args) == 0) {
            ::new (static_cast<void*>(storage.data)) T;
        } else {
            ::new (static_cast<void*>(storage.data)) T(std::forward<Args>(args)...);
        }
    }
    std::exception_ptr* exception_ptr() noexcept {
        return std::launder(reinterpret_cast<std::exception_ptr*>(storage.data));
    }
    const std::exception_ptr* exception_ptr() const noexcept {
        return std::launder(reinterpret_cast<const std::exception_ptr*>(storage.data));
    }
};

template <>
struct result_state<void, true> : private result_state_throw_base<result_state<void, true>> {
    using base_type = result_state_throw_base<result_state<void, true>>;
    static constexpr unsigned char state_empty = base_type::state_empty;
    static constexpr unsigned char state_cancelled = base_type::state_cancelled;
    static constexpr unsigned char state_value = base_type::state_value;
    static constexpr unsigned char state_exception = base_type::state_exception;

    result_state() : state_bits_field(state_empty) {}
    result_state(const result_state&) = delete;
    result_state& operator=(const result_state&) = delete;
    result_state(result_state&&) = delete;
    result_state& operator=(result_state&&) = delete;
    ~result_state() = default;

    using base_type::get_exception;
    using base_type::has_exception;
    using base_type::try_set_exception;
    void reset() {
        const auto previous = state_bits_field;
        if (previous == state_empty) {
            return;
        }
        state_bits_field = state_empty;
        if (previous == state_exception || previous == state_cancelled) {
            exception_ptr()->~exception_ptr();
        }
    }

  private:
    static constexpr std::size_t storage_size = sizeof(std::exception_ptr);
    struct storage_t {
        alignas(alignof(std::exception_ptr)) std::byte data[storage_size];
    };
    template <typename Derived>
    friend struct result_state_throw_base;
    unsigned char state_bits() const noexcept { return state_bits_field; }
    void set_state_bits(unsigned char state) noexcept { state_bits_field = state; }
    storage_t storage{};
    unsigned char state_bits_field : 2;
    std::exception_ptr* exception_ptr() noexcept {
        return std::launder(reinterpret_cast<std::exception_ptr*>(storage.data));
    }
    const std::exception_ptr* exception_ptr() const noexcept {
        return std::launder(reinterpret_cast<const std::exception_ptr*>(storage.data));
    }
};
#endif

template <>
struct result_state<void, false> {
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
#define OOX_TASK_EXECUTE_LIFETIME_GUARD ::oox::internal::task::execute_lifetime_guard oox_execute_lifetime_guard{this}
#define OOX_TASK_EXECUTE_LIFETIME_REF 1

struct task : public tbb_task, task_life {
    tbb::detail::d1::wait_context waiter{1};
#ifndef OOX_USE_STDMALLOC
    small_object_allocator alloc{};
#endif
    struct execute_lifetime_guard {
        task* self;
        ~execute_lifetime_guard() {
            self->release(1);
        }
    };
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
#define OOX_TASK_EXECUTE_LIFETIME_REF 1

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
#elif HAVE_TWIST /////////////////////// Twist ///////////////////////////////////////
#define OOX_USING_TWIST
#define TASK_EXECUTE_METHOD void* execute() override
#define OOX_TASK_EXECUTE_LIFETIME_REF 1

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
            this->execute();
            this->release(1);
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

#ifndef OOX_TASK_EXECUTE_LIFETIME_GUARD
#define OOX_TASK_EXECUTE_LIFETIME_GUARD do { } while (false)
#endif
#ifndef OOX_TASK_EXECUTE_LIFETIME_REF
#define OOX_TASK_EXECUTE_LIFETIME_REF 0
#endif

struct task_node;
struct oox_var_base;

namespace details {

inline constexpr std::uintptr_t k_next_writer_ready_tag = 0x1;
inline constexpr std::uintptr_t k_next_writer_no_owner_tag = 0x3;
inline constexpr std::uintptr_t k_forwarded_storage_ptr_tag = 0x1;

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

inline void** decode_forwarded_storage_ptr(uintptr_t ptr) {
    return reinterpret_cast<void**>(ptr&~k_forwarded_storage_ptr_tag);
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
    static constexpr int start_count_mask = std::numeric_limits<int>::max();
#if OOX_EXCEPTIONS_ENABLED
    // The high bit is a hot-path gate: when it is clear, remove_prerequisite()
    // can spawn directly without entering virtual failure-state decoding.
    static constexpr int start_failure_bit = std::numeric_limits<int>::min();
#endif
    sync::atomic<int> start_count{0};
#if OOX_EXCEPTIONS_ENABLED
    virtual void try_set_exception(std::exception_ptr eptr) noexcept = 0;
    virtual bool has_exception() const noexcept = 0;
    virtual std::exception_ptr get_exception() const noexcept = 0;
    virtual task_node* exception_origin() const noexcept = 0;
    virtual void publish_incoming_failure(task_node* source, int source_port) noexcept = 0;
    virtual bool apply_incoming_failure() noexcept = 0;

    void mark_failure_signal() noexcept {
        start_count.fetch_or(start_failure_bit, std::memory_order_release);
    }

    bool has_failure_signal() const noexcept {
        return (start_count.load(std::memory_order_acquire) & start_failure_bit) != 0;
    }

    void add_suspended_prerequisite() noexcept {
        start_count.fetch_add(1, std::memory_order_release);
    }

    void set_exception(std::exception_ptr ep) noexcept {
        mark_failure_signal();
        try_set_exception(std::move(ep));
    }

    void cancel() noexcept {
        publish_incoming_failure(nullptr, 0);
        if ((start_count.load(std::memory_order_acquire) & start_count_mask) == 0) {
            set_exception(std::make_exception_ptr(cancelled_by_user{}));
        }
    }

    std::exception_ptr exception_for_port([[maybe_unused]] int port) const noexcept {
        if (port > 0) {
            return std::exception_ptr{}; //cancelled
        }
        return get_exception();
    }
#else
    void cancel() noexcept {}
#endif

    task_node() { } // prepare the task for waiting on it directly
    virtual ~task_node() = default;

    // Result output node
    inline output_node& out(int n) const;
    // Add a prerequisite
    int  assign_prerequisite( task_node *n, int req_port );
    // Process flow- and anti-dependence arcs
    template<bool MayFail>
    void do_notify_arcs_impl( arc* r, int *count );
    void do_notify_arcs( arc* r, int *count );
    // Process output dependence
    int  do_notify_out( int port, int count );
    // Process flow and output arcs. Returns number of finished output nodes
    template<bool MayFail>
    int  notify_successors_impl( int output_slots, int *counters );
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
    template<int slots>
    void notify_successors_success();
#if OOX_EXCEPTIONS_ENABLED
    virtual void notify_successors_virtual() = 0;
#endif
    // Call base forward successors
    template<int slots>
    void forward_successors( oox_var_base& );

    // It is called when producer is done and notifies consumers to copy the value
    virtual void on_ready(int) { __OOX_ASSERT(false, "not implemented"); }
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
#if OOX_EXCEPTIONS_ENABLED
    if (n->has_failure_signal()) [[unlikely]] {
        this->publish_incoming_failure(n, req_port);
    }
#endif
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

template<bool MayFail>
void task_node::do_notify_arcs_impl( arc* r, int *count ) {
    // Notify successors that value is available
#if OOX_EXCEPTIONS_ENABLED
    bool producer_failed = false;
    if constexpr (MayFail) {
        producer_failed = this->has_failure_signal();
    }
#endif
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
            auto arc_port = j->port;
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
            } else if( j->kind == arc::flow_copy ) {
                n->on_ready( j->port );
                delete j;
            } else if( j->kind == arc::flow_only ) {
                delete j;
            } else if( j->kind == arc::forward_copy )
                __OOX_ASSERT(false, "incorrect forwarding"); // has to be processed by forward_successors only
            // Let "n" know that prerequisite "this" is ready.
            __OOX_TRACE("%p notify: %p->remove_prequisite()",this,n);
#if OOX_EXCEPTIONS_ENABLED
            if constexpr (MayFail) {
                if (producer_failed) [[unlikely]] {
                    n->publish_incoming_failure(this, arc_port);
                }
            }
#endif
            n->remove_prerequisite();
        }
    } while( r );
}

void task_node::do_notify_arcs( arc* r, int *count ) {
    do_notify_arcs_impl<true>( r, count );
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

template<bool MayFail>
int task_node::notify_successors_impl( int output_slots, int *count ) {
    for( int i = 0; i <  output_slots; i++ ) {
        // it should be safe to assign countdowns here because no successors were notified yet
        out(i).countdown.store( count[i] = std::numeric_limits<int>::max()/2, std::memory_order_release );
    }
   __OOX_TRACE("%p notify successors",this);
    // Grab list of successors and mark as competed.
    // Note that countdowns can change asynchronously after this point

   sync::preemption_point();
   if( arc* r = head.exchange( (arc*)k_task_done_tag ) )
        do_notify_arcs_impl<MayFail>( r, count );
    int refs = 0;
    for( int i = 0; i <  output_slots; i++ )
        refs += do_notify_out( i, count[i] );
    __OOX_ASSERT(refs>=0, "");
    return refs;
}

int task_node::notify_successors( int output_slots, int *count ) {
    return notify_successors_impl<true>( output_slots, count );
}

void task_node::remove_prerequisite( int n ) {
#if OOX_EXCEPTIONS_ENABLED
    int k = start_count.fetch_sub(n, std::memory_order_release) - n;
    const int count = k & start_count_mask;
    __OOX_ASSERT(count <= start_count_mask - n, "invalid start_count detected while removing prerequisite");
    if( count != 0 ) [[likely]] {
        return;
    }
    __OOX_TRACE("%p remove_prerequisite: spawning",this);
    sync::preemption_point();
    if (k < 0) [[unlikely]] {
        if (apply_incoming_failure()) [[unlikely]] {
            if constexpr (OOX_TASK_EXECUTE_LIFETIME_REF != 0) {
                // Task body will not run, so balance ctor-acquired execute ref.
                release(OOX_TASK_EXECUTE_LIFETIME_REF);
            }
            notify_successors_virtual();
            return;
        }
    }
    spawn();
#else
    int k = start_count-=n;
    __OOX_ASSERT(k>=0,"invalid start_count detected while removing prerequisite");
    if( k==0 ) {
        __OOX_TRACE("%p remove_prerequisite: spawning",this);
        sync::preemption_point();
        spawn();
    }
#endif
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
        sync::preemption_point();
        return notify_next_writer( out(output_port).next_writer.load(std::memory_order_acquire) );
    }
    return 0;
}

void task_node::set_next_writer( int output_port, task_node* d ) {
    __OOX_ASSERT( !details::is_next_writer_ready_marker(d), "" );
    sync::preemption_point();
    task_node* o = out(output_port).next_writer.exchange(d);
    sync::preemption_point();
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
    // With user-cancel, execute() can still run and
    // wake a waiter that immediately drops the last ref. A trailing release(0)
    // would then touch lifetime atomics on a reclaimed task which is UB.
    if( n )
        release(n);
}

template<int slots>
void task_node::notify_successors_success() {
    int counters[slots];
    int n = notify_successors_impl<false>( slots, counters );
    wakeup();
    // Same race as above: after wakeup(), waiter-side teardown may destroy this
    // task before completion thread reaches release(). Skip zero-release paths.
    if( n )
        release(n);
}

#if OOX_EXCEPTIONS_ENABLED
template <bool CanThrow>
struct incoming_failure_state {
    void publish_incoming_failure(task_node*, int) noexcept {}
    bool has_incoming_failure() const noexcept { return false; }
    template <typename Node>
    bool apply_incoming_failure(Node&) noexcept {
        return false;
    }
};

template <>
struct incoming_failure_state<true> {
    static constexpr uintptr_t incoming_failure_none = 0;
    static constexpr uintptr_t incoming_failure_flag = 1;
    static constexpr uintptr_t incoming_failure_source_tag = 2;
    static constexpr uintptr_t incoming_failure_cancelled = incoming_failure_flag;
    static constexpr uintptr_t incoming_failure_user_cancelled = incoming_failure_flag | 4;
    static constexpr uintptr_t incoming_failure_source = incoming_failure_flag | incoming_failure_source_tag;
    static constexpr uintptr_t incoming_failure_tag_mask = 7;

    sync::atomic<uintptr_t> incoming_failure_word{incoming_failure_none};

    void publish_incoming_failure(task_node* source, int source_port) noexcept {
        if (!source) {
            publish_user_cancelled();
            return;
        }
        if (source_port != 0) {
            publish_cancelled();
            return;
        }
        // Store the originator (task that holds the actual exception_ptr), not the immediate source.
        auto* origin = source->exception_origin();
        if (!origin) {
            publish_cancelled();
            return;
        }
        publish_exception_source(origin);
    }

    template <typename Node>
    bool apply_incoming_failure(Node& node) noexcept {
        const auto state = incoming_failure_word.load(std::memory_order_acquire);
        if (state == incoming_failure_none) {
            return false;
        }
        if ((state & incoming_failure_source) == incoming_failure_source &&
            state > incoming_failure_tag_mask) {
            if (node.has_exception()) {
                return true;
            }
            auto* origin = reinterpret_cast<task_node*>(state & ~incoming_failure_tag_mask);
            node.set_exception(origin->get_exception());
            return node.has_exception();
        }
        if (state == incoming_failure_user_cancelled) {
            node.set_exception(std::make_exception_ptr(cancelled_by_user{}));
            // Best-effort user cancellation: keep cancellation visible, but do not force
            // an early skip from prerequisite resolution.
            return false;
        }
        if (state & incoming_failure_cancelled) {
            node.set_exception(std::exception_ptr{});
            return node.has_exception();
        }
        return false;
    }

    bool has_incoming_failure() const noexcept {
        return (incoming_failure_word.load(std::memory_order_acquire) & incoming_failure_flag) != 0;
    }

    bool has_incoming_exception_source() const noexcept {
        const auto state = incoming_failure_word.load(std::memory_order_acquire);
        return (state & incoming_failure_source) == incoming_failure_source &&
               state > incoming_failure_tag_mask;
    }

    task_node* incoming_exception_source() const noexcept {
        const auto state = incoming_failure_word.load(std::memory_order_acquire);
        if ((state & incoming_failure_source) == incoming_failure_source &&
            state > incoming_failure_tag_mask) {
            return reinterpret_cast<task_node*>(state & ~incoming_failure_tag_mask);
        }
        return nullptr;
    }

  private:
    void publish_cancelled() noexcept {
        incoming_failure_word.fetch_or(incoming_failure_cancelled, std::memory_order_acq_rel);
    }

    void publish_user_cancelled() noexcept {
        incoming_failure_word.fetch_or(incoming_failure_user_cancelled, std::memory_order_acq_rel);
    }

    void publish_exception_source(task_node* source) noexcept {
        const auto source_word = reinterpret_cast<uintptr_t>(source);
        __OOX_ASSERT_EX((source_word & incoming_failure_tag_mask) == 0,
                        "incoming failure source pointer must be aligned");
        const auto encoded = source_word | incoming_failure_source;
        incoming_failure_word.store(encoded, std::memory_order_release);
    }
};
#endif

template<int N>
struct output_slots_storage {
    output_node output_nodes[N];
};

template<int slots>
struct task_node_slots : task_node, output_slots_storage<slots> {
    TASK_EXECUTE_METHOD { __OOX_ASSERT(false, "not runnable"); return nullptr; }
#if OOX_EXCEPTIONS_ENABLED
    void notify_successors_virtual() override {
        task_node::notify_successors<slots>();
    }
#endif
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
    using result_base = result_state<T, false>;
    TASK_EXECUTE_METHOD { __OOX_ASSERT(false, "not runnable"); return nullptr; }
    storage_task() = default;
    storage_task(T&& t) { result_base::emplace(std::move(t)); }
    storage_task(const T& t) { result_base::emplace(t); }
#if OOX_EXCEPTIONS_ENABLED
    void try_set_exception(std::exception_ptr) noexcept override {}
    bool has_exception() const noexcept override { return false; }
    std::exception_ptr get_exception() const noexcept override { return std::exception_ptr{}; }
    task_node* exception_origin() const noexcept override { return nullptr; }
    void publish_incoming_failure(task_node*, int) noexcept override {}
    bool apply_incoming_failure() noexcept override { return false; }
#endif
    ~storage_task() override { result_base::reset(); }
};

struct oox_var_base {
    //TODO: make it a class with private members
    oox_var_base &operator=(const oox_var_base &) = delete;
    static constexpr int k_port_bits = 13;
    static constexpr int k_max_port = (1 << k_port_bits) - 1;

    struct current_port_and_flags_t {
        std::uint16_t port : k_port_bits;
        bool is_forwarded : 1;
        bool is_deferred : 1;
        bool storage_state_can_throw : 1;
    };
    static_assert(sizeof(current_port_and_flags_t) == sizeof(std::uint16_t),
                  "oox::var packed port/flags must stay 16-bit");

    template< typename T, bool > friend struct gen_oox;
    task_node*  current_task = nullptr;
    void*       storage_ptr;
    int         storage_offset; // task_node* original = ptr - offset
    current_port_and_flags_t current_port_and_flags{}; // port plus var-local forward/deferred flags

    int current_port() const noexcept {
        return current_port_and_flags.port;
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
    void bind_to( task_node * t, void* ptr, int lifetime, bool fwd = false, bool deferred = false, bool state_can_throw = false ) {
        current_task = t, storage_ptr = ptr, current_port_and_flags = {};
        current_port_and_flags.is_forwarded = fwd;
        current_port_and_flags.is_deferred = deferred;
        current_port_and_flags.storage_state_can_throw = state_can_throw;
        storage_offset = uintptr_t(storage_ptr) - uintptr_t(current_task);
        t->life_add_count(lifetime);
        __OOX_TRACE("%p bind: store=%p life=%d fwd=%d deferred=%d throw_state=%d",t,ptr,lifetime,fwd,deferred,state_can_throw);
    }
    void wait() {
        __OOX_ASSERT_EX(current_task, "wait for empty oox::var");
        // if head == 1, the producer is already "done":
        // - either a constant storage_task, or
        // - a completed functional_task.
        arc* h = current_task->head.load(std::memory_order_acquire);
        if (k_task_done_tag == (uintptr_t)h){
            return;
        }
        current_task->wait();
        h = current_task->head.load(std::memory_order_acquire);
        __OOX_ASSERT(k_task_done_tag == (uintptr_t)h, "wait returned before task completed");
    }
    void cancel_current_task() noexcept {
#if OOX_EXCEPTIONS_ENABLED
        if (current_task) {
            current_task->cancel();
        }
#endif
    }
    void release() {
        if( current_task ) {
            task_node* next = details::next_writer_no_owner_marker();
            if (storage_offset) {
                auto* owner = reinterpret_cast<task_node*>(uintptr_t(storage_ptr) - storage_offset);
                next = details::encode_next_writer_owner(owner);
            }
            current_task->set_next_writer(current_port(), next);
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
            __OOX_ASSERT( uintptr_t(l) == k_task_done_tag, "arc lists merge is not implemented" ); // TODO
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

template< typename T, bool CanThrow > struct gen_oox;

} // namespace internal


template< typename T, bool CanThrow = default_exception_policy >
class var : public internal::oox_var_base {
    static_assert(std::is_same_v<T, std::decay_t<T>>,
                  "Specialize oox::var only by plain types and pointers."
                  "For references, use reference_wrapper,"
                  "for const types use shared_ptr<T>.");
    static_assert(OOX_EXCEPTIONS_ENABLED || !CanThrow, "oox::var<T, true> requires OOX_EXCEPTIONS_ENABLED=1");

    void* allocate_new() noexcept {
        auto *v = internal::task::allocate<internal::storage_task<1, T>>();
        __OOX_TRACE("%p oox::var",v);
        v->out(0).next_writer.store(internal::details::next_writer_ready_marker(), std::memory_order_release);
        v->head.store((internal::arc*)internal::k_task_done_tag, std::memory_order_release);
        // nobody wait on this task
        this->bind_to(v, static_cast<internal::result_state<T, false>*>(v), 2, false, false, false);
        return storage_ptr;
    }

    void* allocate_deferred() noexcept {
        auto *v = internal::task::allocate<internal::storage_task<1, T>>();
        __OOX_TRACE("%p oox::var(deferred)", v);
        // Make writers behave like for a normal initial value
        // (next_writer=details::next_writer_ready_marker()),
        // BUT do NOT mark the node completed (head stays nullptr), so readers block.
        v->out(0).next_writer.store(internal::details::next_writer_ready_marker(), std::memory_order_release);
        // v->head is intentionally left as nullptr (not ready)
        this->bind_to(v, static_cast<internal::result_state<T, false>*>(v), 2, false, true, false);
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
    var(var<T, CanThrow>&& t) : internal::oox_var_base(std::move(t)) { t.current_task = nullptr; }
    var& operator=(var<T, CanThrow>&& t) {
        release();
        new(this) internal::oox_var_base(std::move(t));
        __OOX_ASSERT_EX(current_task, "");
        t.current_task = nullptr;
        return *this;
    }
    ~var() { release(); }
    [[nodiscard]] T get() {
        wait();
        if constexpr (CanThrow) {
#if OOX_EXCEPTIONS_ENABLED
            if (current_port_and_flags.storage_state_can_throw) {
                return static_cast<internal::result_state<T, true>*>(storage_ptr)->value();
            }
#endif
        }
        return static_cast<internal::result_state<T, false>*>(storage_ptr)->value();
    }
    void cancel() noexcept {
#if OOX_EXCEPTIONS_ENABLED
        if constexpr (CanThrow) {
            this->cancel_current_task();
        }
#else
        __OOX_ASSERT(false, "cancel when exceptions are disabled is not supported");
#endif
    }
};

template<bool CanThrow>
class var<void, CanThrow> : public internal::oox_var_base {
    template< typename T, bool > friend struct gen_oox;
    static_assert(OOX_EXCEPTIONS_ENABLED || !CanThrow, "oox::var<void, true> requires OOX_EXCEPTIONS_ENABLED=1");
public:
    var() {}
    template<typename D, bool DCanThrow>
    var(var<D, DCanThrow>&& src) : internal::oox_var_base(src) {
        static_assert(CanThrow || !DCanThrow, "non-throwing void var cannot bind to throwing source");
        ((internal::task_node*)(uintptr_t(src.storage_ptr)-src.storage_offset))->release();
        src.current_task = nullptr;
    }
    void cancel() noexcept {
#if OOX_EXCEPTIONS_ENABLED
        if constexpr (CanThrow) {
            this->cancel_current_task();
        }
#else
        __OOX_ASSERT(false, "cancel when exceptions are disabled is not supported");
#endif
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
template< typename Types, bool SelfCanThrow, typename... Args > struct base_args;
// User functor might have default arguments which are not specified thus ignoring them
template< typename IgnoredTypes, bool SelfCanThrow > struct base_args<IgnoredTypes, SelfCanThrow> {
    static constexpr int write_nodes_count = 1; // for resulting node
    int setup(int, internal::task_node *) { return 0 /* resulting node is ready initially*/; }
};

template< typename T, typename... Types, typename A, typename... Args, bool SelfCanThrow >
struct base_args<types<T, Types...>, SelfCanThrow, A, Args...>
    : base_args<types<Types...>, SelfCanThrow, Args...> {
    using base_type = base_args<types<Types...>, SelfCanThrow, Args...>;

    std::decay_t<A> my_value;

    base_args( A&& a, Args&&... args ) : base_type( std::forward<Args>(args)... ), my_value(std::forward<A>(a)) {}
    std::decay_t<A>&& consume() { return std::move(my_value); }
    static constexpr int write_nodes_count = base_type::write_nodes_count;
    int setup( int port, internal::task_node *self, A&& a, Args&&... args ) {
        //__OOX_ASSERT(my_value == a, "");
        return base_type::setup( port, self, std::forward<Args>(args)...);
    }
};

template< typename Types, bool SelfCanThrow, typename C, bool VarCanThrow, typename... Args > struct oox_var_args;
template< typename T, typename... Types, typename C, bool VarCanThrow, typename... Args, bool SelfCanThrow >
struct oox_var_args<types<T, Types...>, SelfCanThrow, C, VarCanThrow, Args...>
    : base_args<types<Types...>, SelfCanThrow, Args...> {
    using base_type = base_args<types<Types...>, SelfCanThrow, Args...>;
    using ooxed_type = std::decay_t<C>;
    using var_type = var<ooxed_type, VarCanThrow>;

    static constexpr uintptr_t k_forward_ptr_tag = uintptr_t(1);
    static constexpr uintptr_t k_throw_state_tag = uintptr_t(2);
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
        if constexpr (is_writer) {
            static_assert(VarCanThrow || !SelfCanThrow, "throwing task cannot write to non-throwing var");
            auto &ov = const_cast<var_type&>(cov); // actual type is non-const due to is_writer
            ov.set_next_writer( port, self );// TODO: add 'count =' because no need in sync here
        } else {
            static_assert(SelfCanThrow || !VarCanThrow, "non-throwing task cannot depend on throwing task");
            count = self->assign_prerequisite( cov.current_task, cov.current_port() );
        }
        if( cov.current_port_and_flags.is_forwarded ) {
            auto* next = static_cast<oox_var_base*>(cov.storage_ptr);
            const auto next_word = reinterpret_cast<uintptr_t>(next);
            __OOX_ASSERT((next_word & k_forward_ptr_tag) == 0, "forward var pointer must be aligned");
            my_ptr = k_forward_ptr_tag | next_word;
        } else {
            const uintptr_t tags_mask = k_forward_ptr_tag | k_throw_state_tag;
            const auto storage_word = reinterpret_cast<uintptr_t>(cov.storage_ptr);
            __OOX_ASSERT((storage_word & tags_mask) == 0,
                            "result_state pointer must be at least 4-byte aligned");
            my_ptr = storage_word
                     | (cov.current_port_and_flags.storage_state_can_throw ? k_throw_state_tag : 0);
        }
        //TODO: broken? if( !std::is_lvalue_reference_v<C> ) // consume oox::var
        //    ov.~var(); // TODO: no need in sync for not yet published task
        return count + base_type::setup( port+is_writer, self, std::forward<Args>(args)...);
    }
    C&& consume() {
        void* state_ptr = nullptr;
        bool state_can_throw = (my_ptr & k_throw_state_tag) != 0;
        if( my_ptr & k_forward_ptr_tag ) {
            oox_var_base* next = reinterpret_cast<oox_var_base*>(my_ptr & ~k_forward_ptr_tag);
            state_ptr = next->storage_ptr;
            state_can_throw = next->current_port_and_flags.storage_state_can_throw;
        } else {
            state_ptr = reinterpret_cast<void*>(my_ptr & ~k_throw_state_tag);
        }
        __OOX_ASSERT_EX(state_ptr, "null result_state storage");

        auto consume_state = [](auto* state) -> C&& {
            if constexpr (std::is_lvalue_reference_v<T> && !std::is_const_v<std::remove_reference_t<T>>) {
                if(!state->has_value()) {
                    state->emplace(); // requires default-constructible T
                }
            }
            __OOX_ASSERT_EX(state->has_value(), "read from empty result_state");
            return static_cast<C&&>(state->value());
        };

#if OOX_EXCEPTIONS_ENABLED
        if (state_can_throw) {
            return consume_state(static_cast<internal::result_state<ooxed_type, true>*>(state_ptr));
        }
#endif
        return consume_state(static_cast<internal::result_state<ooxed_type, false>*>(state_ptr));
    }
};
template< typename T, typename... Types, typename A, bool VarCanThrow, typename... Args, bool SelfCanThrow >
struct base_args<types<T, Types...>, SelfCanThrow, var<A, VarCanThrow>&, Args...>
    : oox_var_args<types<T, Types...>, SelfCanThrow, A&, VarCanThrow, Args...> {
    using oox_var_args<types<T, Types...>, SelfCanThrow, A&, VarCanThrow, Args...>::oox_var_args;
};
template< typename T, typename... Types, typename A, bool VarCanThrow, typename... Args, bool SelfCanThrow >
struct base_args<types<T, Types...>, SelfCanThrow, const var<A, VarCanThrow>&, Args...>
    : oox_var_args<types<T, Types...>, SelfCanThrow, const A&, VarCanThrow, Args...> {
    using oox_var_args<types<T, Types...>, SelfCanThrow, const A&, VarCanThrow, Args...>::oox_var_args;
};
template< typename T, typename... Types, typename A, bool VarCanThrow, typename... Args, bool SelfCanThrow >
struct base_args<types<T, Types...>, SelfCanThrow, var<A, VarCanThrow>&&, Args...>
    : oox_var_args<types<T, Types...>, SelfCanThrow, A&&, VarCanThrow, Args...> {
    using oox_var_args<types<T, Types...>, SelfCanThrow, A&&, VarCanThrow, Args...>::oox_var_args;
};

template< typename F, typename... Preceding, typename Args >
auto apply_args( F&& f, Args&& pack, Preceding&&... params ) {
    return apply_args(std::forward<F>(f),
                      std::forward<typename Args::base_type>(pack),
                      std::forward<Preceding>(params)...,
                      pack.consume());
}

template< typename F, typename... Preceding, typename Last, bool SelfCanThrow >
auto apply_args( F&& f, base_args<Last, SelfCanThrow>&& /*pack*/, Preceding&&... params ) {
    return std::forward<F>(f)(std::forward<Preceding>(params)...);
}

template< typename F, typename Args >
struct oox_bind {
    F my_func;
    Args my_args;
    oox_bind(F&& f, Args&& a) : my_func(std::forward<F>(f)), my_args(std::move(a)) {}
    auto operator()() { return apply_args(std::move(my_func), std::move(my_args)); }
};

template<int slots, typename F, typename R, bool CanThrow>
struct alignas(64) functional_task : storage_task<slots, F>, result_state<R, CanThrow>
#if OOX_EXCEPTIONS_ENABLED
                                  , private incoming_failure_state<CanThrow>
#endif
{
    using functor_base = storage_task<slots, F>;
    using result_base = result_state<R, CanThrow>;
    template<typename... Args>
    functional_task(Args&&... args) : storage_task<slots, F>(std::forward<Args>(args)...) {
        if constexpr (OOX_TASK_EXECUTE_LIFETIME_REF != 0) {
            this->life_add_count(OOX_TASK_EXECUTE_LIFETIME_REF);
        }
    }

#if OOX_EXCEPTIONS_ENABLED
    using failure_base = incoming_failure_state<CanThrow>;
    void try_set_exception(std::exception_ptr eptr) noexcept override {
        if constexpr (CanThrow) {
            result_base::try_set_exception(std::move(eptr));
        }
    }
    bool has_exception() const noexcept override {
        if constexpr (CanThrow) {
            return result_base::has_exception() || failure_base::has_incoming_exception_source();
        }
        return false;
    }
    std::exception_ptr get_exception() const noexcept override {
        if constexpr (CanThrow) {
            if (result_base::has_exception()) return result_base::get_exception();
            auto* origin = failure_base::incoming_exception_source();
            if (origin) return origin->get_exception();
        }
        return std::exception_ptr{};
    }
    task_node* exception_origin() const noexcept override {
        if constexpr (CanThrow) {
            if (result_base::has_exception()) return const_cast<functional_task*>(static_cast<const functional_task*>(this));
            return failure_base::incoming_exception_source();
        }
        return nullptr;
    }
    void publish_incoming_failure(task_node* source, int source_port) noexcept override {
        if constexpr (CanThrow) {
            if (source && source_port == 0) {
                if (auto eptr = source->get_exception()) {
                    try_set_exception(std::move(eptr));
                }
            }
        }
        failure_base::publish_incoming_failure(source, source_port);
        task_node::mark_failure_signal();
    }
    bool apply_incoming_failure() noexcept override {
        return failure_base::apply_incoming_failure(*this);
    }
#endif

    TASK_EXECUTE_METHOD {
        OOX_TASK_EXECUTE_LIFETIME_GUARD;
#if OOX_EXCEPTIONS_ENABLED
        if constexpr (CanThrow) {
            if (failure_base::has_incoming_failure()) [[unlikely]] {
                task_node::notify_successors<slots>();
                return nullptr;
            }
            try {
                result_base::emplace(this->functor_base::value()());
                task_node::notify_successors_success<slots>();
            } catch(...) {
                this->set_exception(std::current_exception());
                task_node::notify_successors<slots>();
            }
        } else {
            result_base::emplace(this->functor_base::value()());
            task_node::notify_successors_success<slots>();
        }
#else
        result_base::emplace(this->functor_base::value()());
        task_node::notify_successors<slots>();
#endif
        return nullptr;
    }
    ~functional_task() override { result_base::reset(); }
};

template<int slots, typename F, bool CanThrow>
struct functional_task<slots, F, void, CanThrow> : storage_task<slots, F>, result_state<void, CanThrow>
#if OOX_EXCEPTIONS_ENABLED
                                                  , private incoming_failure_state<CanThrow>
#endif
{
    using functor_base = storage_task<slots, F>;
    using result_base = result_state<void, CanThrow>;
    template<typename... Args>
    functional_task(Args&&... args) : storage_task<slots, F>(std::forward<Args>(args)...) {
        if constexpr (OOX_TASK_EXECUTE_LIFETIME_REF != 0) {
            this->life_add_count(OOX_TASK_EXECUTE_LIFETIME_REF);
        }
    }
#if OOX_EXCEPTIONS_ENABLED
    using failure_base = incoming_failure_state<CanThrow>;
    void try_set_exception(std::exception_ptr eptr) noexcept override {
        if constexpr (CanThrow) {
            result_base::try_set_exception(std::move(eptr));
        }
    }
    bool has_exception() const noexcept override {
        if constexpr (CanThrow) {
            return result_base::has_exception() || failure_base::has_incoming_exception_source();
        }
        return false;
    }
    std::exception_ptr get_exception() const noexcept override {
        if constexpr (CanThrow) {
            if (result_base::has_exception()) return result_base::get_exception();
            auto* origin = failure_base::incoming_exception_source();
            if (origin) return origin->get_exception();
        }
        return std::exception_ptr{};
    }
    task_node* exception_origin() const noexcept override {
        if constexpr (CanThrow) {
            if (result_base::has_exception()) return const_cast<functional_task*>(static_cast<const functional_task*>(this));
            return failure_base::incoming_exception_source();
        }
        return nullptr;
    }
    void publish_incoming_failure(task_node* source, int source_port) noexcept override {
        if constexpr (CanThrow) {
            if (source && source_port == 0) {
                if (auto eptr = source->get_exception()) {
                    try_set_exception(std::move(eptr));
                }
            }
        }
        failure_base::publish_incoming_failure(source, source_port);
        task_node::mark_failure_signal();
    }
    bool apply_incoming_failure() noexcept override {
        return failure_base::apply_incoming_failure(*this);
    }
#endif
    TASK_EXECUTE_METHOD {
        OOX_TASK_EXECUTE_LIFETIME_GUARD;
        __OOX_TRACE("%p do_run: start",this);
#if OOX_EXCEPTIONS_ENABLED
        if constexpr (CanThrow) {
            if (failure_base::has_incoming_failure()) [[unlikely]] {
                task_node::notify_successors<slots>();
                return nullptr;
            }
            try {
                this->functor_base::value()();
                task_node::notify_successors_success<slots>();
            } catch(...) {
                this->set_exception(std::current_exception());
                task_node::notify_successors<slots>();
            }
        } else {
            this->functor_base::value()();
            task_node::notify_successors_success<slots>();
        }
#else
        this->functor_base::value()();
        task_node::notify_successors<slots>();
#endif
        return nullptr;
    }
    ~functional_task() override {
        if constexpr (CanThrow) {
            result_base::reset();
        }
    }
};

template<int slots, typename F, typename VT, bool VarCanThrow, bool CanThrow>
struct functional_task<slots, F, var<VT, VarCanThrow>, CanThrow> : storage_task<slots, F>
                                                                  , result_state<var<VT, VarCanThrow>, CanThrow>
#if OOX_EXCEPTIONS_ENABLED
                                                                  , private incoming_failure_state<CanThrow>
#endif
{
    // TODO: NRVO optimized forwarding
    static_assert(VarCanThrow == CanThrow, "returning var<T, P> requires task policy to match P");
    using functor_base = storage_task<slots, F>;
    using var_type = var<VT, VarCanThrow>;
    using result_base = result_state<var_type, CanThrow>;
    template<typename... Args>
    functional_task(Args&&... args) : storage_task<slots, F>(std::forward<Args>(args)...) {
        if constexpr (OOX_TASK_EXECUTE_LIFETIME_REF != 0) {
            this->life_add_count(OOX_TASK_EXECUTE_LIFETIME_REF);
        }
    }
#if OOX_EXCEPTIONS_ENABLED
    using failure_base = incoming_failure_state<CanThrow>;
    void try_set_exception(std::exception_ptr eptr) noexcept override {
        if constexpr (!CanThrow) {
            return;
        }
        result_base::try_set_exception(eptr);
        if (result_base::has_value()) {
            auto& result = result_base::value();
            __OOX_ASSERT_EX(result.current_task, "forwarding result var has no current task");
            result.current_task->set_exception(std::move(eptr));
        }
    }
    void publish_incoming_failure(task_node* source, int source_port) noexcept override {
        if constexpr (CanThrow) {
            if (source && source_port == 0) {
                if (auto eptr = source->get_exception()) {
                    try_set_exception(std::move(eptr));
                }
            }
        }
        failure_base::publish_incoming_failure(source, source_port);
        task_node::mark_failure_signal();
    }
    bool apply_incoming_failure() noexcept override {
        return failure_base::apply_incoming_failure(*this);
    }
    bool has_exception() const noexcept override {
        if constexpr (!CanThrow) {
            return false;
        }
        if (result_base::has_exception()) return true;
        if (failure_base::has_incoming_exception_source()) return true;
        if (!result_base::has_value()) return false;
        const auto& result = result_base::value();
        return result.current_task && result.current_task->has_exception();
    }
    std::exception_ptr get_exception() const noexcept override {
        if constexpr (!CanThrow) {
            return std::exception_ptr{};
        }
        if (result_base::has_exception()) return result_base::get_exception();
        auto* origin = failure_base::incoming_exception_source();
        if (origin) return origin->get_exception();
        if (!result_base::has_value()) return std::exception_ptr{};
        const auto& result = result_base::value();
        if (!result.current_task) return std::exception_ptr{};
        return result.current_task->get_exception();
    }
    task_node* exception_origin() const noexcept override {
        if constexpr (!CanThrow) {
            return nullptr;
        }
        if (result_base::has_exception()) return const_cast<functional_task*>(static_cast<const functional_task*>(this));
        auto* origin = failure_base::incoming_exception_source();
        if (origin) return origin;
        if (!result_base::has_value()) return nullptr;
        const auto& result = result_base::value();
        if (!result.current_task) return nullptr;
        return result.current_task->exception_origin();
    }
#endif
    TASK_EXECUTE_METHOD {
        OOX_TASK_EXECUTE_LIFETIME_GUARD;
    //explicit copy paste below, but we cannot afford creating lambda. The compiler might not optimize it.
#if OOX_EXCEPTIONS_ENABLED
        if constexpr (CanThrow) {
            if (failure_base::has_incoming_failure()) [[unlikely]] {
                task_node::notify_successors<slots>();
                return nullptr;
            }
            try {
                if ( !result_base::has_value() ) {
                    __OOX_TRACE("%p do_run: start forward",this);
                    result_base::emplace(this->functor_base::value()());
                    this->add_suspended_prerequisite();
                    arc* j = new arc(this, 0, arc::flow_only);
                    auto& result = result_base::value();
                    __OOX_ASSERT_EX(!result.current_port_and_flags.is_deferred, "task functors must not return deferred oox::var");
                    __OOX_ASSERT_EX(result.current_task, "forwarding functor returned empty var");
                    if constexpr (OOX_TASK_EXECUTE_LIFETIME_REF != 0) {
                        this->life_add_count(OOX_TASK_EXECUTE_LIFETIME_REF);
                    }
                    if(result.current_task->add_arc(j)) {
                        __OOX_TRACE("%p do_run: add_arc", this);
                        return nullptr;
                    }
                    if constexpr (OOX_TASK_EXECUTE_LIFETIME_REF != 0) {
                        this->release(OOX_TASK_EXECUTE_LIFETIME_REF);
                    }
                    delete j;
                    if (result.current_task->has_failure_signal()) [[unlikely]] {
                        this->publish_incoming_failure(result.current_task, 0);
                        if (this->apply_incoming_failure()) {
                            task_node::notify_successors<slots>();
                            return nullptr;
                        }
                    }
                }
            } catch(...) {
                this->set_exception(std::current_exception());
                task_node::notify_successors<slots>();
                return nullptr;
            }
        } else {
            if ( !result_base::has_value() ) {
                __OOX_TRACE("%p do_run: start forward",this);
                result_base::emplace(this->functor_base::value()());
                this->add_suspended_prerequisite();
                arc* j = new arc(this, 0, arc::flow_only); // TODO: embed into the task
                auto& result = result_base::value();
                __OOX_ASSERT_EX(!result.current_port_and_flags.is_deferred, "task functors must not return deferred oox::var");
                __OOX_ASSERT_EX(result.current_task, "forwarding functor returned empty var");
                if constexpr (OOX_TASK_EXECUTE_LIFETIME_REF != 0) {
                    this->life_add_count(OOX_TASK_EXECUTE_LIFETIME_REF);
                }
                if(result.current_task->add_arc(j)) {
                    __OOX_TRACE("%p do_run: add_arc", this);
                    return nullptr;
                }
                if constexpr (OOX_TASK_EXECUTE_LIFETIME_REF != 0) {
                    this->release(OOX_TASK_EXECUTE_LIFETIME_REF);
                }
                delete j;
            }
        }
        __OOX_TRACE("%p do_run: notify forward",this);
        task_node::notify_successors_success<slots>();
#else
        if( !result_base::has_value() ) {
            __OOX_TRACE("%p do_run: start forward",this);
            result_base::emplace(this->functor_base::value()());
            this->start_count.store(1, std::memory_order_release);
            arc* j = new arc(this, 0, arc::flow_only);  // TODO: embed into the task
            auto& result = result_base::value();
            __OOX_ASSERT_EX(!result.current_port_and_flags.is_deferred, "task functors must not return deferred oox::var");
            __OOX_ASSERT_EX(result.current_task, "forwarding functor returned empty var");
            if constexpr (OOX_TASK_EXECUTE_LIFETIME_REF != 0) {
                this->life_add_count(OOX_TASK_EXECUTE_LIFETIME_REF);
            }
            if(result.current_task->add_arc(j)) {
                __OOX_TRACE("%p do_run: add_arc", this); // recycle_as_continuation was here
                return nullptr;
            }
            if constexpr (OOX_TASK_EXECUTE_LIFETIME_REF != 0) {
                this->release(OOX_TASK_EXECUTE_LIFETIME_REF);
            }
            delete j;
        }
        __OOX_TRACE("%p do_run: notify forward",this);
        task_node::notify_successors<slots>();
#endif
        return nullptr;
    }

    ~functional_task() override {
        result_base::reset();
    }
};

template< typename T, bool CanThrow >
struct gen_oox {
    using type = var<T, CanThrow>;
    template< int slots, typename F >
    static type bind_to(internal::functional_task<slots, F, T, CanThrow> * t) {
        type oox; oox.bind_to(t, static_cast<internal::result_state<T, CanThrow>*>(t), slots + 1, false, false, CanThrow); return oox;
    }
};
template<bool CanThrow>
struct gen_oox<void, CanThrow> {
    using type = var<void, CanThrow>;
    template< int slots, typename F >
    static type bind_to(internal::functional_task<slots, F, void, CanThrow> * t) {
        type oox; oox.bind_to( t, t, slots ); return oox;
    }
};
template< typename VT, bool CanThrow >
struct gen_oox<var<VT, CanThrow>, CanThrow> {
    using type = var<VT, CanThrow>;
    template< int slots, typename F >
    static type bind_to(internal::functional_task<slots, F, var<VT, CanThrow>, CanThrow > * t) {
        type oox; oox.bind_to( t, static_cast<internal::result_state<type, CanThrow>*>(t)->ptr(), slots+1, true ); return oox;
    }
};
template< typename T, bool CanThrow>
using var_type = typename gen_oox<T, CanThrow>::type;

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

template< bool CanThrow = default_exception_policy, typename F, typename... Args > // ->...decltype(f(internal::unoox(args)...))
auto run(F&& f, Args&&... args)->internal::var_type<internal::result_type_of<F>, CanThrow>
{
    static_assert(OOX_EXCEPTIONS_ENABLED || !CanThrow, "oox::run<true>(...) requires OOX_EXCEPTIONS_ENABLED=1");
    using r_type = internal::result_type_of<F>;
    using call_args_type = internal::args_list_of<F>;
    using args_type = internal::base_args<call_args_type, CanThrow, Args&&...>;
    using functor_type = internal::oox_bind<F, args_type>;
    using task_type = internal::functional_task<args_type::write_nodes_count, functor_type, r_type, CanThrow>;

    task_type *t = internal::task::allocate<task_type>( functor_type(std::forward<F>(f), args_type(std::forward<Args>(args)...)) );
    __OOX_TRACE("%p oox::run: write ports %d",t,args_type::write_nodes_count);
    int protect_count = std::numeric_limits<int>::max();
    t->start_count.store(protect_count, std::memory_order_release);
    // process functor types
    protect_count -= static_cast<internal::storage_task<args_type::write_nodes_count, functor_type>*>(t)
                         ->value()
                         .my_args.setup(1, t, std::forward<Args>(args)...);
    auto r = internal::gen_oox<r_type, CanThrow>::bind_to( t );
    t->remove_prerequisite( protect_count ); // publish it
    return r;
}

#if OOX_EXCEPTIONS_ENABLED
namespace internal {
template <bool ThrowOnCancellation>
wait_status failure_wait_status(task_node* task, int port) {
    if (!task || !task->has_exception()) {
        return wait_status::ready;
    }

    auto ep = task->exception_for_port(port);
    if constexpr (ThrowOnCancellation) {
        if (ep) {
            std::rethrow_exception(ep);
        }
        throw cancelled_by_exception{};
    } else {
        if (!ep) {
            return port > 0 ? wait_status::dependency_cancelled : wait_status::dependency_cancelled;
        }
        try {
            std::rethrow_exception(ep);
        } catch (const cancelled_by_user&) {
            return wait_status::user_cancelled;
        } catch (const cancelled_by_exception&) {
            return wait_status::dependency_cancelled;
        } catch (...) {
            throw;
        }
    }

    return wait_status::ready;
}
} // namespace internal
#endif

template <bool ThrowOnCancellation = true>
wait_status wait_for_all_status(internal::oox_var_base& on) {
    on.wait();
#if OOX_EXCEPTIONS_ENABLED
    return internal::failure_wait_status<ThrowOnCancellation>(on.current_task, on.current_port());
#else
    return wait_status::ready;
#endif
}

void wait_for_all(internal::oox_var_base& on ) {
    wait_for_all_status<true>(on);
}

template <bool ThrowOnCancellation = true, typename T, bool CanThrow>
wait_status wait_for_all_status(const var<T, CanThrow>& on) {
    const_cast<var<T, CanThrow>&>(on).wait();
    if constexpr (CanThrow) {
#if OOX_EXCEPTIONS_ENABLED
        return internal::failure_wait_status<ThrowOnCancellation>(on.current_task, on.current_port());
#endif
    }
    return wait_status::ready;
}

template<typename T, bool CanThrow>
void wait_for_all(const var<T, CanThrow>& on) {
    wait_for_all_status<true>(on);
}

template<typename T, bool CanThrow>
[[nodiscard]] T wait_and_get(const var<T, CanThrow> &ov) {
    auto &v = const_cast<var<T, CanThrow>&>(ov);
    wait_for_all(v);

    // Follow forwarding chain until we reach a non-forward var
    internal::oox_var_base* base = &v;
    while (base->current_port_and_flags.is_forwarded) {
#if OOX_EXCEPTIONS_ENABLED
        if constexpr (CanThrow) {
            internal::failure_wait_status<true>(base->current_task, base->current_port());
        }
#endif
        __OOX_ASSERT_EX(base->storage_ptr, "forwarded var has null storage_ptr in wait_and_get");
        base = reinterpret_cast<internal::oox_var_base*>(base->storage_ptr);
    }

#if OOX_EXCEPTIONS_ENABLED
    if constexpr (CanThrow) {
        internal::failure_wait_status<true>(base->current_task, base->current_port());
    }
#endif
    __OOX_ASSERT_EX(base->storage_ptr, "var has null storage_ptr in wait_and_get");
    if constexpr (CanThrow) {
#if OOX_EXCEPTIONS_ENABLED
        if (base->current_port_and_flags.storage_state_can_throw) {
            return static_cast<internal::result_state<T, true>*>(base->storage_ptr)->value();
        }
#endif
    }
    return static_cast<internal::result_state<T, false>*>(base->storage_ptr)->value();
}

template<typename T, bool CanThrow>
[[nodiscard]] T wait_and_get(var<T, CanThrow> &ov) { return wait_and_get(static_cast<const var<T, CanThrow>&>(ov)); }
template<typename T, bool CanThrow>
[[nodiscard]] T wait_and_get(var<T, CanThrow> &&ov) { return wait_and_get(static_cast<const var<T, CanThrow>&>(ov)); }

#undef TASK_EXECUTE_METHOD

} // namespace oox
#endif // __OOX_H__
