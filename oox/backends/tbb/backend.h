// SPDX-License-Identifier: Apache-2.0

#ifndef OOX_BACKENDS_TBB_BACKEND_H
#define OOX_BACKENDS_TBB_BACKEND_H

#ifndef TBB_USE_ASSERT
#define TBB_USE_ASSERT 0
#endif
#include <oneapi/tbb/detail/_task.h>
#include <oneapi/tbb/task_group.h>

namespace oox {
namespace internal {

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
        if(!is_spawned.load(std::memory_order_acquire))
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

    void release(int n = 1) {
        if (life_release(n)) {
#if OOX_USE_STDMALLOC
            delete this;
#else
            this->~task();
            alloc.deallocate(this);
#endif
        }
    }

    template<typename T, typename... Args>
    static T* allocate(Args&&... args) {
#if OOX_USE_STDMALLOC
        return new T(std::forward<Args>(args)...);
#else
        small_object_allocator a{};
        auto* t = a.new_object<T>(std::forward<Args>(args)...);
        t->alloc = a;
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

} // namespace internal
} // namespace oox

#endif // OOX_BACKENDS_TBB_BACKEND_H
