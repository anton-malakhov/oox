// SPDX-License-Identifier: Apache-2.0

#ifndef OOX_BACKENDS_TASKFLOW_BACKEND_H
#define OOX_BACKENDS_TASKFLOW_BACKEND_H

#include <future>
#include <mutex>
#include <taskflow/taskflow.hpp>

namespace oox {
namespace internal {

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

    void release(int n = 1) {
        if (life_release(n))
            delete this;
    }

    template<typename T, typename... Args>
    static T* allocate(Args&&... args) {
        return new T(std::forward<Args>(args)...);
    }

    void spawn() {
#if defined(OOX_TEST_INJECT_TASK_SPAWN_FAILURE) && OOX_TEST_INJECT_TASK_SPAWN_FAILURE
        maybe_inject_task_spawn_failure();
#endif
        get_tf_pool().silent_async([this] {
            this->execute();
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

} // namespace internal
} // namespace oox

#endif // OOX_BACKENDS_TASKFLOW_BACKEND_H
