// SPDX-License-Identifier: Apache-2.0

#ifndef OOX_BACKENDS_STD_BACKEND_H
#define OOX_BACKENDS_STD_BACKEND_H

#include <future>

namespace oox {
namespace internal {

#define OOX_USING_STD
#define TASK_EXECUTE_METHOD void* execute() override

struct task : task_life {
    std::promise<void> waiter;

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
        std::async(std::launch::async, &task::execute, this);
    }

    void wait() {
        waiter.get_future().wait();
    }

    void wakeup() {
        waiter.set_value();
    }
};

} // namespace internal
} // namespace oox

#endif // OOX_BACKENDS_STD_BACKEND_H
