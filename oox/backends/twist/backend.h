// SPDX-License-Identifier: Apache-2.0

#ifndef OOX_BACKENDS_TWIST_BACKEND_H
#define OOX_BACKENDS_TWIST_BACKEND_H

#include <mutex>

namespace oox {
namespace internal {

#define OOX_USING_TWIST
#define TASK_EXECUTE_METHOD void* execute() override

struct task : task_life {
    sync::mutex waiter_mutex;
    sync::condition_variable waiter_cv;
    bool completed = false;

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
#if OOX_TWIST_TEST
        auto* tracker = active_twist_task_tracker;
#endif
        sync::thread worker([this] {
            sync::preemption_point();
            this->execute(); // releases OOX_TASK_EXECUTE_LIFETIME_REF via the in-execute guard
        });
#if OOX_TWIST_TEST
        if (tracker) {
            tracker->track(std::move(worker));
        } else {
            worker.detach();
        }
#else
        worker.detach();
#endif
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

} // namespace internal
} // namespace oox

#endif // OOX_BACKENDS_TWIST_BACKEND_H
