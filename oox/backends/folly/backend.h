// SPDX-License-Identifier: Apache-2.0

#ifndef OOX_BACKENDS_FOLLY_BACKEND_H
#define OOX_BACKENDS_FOLLY_BACKEND_H

#include <memory>
#include <mutex>
#include <thread>
#include <folly/fibers/Baton.h>
#include <folly/fibers/FiberManager.h>
#include <folly/fibers/FiberManagerMap.h>
#include <folly/fibers/FiberManagerInternal.h>
#include <folly/fibers/SimpleLoopController.h>

namespace oox {
namespace internal {

#define OOX_USING_FOLLY
#define TASK_EXECUTE_METHOD void* execute() override

folly::fibers::FiberManager& get_fiber_manager() {
    static folly::fibers::FiberManager* fiber_manager = nullptr;
    static std::once_flag once;
    std::call_once(once, [] {
        auto evb = std::make_unique<folly::EventBase>();
        auto loop_controller = std::make_unique<folly::fibers::EventBaseLoopController>();
        loop_controller->attachEventBase(*evb);
        fiber_manager = new folly::fibers::FiberManager(std::move(loop_controller));

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

    void release(int n = 1) {
        if (life_release(n))
            delete this;
    }

    template<typename T, typename... Args>
    static T* allocate(Args&&... args) {
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

} // namespace internal
} // namespace oox

#endif // OOX_BACKENDS_FOLLY_BACKEND_H
