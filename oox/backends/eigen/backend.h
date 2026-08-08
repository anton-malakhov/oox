// SPDX-License-Identifier: Apache-2.0

#ifndef OOX_BACKENDS_EIGEN_BACKEND_H
#define OOX_BACKENDS_EIGEN_BACKEND_H

#include <algorithm>
#include <thread>
#include "../../eigen/nonblocking_thread_pool.h"

namespace oox {
namespace internal {

#define OOX_USING_EIGEN
#define TASK_EXECUTE_METHOD void* execute() override

inline Eigen::ThreadPool& get_eigen_pool() {
    static Eigen::ThreadPool pool(std::max(1u, std::thread::hardware_concurrency()));
    return pool;
}

struct task : task_life {
    std::atomic<bool> done{false};
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
        get_eigen_pool().Schedule(Eigen::MakeTask([this] { this->execute(); }));
    }

    void wait() {
        while (!done.load(std::memory_order_acquire)) {
            if (!get_eigen_pool().TryExecuteSomething())
                std::this_thread::yield();
        }
    }

    void wakeup() {
        done.store(true, std::memory_order_release);
    }
};

} // namespace internal
} // namespace oox

#endif // OOX_BACKENDS_EIGEN_BACKEND_H
