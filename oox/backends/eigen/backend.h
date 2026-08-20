// SPDX-License-Identifier: Apache-2.0

#ifndef OOX_BACKENDS_EIGEN_BACKEND_H
#define OOX_BACKENDS_EIGEN_BACKEND_H

#include <atomic>
#include <thread>
#include <utility>
#include "../../eigen/nonblocking_thread_pool.h"

#ifndef OOX_EIGEN_NUM_THREADS
#define OOX_EIGEN_NUM_THREADS 0
#endif

namespace oox {
namespace internal {

#define OOX_USING_EIGEN
#define TASK_EXECUTE_METHOD void* execute() override

using eigen_thread_pool = detail::eigen_pool::ThreadPool;

inline int resolved_eigen_thread_count() {
    constexpr long long build_threads = OOX_EIGEN_NUM_THREADS;
    static_assert(build_threads >= 0 && build_threads < (1 << 16),
                  "OOX_EIGEN_NUM_THREADS must be in [0, 65535]");
    if constexpr (build_threads > 0) {
        return static_cast<int>(build_threads);
    }
    const unsigned detected = std::thread::hardware_concurrency();
    return detected == 0 ? 1 : static_cast<int>(detected);
}

inline eigen_thread_pool& get_eigen_pool() {
    static eigen_thread_pool pool(resolved_eigen_thread_count());
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
        get_eigen_pool().Schedule(
            detail::eigen_pool::MakeTask([this] { this->execute(); })
        );
    }

    void wait() {
        get_eigen_pool().Wait(
            [this] { return done.load(std::memory_order_acquire); }
        );
    }

    void wakeup() {
        done.store(true, std::memory_order_release);
        get_eigen_pool().NotifyTaskCompletion();
    }
};

} // namespace internal
} // namespace oox

#endif // OOX_BACKENDS_EIGEN_BACKEND_H
