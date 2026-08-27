// SPDX-License-Identifier: Apache-2.0

#ifndef OOX_BACKENDS_OPENMP_BACKEND_H
#define OOX_BACKENDS_OPENMP_BACKEND_H

#include <omp.h>
#include <setjmp.h>

namespace oox {
namespace internal {

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

    void release(int n = 1) {
        if (life_release(n))
            delete this;
    }

    template<typename T, typename... Args>
    static T* allocate(Args&&... args) {
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

    void wakeup() {}
};

} // namespace internal
} // namespace oox

#endif // OOX_BACKENDS_OPENMP_BACKEND_H
