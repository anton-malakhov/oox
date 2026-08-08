// SPDX-License-Identifier: Apache-2.0

#ifndef OOX_BACKENDS_SERIAL_BACKEND_H
#define OOX_BACKENDS_SERIAL_BACKEND_H

namespace oox {
namespace internal {

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

    void spawn() {
        this->execute();
    }

    void wait() {}
    void wakeup() {}
};

} // namespace internal
} // namespace oox

#endif // OOX_BACKENDS_SERIAL_BACKEND_H
