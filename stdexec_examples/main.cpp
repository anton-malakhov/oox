#include <stdexec/execution.hpp>
#include <exec/static_thread_pool.hpp>
#include <iostream>

#include "fibonacci.h"
#include "reduce.h"

void time(auto f) {
    using namespace std::chrono;
    auto start = steady_clock::now();
    f();
    std::cout << "elapsed: " << duration_cast<nanoseconds>(steady_clock::now() - start).count() << "ns\n";
}

int main() {
    auto plus = std::plus<int64_t>{};
    auto pool = exec::static_thread_pool(4);
    auto sched = pool.get_scheduler();

    time([&]() {
        std::cout << "fib\n";
        auto work = ex::starts_on(
            sched,
            fib(8)
        );
        auto [f] = ex::sync_wait(std::move(work)).value();
        std::cout << f << std::endl;
    });

    time([&]() {
        std::cout << "reduce_2_recursive\n";
        auto work = ex::starts_on(
            sched,
            reduce_2_recursive(0, 1<<10, int64_t(0), plus, plus)
        );
        auto [r2] = ex::sync_wait(std::move(work)).value();
        std::cout << r2 << std::endl;
    });

    time([&]() {
        std::cout << "reduce_n_once\n";
        auto work = ex::starts_on(
            sched,
            reduce_n_once<4>(0, 1<<10, int64_t(0), plus, plus)
        );
        auto [rn] = ex::sync_wait(std::move(work)).value();
        std::cout << rn << std::endl;
    });

    time([&]() {
        std::cout << "reduce_n_recursive\n";

        auto work = ex::starts_on(
            sched,
            reduce_n_recursive<4>(0, 1<<10, int64_t(0), plus, plus)
        );
        auto [rn] = ex::sync_wait(std::move(work)).value();
        std::cout << rn << std::endl;
    });
}
