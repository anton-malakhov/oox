#include "any_sender_of.h"

inline auto fib(int n) -> any_sender_of<int> {
    if (n < 2) {
        return ex::just(1);
    }

    auto work = ex::when_all(fib(n - 1), fib(n - 2)) | ex::then(std::plus<int>{});
    
    return work;
}
