#include "any_sender_of.h"

template <typename Iter, typename Value, typename Op, typename Join>
auto reduce_2_recursive(Iter start, Iter end, Value identity, Op op, Join join) -> any_sender_of<Value> {
    if (end - start < 16) {
        Value res = identity;
        for (auto i = start; i != end; ++i) {
            res = op(res, i);
        }
        return ex::just(res);
    }
    
    auto chunk_sz = (end - start) / 2;
    auto left = reduce_2_recursive(start, start + chunk_sz, identity, op, join);
    auto right = reduce_2_recursive(start + chunk_sz, end, identity, op, join);

    return ex::when_all(std::move(left), std::move(right)) | ex::then(join);
}

template <int N, typename Iter, typename Value, typename Op, typename Join>
auto reduce_n_once(Iter start, Iter end, Value identity, Op op, Join join) -> any_sender_of<Value> {
    std::array<Value, N> partials;    
    size_t chunk_sz = (end - start) / N;

    return
        ex::just(std::move(partials))
        | ex::bulk(N, [=](size_t idx, auto& partials) {
            partials[idx] = identity;
            for (auto i = start + idx * chunk_sz; i != start + (idx + 1) * chunk_sz; ++i) {
                partials[idx] = op(partials[idx], i);
            }
        })
        | ex::then([=](auto partials) {
            Value res = identity;
            for (auto p : partials) {
                res = join(res, p);
            }
            return res;
        });
}

template <int N, typename Iter, typename Value, typename Op, typename Join>
auto reduce_n_recursive(Iter start, Iter end, Value identity, Op op, Join join) -> any_sender_of<Value> {
    if (end - start < 16) {
        Value res = identity;
        for (auto i = start; i != end; ++i) {
            res = op(res, i);
        }
        return ex::just(res);
    }

    std::array<Value, N> partials;    
    size_t chunk_sz = (end - start) / N;

    return
        ex::just(std::move(partials))
        | ex::bulk(N, [=](size_t idx, auto& partials) {
            partials[idx] = std::get<0>(ex::sync_wait(
                reduce_n_recursive<N>(start + idx * chunk_sz, start + (idx + 1) * chunk_sz, identity, op, join)
            ).value());
        })
        | ex::then([=](auto partials) {
            Value res = identity;
            for (auto p : partials) {
                res = join(res, p);
            }
            return res;
        });
}
