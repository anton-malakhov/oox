#include <oox/oox.h>

namespace Reduce::Serial {

template <typename Iter, typename Value, typename Op>
Value reduce(Iter start, Iter end, Value identity, Op op) {
    Value res = identity;
    for (auto i = start; i != end; ++i) {
        res = op(res, i);
    }
    return res;
}

} // namespace Reduce::Serial

namespace Reduce::OOX_serial {

template <typename Iter, typename Value, typename Op>
oox::var<Value> reduce(Iter start, Iter end, Value identity, Op op) {
    if (start == end) {
        return identity; 
    }

    return oox::run(op, Reduce::OOX_serial::reduce(start + 1, end, identity, op), start);
}

} // namespace Reduce::OOX_serial


namespace Reduce::OOX_parallel {

template <typename Iter, typename Value, typename Op, typename Join>
oox::var<Value> reduce(Iter start, Iter end, Value identity, Op op, Join join) {
    if (end - start < 10) {
        return Reduce::Serial::reduce(start, end, identity, std::move(op));
    } 
    
    auto chunk_sz = (end - start) / 2;

    auto right = oox::run(
        Reduce::OOX_parallel::reduce<Iter, Value, Op, Join>,
        start + chunk_sz, 
        end,
        identity,
        op,
        join
    );

    auto left = Reduce::OOX_parallel::reduce(
        start, 
        start + chunk_sz,
        identity,
        op,
        join
    );

    return oox::run(
        join,
        std::move(left),
        std::move(right)
    );

}

} // namespace Reduce::OOX_parallel

namespace Reduce::OOX_parallel_n {

template <typename T>
oox::var<std::vector<T>> wait_all(const std::vector<oox::var<T>>& vars) {
    oox::var<std::vector<T>> res = std::vector<T>(vars.size());

    for (size_t i = 0; i < vars.size(); ++i) {
        res = oox::run([i](std::vector<T>&& r, const T& cur) {
            r[i] = cur;
            return r;
        },
        std::move(res), vars[i]);
    }

    return res;
}

template <size_t N, typename Iter, typename Value, typename Op, typename Join>
oox::var<Value> reduce(Iter start, Iter end, Value identity, Op op, Join join) {
    if (end - start < 10) {
        return Reduce::Serial::reduce(start, end, identity, std::move(op));
    } 
    
    auto chunk_sz = (end - start) / N;

    std::vector<oox::var<Value>> v;
    for (size_t i = 0; i < N; ++i) {
        v.push_back(reduce<N>(start + i * chunk_sz, start + (i + 1) * chunk_sz, identity, op, join));
    }

    return oox::run(
        [identity=std::move(identity), join = std::move(join)](std::vector<Value> vec) {
            auto res = identity;
            for (const auto& x : vec) {
                res = join(res, x);
            }
            return res;
        },
        wait_all(v)
    );

}

} // namespace Reduce::OOX_parallel_n
