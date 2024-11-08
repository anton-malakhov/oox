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
