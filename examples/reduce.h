#include <oox/oox.h>

namespace Reduce::Serial {

template <typename Iter, typename Value, typename Op>
Value reduce(Iter start, Iter end, Value init, Op op) {
    Value res = init;
    for (auto i = start; i != end; ++i) {
        res = op(res, i);
    }
    return res;
}

} // namespace Reduce::Serial

namespace Reduce::OOX_recursive {

template <typename Iter, typename Value, typename Op>
oox::var<Value> reduce(Iter start, Iter end, Value init, Op op) {
    if (start == end) {
        return init; 
    }

    return oox::run(op, Reduce::OOX_recursive::reduce(start + 1, end, init, op), start);
}

}

