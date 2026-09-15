#ifndef PARLAY_INTERNAL_SCHEDULER_PLUGINS_OPENCL_H_
#define PARLAY_INTERNAL_SCHEDULER_PLUGINS_OPENCL_H_

#include <OpenCL/cl.hpp>

namespace parlay {

inline size_t num_workers() {
    // TODO
}

inline size_t worker_id() {
    // TODO
}

template <typename F>
inline void parallel_for(size_t start, size_t end, F&& f, long granularity, bool) {
    // TODO
}

template <typename Lf, typename Rf>
inline void par_do(Lf&& left, Rf&& right, bool) {
    // TODO
}

inline void init_plugin_internal() {}

template <typename... Fs>
void execute_with_scheduler(Fs...) {
    struct Illegal {};
    static_assert((std::is_same_v<Illegal, Fs> && ...), "parlay::execute_with_scheduler is only available in the Parlay scheduler and is not compatible with OpenCL");
}

}  // namespace parlay

#endif  // PARLAY_INTERNAL_SCHEDULER_PLUGINS_OPENCL_H_
