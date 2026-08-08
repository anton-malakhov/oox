# Experimental Eigen benchmark layer

These headers build benchmark-only parallel-for policies on top of the runtime
mailbox scheduler in `oox/eigen`. They are not part of the OOX backend or its
public interface.

- `eigen_pool.h` and `parallel_for.h` provide the experimental frontend.
- `timespan_partitioner.h` and `intrusive_ptr.h` implement adaptive splitting.
- `modes.h`, `num_threads.h`, `util.h`, and `thread_index.h` configure runs.
- `poor_barrier.h`, `eigen_pinner.h`, and `tbb_pinner.h` support measurements.
