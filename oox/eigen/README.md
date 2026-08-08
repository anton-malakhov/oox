# Vendored PBBS Eigen mailbox scheduler

This directory contains OOX's maintained port of the mailbox-capable Eigen
scheduler from [`EgorkaZ/pbbsbench`](https://github.com/EgorkaZ/pbbsbench/tree/eigen-mailbox),
pinned for comparison at commit `396a299f03c58dbe9e7604daab38a65781227b75`.
The mailbox mechanism first appeared in upstream commit `e857cdd`.

This is the runtime scheduler source used by the OOX Eigen backend:

- `mpmc_queue.h`: the bounded MPMC queue used as each worker's mailbox.
- `nonblocking_thread_pool.h`: the private-deque/mailbox publication and
  consumption protocol.
- `run_queue.h`: each worker's private work queue.
- `stl_thread_env.h`: the worker-thread environment used by the pool.
- `max_size_vector.h` and `memory.h`: fixed-capacity aligned worker storage.
- `stack_depth.h`: portable stack-depth protection.
- `tracing.h`: disabled-by-default scheduler tracing hooks.

OOX deliberately retains several changes relative to the historical snapshot:

- headers are self-contained and live under `oox/eigen`;
- stack-depth checks are portable and isolated in `stack_depth.h`;
- tracing is disabled by default so the backend does not pay for metrics;
- the fallback cache-line size is 128 bytes for Apple Silicon;
- allocations are portable.

Remote publication always uses a mailbox rather than modifying another
worker's private deque. The OOX backend keeps mailbox stealing enabled.
