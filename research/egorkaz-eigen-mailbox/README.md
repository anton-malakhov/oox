# EgorkaZ Eigen mailbox scheduler snapshot

This directory preserves the experimental scheduler for mailbox research. It
is reference material, not production OOX code, and is deliberately excluded
from the build and install trees.

## Provenance

* Repository: <https://github.com/EgorkaZ/pbbsbench>
* Branch: `eigen-mailbox`
* Commit: `396a299f03c58dbe9e7604daab38a65781227b75`
* Original path: `parlaylib/include/parlay/internal/scheduler_plugins/eigen/nonblocking_thread_pool.h`
* Snapshot SHA-256: `aef2569d0ce38d949d39dcfa489191b75192cd061117109193503fa3ccd8097d`
* License: MPL-2.0, retained in the source file

`nonblocking_thread_pool.h` is byte-for-byte identical to that revision. Its
relative includes intentionally do not resolve here because the file is an
archival artifact. Consult the pinned repository for the complete dependency
set. The maintained OOX port is
[`../../oox/eigen/nonblocking_thread_pool.h`](../../oox/eigen/nonblocking_thread_pool.h).

## Mailbox topology

Each logical worker owns two queues:

1. A fixed-size Eigen `RunQueue` for owner-local pushes and deque-style steals.
2. A bounded Rigtorp MPMC mailbox with capacity 1024 for remote publication.

Local scheduling pushes onto the front of the local deque. External scheduling
selects a worker and attempts `mailbox.try_push()`. A worker checks its local
deque and then its mailbox. Thieves check the victim mailbox before optionally
stealing from the victim deque. When publication fails, the submitter executes
the task inline.

The file also contains a partially disabled `runnext`/`IDLE` protocol. Its
publication path is commented out, while pop and idle transitions remain.

## Known correctness and progress issues

These are the findings established during the MR #35 audit. They describe this
snapshot; they are not changes to the archived source.

* `use_main_thread` is ignored. Slot 0 is always reserved for the constructing
  thread, so a one-thread pool creates no background worker.
* Progress permanently depends on the constructing thread's TLS registration.
  Work can outlive that thread, and stale registration is not restored.
* `num_threads` is not validated as positive. Zero also invalidates partition,
  random-target, and coprime indexing assumptions.
* `WorkerLoop()` continually scans empty queues. There is no event count,
  condition variable, atomic wait, or other parking mechanism.
* `allow_spinning_`, `blocked_`, and `spinning_` do not implement a working
  spin-then-park policy.
* Task publication has no wake-up protocol. Adding sleep without redesigning
  publication would introduce a check-to-sleep lost-wakeup race.
* Registered waiting is exposed through `TryExecuteSomething()`. The OOX
  integration repeatedly yielded around it, causing non-worker busy-waiting.
* External helping is gated by `is_stack_half_full()`, making normal-depth
  progress depend on a stack-pressure predicate with inverted-looking intent.
* Both a full local deque and a full mailbox fall back to inline execution.
  Nested spawning can therefore recurse without a scheduler-imposed bound.
* All new external threads begin with the same zero PRNG state, initially
  concentrating publication on the same target mailbox.
* `RunOnThread()` decides that a push is owner-local from the numeric thread ID
  without first confirming that the TLS registration belongs to this pool.
* A registered worker ignores a `ScheduleWithHint()` partition that excludes
  its own worker ID.
* The constructor is not exception-safe if thread creation fails: already
  started workers can spin while member destruction attempts to join them.
* When `EIGEN_THREAD_ENV_SUPPORTS_CANCELLATION` is enabled, cancellation calls
  `OnCancel()` through every `thread` pointer, although slot 0 has no
  `EnvThread` object when main-thread reservation is active.
* Cancellation flushes queues before joining workers. Mailbox `empty()` is only
  a snapshot, so concurrent consumers can invalidate the subsequent blocking
  `pop()` assumption.
* `ThreadData::Flush()` discards task pointers without deleting their task
  objects, leaking cancelled queued work.
* Destruction and cancellation have no parked-worker wake-all operation because
  the implementation never parks; this must be added together with parking.

## Dormant and invalid mechanisms

* `NonEmptyQueueIndex()` accesses `thread_data_[victim].queue`, but the member
  is named `local_tasks` and mailbox state is separate.
* `StealWithRunnext()` calls `PopBack()` without its required `bool force`
  argument.
* `runnext` publication is commented out, but `PopRunnext()`, `SetIdle()`,
  `ResetIdle()`, and the `IDLE` sentinel remain active.
* `ProxyTask` and `MakeProxyTask()` are unused by the scheduler.
* The adjacent `RunQueue::PopBackHalf()` uses the unavailable
  `eigen_plain_assert` and has no call sites.
* Stack-size state and several blocked/spinning fields are written or retained
  without participating in a complete policy.

## Integration and portability issues

* The implementation defines incompatible scheduler types in namespace
  `Eigen` and reuses Eigen's include guard and feature macros.
* It depends on adjacent PBBS tracing, utility, fixed-vector, mailbox, and
  environment headers rather than being an isolated OOX component.
* The tracing subsystem is heavy even when tracing is unwanted and its shared
  storage/output design is not safe for concurrent writers.
* Several helpers use GCC-specific `__attribute__((always_inline))` syntax.
* Generic global macros such as `EIGEN_POOL_RUNNEXT` can collide with real
  Eigen or another embedded copy.
* The mailbox's hardware-interference layout choice needs a stable cross-TU
  value when used as a header-defined type.

## Mailbox research questions

* Does one mailbox per worker outperform a single guarded overflow queue under
  many external producers?
* What capacity avoids inline fallback without imposing excessive per-worker
  memory cost?
* Should thieves consume mailbox work before deque work, and how does that
  ordering affect locality and fairness?
* Should a full target mailbox probe other mailboxes before spilling to a
  shared overflow queue?
* Is batch mailbox draining beneficial, and under which producer/consumer
  ratios?
* Can a mailbox publication be integrated with an event-count parking protocol
  without a notification per task?
* Is randomized target selection sufficient, or should publication account for
  queue pressure and sleeping workers?
* Does a corrected run-next slot provide measurable benefit beyond local-deque
  front insertion?

The current OOX regression scenarios are collected in
[`../../tests/eigen_pool_test.cpp`](../../tests/eigen_pool_test.cpp). Any
mailbox experiment should retain the one-worker, nested-wait, concurrent-
producer, overflow, idle CPU, cancellation, and destruction checks.

## Non-findings

`UniqueTask::delete this` is an explicit ownership convention and is not, by
itself, evidence of a defect. Likewise, partition encoding should not be
changed without demonstrating an invariant violation.
