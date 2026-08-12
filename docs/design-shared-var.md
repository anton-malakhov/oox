# OOX design: `oox::shared_var<T>` — thread-safe shared handle

Status: implemented (variant 1, "thick handle")
Scope: v1 — see "Decisions" for explicitly excluded features.

## 1. Problem

`oox::var<T>` is a single-owner, move-only handle to a node in the OOX
dependency graph. One thread may use it: register consumers/writers via
`oox::run`, call `get()`/`wait()`, and eventually release it. The underlying
graph (`task_node`, `arc_list`, `output_node`, `life_count`, `start_count`) is
already thread-safe — it uses atomics and is verified by Twist tests. The
data races live entirely in the **handle** (`oox_var_base` fields are plain,
non-atomic members) and in the non-atomic `result_state::state_bits_field`.

We want `oox::shared_var<T>`: a copyable, reference-counted handle that
"behaves like `oox::var`" but can be safely used by multiple threads at once.

## 2. Decisions (v1)

1. `shared_var` is a **run argument only**. There is no `shared_run`:
   `oox::run(f, sv)` accepts `shared_var` among its arguments and still
   returns a plain `oox::var<R>` result. Use a `shared_var` when the *input*
   must be shareable across threads; the result of a task is owned by its
   caller as usual.
2. `get()` returns a **copy** (`T` must be copyable). It never returns a
   reference to the stored value.
3. **Forwarding is supported for adopted vars**: a `shared_var` constructed
   from a forwarded `var` (a producer that returned a var) resolves the chain
   on `get()`/`wait()` — the producer is waited for first (its result storage
   is materialized during execution), then the chain is walked to the final
   slot. Registering a forwarded shared_var as a *run argument* stays one
   level deep (same limitation as `oox::var`'s own consume path).
4. The **Folly backend limitation** is documented, not fixed: `folly::fibers::Baton`
   is a single-waiter primitive, so `get()`/`wait()` on the same node from
   multiple fibers is not supported on the Folly backend.
5. Implementation strategy: **variant 1, "thick handle"** — one mutex per
   shared state, zero changes to the graph machinery.

## 3. Architecture (variant 1)

```
shared_var<T, CanThrow>
├── std::shared_ptr<shared_state<T, CanThrow>>   // copyable, refcounted handle
└── shared_state
    ├── shared_var_mutex mtx   // serializes every mutation of the handle state
    └── var<T, CanThrow> inner // the "eternal owner" handle: keeps slots alive
```

- All handle-state mutations (writer registration, reader registration, lazy
  materialization, `get`/`wait` snapshot, `cancel`, value assignment) run
  under `mtx`.
- The graph operations themselves (`arc_list::add_arc`,
  `output_node::next_writer` exchange, `countdown`, `head` exchange,
  `start_count` decrement) are unchanged and stay lock-free; they are merely
  *invoked* from inside the critical section.
- `inner` behaves exactly like a single-threaded `oox::var` that lives until
  the last `shared_var` handle is destroyed.

### Why `inner` (a real `var`) instead of raw handle fields

`var` already implements the tricky parts correctly:

- writer chaining via `set_next_writer` (with deferred redirect);
- reader registration via `assign_prerequisite`;
- lazy default materialization;
- "owner end" semantics on destruction (`release()` installs the terminal
  `next_writer` marker), which keeps the current value slot alive while the
  handle lives.

Reusing it means the shared layer adds only a mutex and a refcount, and the
existing Twist-verified lifetime machinery does the rest.

## 4. Synchronization protocol per operation

### 4.1 Registration: `oox::run(f, shared_var<A> ...)` — any thread

`internal::shared_var_args::setup()` (specializations of `base_args` for
`shared_var<A,VC>&`, `const shared_var<A,VC>&`, `shared_var<A,VC>&&`) defers
the actual registration to a thread-local setup context:

1. Each shared_var argument records a pending registration (its state, the
   task, the port, and whether it is a writer) instead of locking immediately.
2. The **outermost** shared_var argument commits the whole batch: all involved
   states are locked **in canonical (address-sorted) order**, then every
   registration is applied as one atomic unit:
   - lazy materialization: if `inner.current_task == nullptr`, assign a
     default `var<A>(A{})` (same requirement as `var`: `A` default-constructible);
   - writer category (`A&` or `A&&` functor arg): `inner.set_next_writer(port, self)`;
   - reader/copy category: `self->assign_prerequisite(inner.current_task,
     inner.current_port())` and capture `inner.storage_ptr` into `my_ptr`.
3. `unlock`. Task execution stays fully parallel.

**Why atomic multi-state registration**: two threads registering writers on the
same two vars in opposite orders (`run(f, sv1, sv2)` vs `run(g, sv2, sv1)`)
can interleave their per-var chains and create a **task cycle** (each writer
waits for the other) that deadlocks. Registering each `run()`'s chains as one
atomic unit — with the second run chaining onto the complete result of the
first — makes the chains consistent and acyclic. The canonical lock order also
prevents AB-BA lock-order deadlocks between two such runs.

`consume()` (inside the worker task) needs **no lock**: it reads the value
from the storage pointer captured at registration time; the graph orders the
write before the read (flow dependency), and the slot stays alive while the
result var of its producer lives (existing lifetime rules).

`oox::run` arguments are handled **per argument kind**: a single task may mix
plain `oox::var` and `oox::shared_var` arguments (each kind matched by its own
`base_args` specialization). The plain-var parts keep their single-threaded
semantics; the `shared_var` parts are registered atomically as one batch
(§4.1).

### 4.2 `get()` / `wait()` — any thread, concurrently (A1: waiting is a graph edge)

1. `lock(state->mtx)`.
2. Materialize a default value first if `inner` is empty.
3. If the inner var is **forwarded** (adopted from a producer that returned a
   var), wait for the producer task first: the chain target is materialized
   inside the producer's result storage during its execution, so the chain is
   not walkable until the producer completes.
4. Resolve the forwarding chain (walk `storage_ptr` while `is_forwarded`) and
   snapshot `task`, `storage`, `port` from the final var.
5. If the slot is already produced (`head` is the done marker) — read under
   the lock.
6. Otherwise, **wait through the graph**: allocate a waiter node
   (`shared_var_waiter`, a `task_node` subclass), register it as a successor
   of the current task via the same lock-free `add_arc` the graph's readers
   use (a flow arc — the graph's own wait queue), `unlock`, and block on the
   waiter's completion through the **backend's native task wait**
   (`task::wait()` — TBB `wait_context`, std promise, TF future, ...). The
   graph notifies the waiter at the task's completion (`do_notify_arcs` →
   `remove_prerequisite` → spawn → `execute` → `wakeup`). If the slot was
   switched while we waited, re-snapshot and retry.
7. Re-lock and read the value copy from `storage` (failure check first when
   `CanThrow`); release the getter's hold on the waiter.

### Why waiting is a graph edge (and not a CV, a spin, or a retain)

The current slot's task can be freed **at its own completion** when the next
writer is chained before it completes (the chain consumes the slot-1 hold at
the completion, so the task's life hits zero there — not at the transition).
That free is not gated by the state mutex, so any wait that touches the task's
own members (its waiter/cv) after the completion is a use-after-free — this
is the bug class the wait-under-lock, the CV-based deferred wait, and the
polling attempts all ran into.

- A **retain** (`task_life::life_add_count`) is incompatible with the graph's
  raw `release(n)` accounting (an owner-marker release is `release(2)` and
  would consume the retained ref) — verified empirically under Twist/TSAN.
- A **CV on the state** serializes registration during the wait and needs a
  notification path that does not exist.
- A **spin/poll** burns CPU on pool threads, against the project philosophy.

The A1 waiter solves it structurally: the getter registers the waiter into the
graph's own lock-free arc queue and **never touches the current task after the
registration**, so the task can be freed safely. The getter blocks on the
waiter's completion through the pool's native wait — pool threads never wait
on foreign primitives. The **deferred placeholder** is covered by the same
path: the first writer's deferred redirect forwards the waiter's arc to the
writer task, so no deferred-specific wait exists. No condition variables
remain in the shared_var layer.

### 4.3 Assignment `sv = value` — any thread

`lock(mtx)`; `inner = var<A>(value)` (this releases the previous current slot
via `inner`'s own `release()` path and binds a fresh constant slot);
`unlock`. Concurrent assignments serialize; the last one wins.

### 4.4 Destruction of the last handle

`shared_ptr` releases `shared_state`; `~shared_state` destroys `inner`, which
releases its "owner end" and frees the current slot (existing `var` rules).
In-flight `get()`/`wait()` hold their own `shared_ptr` copy, so the state
cannot die under them.

## 5. API surface

```cpp
namespace oox {

template <typename T, bool CanThrow = default_exception_policy>
class shared_var {
public:
    shared_var();                              // lazy default value
    explicit shared_var(deferred_t);           // deferred publication
    shared_var(const T& t);                    // ready value (copy)
    shared_var(T&& t);                         // ready value (move)
    shared_var(var<T, CanThrow>&& v);          // adopt ownership of a var

    shared_var(const shared_var&);             // refcount++
    shared_var& operator=(const shared_var&);
    shared_var(shared_var&&) noexcept;
    shared_var& operator=(shared_var&&) noexcept;

    shared_var& operator=(const T&);
    shared_var& operator=(T&&);

    [[nodiscard]] T get() const;             // copy; requires copyable T
    void wait() const;
    void cancel() noexcept;
};

// free helpers (mirror the var overloads)
template <bool ThrowOnCancellation = true, typename T, bool CanThrow>
wait_status wait_for_all_status(const shared_var<T, CanThrow>& on);
template <typename T, bool CanThrow>
void wait_for_all(const shared_var<T, CanThrow>& on);
template <typename T, bool CanThrow>
[[nodiscard]] T wait_and_get(const shared_var<T, CanThrow>& sv);

} // namespace oox
```

Access categories through `oox::run` are the same as for `var`:

- functor arg `const A&` or `A` → shared read / copy (parallel readers);
- functor arg `A&` → exclusive write, serialized across threads;
- rvalue `shared_var<A>&&` argument → final consumption (move semantics,
  single caller, like `std::shared_ptr` move).

## 6. Semantics guarantees (v1)

- A `shared_var` object may be used by any number of threads concurrently:
  `run` registration, `get`, `wait`, `cancel`, assignment, copy.
- Writer chain order for concurrently registered writers is linearized by the
  mutex; every reader observes a value from the chain consistent with that
  order (same model as single-threaded `var`, now with a lock-defined total
  order).
- `get()` returns the value of the slot that was current at the linearization
  point of the call.
- Moving a `shared_var` from two threads simultaneously is a data race on the
  user side (same contract as `std::shared_ptr`).
- Folly backend: the ordinary `get`/`wait` uses a per-call waiter node and is
  multi-waiter safe; the direct wait on a task (e.g. the forwarded-producer
  wait) retains the single-waiter Baton limitation.

## 7. Out of scope / future work

- **Forwarding of `shared_var` *as a producer*'s result**: a `shared_var`
  cannot be *created* by a task returning one (there is no `shared_run`); a
  `shared_var` value can still be an *input* to any task, including ones that
  return `var` (and the adopted result may itself be a forwarded `var`, which
  is resolved on `get()`/`wait()`, see Decision #3).
- **Registration of a forwarded shared_var as a run argument** stays one
  level deep (same limitation as `oox::var`'s own consume path).
- **Lock-free handle** (variant 3): pack `{task*, port, flags}` into atomics,
  CAS-based writer chain, seqlock reads. Only if benchmarks show the mutex to
  be a bottleneck at the required concurrency level.
- **`read(callback)` for `const T&` under the lock**: not needed yet; `get()`
  returns a copy.
- **Folly multi-waiter on direct task waits**: the ordinary `get`/`wait` is
  multi-waiter safe (per-call waiter node); only direct waits on a task (the
  forwarded-producer wait) hit the single-waiter Baton.

## 8. Verification

- Unit tests (`tests/test_shared_var.cpp`): API coverage, copy semantics,
  deferred, multi-writer, exceptions, cross-thread smoke, the fast-writer +
  reader race regression (`GetWhileFastWriters`).
- Twist scenarios (`tests/twist/test_twist_shared_var.cpp`):
  - `ConcurrentReaderAndWriterRegistration` — concurrent `run` registration
    on one `shared_var` (reader + writer), deterministic result;
  - `ConcurrentGet` — many threads `get()` the same `shared_var`;
  - `WriterSwitchWhileGetPending` — validates that a concurrent writer
    switch cannot dangle the slot a `get()` is waiting on;
  - `DeferredSharedPublication` — `shared_var(deferred)` with concurrent
    reader/writer publication;
  - `HandleCopyLifecycle` — copies used/destroyed in other threads;
  - `ConcurrentOppositeOrderMultiVarWritersComplete` — opposite-order
    multi-var writer registration (the anti-cycle guarantee);
  - `GetWhileFastWriters` — the waiter-based wait racing a fast writer chain
    (the UAF regression).
- Twist runtimes: `twist-fault` (real threads + fault injection) and
  `twist-sim` (RandomSeeds; DFS requires `TWIST_SIM_ISOLATION=ON`, blocked in
  some environments by an upstream sure/twist gap).
- TSAN build (`twist-fault-tsan` preset) as a release gate.
- Benchmarks: `bench_accounts` (transfers), `bench_shared_var_get_heavy`
  (concurrent get + registration).
- Example: `examples/accounts.cpp` — `std::vector<shared_var<account>>`,
  100 000 random point transfers between accounts.
