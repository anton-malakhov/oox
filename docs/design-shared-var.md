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
   reference to the stored value. The copy is made under the state mutex so a
   concurrent writer cannot retire the captured slot; v1 therefore rejects a
   nested `shared_var` value and expects `T`'s copy operation not to re-enter
   the same handle indirectly.
3. **Forwarding is supported for an adopted plain `var`**: a `shared_var`
   constructed from a forwarded `var` records a descriptor for the future
   `var` object. Both registration and `get()`/`wait()` resolve that descriptor
   only after the outer producer completes, then walk the complete forwarding
   chain. A task-produced/forwarded `shared_var` is not part of this contract.
4. Blocking uses one waiter node per call, including the adopted-forwarded-var
   path. No backend task object is used as a shared multi-waiter primitive.
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

- Handle-state inspection and mutation run under `mtx`. User-controlled
  default construction (`T{}`), blocking waits, task publication, and task
  execution run without any shared-state mutex held.
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
2. A TLS batch is identified by the task being registered, rather than merely
   by “some batch is active”. A nested `oox::run` therefore gets an independent
   batch even if user code re-enters during another argument's setup. After
   argument collection, the outer guard detaches its batch before materializing
   values and restores any enclosing batch when a nested run returns.
3. Every unique empty state is lazily materialized before the multi-state lock
   set is acquired. `A{}` constructs a candidate outside `mtx`; a short locked
   compare/install chooses one candidate if several threads race. Therefore a
   slow or re-entrant constructor never runs under one or more state locks.
   More than one candidate (and thus more than one constructor side effect) may
   occur in that first-materialization race, but exactly one value is installed.
4. All involved states are locked **in canonical (address-sorted) order** and
   registrations are grouped by state and applied as one atomic unit. If
   several arguments alias one state, one writer registration (or one reader
   when no writer is present) represents that task in the graph; every alias
   receives the same storage descriptor. A skipped mutable alias also closes
   its compile-time output port with `next_writer_no_owner_marker()`, balancing
   the port's lifetime hold instead of leaking the task.
   - writer category (`A&` or `A&&` functor arg): `inner.set_next_writer(port, self)`;
   - reader/copy category: `self->assign_prerequisite(inner.current_task,
     inner.current_port())` and capture the current storage descriptor.
5. Unlock all states before `oox::run` publishes the task. Task execution and
   any synchronously triggered continuation therefore cannot re-enter a held
   shared-state mutex.

**Why atomic multi-state registration**: two threads registering writers on the
same two vars in opposite orders (`run(f, sv1, sv2)` vs `run(g, sv2, sv1)`)
can interleave their per-var chains and create a **task cycle** (each writer
waits for the other) that deadlocks. Registering each `run()`'s chains as one
atomic unit — with the second run chaining onto the complete result of the
first — makes the chains consistent and acyclic. The canonical lock order also
prevents AB-BA lock-order deadlocks between two such runs.

`consume()` (inside the worker task) needs **no lock**. A direct descriptor is
already a result-state pointer. For an adopted forwarded `var`, the descriptor
instead points to the future `var` object inside the outer producer's result
storage; the graph guarantees that producer completed before `consume()` runs,
so the object's lifetime has begun and the full forwarding chain can safely be
walked. The slot remains alive through the existing graph ownership rules.

`oox::run` arguments are handled **per argument kind**: a single task may mix
plain `oox::var` and `oox::shared_var` arguments (each kind matched by its own
`base_args` specialization). The plain-var parts keep their single-threaded
semantics; the `shared_var` parts are registered atomically as one batch
(§4.1).

### 4.2 `get()` / `wait()` — any thread, concurrently (A1: waiting is a graph edge)

1. Materialize a default value if `inner` is empty, constructing `T{}` outside
   the state mutex and locking only for the compare/install.
2. `lock(state->mtx)`.
3. If the inner var is **forwarded** (an adopted producer-returned plain
   `var`), attach a per-call waiter node to its current producer, unlock, and
   block through the backend-native wait. Re-lock and revalidate the current
   producer; repeat if a writer switched the slot. Check failure before
   dereferencing the producer's result storage.
4. Resolve the complete forwarding chain (walk `storage_ptr` while
   `is_forwarded`) and
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
7. Re-lock, revalidate, and read the value copy from `storage` (failure check
   first when `CanThrow`); release the getter's hold on the waiter.

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
on foreign primitives. This same helper is used while an adopted forwarded
`var` is waiting for its outer producer, so that wait also releases `mtx`.
The **deferred placeholder** is covered by the same path: the first writer's
deferred redirect forwards the waiter's arc to the writer task, so no
deferred-specific wait exists. No condition variables remain in the
shared_var layer.

### 4.3 Assignment `sv = value` — any thread

Assignment calls `oox::run` with a small mutable writer on `*this`, using the
same shared registration batch as an explicit writer task. The batch locks and
updates the graph, unlocks, and only then publishes the assignment task. This
publishes deferred states, remains ordered after existing readers/writers, and
does not invoke synchronous continuations while `mtx` is held. Concurrent
assignments are writer-chain ordered; the last registered assignment wins.

The `const T&` overload requires copy construction plus copy assignment. The
`T&&` overload requires move construction plus move assignment. With a
non-throwing policy (`CanThrow == false`), the selected assignment operation
must additionally be `noexcept`; a potentially throwing assignment is accepted
only by `CanThrow == true` and becomes graph failure instead of terminating the
process. Deleted negative overloads prevent implicit conversion through
`var(T)` or `shared_var(T)` from bypassing those constraints. Plain `oox::var`
value assignment uses the analogous writer task when the var already has a
slot. A copy-assignment payload has shared ownership so the task functor
remains movable even when `T` itself is copy-only.

The same policy applies before the callable body: `run<false>` checks the value
categories actually returned by `consume()`, not only the callable's declared
parameter list. A deferred writer therefore requires nothrow materialization,
and passing the stored `T&` to a by-value `T` parameter requires a nothrow copy.
With `run<true>`, either failure is caught by the task and propagated normally.

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

    shared_var& operator=(const T&);          // copy construct/assign; noexcept assign if !CanThrow
    shared_var& operator=(T&&);               // move construct/assign; noexcept assign if !CanThrow

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

- The same `shared_var` handle object may be used concurrently for `run`
  registration, `get`, `wait`, `cancel`, copy construction, and assignment of
  a `T` value. Distinct handle copies may be used independently by any thread.
  Copy/move assignment from another `shared_var` rebinds `state_`; rebinding
  the same handle object concurrently with any operation on that object is a
  data race and requires external synchronization, matching the
  `std::shared_ptr` object contract.
- `T` must be default-constructible and move- or copy-constructible, and cannot
  itself be a `shared_var` specialization. A non-throwing policy requires
  default construction and the selected move/copy materialization path to be
  `noexcept`. Value assignment additionally requires the matching
  constructible-and-assignable concept, with nothrow assignment under a
  non-throwing policy.
- Writer chain order for concurrently registered writers is linearized by the
  mutex; every reader observes a value from the chain consistent with that
  order (same model as single-threaded `var`, now with a lock-defined total
  order).
- `get()` returns the value of the slot that was current at the linearization
  point of the call.
- Moving from or rebinding one handle object concurrently with another access
  to that object is a data race on the user side.
- Every blocking `get`/`wait` path, including an adopted forwarded `var`, uses
  a distinct waiter node and releases the shared-state mutex while blocked.

## 7. Out of scope / future work

- **Task-produced/forwarded `shared_var`**: the supported forwarding case is a
  `shared_var<T>` adopting a plain `var<T>` whose producer returns another
  plain `var<T>`. A task-produced `shared_var` is not adopted or forwarded by
  this API, and `shared_var<shared_var<U>>` is rejected.
- **Lock-free handle** (variant 3): pack `{task*, port, flags}` into atomics,
  CAS-based writer chain, seqlock reads. Only if benchmarks show the mutex to
  be a bottleneck at the required concurrency level.
- **`read(callback)` for `const T&` under the lock**: not needed yet; `get()`
  returns a copy.

## 8. Verification

- Unit tests (`tests/test_shared_var.cpp`): API and constraint coverage, copy
  semantics, deferred assignment, mutable aliases and their lifetime holds,
  re-entrant/concurrent lazy materialization, adopted-forwarded registration
  before producer execution, exception/cancellation propagation, forwarded
  wait unlock, and the fast-writer + reader race regression.
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
  - `MutableAliasesShareOneWriterRegistration` — aliased mutable arguments
    share one graph registration and skipped output ports have no owner;
  - `ConcurrentLazyMaterializationRunsOutsideRegistrationLocks` — `T{}` may
    block and recursively call `run` without corrupting TLS or deadlocking;
  - `ReentrantPlainVarSetupUsesAnIndependentRegistrationBatch` — user code
    reached from another argument's setup cannot join the enclosing TLS batch;
  - forwarded-var countertests — registration before the producer constructs
    its result object and writer registration while `get()` waits;
  - exception/cancellation scenarios for both ordinary and forwarded producers;
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
