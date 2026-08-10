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
3. **Forwarding is banned for `shared_var`** in v1: a `shared_var` cannot be
   produced by a task returning a `var` (`is_forwarded` path is not
   supported). Ban is enforced by construction: `shared_var` is only ever
   created from values/`deferred`, and its inner handle is never forwarded.
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
    ├── sync::mutex mtx            // serializes every mutation of the handle state
    └── var<T, CanThrow> inner     // the "eternal owner" handle: keeps slots alive
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
`shared_var<A,VC>&`, `const shared_var<A,VC>&`, `shared_var<A,VC>&&`):

1. `lock(state->mtx)`.
2. Lazy materialization: if `inner.current_task == nullptr`, assign a default
   `var<A>(A{})` to `inner` (same requirement as `var`: `A` default-constructible).
3. Writer category (`A&` or `A&&` functor arg): `inner.set_next_writer(port, self)`.
   Multiple threads registering writers serialize on `mtx`, so the writer
   chain order is linearized by lock acquisition order — this is the
   multi-thread writer serialization guarantee.
4. Reader/copy category: `self->assign_prerequisite(inner.current_task,
   inner.current_port())` and capture `inner.storage_ptr` into `my_ptr`.
5. `unlock`. Task execution stays fully parallel.

`consume()` (inside the worker task) needs **no lock**: it reads the value
from the storage pointer captured at registration time; the graph orders the
write before the read (flow dependency), and the slot stays alive while the
result var of its producer lives (existing lifetime rules).

### 4.2 `get()` / `wait()` — any thread, concurrently

1. `lock(state->mtx)`.
2. Snapshot `task = inner.current_task`, `storage = inner.storage_ptr`,
   `port = inner.current_port()` (materializing a default value first if
   `inner` is empty).
3. If the slot is not done yet (`head` is not the done marker), `task->wait()`
   **while still holding the lock**.
4. Read the value copy from `storage` (failure check first when `CanThrow`).
5. `unlock`.

### Why the wait happens under the lock (and not a retain)

The value slot a `get()` refers to can be freed while `get()` is in flight:

- while the slot is the *current* one, `inner`'s countdown hold keeps it alive;
- the moment another thread registers a new writer or assigns a new value,
  `task_node::set_next_writer` / `var::release` runs `remove_back_arc` →
  `notify_next_writer`, which **releases the slot** (`release(1)` or
  `release(2)` for owner markers) and may delete it.

A naive "retain" (bumping `task_life::life_count`) does **not** work: the
graph's `release(n)` paths are raw decrements that do not distinguish the
retained reference from the slot-hold refs, so an owner-marker `release(2)`
would consume the retained `+1` and corrupt the lifetime accounting (observed
in practice as `life_count` going negative under Twist).

The correct mechanism is mutual exclusion: all slot transitions (writer
registration, value assignment, deferred redirect) happen under the same state
mutex, so holding the mutex for the duration of the wait + read guarantees the
slot cannot be freed underneath us. Task execution never takes the state mutex,
so the wait is deadlock-free. Trade-off: a blocking `get()`/`wait()` holds the
state mutex and briefly serializes registration on that `shared_var`.

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

    [[nodiscard]] T get();                     // copy; requires copyable T
    void wait();
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
- Folly backend: multi-waiter `get`/`wait` unsupported (single-waiter Baton).

## 7. Out of scope / future work

- **Forwarding** (`var`-returning producers feeding a `shared_var`): banned in
  v1. A `shared_var` value can still be an *input* to any task, including ones
  that return `var`.
- **Lock-free handle** (variant 3): pack `{task*, port, flags}` into atomics,
  CAS-based writer chain, seqlock reads. Only if benchmarks show the mutex to
  be a bottleneck at the required concurrency level.
- **`read(callback)` for `const T&` under the lock**: not needed yet; `get()`
  returns a copy.
- **Folly multi-waiter**: would require a CV + notification hook in
  `notify_successors` for the shared layer.

## 8. Verification plan

- Unit tests (`tests/test_shared_var.cpp`): API coverage, copy semantics,
  deferred, multi-writer, exceptions, cross-thread smoke.
- Twist scenarios (`tests/twist/test_twist_shared_var.cpp`):
  - `ConcurrentReaderAndWriterRegistration` — concurrent `run` registration
    on one `shared_var` (reader + writer), deterministic result;
  - `ConcurrentGet` — many threads `get()` the same `shared_var`;
  - `WriterSwitchWhileGetPending` — validates that a concurrent writer
    switch cannot dangle the slot a `get()` is waiting on;
  - `DeferredSharedPublication` — `shared_var(deferred)` with concurrent
    reader/writer publication;
  - `HandleCopyLifecycle` — copies used/destroyed in other threads.
- TSAN build (`twist-fault-tsan` preset) as a release gate.
- Example: `examples/accounts.cpp` — `std::vector<shared_var<account>>`,
  100 000 random point transfers between accounts.
