#pragma once
// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Copyright (C) 2016 Dmitry Vyukov <dvyukov@google.com>
//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

#include "mpmc_queue.h"
#include "../tracing.h"
#ifndef EIGEN_CXX11_THREADPOOL_NONBLOCKING_THREAD_POOL_H
#define EIGEN_CXX11_THREADPOOL_NONBLOCKING_THREAD_POOL_H
#define EIGEN_POOL_RUNNEXT

#include "max_size_vector.h"
#include "run_queue.h"
#include "stl_thread_env.h"
#include "../util.h"

#include <atomic>
#include <cassert>
#include <functional>
#include <iostream>
#include <memory>
#include <ostream>
#include <thread>

namespace Eigen {

struct Task {
  virtual void operator()() = 0;
  virtual ~Task() = default;
};

template <typename F> struct UniqueTask : Task {

  UniqueTask(F &&f) : f(std::move(f)) {}

  void operator()() override {
    f();
    delete this; // really safe to do heere
  }

  std::decay_t<F> f;
};

template <typename F> Task *MakeTask(F &&f) {
  return new UniqueTask<decltype(std::forward<F>(f))>{std::forward<F>(f)};
}

/**/
template <typename F>
struct ProxyTask : Task {
  ProxyTask(F&& func)
    : Func_{std::move(func)}
  {}

  void operator()() override {
    // We should enter this function 2 times
    // prevState:
    // 0 => first entrance, run Func_
    // 1 in same thread  => this thread finished Func_ before other thread entered
    // 1 in other thread => other thread entered function before Func_ is finished
    // 2 this is the last change of state, drop object
    auto prevState = State_.fetch_add(1, std::memory_order_seq_cst);
    if (prevState == 0) {
      Func_();
      prevState = State_.fetch_add(1, std::memory_order_seq_cst);
    }
    if (prevState == 2) {
      delete this;
    }
  }

  std::decay_t<F> Func_;
  std::atomic_uint_fast32_t State_ = 0;
};

template <typename F> Task *MakeProxyTask(F &&f) {
  return new ProxyTask{std::forward<F>(f)};
}
/**/

// This defines an interface that ThreadPoolDevice can take to use
// custom thread pools underneath.
class ThreadPoolInterface {
public:
  // Submits a closure to be run by a thread in the pool.
  virtual void Schedule(Task *task) = 0;

  // Submits a closure to be run by threads in the range [start, end) in the
  // pool.
  virtual void ScheduleWithHint(Task *task, int /*start*/, int /*end*/) {
    // Just defer to Schedule in case sub-classes aren't interested in
    // overriding this functionality.
    Schedule(task);
  }

  // If implemented, stop processing the closures that have been enqueued.
  // Currently running closures may still be processed.
  // If not implemented, does nothing.
  virtual void Cancel() {}

  // Returns the number of threads in the pool.
  virtual size_t NumThreads() const = 0;

  // Returns a logical thread index between 0 and NumThreads() - 1 if called
  // from one of the threads in the pool. Returns -1 otherwise.
  virtual size_t CurrentThreadId() const = 0;

  virtual ~ThreadPoolInterface() {}
};

template <typename Environment>
class ThreadPoolTempl : public Eigen::ThreadPoolInterface {
public:
  using TaskPtr = Task *;
  using Queue = RunQueue<TaskPtr, 1024>;

  ThreadPoolTempl(int num_threads, Environment env = Environment())
      : ThreadPoolTempl(num_threads, true, false, env) {}

  ThreadPoolTempl(int num_threads, bool allow_spinning, bool use_main_thread,
                  Environment env = Environment())
      : env_(env), num_threads_(num_threads), allow_spinning_(allow_spinning),
        thread_data_(num_threads), all_coprimes_(num_threads),
        global_steal_partition_(EncodePartition(0, num_threads_)), blocked_(0),
        spinning_(0), done_(false), cancelled_(false) {
    // Calculate coprimes of all numbers [1, num_threads].
    // Coprimes are used for random walks over all threads in Steal
    // and NonEmptyQueueIndex. Iteration is based on the fact that if we take
    // a random starting thread index t and calculate num_threads - 1 subsequent
    // indices as (t + coprime) % num_threads, we will cover all threads without
    // repetitions (effectively getting a presudo-random permutation of thread
    // indices).
    assert(num_threads_ < kMaxThreads);
    for (int i = 1; i <= num_threads_; ++i) {
      all_coprimes_.emplace_back(i);
      ComputeCoprimes(i, &all_coprimes_.back());
    }
    thread_data_.resize(num_threads_);
    for (int i = 0; i < num_threads_; i++) {
      SetStealPartition(i, EncodePartition(0, num_threads_));
      if (i == 0) {
        PerThread *pt = GetPerThread();
        pt->pool = this;
        pt->rand = GlobalThreadIdHash();
        pt->thread_id = i;
      } else {
        thread_data_[i].thread.reset(env_.CreateThread([this, i]() {
          PerThread *pt = GetPerThread();
          pt->pool = this;
          pt->rand = GlobalThreadIdHash();
          pt->thread_id = i;
          WorkerLoop();
        }));
      }
    }
  }

  ~ThreadPoolTempl() {
    done_ = true;

    // Now if all threads block without work, they will start exiting.
    // But note that threads can continue to work arbitrary long,
    // block, submit new work, unblock and otherwise live full life.
    if (cancelled_) {
      // Since we were cancelled, there might be entries in the queues.
      // Empty them to prevent their destructor from asserting.
      for (size_t i = 0; i < thread_data_.size(); i++) {
        thread_data_[i].Flush();
      }
    }
    // Join threads explicitly (by destroying) to avoid destruction order within
    // this class.
    for (size_t i = 0; i < thread_data_.size(); ++i)
      thread_data_[i].thread.reset();
  }

  void SetStealPartitions(
      const std::vector<std::pair<unsigned, unsigned>> &partitions) {
    assert(partitions.size() == static_cast<std::size_t>(num_threads_));

    // Pass this information to each thread queue.
    for (int i = 0; i < num_threads_; i++) {
      const auto &pair = partitions[i];
      unsigned start = pair.first, end = pair.second;
      AssertBounds(start, end);
      unsigned val = EncodePartition(start, end);
      SetStealPartition(i, val);
    }
  }

  void Schedule(TaskPtr p) override {
    // schedule on main thread only when explicitly requested
    ScheduleWithHint(p, 0, num_threads_);
  }

  void RunOnThread(TaskPtr t, size_t threadIndex) {
    threadIndex = threadIndex % num_threads_;
    PerThread *pt = GetPerThread();
    if (!thread_data_[threadIndex].PushTask(
            t, (pt && threadIndex == pt->thread_id))) {
      // failed to push, execute directly
      ExecuteTask(t);
    }
  }

  void ScheduleWithHint(TaskPtr t, int start, int limit) override {
    PerThread *pt = GetPerThread();
    bool pushed = false;
    if (pt->pool == this) {
      // Worker thread of this pool, push onto the thread's queue.
      if (thread_data_[pt->thread_id].PushTask(t, true)) {
        return;
      }
    } else {
      // A free-standing thread (or worker of another pool), push onto a random
      // queue.
      assert(start < limit);
      assert(limit <= num_threads_);
      int num_queues = limit - start;
      int rnd = Rand(&pt->rand) % num_queues;
      assert(start + rnd < limit);
      const bool localThread = (start + rnd) == pt->thread_id;
      if (thread_data_[start + rnd].PushTask(t, localThread)) {
        return;
      }
    }
    // Note: below we touch this after making w available to worker threads.
    // Strictly speaking, this can lead to a racy-use-after-free. Consider that
    // Schedule is called from a thread that is neither main thread nor a worker
    // thread of this pool. Then, execution of w directly or indirectly
    // completes overall computations, which in turn leads to destruction of
    // this. We expect that such scenario is prevented by program, that is,
    // this is kept alive while any threads can potentially be in Schedule.
    ExecuteTask(t); // Push failed, execute directly.
  }

  void Cancel() override {
    cancelled_ = true;
    done_ = true;

    // Let each thread know it's been cancelled.
#ifdef EIGEN_THREAD_ENV_SUPPORTS_CANCELLATION
    for (size_t i = 0; i < thread_data_.size(); i++) {
      thread_data_[i].thread->OnCancel();
    }
#endif
  }

  size_t NumThreads() const final { return num_threads_; }

  size_t CurrentThreadId() const final {
    const PerThread *pt = const_cast<ThreadPoolTempl *>(this)->GetPerThread();
    if (pt->pool == this) {
      return pt->thread_id;
    } else {
      return -1;
    }
  }

  // returns true if processed some tasks
  bool JoinMainThread() {
    if (CurrentThreadId() == -1) {
      return false;
    }
    return WorkerLoop(/* external */ true);
  }

  bool TryExecuteSomething() {
    if (CurrentThreadId() == -1) [[unlikely]] {
      return false;
    }
    constexpr bool External = true;
    constexpr bool JustOnce = true;
    return WorkerLoop(External, JustOnce);
  }

private:
  // Create a single atomic<int> that encodes start and limit information for
  // each thread.
  // We expect num_threads_ < 65536, so we can store them in a single
  // std::atomic<unsigned>.
  // Exposed publicly as static functions so that external callers can reuse
  // this encode/decode logic for maintaining their own thread-safe copies of
  // scheduling and steal domain(s).
  static const int kMaxPartitionBits = 16;
  static const int kMaxThreads = 1 << kMaxPartitionBits;

  inline unsigned EncodePartition(unsigned start, unsigned limit) {
    return (start << kMaxPartitionBits) | limit;
  }

  void ExecuteTask(TaskPtr p) { (*p)(); }

  inline void DecodePartition(unsigned val, unsigned *start, unsigned *limit) {
    *limit = val & (kMaxThreads - 1);
    val >>= kMaxPartitionBits;
    *start = val;
  }

  void AssertBounds(int start, int end) {
    assert(start >= 0);
    assert(start < end); // non-zero sized partition
    assert(end <= num_threads_);
  }

  inline void SetStealPartition(size_t i, unsigned val) {
    thread_data_[i].steal_partition.store(val, std::memory_order_relaxed);
  }

  inline unsigned GetStealPartition(int i) {
    return thread_data_[i].steal_partition.load(std::memory_order_relaxed);
  }

  void ComputeCoprimes(int N, MaxSizeVector<unsigned> *coprimes) {
    for (int i = 1; i <= N; i++) {
      unsigned a = i;
      unsigned b = N;
      // If GCD(a, b) == 1, then a and b are coprimes.
      while (b != 0) {
        unsigned tmp = a;
        a = b;
        b = tmp % b;
      }
      if (a == 1) {
        coprimes->push_back(i);
      }
    }
  }

  typedef typename Environment::EnvThread Thread;

  struct PerThread {
    constexpr PerThread() : pool(NULL), rand(0), thread_id(-1) {}
    ThreadPoolTempl *pool; // Parent pool, or null for normal threads.
    uint64_t rand;         // Random generator state.
    int thread_id;         // Worker thread index in pool.
  };

  struct ThreadData {
    constexpr ThreadData() : thread(), steal_partition(0), local_tasks(), mailbox(1024) {}
    std::unique_ptr<Thread> thread;
    std::atomic<unsigned> steal_partition;
    Queue local_tasks;
    rigtorp::mpmc::Queue<TaskPtr> mailbox;
    std::size_t stack_size = size_t{16} * 1024 * 1024;
#ifdef EIGEN_POOL_RUNNEXT
    std::atomic<TaskPtr> runnext{nullptr};
    // use IDLE to indicate that the thread is idling and tasks shouldn't be
    // pushed
    static inline const TaskPtr IDLE = reinterpret_cast<TaskPtr>(1);
#endif

    bool PushTask(TaskPtr p, bool localThread) {
      if (localThread) {
// #ifdef EIGEN_POOL_RUNNEXT
//         if (runnext.load(std::memory_order_relaxed) == nullptr) {
//           TaskPtr expected = nullptr;
//           if (runnext.compare_exchange_strong(expected, p,
//                                               std::memory_order_release)) {
//             return true;
//           }
//         }
// #endif
        return local_tasks.PushFront(p);
      } else {
        return mailbox.try_push(p);
      }
    }

    bool SetIdle() {
      auto current = runnext.load(std::memory_order_relaxed);
      if (current == nullptr) {
        return runnext.compare_exchange_strong(current, IDLE,
                                               std::memory_order_relaxed);
      }
      return current == IDLE;
    }

    void ResetIdle() {
      auto current = runnext.load(std::memory_order_relaxed);
      if (current == IDLE) {
        runnext.compare_exchange_strong(current, nullptr,
                                        std::memory_order_relaxed);
      }
    }

    TaskPtr PopFront() {
#ifdef EIGEN_POOL_RUNNEXT
      if (auto p = PopRunnext(); p && p != IDLE) {
        return p;
      }
#endif
      if (auto p = local_tasks.PopFront()) {
        return p;
      }
      TaskPtr task = nullptr;
      mailbox.try_pop(task);
      return task;
    }

    TaskPtr PopBack(bool force) {
      TaskPtr task = nullptr;
#if defined(EIGEN_SHARING) or defined(EIGEN_SHARING_STEALING)
      mailbox.try_pop(task);
#endif
      if (!task && force) {
        task = local_tasks.PopBack();
      }
      return task;
    }

    void Flush() {
      while (!mailbox.empty()) {
        TaskPtr task = nullptr;
        mailbox.pop(task);
      }
      while (!local_tasks.Empty()) {
        local_tasks.PopFront();
      }
    }

#ifdef EIGEN_POOL_RUNNEXT
    TaskPtr PopRunnext() {
      if (auto p = runnext.load(std::memory_order_relaxed); p) {
        auto success = runnext.compare_exchange_strong(
            p, nullptr, std::memory_order_acquire);
        if (success) {
          return p;
        }
      }
      return nullptr;
    }

    TaskPtr StealWithRunnext() {
      TaskPtr t = PopBack();
      if (!t) {
        t = PopRunnext();
      }
      return t;
    }
#endif
  };

  Environment env_;
  const int num_threads_;
  const bool allow_spinning_;
  MaxSizeVector<ThreadData> thread_data_;
  MaxSizeVector<MaxSizeVector<unsigned>> all_coprimes_;
  unsigned global_steal_partition_;
  std::atomic<unsigned> blocked_;
  std::atomic<bool> spinning_;
  std::atomic<bool> done_;
  std::atomic<bool> cancelled_;

  // Main worker thread loop. Returns true if processed some tasks
  bool WorkerLoop(bool external = false, bool once = false) {
    PerThread *pt = GetPerThread();
    auto thread_id = pt->thread_id;
    auto &threadData = thread_data_[thread_id];

    auto can_steal = is_stack_half_full();

    threadData.ResetIdle();
    bool processed_anything = false;
    bool all_empty = false;
    while (!cancelled_) {
      TaskPtr t = threadData.PopFront();
      if (!t && (!external || can_steal)) {
        t = LocalSteal(all_empty);
        if (t) {
          Tracing::TaskStolen();
        }
      }
      if (!t && (!external || can_steal)) {
        t = GlobalSteal(all_empty);
        if (t) {
          Tracing::TaskStolen();
        }
      }
      if (!t && external && threadData.SetIdle()) {
        // external thread shouldn't wait for work, it should just exit.
        return processed_anything;
      }
      if (t) {
        ExecuteTask(t);
        processed_anything = true;
        all_empty = false;
      } else if (done_) {
        return processed_anything;
      } else {
        all_empty = true;
      }
      if (once) {
        break;
      }
    }

    return processed_anything;
  }

  // Steal tries to steal work from other worker threads in the range [start,
  // limit) in best-effort manner.
  TaskPtr Steal(unsigned start, unsigned limit, bool force) {
    PerThread *pt = GetPerThread();
    const size_t size = limit - start;
    unsigned r = Rand(&pt->rand);
    // Reduce r into [0, size) range, this utilizes trick from
    // https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
    assert(all_coprimes_[size - 1].size() < (1 << 30));
    unsigned victim = ((uint64_t)r * (uint64_t)size) >> 32;
    unsigned index =
        ((uint64_t)all_coprimes_[size - 1].size() * (uint64_t)r) >> 32;
    unsigned inc = all_coprimes_[size - 1][index];

    for (unsigned i = 0; i < size; i++) {
      assert(start + victim < limit);
      TaskPtr t = thread_data_[start + victim].PopBack(force);
      if (t) {
        return t;
      }
      victim += inc;
      if (victim >= size) {
        victim -= size;
      }
    }
    return nullptr;
  }

  // Steals work within threads belonging to the partition.
  TaskPtr LocalSteal(bool force) {
    PerThread *pt = GetPerThread();
    unsigned partition = GetStealPartition(pt->thread_id);
    // If thread steal partition is the same as global partition, there is no
    // need to go through the steal loop twice.
    if (global_steal_partition_ == partition)
      return nullptr;
    unsigned start, limit;
    DecodePartition(partition, &start, &limit);
    AssertBounds(start, limit);

    return Steal(start, limit, force);
  }

  // Steals work from any other thread in the pool.
  TaskPtr GlobalSteal(bool force) { return Steal(0, num_threads_, force); }

  int NonEmptyQueueIndex() {
    PerThread *pt = GetPerThread();
    // We intentionally design NonEmptyQueueIndex to steal work from
    // anywhere in the queue so threads don't block in WaitForWork() forever
    // when all threads in their partition go to sleep. Steal is still local.
    const size_t size = thread_data_.size();
    unsigned r = Rand(&pt->rand);
    unsigned inc = all_coprimes_[size - 1][r % all_coprimes_[size - 1].size()];
    unsigned victim = r % size;
    for (unsigned i = 0; i < size; i++) {
      if (!thread_data_[victim].queue.Empty()) {
        return victim;
      }
      victim += inc;
      if (victim >= size) {
        victim -= size;
      }
    }
    return -1;
  }

  static __attribute__((always_inline)) inline uint64_t GlobalThreadIdHash() {
    return std::hash<std::thread::id>()(std::this_thread::get_id());
  }

  __attribute__((always_inline)) inline PerThread *GetPerThread() {
    static thread_local PerThread per_thread_;
    PerThread *pt = &per_thread_;
    return pt;
  }

  static __attribute__((always_inline)) inline unsigned Rand(uint64_t *state) {
    uint64_t current = *state;
    // Update the internal state
    *state = current * 6364136223846793005ULL + 0xda3e39cb94b95bdbULL;
    // Generate the random output (using the PCG-XSH-RS scheme)
    return static_cast<unsigned>((current ^ (current >> 22)) >>
                                 (22 + (current >> 61)));
  }
};

typedef ThreadPoolTempl<StlThreadEnvironment> ThreadPool;

} // namespace Eigen

#endif // EIGEN_CXX11_THREADPOOL_NONBLOCKING_THREAD_POOL_H
