#pragma once
#include "modes.h"
// #define EIGEN_MODE EIGEN_TIMESPAN_GRAINSIZE

#include "eigen_pool.h"
#include "intrusive_ptr.h"
#include "mpmc_queue.h"
#include "num_threads.h"
#include "thread_index.h"
#include "util.h"
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <string>
#include <utility>

namespace EigenPartitioner {

struct Range {
  size_t From;
  size_t To;

  size_t Size() { return To - From; }
};

struct SplitData {
  static constexpr size_t K_SPLIT = 2;
  Range Threads;
  size_t GrainSize = 1;
  size_t Depth = 0;
};

namespace detail {

class TaskStack {
public:
  TaskStack() {}

  void Add(TaskStack& ts) noexcept {
    ts.prev = prev;
    prev = &ts;
  }

  void Pop() noexcept {
    assert(!IsEmpty());
    prev = prev->prev;
  }

  bool IsEmpty() const noexcept {
    return prev == nullptr;
  }
private:
  TaskStack* prev = nullptr;
};

inline TaskStack& ThreadLocalTaskStack() {
  static thread_local TaskStack stack;
  return stack;
}
}

struct TaskNode : intrusive_ref_counter<TaskNode> {
  using NodePtr = IntrusivePtr<TaskNode>;

  TaskNode(NodePtr parent = NodePtr{nullptr}) : Parent(std::move(parent)) {}

  void SpawnChild(size_t count = 1) {
    ChildWaitingSteal_.fetch_add(count, std::memory_order_relaxed);
  }

  void OnStolen() {
    Parent->ChildWaitingSteal_.fetch_sub(1, std::memory_order_relaxed);
  }

  bool AllStolen() {
    return ChildWaitingSteal_.load(std::memory_order_relaxed) == 0;
  }

  NodePtr Parent;

  std::atomic<size_t> ChildWaitingSteal_{0};
};

enum class Balance { OFF, SIMPLE, DELAYED };

enum class Initial { TRUE, FALSE };

enum class GrainSize { DEFAULT, AUTO };

template <typename Func, Balance balance,
          GrainSize grainSizeMode, Initial initial = Initial::FALSE>
struct Task {
  using Scheduler = EigenPoolWrapper;

  static inline const uint64_t INIT_TIME = [] {
  // should be calculated using timespan_tuner with EIGEN_SIMPLE
  // currently 0.99 percentile for maximums is used: 99% of iterations should
  // fit scheduling in timespan
#if defined(__x86_64__)
    if (Eigen::internal::GetNumThreads() == 48) {
      return 16500;
    }
    // return 13500;
    return 75000000;
#elif defined(__aarch64__)
    return 1800;
#else
#error "Unsupported architecture"
#endif
  }();

  using StolenFlag = std::atomic<bool>;

  Task(Scheduler &sched, TaskNode::NodePtr node, size_t from, size_t to,
       Func func, SplitData split)
      : Sched_(sched), CurrentNode_(std::move(node)), Current_(from), End_(to),
        Func_(std::move(func)), Split_(split) {
  }

  bool IsDivisible() const {
    return (Current_ + Split_.GrainSize < End_) && !is_stack_half_full();
  }

  void DistributeWork() {
    if (Split_.Threads.Size() != 1 && IsDivisible()) {
      // take 1/parts of iterations for current thread
      Range otherData{Current_ + (End_ - Current_ + Split_.Threads.Size() - 1) /
                                     Split_.Threads.Size(),
                      End_};
      if (otherData.From < otherData.To) {
        End_ = otherData.From;
        Range otherThreads{Split_.Threads.From + 1, Split_.Threads.To};
        size_t parts = std::min(std::min(Split_.K_SPLIT, otherThreads.Size()),
                                otherData.Size());
        auto threadStep = otherThreads.Size() / parts;
        auto threadsMod = otherThreads.Size() % parts;
        auto dataStep = otherData.Size() / parts;
        auto dataMod = otherData.Size() % parts;
        for (size_t i = 0; i != parts; ++i) {
          auto threadSplit =
              std::min(otherThreads.To,
                       otherThreads.From + threadStep +
                           static_cast<size_t>((parts - 1 - i) < threadsMod));
          // if threads are divided equally, distribute one more task for first
          // parts of threads otherwise distribute one more task for last parts
          // of threads
          auto dataSplit = std::min(
              otherData.To,
              otherData.From + dataStep +
                  static_cast<size_t>((threadsMod == 0 ? i : (parts - 1 - i)) <
                                      dataMod));
          assert(otherData.From < dataSplit);
          assert(otherThreads.From < threadSplit);

          static_assert(alignof(TaskNode) >= 8);
          auto nodeRaw = aligned_alloc(alignof(TaskNode), sizeof(TaskNode));
          auto newNode = new(nodeRaw) TaskNode{CurrentNode_};
          IntrusivePtr newNodePtr{newNode};

          Sched_.run_on_thread(
              Task<Func, balance, grainSizeMode, Initial::TRUE>{
                  Sched_, std::move(newNodePtr), otherData.From, dataSplit,
                  Func_,
                  SplitData{.Threads = {otherThreads.From, threadSplit},
                            .GrainSize = Split_.GrainSize}},
              otherThreads.From);
          otherThreads.From = threadSplit;
          otherData.From = dataSplit;
        }
        assert(otherData.From == otherData.To);
        assert(otherThreads.From == otherThreads.To ||
               parts < Split_.K_SPLIT &&
                   otherThreads.From + (Split_.K_SPLIT - parts) ==
                       otherThreads.To);
      }
    }
  }

  void operator()() {
    detail::TaskStack ts;
    auto& stack = detail::ThreadLocalTaskStack();
    stack.Add(ts);
    if constexpr (initial == Initial::TRUE) {
      DistributeWork();
    }

    if constexpr (balance == Balance::DELAYED) {
      // at first we are executing job for INIT_TIME
      // and then create balancing task
      auto start = Now();
      while (Current_ < End_) {
        Execute();
        if (Now() - start > INIT_TIME) {
          break;
        }
        if constexpr (grainSizeMode == GrainSize::AUTO) {
          Split_.GrainSize++;
        }
      }
    }

    if constexpr (balance != Balance::OFF) {
      while (Current_ != End_ && IsDivisible()) {
        // make balancing tasks for remaining iterations
        // TODO: check stolen? maybe not each time?
        // if (CurrentNode_->AllStolen()) {
        // TODO: maybe we need to check "depth" - number of being stolen
        // times?
        size_t mid = Current_ + (End_ - Current_) / 2;
        // CurrentNode_->SpawnChild();
        // eigen's scheduler will push task to the current thread queue,
        // then some other thread can steal this
        auto nodeRaw = aligned_alloc(alignof(TaskNode), sizeof(TaskNode));
        auto* newNode = new(nodeRaw) TaskNode{CurrentNode_};
        IntrusivePtr<TaskNode> nodePtr{newNode};
        Sched_.run(Task<Func, Balance::SIMPLE, GrainSize::DEFAULT>{
            Sched_, std::move(nodePtr), mid, End_, Func_,
            SplitData{.GrainSize = Split_.GrainSize, .Depth = Split_.Depth + 1}});
        End_ = mid;
      }
    }

    while (Current_ != End_) {
      Execute();
    }
    CurrentNode_.Reset();
    stack.Pop();
  }

private:
  void Execute() {
    Func_(Current_);
    ++Current_;
  }

  Scheduler &Sched_;
  size_t Current_;
  size_t End_;
  Func Func_;
  SplitData Split_;
  // ThreadId SupposedThread_;

  IntrusivePtr<TaskNode> CurrentNode_;
};

template <Balance balance, GrainSize grainSizeMode, typename F>
auto MakeInitialTask(EigenPoolWrapper &sched, TaskNode::NodePtr node, size_t from,
                     size_t to, F func, size_t threadCount,
                     size_t grainSize = 1) {
  return Task<F, balance, grainSizeMode, Initial::TRUE>{
      sched,
      std::move(node),
      from,
      to,
      std::move(func),
      SplitData{.Threads = {0, threadCount}, .GrainSize = grainSize},
      GetThreadIndex()};
}

template <Balance balance, GrainSize grainSizeMode, typename F>
void ParallelFor(size_t from, size_t to, F func) {
  EigenPoolWrapper sched;
  // allocating only for top-level nodes
  TaskNode rootNode;
  IntrusivePtrAddRef(&rootNode); // avoid deletion
  // if (detail::ThreadLocalTaskStack().IsEmpty()) {
    Task<F, balance, grainSizeMode, Initial::TRUE> task{
        sched,
        IntrusivePtr<TaskNode>(&rootNode),
        from,
        to,
        std::move(func),
        SplitData{.Threads = {0, static_cast<size_t>(Eigen::internal::GetNumThreads())},
                  .GrainSize = 1}};
    task();
  /*} else {
    Task<F, balance, grainSizeMode, Initial::FALSE> task{
        sched,
        IntrusivePtr<TaskNode>(&rootNode),
        from,
        to,
        std::move(func),
        SplitData{.Threads = {0, static_cast<size_t>(Eigen::internal::GetNumThreads())},
                  .GrainSize = 1}};
    task();
  }
  */

  while (IntrusivePtrLoadRef(&rootNode) != 1) {
    sched.execute_something_else();
  }
}

namespace detail {

template <typename F>
auto WrapAsTask(F&& func, const IntrusivePtr<TaskNode>& node) {
  return [&func, ref = node]() {
    TaskStack ts;
    auto& threadTaskStack = ThreadLocalTaskStack();
    threadTaskStack.Add(ts);

    std::forward<F>(func)();
    threadTaskStack.Pop();
  };
}
}

template <typename F1, typename F2>
void ParallelDo(F1&& fst, F2&& sec) {
  EigenPoolWrapper sched;
  // allocating only for top-level nodes
  TaskNode rootNode;
  IntrusivePtrAddRef(&rootNode); // avoid deletion

  sched.run(detail::WrapAsTask(std::forward<F1>(fst), IntrusivePtr{&rootNode}));
  std::forward<F2>(sec)();

  while (IntrusivePtrLoadRef(&rootNode) != 1) {
    sched.execute_something_else();
  }
}

template <typename Sched, GrainSize grainSizeMode, typename F>
void ParallelForTimespan(size_t from, size_t to, F func) {
  ParallelFor<Balance::DELAYED, grainSizeMode, F>(from, to,
                                                         std::move(func));
}

template <typename Sched, typename F>
void ParallelForSimple(size_t from, size_t to, F func) {
  ParallelFor<Balance::SIMPLE, GrainSize::DEFAULT, F>(from, to,
                                                             std::move(func));
}

template <typename Sched, typename F>
void ParallelForStatic(size_t from, size_t to, F func) {
  ParallelFor<Balance::OFF, GrainSize::DEFAULT, F>(from, to,
                                                          std::move(func));
}

} // namespace EigenPartitioner
