#pragma once
#include "modes.h"
// #define EIGEN_MODE EIGEN_TIMESPAN_GRAINSIZE

#include "eigen_pool.h"
#include "intrusive_ptr.h"
#include "modes.h"
#include "num_threads.h"
#include "thread_index.h"
#include "util.h"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdlib>
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

enum class Initial { TRUE, FALSE };

enum class Sharing { ENABLED, DISABLED };

enum class Balancing { STATIC, TIMESPAN };

template <Sharing SharingPolicy, Balancing BalancingPolicy, typename F>
struct Task {
  using Scheduler = EigenPoolWrapper;
  using Func = std::decay_t<F>;

  static inline const uint64_t INIT_TIME = [] {
  // should be calculated using timespan_tuner with EIGEN_SIMPLE
  // currently 0.99 percentile for maximums is used: 99% of iterations should
  // fit scheduling in timespan
#if defined(__x86_64__)
    if (GetNumThreads() == 48) {
      return 16500;
    }
    // return 13500;
    return 205000;
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
          IntrusivePtr newNodePtr{new TaskNode{CurrentNode_}};

          Sched_.run_on_thread(
              Task<SharingPolicy, BalancingPolicy, Func>{
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
    if constexpr (SharingPolicy == Sharing::ENABLED) {
      DistributeWork();
    }

    if constexpr (BalancingPolicy == Balancing::TIMESPAN) {
      // at first we are executing job for INIT_TIME
      // and then create balancing task
      Split_.GrainSize = 1;
      auto start = Now();
      while (Current_ < End_) {
        Execute();
        if (Now() - start > INIT_TIME) {
          break;
        }
        Split_.GrainSize++;
      }
    }

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
      IntrusivePtr<TaskNode> nodePtr{new TaskNode{CurrentNode_}};
      Sched_.run(Task<Sharing::DISABLED, Balancing::STATIC, Func>{ // already shared and counted grainsize
          Sched_, std::move(nodePtr), mid, End_, Func_,
          SplitData{.GrainSize = Split_.GrainSize, .Depth = Split_.Depth + 1}});
      End_ = mid;
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

namespace detail {

template <int Mode>
struct ParForTraits;

template <>
struct ParForTraits<EIGEN_STEALING> {
  static constexpr Balancing BalancingPolicy = Balancing::STATIC;
  static constexpr Sharing SharingPolicy = Sharing::DISABLED;
};

template <>
struct ParForTraits<EIGEN_SHARING> {
  static constexpr Balancing BalancingPolicy = Balancing::STATIC;
  static constexpr Sharing SharingPolicy = Sharing::ENABLED;
};

template <>
struct ParForTraits<EIGEN_STEALING_GRAINSIZE> {
  static constexpr Balancing BalancingPolicy = Balancing::TIMESPAN;
  static constexpr Sharing SharingPolicy = Sharing::DISABLED;
};

template <>
struct ParForTraits<EIGEN_SHARING_STEALING> {
  static constexpr Balancing BalancingPolicy = Balancing::TIMESPAN;
  static constexpr Sharing SharingPolicy = Sharing::ENABLED;
};


} // namespace detail

template <int Mode, typename F>
void ParallelFor(size_t from, size_t to, F&& func, size_t grainsize) {
  using Traits = detail::ParForTraits<Mode>;
  EigenPoolWrapper sched;
  // allocating only for top-level nodes
  TaskNode rootNode;
  IntrusivePtrAddRef(&rootNode); // avoid deletion
  SplitData splitData{
    .Threads = {0, static_cast<size_t>(GetNumThreads())},
    .GrainSize = grainsize,
  };
  if (detail::ThreadLocalTaskStack().IsEmpty()) {
    Task<Traits::SharingPolicy, Traits::BalancingPolicy, F> task{
        sched,
        IntrusivePtr<TaskNode>(&rootNode),
        from, to,
        std::forward<F>(func),
        splitData};
    task();
  } else {
    Task<Sharing::DISABLED, Traits::BalancingPolicy, F> task{
        sched,
        IntrusivePtr<TaskNode>(&rootNode),
        from, to,
        std::forward<F>(func),
        splitData};
    task();
  }

  while (IntrusivePtrLoadRef(&rootNode) != 1) {
    sched.execute_something_else();
  }
}

template <typename Func>
void ParallelFor(size_t from, size_t to, Func&& func, size_t grainsize = 1) {
  grainsize = std::max(grainsize, size_t{1});
  return ParallelFor<EIGEN_MODE>(from, to, std::forward<Func>(func), grainsize);
}

} // namespace EigenPartitioner
