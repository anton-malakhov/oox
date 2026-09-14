#pragma once
#include "parallel_for.h"
#include "util.h"
#include <sstream>
#include <vector>
#include <unordered_map>

namespace Tracing {
struct TaskTrace {
  TaskTrace() = default;

  TaskTrace(Timestamp iterationStart, Timestamp prevTrace)
      : IterationStart{iterationStart}, PreviousTrace(prevTrace),
        ExecutionStart(Now() - iterationStart) {}

  void OnExecuted() { ExecutionEnd = Now() - IterationStart; }

  Timestamp IterationStart{};
  Timestamp PreviousTrace{};
  Timestamp ExecutionStart{};
  Timestamp ExecutionEnd{};
  Timestamp TraceWrite{};
};

struct TaskInfo {
  TaskInfo() = default;

  TaskInfo(size_t taskIdx, Timestamp iterationStart, Timestamp prevTrace)
      : Trace(iterationStart, prevTrace) {
    TaskIdx = taskIdx;
    ThreadIdx = GetThreadIndex();
    SchedCpu = sched_getcpu();
  }

  bool operator<(const TaskInfo &other) { return TaskIdx < other.TaskIdx; }

  size_t TaskIdx;
  int ThreadIdx;
  int SchedCpu;
  TaskTrace Trace;
};

struct IterationResult {
  IterationResult(size_t count) : Tasks{count}, End{} { Start = Now(); }

  void StartTask(size_t taskIdx) {
    Tasks[taskIdx] = TaskInfo(taskIdx, Start, WroteTrace);
  }

  void EndTask(size_t taskIdx) {
    Tasks[taskIdx].Trace.OnExecuted();
    WroteTrace = Now() - Start;
  }

  void EndIteration() { End = Now() - Start; }

  static inline thread_local Timestamp WroteTrace = 0; // previous trace

  Timestamp Start;
  std::vector<Tracing::TaskInfo> Tasks;
  Timestamp End;
};

class Tracer {
public:
  Tracer() = default;

  void StartIteration(size_t tasksCount) {
    Iterations.emplace_back(tasksCount);
  }

  void EndIteration() { Iterations.back().EndIteration(); }

  void StartTask(size_t taskIdx) { Iterations.back().StartTask(taskIdx); }

  void EndTask(size_t taskIdx) { Iterations.back().EndTask(taskIdx); }

  std::vector<IterationResult> GetIterations() { return Iterations; }

  template <typename F> void RunIteration(size_t tasks, F &&f) {
    StartIteration(tasks);
    ParallelFor(0, tasks, [&](size_t i) {
      StartTask(i);
      f(i);
      // TODO: fence?
      EndTask(i);
    });
    // TODO: fence?
    EndIteration();
  }

  std::string ToJson(size_t threadNum) {
    std::stringstream stream;
    auto &results = Iterations;
    stream << "{\n"
           << "\"thread_num\": " << threadNum << ",\n"
           << "\"tasks_num\": " << results.front().Tasks.size() << ",\n"
           << "\"results\": [\n";
    for (size_t iter = 0; iter != results.size(); ++iter) {
      auto &&res = results[iter].Tasks;
      std::unordered_map<ThreadId, std::vector<Tracing::TaskInfo>>
          resultPerThread;
      for (size_t i = 0; i < res.size(); ++i) {
        auto task = res[i];
        resultPerThread[task.ThreadIdx].emplace_back(task);
      }
      stream << "  {\n"
             << "    \"start\": " << results[iter].Start << ",\n"
             << "    \"end\": " << results[iter].End << ",\n"
             << "    \"tasks\": {\n";
      size_t total = 0;
      for (auto &&[id, tasks] : resultPerThread) {
        stream << "        \"" << id << "\": [";
        std::sort(tasks.begin(), tasks.end());
        for (size_t i = 0; i != tasks.size(); ++i) {
          auto task = tasks[i];
          stream << "{\"index\": " << task.TaskIdx << ", \"trace\": {\""
                 << "prev_trace\": " << task.Trace.PreviousTrace
                 << ", \"execution_start\": " << task.Trace.ExecutionStart
                 << ", \"execution_end\": " << task.Trace.ExecutionEnd
                 << "}, \"cpu\": " << task.SchedCpu << "}"
                 << (i == tasks.size() - 1 ? "" : ", ");
        }
        stream << (++total == resultPerThread.size() ? "]" : "],") << "\n";
      }
      stream << "    }"
             << "\n"
             << (iter + 1 == results.size() ? "  }" : "  },") << "\n";
    }
    stream << "]\n"
           << "}\n";
    return stream.str();
  }

private:
  std::vector<IterationResult> Iterations;
};

} // namespace Tracing
