#pragma once

#include <cstdint>
#include <deque>
#include <filesystem>
#include <fstream>
#include <list>
#include <memory>
#include <random>
#include <sstream>
#include <utility>
#include <vector>

// #include "thread_index.h"

namespace Eigen::Tracing {

struct Trace {
    uintptr_t root = 0;
    uintptr_t task = 0;
    std::string_view event;
    uint32_t orig_thread_id = 0;
    uint32_t trgt_thread_id = 0;
};

inline std::ostream& operator<<(std::ostream& strm, const Trace& trace) {
    strm << '{';
    strm << "\"root\":" << trace.root;
    if (!trace.event.empty()) {
        strm << ",\"event\":\"" << trace.event << '"';
    }
#define PRINT_FIELD(field) \
    if (trace.field != decltype(trace.field){}) { \
        strm << ",\"" #field "\":" << trace.field; \
    }
    PRINT_FIELD(task)
    PRINT_FIELD(orig_thread_id)
    PRINT_FIELD(trgt_thread_id)
#undef PRINT_FIELD
    strm << '}';
    return strm;
}

class TraceStorage;

class Queue {
public:
    Queue(TraceStorage& holder, const std::filesystem::path& trace_dir);

    void push(const Trace& trace) {
        if (traces_.size() < 10'000) {
            traces_.push_back(trace);
        }
        // if (traces_.size() >= 1'000'000) [[unlikely]] {
        //     flush();
        //     traces_.clear();
        // }
    }

    uint32_t thread_id() const noexcept { return thread_id_; }

    const std::deque<Trace>& traces() const noexcept { return traces_; }

    void flush() {
        // std::cout << "flush " << thread_id_ << std::endl;
        for (const auto& trace : traces_) {
            out_file_ << "{\"trace\":" << trace << ",\"thread_id\":" << thread_id_ << "}\n";
        }
        traces_.clear();
    }

    static Queue& this_thread();
private:
    std::deque<Trace> traces_;
    TraceStorage& holder_;
    uint32_t thread_id_ = 0;
    std::ofstream out_file_;
};

struct Metrics {
    uint64_t par_fors = 0;
    uint64_t cur_par_fors = 0;
    uint64_t max_par_fors = 0;
    uint64_t tasks_created = 0;
    uint64_t tasks_stolen = 0;
    uint64_t tasks_shared = 0;
    uint64_t tasks_undivided = 0;

    static Metrics& this_thread();

    void start_par_for() {
        par_fors++;
        cur_par_fors++;
        max_par_fors = std::max(max_par_fors, cur_par_fors);
    }

    void end_par_for() {
        cur_par_fors--;
    }

    Metrics& operator+=(const Metrics& rhs) {
        par_fors += rhs.par_fors;
        cur_par_fors += rhs.cur_par_fors;
        max_par_fors += rhs.max_par_fors;
        tasks_created += rhs.tasks_created;
        tasks_stolen += rhs.tasks_stolen;
        tasks_shared += rhs.tasks_shared;
        tasks_undivided += rhs.tasks_undivided;
        return *this;
    }
};

inline std::ostream& operator<<(std::ostream& strm, const Metrics& metrics) {
    strm << "{\"par_fors\":" << metrics.par_fors;
#define PRINT_FIELD(field) \
    strm << ",\"" #field "\":" << metrics.field;

    PRINT_FIELD(max_par_fors)
    PRINT_FIELD(tasks_created)
    PRINT_FIELD(tasks_stolen)
    PRINT_FIELD(tasks_shared)
    PRINT_FIELD(tasks_undivided)
#undef PRINT_FIELD
    strm << "}";
    return strm;
}

class TraceStorage {
    TraceStorage() {
        std::stringstream filename;
        std::mt19937 rng{std::random_device{}()};
        std::uniform_int_distribution<uint32_t> distr;

        filename << "/tmp/trace-" << distr(rng);
        out_dir_ = filename.str();
        std::filesystem::create_directories(out_dir_);
    }
public:

    ~TraceStorage() {
        flush();
    }

    static TraceStorage& instance() {
        static TraceStorage storage;
        return storage;
    }

    void flush() {
        for (const auto& queue : queues_) {
            queue->flush();
        }

        Metrics total;
        for (const auto& metrics : metrics_) {
            total += *metrics;
        }
        std::ofstream ofile{out_dir_ / "metrics.json"};
        ofile << total << '\n';
    }

    Queue& allocate_queue() {
        queues_.push_back(std::make_unique<Queue>(*this, out_dir_));
        return *queues_.back();
    }

    Metrics& allocate_metrics() {
        metrics_.push_back(std::make_unique<Metrics>());
        return *metrics_.back();
    }
private:
    std::list<std::unique_ptr<Queue>> queues_;
    std::list<std::unique_ptr<Metrics>> metrics_;
    std::filesystem::path out_dir_;
};

inline Queue::Queue(TraceStorage& holder, const std::filesystem::path& trace_dir)
    : holder_{holder}
    // , thread_id_{static_cast<uint32_t>(GetThreadIndex())}
    , thread_id_{0}
    , out_file_(trace_dir / (std::to_string(thread_id_) + ".json"))
{
}

inline Queue& Queue::this_thread() {
    static thread_local Queue* queue = &TraceStorage::instance().allocate_queue();
    return *queue;
}

inline Metrics& Metrics::this_thread() {
    static thread_local Metrics* metrics = &TraceStorage::instance().allocate_metrics();
    return *metrics;
}

inline void ParForStart(const void* root) {
    // auto this_thread = GetThreadIndex();
    Metrics::this_thread().start_par_for();
    // Queue::this_thread().push(Trace{
    //     .root = reinterpret_cast<uintptr_t>(root),
    //     .event = "par_for_start",
    // });
}

inline void ParForEnd(const void* root) {
    // auto this_thread = GetThreadIndex();
    Metrics::this_thread().end_par_for();
    // Queue::this_thread().push(Trace{
    //     .root = reinterpret_cast<uintptr_t>(root),
    //     .event = "par_for_end",
    // });
}

inline void ParDoStart(const void* root) {
    // auto this_thread = GetThreadIndex();
    Metrics::this_thread().start_par_for();
    // Queue::this_thread().push(Trace{
    //     .root = reinterpret_cast<uintptr_t>(root),
    //     .event = "par_do_start",
    // });
}

inline void ParDoEnd(const void* root) {
    // auto this_thread = GetThreadIndex();
    Metrics::this_thread().end_par_for();
    // Queue::this_thread().push(Trace{
    //     .root = reinterpret_cast<uintptr_t>(root),
    //     .event = "par_do_end",
    // });
}

// void TaskCreated(const void* root, const void* task) {
//     auto this_thread = GetThreadIndex();
//     Queue::this_thread().push(Trace{
//         .root = reinterpret_cast<uintptr_t>(root),
//         .task = reinterpret_cast<uintptr_t>(task),
//         .event = "task_created",
//         .curr_thread_id = static_cast<uint32_t>(this_thread),
//     });
// }

inline void TaskScheduled(const void* root, const void* task, uint32_t trgt_thread_id) {
    // auto this_thread = GetThreadIndex();
    // Queue::this_thread().push(Trace{
    //     .root = reinterpret_cast<uintptr_t>(root),
    //     .task = reinterpret_cast<uintptr_t>(task),
    //     .event = "task_scheduled",
    //     .trgt_thread_id = trgt_thread_id,
    // });
}


inline void TaskStarted(const void* root, const void* task) {
    // auto this_thread = GetThreadIndex();
    Metrics::this_thread().tasks_created++;
    // Queue::this_thread().push(Trace{
    //     .root = reinterpret_cast<uintptr_t>(root),
    //     .task = reinterpret_cast<uintptr_t>(task),
    //     .event = "task_started",
    // });
}

inline void TaskEnded(const void* root, const void* task) {
    // auto this_thread = GetThreadIndex();
    // Queue::this_thread().push(Trace{
    //     .root = reinterpret_cast<uintptr_t>(root),
    //     .task = reinterpret_cast<uintptr_t>(task),
    //     .event = "task_ended",
    // });
}

inline void TaskShared() {
    Metrics::this_thread().tasks_shared++;
}

inline void TaskStolen() {
    Metrics::this_thread().tasks_stolen++;
}

inline void TaskUndivided() {
    Metrics::this_thread().tasks_undivided++;
}

} // namespace Eigen::Tracing
