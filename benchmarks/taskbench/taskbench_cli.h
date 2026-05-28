#ifndef OOX_BENCH_TASKBENCH_CLI_H
#define OOX_BENCH_TASKBENCH_CLI_H

#include "taskbench_config.h"

#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

namespace oox_bench::taskbench {

enum class Runner {
  OOX,
  Serial,
  TbbFlow,
  Taskflow,
  OpenMP,
  Folly
};

struct Cli {
  Config config;
  Runner runner = Runner::OOX;
  bool help = false;
};

inline bool parse_bool(const std::string& value) {
  if (value == "1" || value == "true" || value == "on" || value == "yes") return true;
  if (value == "0" || value == "false" || value == "off" || value == "no") return false;
  throw std::invalid_argument("expected boolean, got: " + value);
}

inline Runner parse_runner(const std::string& value) {
  if (value == "oox") return Runner::OOX;
  if (value == "serial") return Runner::Serial;
  if (value == "tbb-flow" || value == "tbb_flow" || value == "one-tbb-flow" || value == "flow") return Runner::TbbFlow;
  if (value == "taskflow" || value == "tf") return Runner::Taskflow;
  if (value == "openmp" || value == "omp") return Runner::OpenMP;
  if (value == "folly") return Runner::Folly;
  throw std::invalid_argument("unknown runner: " + value);
}

inline const char* to_string(Runner runner) {
  switch (runner) {
    case Runner::OOX: return "oox";
    case Runner::Serial: return "serial";
    case Runner::TbbFlow: return "tbb-flow";
    case Runner::Taskflow: return "taskflow";
    case Runner::OpenMP: return "openmp";
    case Runner::Folly: return "folly";
  }
  return "unknown";
}

inline std::string arg_value(std::string_view arg) {
  const auto pos = arg.find('=');
  if (pos == std::string_view::npos) {
    throw std::invalid_argument("expected --key=value argument: " + std::string(arg));
  }
  return std::string(arg.substr(pos + 1));
}

inline Cli parse_cli(int argc, char** argv) {
  Cli cli;
  for (int idx = 1; idx < argc; ++idx) {
    const std::string_view arg(argv[idx]);
    if (arg == "--help" || arg == "-h") {
      cli.help = true;
      continue;
    }
    if (arg == "--csv") {
      cli.config.csv = true;
      continue;
    }
    if (arg == "--no-csv") {
      cli.config.csv = false;
      continue;
    }

    const auto value = arg_value(arg);
    if (arg.rfind("--height=", 0) == 0) cli.config.height = std::stoi(value);
    else if (arg.rfind("--width=", 0) == 0) cli.config.width = value == "auto" ? 0 : std::stoi(value);
    else if (arg.rfind("--threads=", 0) == 0) cli.config.threads = value == "auto" ? 0 : std::stoi(value);
    else if (arg.rfind("--graphs=", 0) == 0) cli.config.graphs = std::stoi(value);
    else if (arg.rfind("--pattern=", 0) == 0) cli.config.pattern = parse_pattern(value);
    else if (arg.rfind("--kernel=", 0) == 0) cli.config.kernel = parse_kernel(value);
    else if (arg.rfind("--radix=", 0) == 0) cli.config.radix = std::stoi(value);
    else if (arg.rfind("--iterations=", 0) == 0) cli.config.iterations = std::stol(value);
    else if (arg.rfind("--output-bytes=", 0) == 0) cli.config.output_bytes = static_cast<std::size_t>(std::stoull(value));
    else if (arg.rfind("--scratch-bytes=", 0) == 0) cli.config.scratch_bytes = static_cast<std::size_t>(std::stoull(value));
    else if (arg.rfind("--imbalance=", 0) == 0) cli.config.imbalance = std::stod(value);
    else if (arg.rfind("--seed=", 0) == 0) cli.config.seed = static_cast<std::uint64_t>(std::stoull(value));
    else if (arg.rfind("--validate=", 0) == 0) cli.config.validate = parse_bool(value);
    else if (arg.rfind("--repetitions=", 0) == 0) cli.config.repetitions = std::stoi(value);
    else if (arg.rfind("--warmups=", 0) == 0) cli.config.warmups = std::stoi(value);
    else if (arg.rfind("--runner=", 0) == 0) cli.runner = parse_runner(value);
    else if (arg.rfind("--csv=", 0) == 0) cli.config.csv = parse_bool(value);
    else throw std::invalid_argument("unknown argument: " + std::string(arg));
  }
  validate_config(cli.config);
  return cli;
}

inline void print_help(std::ostream& os, const char* exe) {
  os << "Usage: " << exe << " [--height=N] [--width=N|auto] [--threads=N|auto] [--graphs=N]\n"
     << "  [--pattern=trivial|stencil|sweep|nearest|spread|fft|tree|random]\n"
     << "  [--kernel=empty|compute|memory] [--radix=N] [--iterations=N]\n"
     << "  [--output-bytes=N] [--scratch-bytes=N] [--imbalance=X]\n"
     << "  [--validate=0|1] [--repetitions=N] [--warmups=N]\n"
     << "  [--runner=oox|serial|tbb-flow|taskflow|openmp|folly] [--csv|--no-csv]\n";
}

} // namespace oox_bench::taskbench

#endif // OOX_BENCH_TASKBENCH_CLI_H
