#ifndef OOX_BENCH_TASKBENCH_CONFIG_H
#define OOX_BENCH_TASKBENCH_CONFIG_H

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <thread>

#if HAVE_OMP
#include <omp.h>
#endif
#if HAVE_TBB
#ifndef TBB_USE_ASSERT
#define TBB_USE_ASSERT 0
#endif
#include <tbb/info.h>
#endif

namespace oox_bench::taskbench {

enum class Pattern {
  Trivial,
  Stencil,
  Sweep,
  Nearest,
  Spread,
  FFT,
  Tree,
  Random
};

enum class Kernel {
  Empty,
  Compute,
  Memory
};

struct Config {
  int height = 1000;
  int width = 0;
  int threads = 0;
  int graphs = 1;
  Pattern pattern = Pattern::Stencil;
  Kernel kernel = Kernel::Compute;
  int radix = 3;
  long iterations = 1;
  std::size_t output_bytes = 16;
  std::size_t scratch_bytes = 0;
  double imbalance = 0.0;
  std::uint64_t seed = 1;
  bool validate = true;
  bool csv = true;
  int repetitions = 5;
  int warmups = 1;
};

inline const char* to_string(Pattern pattern) {
  switch (pattern) {
    case Pattern::Trivial: return "trivial";
    case Pattern::Stencil: return "stencil";
    case Pattern::Sweep: return "sweep";
    case Pattern::Nearest: return "nearest";
    case Pattern::Spread: return "spread";
    case Pattern::FFT: return "fft";
    case Pattern::Tree: return "tree";
    case Pattern::Random: return "random";
  }
  return "unknown";
}

inline const char* to_string(Kernel kernel) {
  switch (kernel) {
    case Kernel::Empty: return "empty";
    case Kernel::Compute: return "compute";
    case Kernel::Memory: return "memory";
  }
  return "unknown";
}

inline Pattern parse_pattern(const std::string& value) {
  if (value == "trivial") return Pattern::Trivial;
  if (value == "stencil") return Pattern::Stencil;
  if (value == "sweep") return Pattern::Sweep;
  if (value == "nearest") return Pattern::Nearest;
  if (value == "spread") return Pattern::Spread;
  if (value == "fft") return Pattern::FFT;
  if (value == "tree") return Pattern::Tree;
  if (value == "random") return Pattern::Random;
  throw std::invalid_argument("unknown pattern: " + value);
}

inline Kernel parse_kernel(const std::string& value) {
  if (value == "empty") return Kernel::Empty;
  if (value == "compute") return Kernel::Compute;
  if (value == "memory") return Kernel::Memory;
  throw std::invalid_argument("unknown kernel: " + value);
}

inline int default_worker_threads() {
#if HAVE_TBB
  return static_cast<int>(tbb::info::default_concurrency());
#elif HAVE_OMP
  return omp_get_max_threads();
#else
  const auto n = std::thread::hardware_concurrency();
  return n == 0 ? 1 : static_cast<int>(n);
#endif
}

inline int resolved_threads(const Config& cfg) {
  return cfg.threads > 0 ? cfg.threads : default_worker_threads();
}

inline int resolved_width(const Config& cfg) {
  return cfg.width > 0 ? cfg.width : resolved_threads(cfg);
}

inline void validate_config(const Config& cfg) {
  if (cfg.height <= 0) throw std::invalid_argument("height must be positive");
  if (cfg.width < 0) throw std::invalid_argument("width must be non-negative");
  if (cfg.threads < 0) throw std::invalid_argument("threads must be non-negative");
  if (cfg.graphs <= 0) throw std::invalid_argument("graphs must be positive");
  if (cfg.radix < 0) throw std::invalid_argument("radix must be non-negative");
  if (cfg.iterations < 0) throw std::invalid_argument("iterations must be non-negative");
  if (cfg.repetitions <= 0) throw std::invalid_argument("repetitions must be positive");
  if (cfg.warmups < 0) throw std::invalid_argument("warmups must be non-negative");
  if (cfg.imbalance < 0.0) throw std::invalid_argument("imbalance must be non-negative");
  const int width = resolved_width(cfg);
  if (width <= 0) throw std::invalid_argument("resolved width must be positive");
  if (cfg.pattern == Pattern::FFT && (width & (width - 1)) != 0) {
    throw std::invalid_argument("fft pattern requires power-of-two width");
  }
}

} // namespace oox_bench::taskbench

#endif // OOX_BENCH_TASKBENCH_CONFIG_H
