#ifndef OOX_BENCH_TASKBENCH_GRAPH_H
#define OOX_BENCH_TASKBENCH_GRAPH_H

#include "taskbench_config.h"
#include "taskbench_kernels.h"
#include "taskbench_patterns.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <stdexcept>
#include <vector>

#ifndef OOX_TASKBENCH_MAX_DEPS
#define OOX_TASKBENCH_MAX_DEPS 8
#endif

namespace oox_bench::taskbench {

struct Token {
  int graph = 0;
  int row = 0;
  int col = 0;
  std::uint64_t checksum = 0;
  std::shared_ptr<std::vector<std::byte>> payload;
};

inline std::uint64_t checksum_token(int graph,
                                    int row,
                                    int col,
                                    std::span<const std::uint64_t> inputs,
                                    std::uint64_t seed) {
  std::uint64_t x = hash_point(seed, graph, row, col);
  x ^= mix64(static_cast<std::uint64_t>(inputs.size()));
  for (std::uint64_t input : inputs) {
    x = mix64(x ^ input);
  }
  return x;
}

inline std::byte payload_byte(std::uint64_t checksum, std::size_t index) {
  const std::uint64_t x = mix64(checksum + (index & ~std::size_t{7}));
  return std::byte{static_cast<unsigned char>((x >> ((index & 7U) * 8U)) & 0xffU)};
}

inline bool payload_matches_checksum(const Token& token, std::size_t expected_bytes) {
  if (!token.payload || token.payload->size() != expected_bytes) return false;
  for (std::size_t i = 0; i < expected_bytes; ++i) {
    if ((*token.payload)[i] != payload_byte(token.checksum, i)) return false;
  }
  return true;
}

inline std::shared_ptr<std::vector<std::byte>> make_payload(std::size_t bytes, std::uint64_t checksum) {
  auto payload = std::make_shared<std::vector<std::byte>>(bytes);
  for (std::size_t i = 0; i < bytes; ++i) {
    (*payload)[i] = payload_byte(checksum, i);
  }
  return payload;
}

class Graph {
public:
  explicit Graph(Config cfg) : cfg_(cfg), width_(resolved_width(cfg)) {
    validate_config(cfg_);
  }

  bool contains_point(int row, int col) const {
    return row >= 0 && row < cfg_.height && col >= 0 && col < width_;
  }

  std::vector<int> deps(int row, int col) const {
    if (!contains_point(row, col)) return {};
    if (row == 0 || cfg_.pattern == Pattern::Trivial) return {};

    return dependency_columns(cfg_, width_, row, col);
  }

  std::vector<int> reverse_deps(int row, int col) const {
    std::vector<int> out;
    if (!contains_point(row, col) || row + 1 >= cfg_.height) return out;
    for (int next_col = 0; next_col < width_; ++next_col) {
      const auto d = deps(row + 1, next_col);
      if (std::find(d.begin(), d.end(), col) != d.end()) {
        out.push_back(next_col);
      }
    }
    return out;
  }

  Token execute_point(int graph_id, int row, int col, std::span<const Token> inputs) const {
    const auto expected = deps(row, col);
    if (cfg_.validate) {
      validate_inputs(graph_id, row, col, expected, inputs);
    }

    std::array<std::uint64_t, OOX_TASKBENCH_MAX_DEPS> checksums{};
    for (std::size_t idx = 0; idx < inputs.size(); ++idx) {
      checksums[idx] = inputs[idx].checksum;
    }
    const auto input_span = std::span<const std::uint64_t>(checksums.data(), inputs.size());
    std::uint64_t checksum = checksum_token(graph_id, row, col, input_span, cfg_.seed);
    checksum ^= run_kernel(cfg_, graph_id, row, col, checksum);

    Token out;
    out.graph = graph_id;
    out.row = row;
    out.col = col;
    out.checksum = checksum;
    out.payload = make_payload(cfg_.output_bytes, checksum);
    return out;
  }

  std::int64_t task_count() const {
    return static_cast<std::int64_t>(cfg_.height) * width_ * cfg_.graphs;
  }

  std::int64_t edge_count() const {
    std::int64_t edges_per_graph = 0;
    for (int row = 0; row < cfg_.height; ++row) {
      for (int col = 0; col < width_; ++col) {
        edges_per_graph += static_cast<std::int64_t>(deps(row, col).size());
      }
    }
    return edges_per_graph * cfg_.graphs;
  }

  const Config& config() const { return cfg_; }
  int width() const { return width_; }

private:
  void validate_inputs(int graph_id,
                       int row,
                       int col,
                       const std::vector<int>& expected,
                       std::span<const Token> inputs) const {
    if (inputs.size() != expected.size()) {
      throw std::runtime_error("wrong dependency count");
    }
    for (std::size_t idx = 0; idx < inputs.size(); ++idx) {
      const Token& input = inputs[idx];
      if (input.graph != graph_id || input.row != row - 1 || input.col != expected[idx]) {
        throw std::runtime_error("wrong dependency token");
      }
      if (!payload_matches_checksum(input, cfg_.output_bytes)) {
        throw std::runtime_error("wrong payload");
      }
    }
    (void)col;
  }

  Config cfg_;
  int width_ = 0;
};

} // namespace oox_bench::taskbench

#endif // OOX_BENCH_TASKBENCH_GRAPH_H
