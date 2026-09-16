#pragma once
#include <cassert>
#include <cstdint>
#include <memory>
namespace internal {
inline void *handmade_aligned_malloc(std::size_t size, std::size_t alignment) {
  assert(alignment >= sizeof(void *) && alignment <= 128 &&
         (alignment & (alignment - 1)) == 0 &&
         "Alignment must be at least sizeof(void*), less than or equal "
         "to 128, and a power of 2");
  void *original = std::malloc(size + alignment);
  if (original == 0)
    return 0;
  uint8_t offset = static_cast<uint8_t>(
      alignment - (reinterpret_cast<std::size_t>(original) & (alignment - 1)));
  void *aligned =
      static_cast<void *>(static_cast<uint8_t *>(original) + offset);
  *(static_cast<uint8_t *>(aligned) - 1) = offset;
  return aligned;
}

inline void handmade_aligned_free(void *ptr) {
  if (ptr) {
    uint8_t offset = static_cast<uint8_t>(*(static_cast<uint8_t *>(ptr) - 1));
    void *original = static_cast<void *>(static_cast<uint8_t *>(ptr) - offset);
    std::free(original);
  }
}
} // namespace internal
