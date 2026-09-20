// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <type_traits>
#include <utility>

#ifndef OOX_EIGEN_SMALL_OBJECT_POOL
#define OOX_EIGEN_SMALL_OBJECT_POOL 1
#endif

namespace oox::detail::eigen_pool {
namespace internal {

#ifdef OOX_EIGEN_SMALL_OBJECT_POOL_TESTING
inline std::atomic<size_t> small_object_blocks{0};
inline std::atomic<size_t> small_object_owners{0};
#endif

// A block retains its owner independently of the object's lifetime. Closing
// the owner drains cached blocks; outstanding objects can still return from
// other threads. No thread-pool or thread-local object is borrowed by a block.
class SmallObjectPool {
  struct State;
  struct Block {
    explicit Block(State *state) noexcept : owner(state) {
#ifdef OOX_EIGEN_SMALL_OBJECT_POOL_TESTING
      small_object_blocks.fetch_add(1, std::memory_order_relaxed);
#endif
    }
    ~Block() {
#ifdef OOX_EIGEN_SMALL_OBJECT_POOL_TESTING
      small_object_blocks.fetch_sub(1, std::memory_order_relaxed);
#endif
    }
    State *const owner;
    Block *next = nullptr;
    alignas(std::max_align_t) unsigned char storage[256];
  };

  struct State {
    State() {
#ifdef OOX_EIGEN_SMALL_OBJECT_POOL_TESTING
      small_object_owners.fetch_add(1, std::memory_order_relaxed);
#endif
    }
    ~State() {
#ifdef OOX_EIGEN_SMALL_OBJECT_POOL_TESTING
      small_object_owners.fetch_sub(1, std::memory_order_relaxed);
#endif
    }

    static Block *Closed() noexcept {
      return reinterpret_cast<Block *>(uintptr_t{1});
    }

    void ReleaseReference() noexcept {
      if (references.fetch_sub(1, std::memory_order_acq_rel) == 1)
        delete this;
    }

    static void FreeBlock(Block *block) noexcept {
      State *owner = block->owner;
      delete block;
      if (owner)
        owner->ReleaseReference();
    }

    Block *Allocate() {
      if (!private_list && remote_list.load(std::memory_order_relaxed)) {
        private_list = remote_list.exchange(nullptr, std::memory_order_acquire);
        for (Block *block = private_list; block; block = block->next)
          ++private_count;
        remote_count.fetch_sub(private_count, std::memory_order_relaxed);
      }
      if (private_list) {
        Block *block = private_list;
        private_list = block->next;
        --private_count;
        return block;
      }
      auto *block = new Block(this);
      references.fetch_add(1, std::memory_order_relaxed);
      return block;
    }

    void ReturnLocal(Block *block) noexcept {
      if (private_count == cache_limit) {
        FreeBlock(block);
        return;
      }
      block->next = private_list;
      private_list = block;
      ++private_count;
    }

    void ReturnRemote(Block *block) noexcept {
      if (remote_count.fetch_add(1, std::memory_order_relaxed) >= cache_limit) {
        remote_count.fetch_sub(1, std::memory_order_relaxed);
        FreeBlock(block);
        return;
      }
      auto *head = remote_list.load(std::memory_order_acquire);
      for (;;) {
        if (head == Closed()) {
          remote_count.fetch_sub(1, std::memory_order_relaxed);
          FreeBlock(block);
          return;
        }
        block->next = head;
        if (remote_list.compare_exchange_weak(
                head, block, std::memory_order_release,
                std::memory_order_relaxed))
          return; // The owner may reclaim both block and State immediately.
      }
    }

    void Close() noexcept {
      Block *remote = remote_list.exchange(Closed(), std::memory_order_acq_rel);
      while (private_list) {
        Block *block = private_list;
        private_list = block->next;
        FreeBlock(block);
      }
      while (remote) {
        Block *block = remote;
        remote = block->next;
        FreeBlock(block);
      }
      ReleaseReference(); // Release the allocating thread's reference last.
    }

    static constexpr size_t cache_limit = 256;
    Block *private_list = nullptr;
    size_t private_count = 0;
    alignas(64) std::atomic<Block *> remote_list{nullptr};
    std::atomic<size_t> remote_count{0};
    std::atomic<size_t> references{1};
  };

  struct LocalOwner {
    ~LocalOwner() {
      exiting_ = true;
      local_state_ = nullptr;
      if (state)
        state->Close();
    }
    State *state = nullptr;
  };

public:
  static constexpr size_t capacity = sizeof(Block::storage);
  static constexpr size_t alignment = alignof(std::max_align_t);

  static void *Allocate() {
    // A later thread-local destructor may allocate after our owner has closed.
    // Such objects use standalone blocks and can be freed by any thread.
    if (exiting_)
      return (new Block(nullptr))->storage;
    static thread_local LocalOwner owner;
    if (!local_state_) {
      owner.state = new State;
      local_state_ = owner.state;
    }
    return local_state_->Allocate()->storage;
  }

  static void Release(void *memory) noexcept {
    auto *block = reinterpret_cast<Block *>(
        static_cast<unsigned char *>(memory) - offsetof(Block, storage));
    State *owner = block->owner;
    if (!owner)
      delete block;
    else if (owner == local_state_)
      owner->ReturnLocal(block);
    else
      owner->ReturnRemote(block);
  }

private:
  inline static thread_local State *local_state_ = nullptr;
  inline static thread_local bool exiting_ = false;
};

} // namespace internal

template <typename T>
inline constexpr bool uses_small_object_pool =
    OOX_EIGEN_SMALL_OBJECT_POOL &&
    sizeof(T) <= internal::SmallObjectPool::capacity &&
    alignof(T) <= internal::SmallObjectPool::alignment;

// Final task types retain ordinary new/delete semantics, including deletion
// through a virtual Task base. The allocation choice is still compile-time.
template <typename T> struct SmallObjectAllocated {
  static void *operator new(size_t bytes) {
    static_assert(std::is_final_v<T>);
    if constexpr (uses_small_object_pool<T>) {
      return internal::SmallObjectPool::Allocate();
    } else if constexpr (alignof(T) > __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
      return ::operator new(bytes, std::align_val_t{alignof(T)});
    } else {
      return ::operator new(bytes);
    }
  }
  static void operator delete(void *memory) noexcept {
    if constexpr (uses_small_object_pool<T>) {
      internal::SmallObjectPool::Release(memory);
    } else if constexpr (alignof(T) > __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
      ::operator delete(memory, std::align_val_t{alignof(T)});
    } else {
      ::operator delete(memory);
    }
  }
};

template <typename T, typename... Args> T *NewSmallObject(Args &&...args) {
  if constexpr (uses_small_object_pool<T>) {
    void *storage = internal::SmallObjectPool::Allocate();
    try {
      return ::new (storage) T(std::forward<Args>(args)...);
    } catch (...) {
      internal::SmallObjectPool::Release(storage);
      throw;
    }
  } else {
    return new T(std::forward<Args>(args)...);
  }
}

template <typename T> void DeleteSmallObject(T *object) noexcept {
  if (!object)
    return;
  if constexpr (uses_small_object_pool<T>) {
    object->~T();
    internal::SmallObjectPool::Release(object);
  } else {
    delete object;
  }
}

} // namespace oox::detail::eigen_pool
