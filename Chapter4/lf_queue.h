#pragma once

#include <iostream>
#include <vector>
#include <atomic>
#include <bit>
#include <cstddef>

#include "macros.h"

namespace Common {
  /// Single-producer / single-consumer lock-free ring buffer of fixed-size elements.
  ///
  /// Same API as before, but the internals were changed for low latency:
  ///  - No shared `num_elements_` counter. Previously BOTH threads did a locked read-modify-write
  ///    on it for every element, so its cache line ping-ponged between the two cores.
  ///    Size is now derived from two monotonically increasing indices.
  ///  - Each index is written by exactly one thread and lives on its own cache line (no false sharing).
  ///  - acquire/release ordering instead of seq_cst (a seq_cst store is an `xchg` / full fence on x86).
  ///  - Capacity is rounded up to a power of two so `% size` (a ~30 cycle `div`) becomes `& mask_`.
  template<typename T>
  class LFQueue final {
  public:
    explicit LFQueue(std::size_t num_elems) :
        capacity_(std::bit_ceil(num_elems ? num_elems : std::size_t{1})), mask_(capacity_ - 1),
        store_(capacity_, T()) /* pre-allocation of vector storage. */ {
    }

    /// Producer: slot to fill next. Call updateWriteIndex() to publish it.
    /// NOTE: like the original, this does not check for a full queue - use full() if you need to.
    auto getNextToWriteTo() noexcept {
      return &store_[write_index_.load(std::memory_order_relaxed) & mask_];
    }

    /// Producer: publish the slot returned by getNextToWriteTo().
    auto updateWriteIndex() noexcept {
      // release: the element's contents become visible to the consumer before the new index does.
      write_index_.store(write_index_.load(std::memory_order_relaxed) + 1, std::memory_order_release);
    }

    /// Consumer: next element to read, or nullptr if the queue is empty.
    auto getNextToRead() const noexcept -> const T * {
      const auto read = read_index_.load(std::memory_order_relaxed);
      return (read != write_index_.load(std::memory_order_acquire) ? &store_[read & mask_] : nullptr);
    }

    /// Consumer: release the element returned by getNextToRead().
    auto updateReadIndex() noexcept {
      const auto read = read_index_.load(std::memory_order_relaxed);
      ASSERT(read != write_index_.load(std::memory_order_acquire), "Read an invalid element in:" + std::to_string(pthread_self()));
      read_index_.store(read + 1, std::memory_order_release);
    }

    auto size() const noexcept {
      return static_cast<std::size_t>(write_index_.load(std::memory_order_acquire) - read_index_.load(std::memory_order_acquire));
    }

    auto full() const noexcept {
      return size() >= capacity_;
    }

    auto capacity() const noexcept {
      return capacity_;
    }

    // Deleted default, copy & move constructors and assignment-operators.
    LFQueue() = delete;

    LFQueue(const LFQueue &) = delete;

    LFQueue(const LFQueue &&) = delete;

    LFQueue &operator=(const LFQueue &) = delete;

    LFQueue &operator=(const LFQueue &&) = delete;

  private:
    static constexpr std::size_t CacheLine = 64;

    // Read-only after construction - shared freely by both cores.
    const std::size_t capacity_;
    const std::size_t mask_;
    std::vector<T> store_;

    // Written only by the producer.
    alignas(CacheLine) std::atomic<uint64_t> write_index_ = {0};

    // Written only by the consumer.
    alignas(CacheLine) std::atomic<uint64_t> read_index_ = {0};

    // Keep whatever follows this object off the consumer's cache line.
    [[maybe_unused]] char pad_[CacheLine - sizeof(std::atomic<uint64_t>)] = {};
  };
}
