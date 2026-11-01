#pragma once
#include <atomic>
#include <array>
#include <semaphore.h>

namespace tdsys {
    // Multi-producer single-consumer queue with a semaphore, for non-latency-critical HFT auxiliary channels
    template<typename T, size_t SZ = 127>
    class MsgQueue {
    private:
        // Queue length rounded up to a power of 2 (standard HFT optimization): LEN = 1<<n, so indices can be computed with bit ops
        static constexpr size_t LEN = 1 << std::bit_width(SZ);
        // Mask: replaces modulo, completes in a single CPU cycle, lowers latency
        static constexpr size_t MASK = LEN - 1;
        // Number of index bits (16): used to solve the ABA problem; high bits hold the version, low 16 bits hold the index
        static constexpr size_t IB = 16;
        // Index mask: extracts the low 16-bit index
        static constexpr size_t IM = (1ULL << IB) - 1;

        // Queue slot wrapper: data holds the payload, ok marks whether the write has completed (required for multiple producers)
        // With concurrent producers, the consumer must only read after the data is fully written, to avoid corrupted reads
        struct W {
            std::atomic<bool> ok{ false };  // Data-ready flag, atomic to guarantee visibility
            T data{};                      // Actual message payload, default-initialized
        };

        // Cache-line alignment (64 bytes): h (head) and vt (versioned tail) each get their own cache line to prevent false sharing
        // False sharing causes cache-line ping-pong between cores and adds latency; this is a key HFT queue optimization
        alignas(64) std::atomic<size_t> h{ 0 }, vt{ 0 };
        sem_t sem{};                          // Semaphore: consumer blocks while waiting instead of busy-spinning
        std::array<W, LEN> buf{};             // Static ring buffer, no dynamic memory allocation

    public:
        // Constructor: initialize the semaphore; pshared=1 means shared across processes (suits multi-process HFT systems), initial value 0 means no messages
        MsgQueue() { sem_init(&sem, 1, 0); }
        // Destructor: destroy the semaphore to avoid resource leaks (HFT systems must not leak resources, or they become unstable)
        ~MsgQueue() { sem_destroy(&sem); }

        // Concurrent multi-producer enqueue (lock-free, CAS to claim the write slot); accepts rvalue references to avoid copies and lower latency
        template<typename U>
        bool push(U&& u) {
            size_t v;  // Temporary for the version number
            while (true) {
                // Load the versioned tail: acquire ordering ensures we see the latest vt value, not a stale one
                auto lvt = vt.load(std::memory_order_acquire);
                auto lt = lvt & IM;                          // Extract the low 16-bit index
                auto n = (lt + 1) & MASK;                    // Compute next write position, bit op instead of modulo
                auto hh = h.load(std::memory_order_acquire) & IM; // Read the head index to check whether the queue is full
                if (n == hh) return false;                   // Queue full, enqueue fails (HFT code would usually retry; kept as in the original)

                // Version + 1: increment the version in the high bits to solve ABA and avoid false CAS success
                v = (lvt >> IB) + 1;
                size_t nvt = (v << IB) | n;                  // New versioned tail

                // CAS to claim the write slot: compare_exchange_strong is atomic; retry on failure
                // acq_rel ordering: after a successful CAS, prior writes are visible and subsequent reads see the latest data
                if (vt.compare_exchange_strong(lvt, nvt, std::memory_order_acq_rel)) {
                    auto& w = buf[lt];
                    w.data = std::forward<U>(u);             // Perfect forwarding of rvalue references, avoids copy overhead
                    w.ok.store(true, std::memory_order_release); // Mark data ready; release makes the write visible
                    sem_post(&sem);                          // Wake the consumer (non-critical path, system calls are allowed)
                    return true;
                }
            }
        }

        // Single-consumer dequeue (blocking), for non-critical paths where slight blocking is acceptable
        bool pop(T& out) {
            sem_wait(&sem);                                  // Block waiting for a message, avoids consumer busy-spinning and saves CPU
            auto lh = h.load(std::memory_order_relaxed);     // Read head; relaxed has no sync overhead, only guarantees atomicity
            auto tail = vt.load(std::memory_order_acquire) & IM; // Read the tail index; acquire guarantees visibility
            if (lh == tail) return false;                   // Queue empty, dequeue fails (shouldn't happen in theory, since the semaphore controls it)
            auto& w = buf[lh];
            if (!w.ok.load(std::memory_order_acquire)) return false; // Ensure the data has been fully written
            out = std::move(w.data);                        // Move semantics, zero copy, lower latency
            w.ok.store(false, std::memory_order_release);   // Reset ready flag, allowing the next write
            h.store((lh + 1) & MASK, std::memory_order_release); // Update head; release guarantees visibility
            return true;
        }
    };
}



#pragma once
#include <atomic>
#include <array>

namespace tdsys {
    // Single-producer single-consumer ultra-low-latency lock-free queue, for the HFT critical path
    template<typename T, size_t SZ = 127>
    class SPSCQueue {
    private:
        // Queue length rounded up to a power of 2, standard HFT design, bit ops instead of modulo
        static constexpr size_t LEN = 1 << std::bit_width(SZ);
        static constexpr size_t MASK = LEN - 1;

        // Head (modified by consumer) and tail (modified by producer), each cache-line aligned to prevent false sharing
        // The single producer only modifies t and the single consumer only modifies h: no contention, no CAS needed
        alignas(64) std::atomic<size_t> h{ 0 }, t{ 0 };
        // Local caches sh, st: reduce atomic loads; atomic ops can be 10x+ slower than plain accesses; a key HFT optimization
        alignas(64) size_t sh = 0, st = 0;
        std::array<T, LEN> buf{}; // Static ring buffer, no dynamic memory allocation, predictable latency

    public:
        // Consumer dequeue: single-threaded, no contention, very low latency
        bool pop(T& out) {
            auto lh = h.load(std::memory_order_relaxed); // Read head; relaxed has no sync overhead
            // Check against cached st first to avoid frequently loading atomic t (atomic loads are expensive)
            if (lh != st && (st = t.load(std::memory_order_acquire)) != lh) {
                out = std::move(buf[lh]);                // Move semantics, zero copy, lower latency
                // Update head: release ordering makes post-dequeue changes visible
                h.store((lh + 1) & MASK, std::memory_order_release);
                return true;
            }
            return false; // Queue empty, dequeue fails (HFT code would usually spin and retry; kept as in the original)
        }

        // Producer enqueue: single-threaded, no contention, no CAS needed
        template<typename U>
        bool push(U&& u) {
            auto lt = t.load(std::memory_order_relaxed); // Read tail; relaxed has no sync overhead
            auto n = (lt + 1) & MASK;                   // Compute next write position, bit op instead of modulo
            // Check against cached sh first to avoid frequently loading atomic h
            if (n != sh && (sh = h.load(std::memory_order_acquire)) != n) {
                buf[lt] = std::forward<U>(u);            // Perfect forwarding, avoids copies
                // Update tail: release ordering makes the enqueued data visible
                t.store((lt + 1) & MASK, std::memory_order_release);
                return true;
            }
            return false; // Queue full, enqueue fails (HFT code would retry; kept as in the original)
        }
    };
}

#pragma once
#include <atomic>
#include <array>

namespace tdsys {
    // Single-producer multi-consumer lock-free queue, for HFT market-data broadcast and parallel multi-strategy consumption
    template<typename T, size_t SZ = 127>
    class SPMCQueue {
    private:
        // Queue length rounded up to a power of 2, standard HFT design, bit ops instead of modulo
        static constexpr size_t LEN = 1 << std::bit_width(SZ);
        static constexpr size_t MASK = LEN - 1;
        // Version in the high bits, index in the low 16 bits, solving ABA under multi-consumer CAS contention
        static constexpr size_t IB = 16;
        static constexpr size_t IM = (1ULL << IB) - 1;

        // Versioned head (contended via CAS by multiple consumers) and tail (modified by the single producer, uncontended)
        // Cache-line aligned to avoid false sharing
        alignas(64) std::atomic<size_t> vh{ 0 }, t{ 0 };
        std::array<T, LEN> buf{}; // Static ring buffer, no dynamic memory allocation

    public:
        // Single-producer enqueue: no contention, no CAS needed, very low latency
        template<typename U>
        bool push(U&& u) {
            auto lt = t.load(std::memory_order_relaxed); // Read tail; relaxed has no sync overhead
            auto n = (lt + 1) & MASK;                   // Compute next write position
            // Read the versioned head to check whether the queue is full; acquire guarantees visibility
            auto hh = vh.load(std::memory_order_acquire) & IM;
            if (n == hh) return false;                   // Queue full, enqueue fails
            buf[lt] = std::forward<U>(u);                // Write data, no contention
            // Update tail; release makes the written data visible
            t.store(n, std::memory_order_release);
            return true;
        }

        // Multi-consumer dequeue: CAS to claim the read, ensuring each message is consumed by exactly one consumer
        bool pop(T& out) {
            while (true) {
                // Load the versioned head; acquire ensures we see the latest version and index
                auto lvh = vh.load(std::memory_order_acquire);
                auto lh = lvh & IM;                      // Extract index
                auto lt = t.load(std::memory_order_relaxed); // Read tail to check whether the queue is empty
                if (lh == lt) return false;              // Queue empty, dequeue fails

                // Version + 1, build the new versioned head for the CAS
                size_t nv = (lvh >> IB) + 1;
                size_t nvh = (nv << IB) | ((lh + 1) & MASK);

                // CAS to claim the read: multiple consumers compete, only one succeeds
                // acq_rel ensures that after a successful CAS, prior reads and subsequent writes are visible
                if (vh.compare_exchange_strong(lvh, nvh, std::memory_order_acq_rel)) {
                    out = std::move(buf[lh]);            // Move data, zero copy
                    return true;
                }
                // CAS failed, retry (HFT code usually spins and retries rather than blocking)
            }
        }
    };
}


#pragma once
#include <atomic>
#include <array>

namespace tdsys {
    // Multi-producer single-consumer lock-free queue, for HFT multi-strategy order submission and multi-source aggregation
    template<typename T, size_t SZ = 127>
    class MPSCQueue {
    private:
        // Queue length rounded up to a power of 2, standard HFT optimization, bit ops instead of modulo
        static constexpr size_t LEN = 1 << std::bit_width(SZ);
        static constexpr size_t MASK = LEN - 1;
        // Version in the high bits, index in the low 16 bits, solving ABA under multi-producer CAS contention
        static constexpr size_t IB = 16;
        static constexpr size_t IM = (1ULL << IB) - 1;

        // Queue slot wrapper: ok marks whether the write has completed, required for multiple producers
        struct W {
            std::atomic<bool> ok{ false };
            T data{};
        };

        // Head (modified by consumer) and versioned tail (contended via CAS by multiple producers), cache-line aligned
        alignas(64) std::atomic<size_t> h{ 0 }, vt{ 0 };
        std::array<W, LEN> buf{}; // Static ring buffer, no dynamic memory allocation

    public:
        // Single-consumer dequeue: no contention, very low latency
        bool pop(T& out) {
            auto lh = h.load(std::memory_order_relaxed); // Read head; relaxed has no sync overhead
            // Read the versioned tail; acquire guarantees visibility
            auto tail = vt.load(std::memory_order_acquire) & IM;
            if (lh == tail) return false;               // Queue empty, dequeue fails
            auto& w = buf[lh];
            if (!w.ok.load(std::memory_order_acquire)) return false; // Data not ready yet
            out = std::move(w.data);                    // Move semantics, zero copy
            w.ok.store(false, std::memory_order_release); // Reset flag, allowing the next write
            // Update head; release guarantees visibility
            h.store((lh + 1) & MASK, std::memory_order_release);
            return true;
        }

        // Concurrent multi-producer enqueue: CAS to claim the write slot, lock-free and non-blocking
        template<typename U>
        bool push(U&& u) {
            size_t v;
            while (true) {
                // Load the versioned tail; acquire ensures we see the latest value
                auto lvt = vt.load(std::memory_order_acquire);
                auto lt = lvt & IM;                      // Extract index
                auto n = (lt + 1) & MASK;                // Compute next write position
                // Read head to check whether the queue is full
                auto hh = h.load(std::memory_order_acquire) & IM;
                if (n == hh) return false;               // Queue full, enqueue fails

                // Version + 1, build the new versioned tail
                v = (lvt >> IB) + 1;
                size_t nvt = (v << IB) | n;

                // CAS to claim the write slot: multiple producers compete, only one succeeds
                if (vt.compare_exchange_strong(lvt, nvt, std::memory_order_acq_rel)) {
                    auto& w = buf[lt];
                    w.data = std::forward<U>(u);         // Perfect forwarding, avoids copies
                    w.ok.store(true, std::memory_order_release); // Mark data ready
                    return true;
                }
                // CAS failed, retry (HFT code spins and retries, non-blocking)
            }
        }
    };
}
