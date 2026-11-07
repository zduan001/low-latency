#pragma once
#include <atomic>
#include <array>
#include <semaphore.h>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace tdsys {
    // Bounded multi-producer multi-consumer queue (Vyukov-style sequence
    // numbers). push/pop never block and never take a lock: a thread only
    // retries when another thread made progress, so the queue is lock-free.
    //
    // How it works:
    // - head and tail are 64-bit counters that only ever grow. A counter
    //   value is a "ticket"; its slot is buf[ticket & MASK]. Because the
    //   counters never wrap in practice, there is no ABA problem and no
    //   version bits are needed.
    // - Every slot carries a sequence number seq that says whose turn it is:
    //     seq == ticket         -> slot is empty, producer `ticket` may write
    //     seq == ticket + 1     -> slot is full, consumer `ticket` may read
    //     seq == ticket + LEN   -> consumer done, slot is empty for the
    //                              producer one lap later
    // - A producer reads tail, checks the slot's seq, and claims the ticket
    //   with a CAS on tail. Only the CAS winner touches that slot, then it
    //   publishes the data by storing seq = ticket + 1 (release).
    // - A consumer does the mirror image on head, then frees the slot for
    //   the next lap by storing seq = ticket + LEN (release).
    // - The acquire load of seq pairs with those release stores, so a
    //   consumer that sees seq == ticket + 1 also sees the payload, and a
    //   producer that sees seq == ticket also sees the consumer finished.
    template<typename T, size_t SZ = 127>
    class MPMCQueue {
    private:
        // Power-of-2 length so ticket & MASK replaces ticket % LEN
        static constexpr size_t LEN = 1 << std::bit_width(SZ);
        static constexpr size_t MASK = LEN - 1;

        // Each slot on its own cache line: neighbouring slots are written by
        // different threads, so sharing a line would cause false sharing
        struct alignas(64) Slot {
            std::atomic<size_t> seq;
            T data{};
        };

        // Producers contend on tail, consumers on head: separate cache lines
        alignas(64) std::atomic<size_t> head{ 0 };
        alignas(64) std::atomic<size_t> tail{ 0 };
        std::array<Slot, LEN> buf;

    public:
        // Slot i starts empty and waiting for producer ticket i
        MPMCQueue() {
            for (size_t i = 0; i < LEN; ++i)
                buf[i].seq.store(i, std::memory_order_relaxed);
        }

        // Returns false if the queue is full
        template<typename U>
        bool push(U&& u) {
            size_t pos = tail.load(std::memory_order_relaxed);
            while (true) {
                Slot& s = buf[pos & MASK];
                size_t seq = s.seq.load(std::memory_order_acquire);
                // Signed difference handles counter wrap-around correctly
                auto diff = static_cast<intptr_t>(seq)
                          - static_cast<intptr_t>(pos);
                if (diff == 0) {
                    // Slot is free for ticket pos: try to claim it. On
                    // failure the CAS reloads pos with the current tail.
                    if (tail.compare_exchange_weak(
                            pos, pos + 1, std::memory_order_relaxed)) {
                        s.data = std::forward<U>(u);
                        // Hand the slot to consumer ticket pos
                        s.seq.store(pos + 1, std::memory_order_release);
                        return true;
                    }
                } else if (diff < 0) {
                    // Slot still holds data from the previous lap: full
                    return false;
                } else {
                    // Another producer already took pos: catch up
                    pos = tail.load(std::memory_order_relaxed);
                }
            }
        }

        // Returns false if the queue is empty
        bool pop(T& out) {
            size_t pos = head.load(std::memory_order_relaxed);
            while (true) {
                Slot& s = buf[pos & MASK];
                size_t seq = s.seq.load(std::memory_order_acquire);
                auto diff = static_cast<intptr_t>(seq)
                          - static_cast<intptr_t>(pos + 1);
                if (diff == 0) {
                    // Slot holds data for ticket pos: try to claim it
                    if (head.compare_exchange_weak(
                            pos, pos + 1, std::memory_order_relaxed)) {
                        out = std::move(s.data);
                        // Free the slot for producer ticket pos + LEN
                        s.seq.store(pos + LEN, std::memory_order_release);
                        return true;
                    }
                } else if (diff < 0) {
                    // Producer for this ticket has not published yet: empty
                    return false;
                } else {
                    // Another consumer already took pos: catch up
                    pos = head.load(std::memory_order_relaxed);
                }
            }
        }
    };

    // Multi-producer single-consumer queue with a semaphore, for
    // non-latency-critical HFT auxiliary channels
    template<typename T, size_t SZ = 127>
    class MsgQueue {
    private:
        // Queue length rounded up to a power of 2 (standard HFT optimization):
        // LEN = 1<<n, so indices can be computed with bit ops
        static constexpr size_t LEN = 1 << std::bit_width(SZ);
        // Mask: replaces modulo, completes in a single CPU cycle, lowers
        // latency
        static constexpr size_t MASK = LEN - 1;
        // Number of index bits (16): used to solve the ABA problem; high bits
        // hold the version, low 16 bits hold the index
        static constexpr size_t IB = 16;
        // Index mask: extracts the low 16-bit index
        static constexpr size_t IM = (1ULL << IB) - 1;

        // Queue slot wrapper: data holds the payload, ok marks whether the
        // write has completed (required for multiple producers)
        // With concurrent producers, the consumer must only read after the data
        // is fully written, to avoid corrupted reads
        struct W {
            // Data-ready flag, atomic to guarantee visibility
            std::atomic<bool> ok{ false };
            // Actual message payload, default-initialized
            T data{};
        };

        // Cache-line alignment (64 bytes): h (head) and vt (versioned tail)
        // each get their own cache line to prevent false sharing
        // False sharing causes cache-line ping-pong between cores and adds
        // latency; this is a key HFT queue optimization
        alignas(64) std::atomic<size_t> h{ 0 }, vt{ 0 };
        // Semaphore: consumer blocks while waiting instead of busy-spinning
        sem_t sem{};
        // Static ring buffer, no dynamic memory allocation
        std::array<W, LEN> buf{};

    public:
        // Constructor: initialize the semaphore; pshared=1 means shared across
        // processes (suits multi-process HFT systems), initial value 0 means no
        // messages
        MsgQueue() { sem_init(&sem, 1, 0); }
        // Destructor: destroy the semaphore to avoid resource leaks (HFT
        // systems must not leak resources, or they become unstable)
        ~MsgQueue() { sem_destroy(&sem); }

        // Concurrent multi-producer enqueue (lock-free, CAS to claim the write
        // slot); accepts rvalue references to avoid copies and lower latency
        template<typename U>
        bool push(U&& u) {
            size_t v;  // Temporary for the version number
            while (true) {
                // Load the versioned tail: acquire ordering ensures we see the
                // latest vt value, not a stale one
                auto lvt = vt.load(std::memory_order_acquire);
                // Extract the low 16-bit index
                auto lt = lvt & IM;
                // Compute next write position, bit op instead of modulo
                auto n = (lt + 1) & MASK;
                // Read the head index to check whether the queue is full
                auto hh = h.load(std::memory_order_acquire) & IM;
                // Queue full, enqueue fails (HFT code would usually retry; kept
                // as in the original)
                if (n == hh) return false;

                // Version + 1: increment the version in the high bits to solve
                // ABA and avoid false CAS success
                v = (lvt >> IB) + 1;
                // New versioned tail
                size_t nvt = (v << IB) | n;

                // CAS to claim the write slot: compare_exchange_strong is
                // atomic; retry on failure
                // acq_rel ordering: after a successful CAS, prior writes are
                // visible and subsequent reads see the latest data
                if (vt.compare_exchange_strong(lvt, nvt, std::memory_order_acq_rel)) {
                    auto& w = buf[lt];
                    // Perfect forwarding of rvalue references, avoids copy
                    // overhead
                    w.data = std::forward<U>(u);
                    // Mark data ready; release makes the write visible
                    w.ok.store(true, std::memory_order_release);
                    // Wake the consumer (non-critical path, system calls are
                    // allowed)
                    sem_post(&sem);
                    return true;
                }
            }
        }

        // Single-consumer dequeue (blocking), for non-critical paths where
        // slight blocking is acceptable
        bool pop(T& out) {
            // Block waiting for a message, avoids consumer busy-spinning and
            // saves CPU
            sem_wait(&sem);
            // Read head; relaxed has no sync overhead, only guarantees
            // atomicity
            auto lh = h.load(std::memory_order_relaxed);
            // Read the tail index; acquire guarantees visibility
            auto tail = vt.load(std::memory_order_acquire) & IM;
            // Queue empty, dequeue fails (shouldn't happen in theory, since the
            // semaphore controls it)
            if (lh == tail) return false;
            auto& w = buf[lh];
            // Ensure the data has been fully written
            if (!w.ok.load(std::memory_order_acquire)) return false;
            // Move semantics, zero copy, lower latency
            out = std::move(w.data);
            // Reset ready flag, allowing the next write
            w.ok.store(false, std::memory_order_release);
            // Update head; release guarantees visibility
            h.store((lh + 1) & MASK, std::memory_order_release);
            return true;
        }
    };
}



#pragma once
#include <atomic>
#include <array>

namespace tdsys {
    // Single-producer single-consumer ultra-low-latency lock-free queue, for
    // the HFT critical path
    template<typename T, size_t SZ = 127>
    class SPSCQueue {
    private:
        // Queue length rounded up to a power of 2, standard HFT design, bit ops
        // instead of modulo
        static constexpr size_t LEN = 1 << std::bit_width(SZ);
        static constexpr size_t MASK = LEN - 1;

        // Head (modified by consumer) and tail (modified by producer), each
        // cache-line aligned to prevent false sharing
        // The single producer only modifies t and the single consumer only
        // modifies h: no contention, no CAS needed
        alignas(64) std::atomic<size_t> h{ 0 }, t{ 0 };
        // Local caches sh, st: reduce atomic loads; atomic ops can be 10x+
        // slower than plain accesses; a key HFT optimization
        alignas(64) size_t sh = 0, st = 0;
        // Static ring buffer, no dynamic memory allocation, predictable latency
        std::array<T, LEN> buf{};

    public:
        // Consumer dequeue: single-threaded, no contention, very low latency
        bool pop(T& out) {
            // Read head; relaxed has no sync overhead
            auto lh = h.load(std::memory_order_relaxed);
            // Check against cached st first to avoid frequently loading atomic
            // t (atomic loads are expensive)
            if (lh != st && (st = t.load(std::memory_order_acquire)) != lh) {
                // Move semantics, zero copy, lower latency
                out = std::move(buf[lh]);
                // Update head: release ordering makes post-dequeue changes
                // visible
                h.store((lh + 1) & MASK, std::memory_order_release);
                return true;
            }
            // Queue empty, dequeue fails (HFT code would usually spin and
            // retry; kept as in the original)
            return false;
        }

        // Producer enqueue: single-threaded, no contention, no CAS needed
        template<typename U>
        bool push(U&& u) {
            // Read tail; relaxed has no sync overhead
            auto lt = t.load(std::memory_order_relaxed);
            // Compute next write position, bit op instead of modulo
            auto n = (lt + 1) & MASK;
            // Check against cached sh first to avoid frequently loading atomic
            // h
            if (n != sh && (sh = h.load(std::memory_order_acquire)) != n) {
                // Perfect forwarding, avoids copies
                buf[lt] = std::forward<U>(u);
                // Update tail: release ordering makes the enqueued data visible
                t.store((lt + 1) & MASK, std::memory_order_release);
                return true;
            }
            // Queue full, enqueue fails (HFT code would retry; kept as in the
            // original)
            return false;
        }
    };
}

#pragma once
#include <atomic>
#include <array>

namespace tdsys {
    // Single-producer multi-consumer lock-free queue, for HFT market-data
    // broadcast and parallel multi-strategy consumption
    template<typename T, size_t SZ = 127>
    class SPMCQueue {
    private:
        // Queue length rounded up to a power of 2, standard HFT design, bit ops
        // instead of modulo
        static constexpr size_t LEN = 1 << std::bit_width(SZ);
        static constexpr size_t MASK = LEN - 1;
        // Version in the high bits, index in the low 16 bits, solving ABA under
        // multi-consumer CAS contention
        static constexpr size_t IB = 16;
        static constexpr size_t IM = (1ULL << IB) - 1;

        // Versioned head (contended via CAS by multiple consumers) and tail
        // (modified by the single producer, uncontended)
        // Cache-line aligned to avoid false sharing
        alignas(64) std::atomic<size_t> vh{ 0 }, t{ 0 };
        // Static ring buffer, no dynamic memory allocation
        std::array<T, LEN> buf{};

    public:
        // Single-producer enqueue: no contention, no CAS needed, very low
        // latency
        template<typename U>
        bool push(U&& u) {
            // Read tail; relaxed has no sync overhead
            auto lt = t.load(std::memory_order_relaxed);
            // Compute next write position
            auto n = (lt + 1) & MASK;
            // Read the versioned head to check whether the queue is full;
            // acquire guarantees visibility
            auto hh = vh.load(std::memory_order_acquire) & IM;
            // Queue full, enqueue fails
            if (n == hh) return false;
            // Write data, no contention
            buf[lt] = std::forward<U>(u);
            // Update tail; release makes the written data visible
            t.store(n, std::memory_order_release);
            return true;
        }

        // Multi-consumer dequeue: CAS to claim the read, ensuring each message
        // is consumed by exactly one consumer
        bool pop(T& out) {
            while (true) {
                // Load the versioned head; acquire ensures we see the latest
                // version and index
                auto lvh = vh.load(std::memory_order_acquire);
                auto lh = lvh & IM;                      // Extract index
                // Read tail to check whether the queue is empty
                auto lt = t.load(std::memory_order_relaxed);
                // Queue empty, dequeue fails
                if (lh == lt) return false;

                // Version + 1, build the new versioned head for the CAS
                size_t nv = (lvh >> IB) + 1;
                size_t nvh = (nv << IB) | ((lh + 1) & MASK);

                // CAS to claim the read: multiple consumers compete, only one
                // succeeds
                // acq_rel ensures that after a successful CAS, prior reads and
                // subsequent writes are visible
                if (vh.compare_exchange_strong(lvh, nvh, std::memory_order_acq_rel)) {
                    out = std::move(buf[lh]);            // Move data, zero copy
                    return true;
                }
                // CAS failed, retry (HFT code usually spins and retries rather
                // than blocking)
            }
        }
    };
}


#pragma once
#include <atomic>
#include <array>

namespace tdsys {
    // Multi-producer single-consumer lock-free queue, for HFT multi-strategy
    // order submission and multi-source aggregation
    template<typename T, size_t SZ = 127>
    class MPSCQueue {
    private:
        // Queue length rounded up to a power of 2, standard HFT optimization,
        // bit ops instead of modulo
        static constexpr size_t LEN = 1 << std::bit_width(SZ);
        static constexpr size_t MASK = LEN - 1;
        // Version in the high bits, index in the low 16 bits, solving ABA under
        // multi-producer CAS contention
        static constexpr size_t IB = 16;
        static constexpr size_t IM = (1ULL << IB) - 1;

        // Queue slot wrapper: ok marks whether the write has completed,
        // required for multiple producers
        struct W {
            std::atomic<bool> ok{ false };
            T data{};
        };

        // Head (modified by consumer) and versioned tail (contended via CAS by
        // multiple producers), cache-line aligned
        alignas(64) std::atomic<size_t> h{ 0 }, vt{ 0 };
        // Static ring buffer, no dynamic memory allocation
        std::array<W, LEN> buf{};

    public:
        // Single-consumer dequeue: no contention, very low latency
        bool pop(T& out) {
            // Read head; relaxed has no sync overhead
            auto lh = h.load(std::memory_order_relaxed);
            // Read the versioned tail; acquire guarantees visibility
            auto tail = vt.load(std::memory_order_acquire) & IM;
            // Queue empty, dequeue fails
            if (lh == tail) return false;
            auto& w = buf[lh];
            // Data not ready yet
            if (!w.ok.load(std::memory_order_acquire)) return false;
            // Move semantics, zero copy
            out = std::move(w.data);
            // Reset flag, allowing the next write
            w.ok.store(false, std::memory_order_release);
            // Update head; release guarantees visibility
            h.store((lh + 1) & MASK, std::memory_order_release);
            return true;
        }

        // Concurrent multi-producer enqueue: CAS to claim the write slot,
        // lock-free and non-blocking
        template<typename U>
        bool push(U&& u) {
            size_t v;
            while (true) {
                // Load the versioned tail; acquire ensures we see the latest
                // value
                auto lvt = vt.load(std::memory_order_acquire);
                auto lt = lvt & IM;                      // Extract index
                // Compute next write position
                auto n = (lt + 1) & MASK;
                // Read head to check whether the queue is full
                auto hh = h.load(std::memory_order_acquire) & IM;
                // Queue full, enqueue fails
                if (n == hh) return false;

                // Version + 1, build the new versioned tail
                v = (lvt >> IB) + 1;
                size_t nvt = (v << IB) | n;

                // CAS to claim the write slot: multiple producers compete, only
                // one succeeds
                if (vt.compare_exchange_strong(lvt, nvt, std::memory_order_acq_rel)) {
                    auto& w = buf[lt];
                    // Perfect forwarding, avoids copies
                    w.data = std::forward<U>(u);
                    // Mark data ready
                    w.ok.store(true, std::memory_order_release);
                    return true;
                }
                // CAS failed, retry (HFT code spins and retries, non-blocking)
            }
        }
    };
}
