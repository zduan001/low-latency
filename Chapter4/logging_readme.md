# Low-latency logger (`logging.h`)

This is an asynchronous logger built for latency-critical threads such as trading, market-data and order-gateway threads. The thread that calls `log()` does only a fixed, small amount of work: it records the raw values and returns. All formatting and file I/O happen later on a background thread.

New to the code? Read [Why this is a low-latency logger](#why-this-is-a-low-latency-logger) first, then [section 13](#13-c-techniques-used-in-loggingh) for the C++ techniques it relies on.

| | Old logger | New logger |
|---|---|---|
| Hot-path work per message | 1 queue push **per character** of the format string and of each string argument, each with 2 seq_cst atomic operations | 1 record per message, published with 1 release store |
| Bytes written per message | 16 bytes × (number of characters + number of arguments) | 32-byte header + raw argument bytes (64 bytes for the benchmark message) |
| Cost on the calling thread (benchmark message below) | p50 ≈ 10–19 µs | p50 ≈ 60 ns as measured (including timer overhead); ≈ 18 ns/call throughput |
| Format/argument mismatch | `exit()` at runtime | compile error |
| Multiple logging threads | data race, garbled output | supported (one lock-free buffer per thread) |
| Queue full | silently overwrites unread data | message dropped, counted, and reported in the log |
| Timestamps | none (callers formatted `ctime()` strings themselves) | `rdtsc` taken at the call site, ns resolution |

---

## Why this is a low-latency logger

The logger is low latency because the thread that calls it does almost no work. It copies a few bytes into memory that only it writes to, and moves everything slow to another thread.

In an HFT system, the latency that matters is how long `LOG_INFO(...)` holds up the trading thread. The time until the line reaches disk barely matters. So the design question is what work the trading thread can avoid. Every expensive operation a logger could do is either removed or moved off that thread.

### 1. Formatting moves to the background thread
Turning `5012.25` into the characters `"5012.25"` is the most expensive part of logging. It involves loops, divisions and branches, and float conversion is especially costly. Locale handling and `ostream` virtual calls add more.

The logger never formats on the trading thread. It copies the raw 8 bytes of the double, plus a pointer to the format string literal and a pointer to a decoder function. The background thread does the formatting later with `std::to_chars`. The format string itself is never copied or scanned at runtime.

**Where in the code:**
- [logging.h:724 `Logger::write()`](logging.h#L724): the whole hot path. It stores the format pointer and the decoder pointer in the header ([logging.h:740](logging.h#L740)), then copies the arguments ([logging.h:745](logging.h#L745)).
- [logging.h:179 `TrivialCopy::encode`](logging.h#L179): a number is a single `memcpy` of its bytes.
- [logging.h:405 `RecordHeader`](logging.h#L405): the 32-byte header with `fmt` and `decode` pointers.
- [logging.h:457 `Decoder<A, Rest...>`](logging.h#L457): runs on the background thread. It walks the format string and turns each argument into text.
- [logging.h:107 `LogWriter::appendNumber`](logging.h#L107): `std::to_chars`, called only on the background thread.

### 2. No system calls
A `write()` to a file traps into the kernel, which costs hundreds of nanoseconds to microseconds and can block on disk I/O. The trading thread never makes one. Only the background thread calls `write()`, and it batches up to 1 MB at a time.

**Where in the code:**
- [logging.h:80 `LogWriter`](logging.h#L80): the 1 MB output buffer, owned by the background thread.
- [logging.h:142 `LogWriter::writeAll`](logging.h#L142): the only place `::write()` is called.
- [logging.h:792 `Logger::run()`](logging.h#L792): the background loop. It flushes when the buffer fills or when it finds nothing left to drain.

### 3. No locks and no waiting
- Each trading thread has its own ring buffer, so trading threads never compete with each other. There is no mutex, no compare-and-swap loop, and no chance of being descheduled while waiting for a lock.
- The only other party touching a ring is the background thread, and the protocol is wait-free for the producer. It reserves space, writes, and publishes with **one release store**. On x86 that compiles to an ordinary `mov`, with no fence and no `lock` prefix.
- If the ring is full, the message is **dropped and counted** rather than making the trading thread wait. This trades completeness for predictable latency: a logger must never stall an order.

**Where in the code:**
- [logging.h:750 `Logger::localRing()`](logging.h#L750): finds the calling thread's own ring through a `thread_local` cache ([logging.h:710](logging.h#L710)). No lock is taken.
- [logging.h:773 `findOrCreateRing()`](logging.h#L773): the **only** mutex ([logging.h:774](logging.h#L774)). It runs once per thread, on that thread's first call or in `registerThread()`.
- [logging.h:469 `SpscByteRing`](logging.h#L469): the single-producer, single-consumer ring.
- [logging.h:524 `commit()`](logging.h#L524): the one `store(release)` that publishes a record.
- [logging.h:494 `reserve()`](logging.h#L494): if the ring is full it increments the drop counter ([logging.h:505](logging.h#L505)) and returns `nullptr` instead of waiting ([logging.h:506](logging.h#L506)).

### 4. Very little cache-coherence traffic
This is the less obvious part, and it was the old queue's biggest hidden cost. When two cores write to the same cache line, the line has to move between them, which costs roughly 50–100 ns each time.
- **The old queue:** both threads incremented and decremented `num_elements_` for every character, so its cache line moved between cores constantly.
- **The new ring:**
  - Each thread writes only its own index, and the two indices sit on separate 64-byte cache lines.
  - The producer keeps a cached copy of the consumer's position. It reads the real value only when the ring looks full, so the common path never touches the consumer's cache line.
  - The background thread doesn't poll in a tight loop. A constant poll would keep pulling the producer's cache line away from it.

**Where in the code:**
- [logging.h:566 `write_pos_`](logging.h#L566) and [logging.h:572 `read_pos_`](logging.h#L572): each index is `alignas(64)`, so each has its own cache line, and each is written by only one thread.
- [logging.h:567 `cached_read_pos_`](logging.h#L567): the producer's private copy of the consumer's position. It is refreshed only when the ring looks full ([logging.h:502](logging.h#L502)).
- [logging.h:538](logging.h#L538): the consumer's version of the same idea. It re-reads `write_pos_` only when it has caught up.
- [logging.h:824 `IdlePauseBatch`](logging.h#L824): the background thread pauses 32 times between polls.
- [lf_queue.h:86](lf_queue.h#L86) / [lf_queue.h:89](lf_queue.h#L89): the same fix in the generic `LFQueue`. The old code's shared `num_elements_++` / `num_elements_--`, which both threads wrote, has been removed.

### 5. No heap allocation and no page faults
- No `new` or `malloc` on the hot path. The old code allocated a `std::string` inside `ASSERT` for every queue read, and copied `std::string` arguments at every level of recursion.
- The ring is allocated once with `mmap`, pre-faulted with `memset`, `mlock`ed and backed by huge pages. A write therefore never triggers a page fault (a kernel entry, possibly microseconds) and rarely misses the TLB.
- Ring setup happens once per thread, in `registerThread()`, before the latency-critical loop starts.

**Where in the code:**
- [logging.h:476](logging.h#L476): `mmap` of the ring, followed by `MADV_HUGEPAGE` ([logging.h:478](logging.h#L478)), the pre-fault `memset` ([logging.h:479](logging.h#L479)) and `mlock` ([logging.h:480](logging.h#L480)).
- [logging.h:669 `registerThread()`](logging.h#L669): moves ring creation out of the latency-critical path.
- [logging.h:745](logging.h#L745): arguments are taken as `const Args&...` and copied straight into the ring, so no `std::string` temporaries are made.
- [macros.h:13 `ASSERT`](macros.h#L13): now a macro, so the message string is only built when the check fails.

### 6. Work done once instead of per message
- **Compile time:**
  - The placeholder count is checked by a `consteval` constructor.
  - The argument encoding for each call site is fixed by templates.
  - `file:line` is pasted into the format literal by the macro, so it costs nothing at runtime.
  - Calls below `LOG_COMPILED_MIN_LEVEL` are removed entirely, and their arguments are never evaluated.
- **Timestamps:** the hot path reads `rdtsc`, which takes a few ns. Converting it to calendar time happens on the background thread, and the `localtime_r` result is cached and reused for a whole second.
- **Fixed-size arithmetic:** buffer positions use `& mask` instead of `%`, which avoids a division costing about 30 cycles.

**Where in the code:**
- [logging.h:375 `FormatString`](logging.h#L375): its `consteval` constructor ([logging.h:379](logging.h#L379)) calls [`countPlaceholders`](logging.h#L360) at compile time.
- [logging.h:733](logging.h#L733): the record size is a fold expression, a compile-time constant for numeric arguments.
- [logging.h:966 `LOG_AT_LEVEL`](logging.h#L966): pastes `file:line` into the literal. The `if constexpr` at [logging.h:968](logging.h#L968) removes disabled levels.
- [logging.h:415 `rdtsc()`](logging.h#L415): the timestamp taken on the hot path ([logging.h:725](logging.h#L725)).
- [logging.h:578 `TscClock`](logging.h#L578): converts ticks to wall-clock time ([logging.h:598](logging.h#L598)) on the background thread only.
- [logging.h:874 `appendTimestamp()`](logging.h#L874): the once-a-second cache of the formatted date/time ([logging.h:879](logging.h#L879)).
- [logging.h:496](logging.h#L496): `write & mask_` in the ring. [lf_queue.h:25](lf_queue.h#L25) rounds the capacity up to a power of two (`std::bit_ceil`).

### 7. A small, fixed amount of work
A typical message results in:
```
rdtsc → 1 TLS load → size sum (mostly constants) → bounds check against a cached index
     → 32-byte header store + a few memcpy → 1 release store
```
That is about 64 bytes written to one or two cache lines, with almost no branching. Because the work is the same size every time, the typical cost and the worst-case cost are close together. In HFT, a predictable tail often matters more than the average. Measured results are in section 10.

**Where in the code:**
- [logging.h:724–748 `Logger::write()`](logging.h#L724-L748): the whole sequence above, in order: `rdtsc`, `localRing()`, size, `reserve()`, header and argument writes, `commit()`.
- [logging_benchmark.cpp](logging_benchmark.cpp): measures this path on the calling thread.

### The cost is paid elsewhere
None of this makes logging free. The work moves to places where it doesn't hurt:
- **A background thread** does the formatting and I/O. Pin it to a housekeeping core, away from the trading cores (section 9).
- **Memory:** a pre-allocated ring per thread.
- **Weaker guarantees** (section 11):
  - Messages can be dropped when the ring is full.
  - Lines from different threads are only approximately in time order.
  - Records still in memory are lost if the process crashes.

These trade-offs are normal for HFT logging. A logger that guaranteed every message reached disk before returning would by definition be slow.

**Where in the code:**
- [logging.h:792 `run()`](logging.h#L792), [logging.h:838 `drainBatch()`](logging.h#L838) and [logging.h:863 `formatRecord()`](logging.h#L863): where the formatting and I/O work actually happens.
- [logging.h:58 `LoggerConfig`](logging.h#L58): `core_id` pins the background thread, and `per_thread_buffer_bytes` sets the ring size.
- [logging.h:494 `reserve()`](logging.h#L494): the drop-instead-of-wait decision.

---

## 1. Quick start

```cpp
#include "logging.h"

Common::Logger logger("trading.log");          // or Logger(file, LoggerConfig{...})

// In every thread that logs, once, before its latency-critical loop:
logger.registerThread();

// Leveled logging: timestamp, level and file:line are added automatically, plus a trailing newline.
LOG_INFO(logger,  "fill oid:% sym:% px:% qty:%", order_id, symbol, price, qty);
LOG_WARN(logger,  "spread wide bid:% ask:%", bid, ask);
LOG_DEBUG(logger, "book level % size %", lvl, sz);

// The original API still works: no prefix, and you add the newline yourself.
logger.log("%:% %() Processing %\n", __FILE__, __LINE__, __FUNCTION__, request_id);
```

Output:

```
2026-11-06 21:56:08.077765379 INFO  strategy.cpp:52 fill oid:42 sym:ESZ6 px:5012.25 qty:10
```

Format rules:
- `%` is replaced by the next argument.
- `%%` prints a literal `%`.
- The format **must be a string literal**. The logger stores only a pointer to it, never a copy.

If the number of `%` placeholders doesn't match the number of arguments, the code doesn't compile:

```
error: call to non-'constexpr' function '...log_format_error_number_of_placeholders_does_not_match_number_of_arguments()'
```

---

## 2. Architecture

```
 trading thread A         trading thread B              background logger thread
 ────────────────         ────────────────              ────────────────────────
 LOG_INFO(...)            LOG_INFO(...)
   │ rdtsc                  │ rdtsc
   │ reserve N bytes        │ reserve N bytes
   │ write header+args      │ write header+args
   │ release-store          │ release-store
   ▼                        ▼
 ┌───────────────┐       ┌───────────────┐
 │ SPSC ring (A) │       │ SPSC ring (B) │   ...up to 64 producer threads
 └──────┬────────┘       └──────┬────────┘
        └──────────┬────────────┘
                   ▼
       pick the oldest head record (by tsc) across all rings
       → decode args → format with std::to_chars into a 1 MB buffer
       → write() to the file when the buffer fills or the thread goes idle
```

The design has three parts, each covered in a section below:

1. **Per-thread SPSC rings (section 4).** Each producer thread has its own ring buffer, so producers never contend with each other and need no locks or compare-and-swap. The only shared state is between one producer and the background thread.
2. **Deferred formatting (section 3).** The hot path copies bytes and nothing else. Turning numbers into text, which is the expensive part, runs on the background thread.
3. **Compile-time work (section 5).** Placeholder counting, argument encoding and the `file:line` prefix are all worked out at compile time.

---

## 3. The hot path in detail

[`Logger::write()`](logging.h) does these steps on the calling thread:

1. **`rdtsc`.** Reads the CPU cycle counter (a few ns). Converting it to wall-clock time happens later, on the background thread.
2. **`localRing()`.** Finds this thread's ring. This is one load from a `thread_local` cache that is constant-initialised, so the access is a plain `%fs`-relative load with no init guard. A thread's first call goes through the slow path described in section 6.
3. **Size the record.** A fold expression over `LogCodec<T>::size(arg)`. For numbers this is a compile-time constant. For strings it is `4 + length`.
4. **`reserve(n)`.** Moves the ring's write position forward. It reads the consumer's read position only when its *cached* copy says the ring might be full, so in the common case it never touches the consumer's cache line.
5. **Write the 32-byte header and `memcpy` each argument's raw bytes.**
6. **`commit()`.** One `store(release)` of the new write position.

There are no locks, syscalls, heap allocations, `std::string` temporaries, virtual calls or formatting on this path.

### Record layout

Every record is 8-byte aligned:

```
offset  size  field
0       4     size     total record bytes (multiple of 8)
4       1     kind     Message | Wrap
5       1     level    Debug/Info/Warn/Error/Raw
6       2     reserved
8       8     tsc      rdtsc at the call site
16      8     fmt      pointer to the format string literal
24      8     decode   pointer to Decoder<ArgTypes...>::run
32      ...   encoded arguments
```

The `decode` pointer is what makes this work. `Decoder<int, std::string, double>` is a separate template instantiation for each argument-type list, and it knows how to walk that record's payload. Because each record carries its own decoder, the background thread needs no runtime type tags and no switch over types.

### How arguments are encoded (`LogCodec<T>`)

| Type | Hot path | Printed as |
|---|---|---|
| `int`, `long`, `uint64_t`, `int8_t`, … | `memcpy` of `sizeof(T)` bytes | decimal (`int8_t`/`uint8_t` print as numbers, not characters) |
| `float`, `double`, `long double` | `memcpy` | shortest round-trip representation (`std::to_chars`) |
| `char` | 1 byte | the character |
| `bool` | 1 byte | `true` / `false` |
| enums | `memcpy` | underlying integer value |
| `T*` (non-char), `nullptr` | `memcpy` of the pointer | `0x7ffe…` / `nullptr` |
| `const char*`, `char*` | `strlen` + `memcpy` of the characters (`(null)` if null) | the string |
| string literals / `char[N]` / `__FUNCTION__` | `strnlen(s, N)` + `memcpy` | the string |
| `std::string`, `std::string_view` | `memcpy` of the characters | the string |
| your own trivially copyable struct (see section 7) | `memcpy` of the object | your `formatLog()` |

Strings are copied, not stored as pointers, because the caller's buffer may be gone by the time the background thread runs. Strings longer than 4096 bytes (`log_detail::MaxStringBytes`) are truncated. A type with no codec fails to compile.

---

## 4. The SPSC byte ring (`log_detail::SpscByteRing`)

This ring holds **variable-length records** in a power-of-two-sized byte buffer.

- **Positions are monotonically increasing 64-bit byte counts.** The offset into the buffer is `pos & mask_`, so there is no modulo or `div` instruction. Used space is `write_pos - read_pos`, so there is no shared counter.
- **Cache-line separation.** The fields are grouped as follows:
  - `buf_`, `capacity_` and `mask_` never change after construction.
  - `write_pos_`, the producer's cached copy of `read_pos`, and the drop counter sit on the producer's cache line.
  - `read_pos_` and the consumer's cached copy of `write_pos` sit on the consumer's cache line.

  Each side writes only to its own line.
- **Cached opposite index.** The producer re-reads `read_pos_` only when it thinks the ring is full. The consumer re-reads `write_pos_` only when it thinks the ring is empty.
- **Wrapping.** A record must be contiguous in memory. If the space left before the end of the buffer is too small, the producer writes an 8-byte `Wrap` marker covering that space and puts the record at offset 0. The marker and the record are published by the same release store.
- **Memory.**
  - The buffer is allocated with `mmap`, with `MADV_HUGEPAGE` (fewer TLB misses).
  - It is pre-faulted with `memset`, so the first write to each page never page-faults on the hot path.
  - It is `mlock`ed if `RLIMIT_MEMLOCK` allows, so it is never swapped out.
- **Prefetch.** After each reservation, the producer issues prefetch-for-write hints for the next two cache lines.
- **When the ring is full, the message is dropped.** The producer increments its drop counter, which it alone writes, so no locked instruction is needed. The logging thread is never blocked. The background thread writes a line like this into the log:
  ```
  ... WARN  [Logger] dropped 17 messages because a producer buffer was full (total 17)
  ```
  You can also read the count with `logger.droppedCount()`.

`lf_queue.h` (the generic fixed-element `LFQueue<T>` used by the rest of the code base) got the same treatment, with its API unchanged:
- The shared `num_elements_` counter is gone.
- The indices are on separate cache lines.
- Memory ordering is acquire/release.
- Capacity is rounded up to a power of two and indexed with a mask.
- New `full()` and `capacity()` helpers were added.

---

## 5. Compile-time work

- **`FormatString<Args...>`** has a `consteval` constructor. It counts the unescaped `%` in the literal and causes a compile error if the count differs from `sizeof...(Args)`. It is used as `FormatString<std::type_identity_t<Args>...>`, so the argument types are deduced only from the arguments.
- **The `LOG_*` macros** paste `file:line ` into the format literal: `__FILE_NAME__ ":" "52" " " fmt`. The source location therefore costs nothing at runtime: it is part of the same static string.
- **`LOG_COMPILED_MIN_LEVEL`** (0 = DEBUG … 3 = ERROR) removes lower-level calls completely through `if constexpr`. Their arguments are not even evaluated. Example: `-DLOG_COMPILED_MIN_LEVEL=1` strips every `LOG_DEBUG`.
- **The runtime level filter**, `logger.setMinLevel(LogLevel::Warn)`, costs one relaxed load and a branch (< 1 ns).

---

## 6. Threads and registration

- **First call from a thread.** The first `log()` from a thread finds no ring in its thread-local cache. It takes a mutex, creates a ring (mmap + pre-fault + mlock of `per_thread_buffer_bytes`), and caches it. **This takes milliseconds**, so call `logger.registerThread()` at thread startup, before the latency-critical loop.
- **Cache keying.** The thread-local cache holds 4 (logger id, ring) entries, so one thread can log to several `Logger`s quickly. Entries are keyed by a unique id per `Logger` instance, never by its address, so a new `Logger` created at the address of a destroyed one can't pick up stale rings.
- **Thread limit.** At most `MaxProducerThreads` = 64 threads can log to one logger. Calls from threads beyond that are dropped and counted.
- **Ring lifetime.** A ring stays alive until its `Logger` is destroyed. If a thread exits and the OS later reuses its thread id, the new thread adopts the old ring. That is safe because only one producer uses a ring at any time.

---

## 7. Logging your own types

To log an order, market update or other type by value:
1. Make the type trivially copyable.
2. Give it a `formatLog` overload in its own namespace, where argument-dependent lookup will find it.

```cpp
namespace Exchange {
  struct MEOrder { uint64_t order_id; int64_t price; uint32_t qty; Side side; };

  inline void formatLog(Common::LogWriter& w, const MEOrder& o) {
    w.append(std::string_view("MEOrder{oid:"));
    w.appendNumber(o.order_id);
    w.append(std::string_view(" px:"));
    w.appendNumber(o.price);
    w.append('}');
  }
}

LOG_INFO(logger, "added %", order);   // hot path: memcpy of sizeof(MEOrder) bytes
```

`formatLog` runs on the background thread, so it may be as slow as it likes. Compare this with calling `order.toString()` at the call site, which builds a `std::string` on the trading thread.

For full control over a type, you can instead specialise `Common::LogCodec<T>` with `size` / `encode` / `decode`.

---

## 8. The background thread

`Logger::run()` repeats these steps:

1. **`drainBatch()`.** Writes up to 1024 records. Each time it peeks the head record of every ring and takes the one with the **smallest tsc**, so the file is in timestamp order across threads. It calls that record's `decode` function and pops it.
2. **Format into `LogWriter`.** `LogWriter` is a 1 MB buffer. Numbers go through `std::to_chars`, which is locale-free and has no virtual calls. The `YYYY-MM-DD HH:MM:SS` part of the timestamp is cached and only recomputed with `localtime_r` + `strftime` once per second. The nanosecond part is written digit by digit.
3. **Flush and housekeeping when idle.** The buffer is `write()`n to the file when it fills, and also as soon as the thread finds nothing to drain. Housekeeping also reports drops and, once a second, re-calibrates the TSC rate.
4. **Idle strategy.** With `busy_spin = true`, the thread spins with `pause` forever. Otherwise it spins about 2000 rounds, then yields, then sleeps 50 µs. Each spin round pauses 32 times before polling the rings again. Polling `write_pos_` in a tight loop would keep pulling the producers' cache lines into shared state, and every commit would then pay a cross-core invalidation.

The **destructor** sets `running_ = false` and joins the thread. The thread checks that flag *before* each drain, so it exits only after a drain finds every ring empty. Every message logged before the destructor ran is written out.

### Timestamps

The TSC rate is calibrated at construction (20 ms) and refined every second against `system_clock`, always measured from the same starting point, so precision improves over time. Conversion is `base_ns + (tsc - base_tsc) * ns_per_tick`, done only on the background thread.

This requires an invariant TSC. Check that `constant_tsc` and `nonstop_tsc` appear in `/proc/cpuinfo`; every modern x86 server CPU has them. On non-x86 builds the code falls back to `steady_clock`.

---

## 9. Configuration and deployment tuning

```cpp
Common::LoggerConfig cfg;
cfg.core_id = 3;                                  // pin the logger thread to a housekeeping core
cfg.per_thread_buffer_bytes = 16 * 1024 * 1024;   // burst absorption per producer thread
cfg.busy_spin = false;                            // true only if the logger has a core to itself
cfg.min_level = Common::LogLevel::Info;
Common::Logger logger("trading.log", cfg);
```

Production checklist:
- **Pin the logger thread** (`core_id`) to a housekeeping core. Do not use a trading core or its hyper-thread sibling. Unpinned, the scheduler can put it on the trading core, where it steals time and evicts that core's caches.
- **Isolate trading cores** (`isolcpus` / `nohz_full`), and keep the logger and the disk-interrupt handling on the other cores.
- **Size the ring for the worst burst:** peak message rate × the longest stall of the background thread or disk, plus a margin. Watch `droppedCount()`.
- **Raise the locked-memory limit** (`ulimit -l`) so `mlock` succeeds.
- **Don't format at the call site.** Existing call sites in the repo that pass `Common::getCurrentTimeStr(&time_str)` or `x.toString()` still pay that cost on the hot path. The logger now adds timestamps itself (use `LOG_*`), and structs should use `formatLog` (section 7).

---

## 10. Benchmark

`logging_benchmark.cpp` is built as the `logging_benchmark` target and run as `./logging_benchmark [n]`. It times each call on the calling thread with `rdtsc` and pauses briefly every 64 messages. The message is the 5-argument `fill oid:% sym:% px:% qty:% side:%`.

Measured on an Intel i7-1360P laptop (not tuned: no isolated cores, frequency scaling on), with 200k messages:

| | p50 | p90 | p99 | p99.9 |
|---|---|---|---|---|
| Old logger (same message, `log()` API) | 10–19 µs | 13–24 µs | 19–42 µs | 23–54 µs |
| New logger | ~62 ns | ~75 ns | ~130 ns | 0.7–2 µs |

In a tight loop with no per-call timer, the same message takes **~18 ns/call** (a single integer, ~16 ns). A call filtered out by the runtime level check takes **< 1 ns**.

About the numbers:
- The per-call p50 includes the cost of the two timing `rdtsc` reads (about 40 ticks) wrapped around the call's own `rdtsc`.
- The p99.9 and max values on this machine come from OS noise: interrupts, migrations and frequency changes. On a server with isolated cores, expect the tail to tighten a lot.

---

## 11. Known limitations and next steps

- **Crash safety.** If the process crashes, records still in the rings or the 1 MB buffer are lost. Options: a fatal-signal handler that drains the rings (it must be async-signal-safe), or making the rings themselves `mmap`'d files.
- **Cross-thread ordering is best effort.** A record becomes visible only when it is committed. If thread A takes its timestamp first but commits after thread B, the lines can appear out of order. Each line carries its own timestamp, so you can sort later if exact order matters.
- **Binary logging.** The background thread still produces text. The fastest systems write the raw records to disk and decode them offline (NanoLog, binlog). That would remove formatting cost entirely and make the file much smaller.
- **Startup cost.** `createAndStartThread` still sleeps 1 s (kept so existing callers behave the same), and calibration adds 20 ms to the `Logger` constructor. Neither affects the hot path.
- **Bounded strings.** `const char*` arguments are scanned twice (`size` + `encode`). Strings over 4096 bytes are truncated.
- **Removed API.** `pushValue()` is gone. Use `log()` / `LOG_*`.

---

## 12. Other files changed alongside the logger

- **[`lf_queue.h`](lf_queue.h).** SPSC queue internals rewritten (section 4); the API is unchanged.
- **[`thread_utils.h`](thread_utils.h).** `createAndStartThread` now captures the name, function and arguments **by value**. The old `[&]` capture made the new thread read temporaries that had already been destroyed. Only the 1 s sleep was hiding that bug.
- **[`macros.h`](macros.h).** `ASSERT` is now a macro, so its message (usually a `std::string` concatenation that allocates) is built only when the assertion fails. As a function, the message was built on every call. `FATAL` is now `[[noreturn]]`.
- **[`logging_example.cpp`](logging_example.cpp).** Shows the old API, the leveled macros, custom types and multi-threaded logging.
- **[`logging_benchmark.cpp`](logging_benchmark.cpp)** + **[`CMakeLists.txt`](CMakeLists.txt).** New benchmark target.

---

## 13. C++ techniques used in `logging.h`

This section lists the C++ techniques in [`logging.h`](logging.h), grouped by purpose. Each row says what the technique is, where it's used, and why it helps here.

**If you're learning:** start with **1, 2, 3, 19, 20 and 22**. Those six explain most of why the logger is fast. Then move to **6, 8 and 12**, which explain most of why the code looks unusual.

### 13.1 Templates and compile-time programming

| # | Technique | Where | What it is | Benefit here |
|---|---|---|---|---|
| 1 | **Variadic templates / parameter packs** | `log(fmt, const Args&... args)` [L639](logging.h#L639), `write()` [L724](logging.h#L724) | A function that takes any number of arguments of any types, with all types known at compile time | One `log()` handles every call signature. The compiler generates code specialized for each call site, so no `va_list`, type tags or runtime dispatch are needed |
| 2 | **Fold expressions** (C++17) | `(0 + ... + size(args))` [L733](logging.h#L733), `((cursor = encode(cursor, args)), ...)` [L745](logging.h#L745) | Applies an operator across every element of a pack in one expression | Sums the record size and encodes every argument with no recursion and no loop. The result compiles to straight-line code |
| 3 | **Class template full specialization** | `template<> struct LogCodec<char>`, `<bool>`, `<std::string_view>` [L326](logging.h#L326) | A custom version of a template for one exact type | Each type gets its own copy and format rules, chosen at compile time |
| 4 | **Class template partial specialization** | `LogCodec<char[N]>` [L315](logging.h#L315), `Decoder<A, Rest...>` [L457](logging.h#L457) | A custom version for a family of types (any `N`, any first type) | Covers every string-literal length and lets `Decoder` peel off one type at a time. Function templates can't be partially specialized, which is why these are structs |
| 5 | **Undefined primary template** | `template<typename T> struct LogCodec;` [L168](logging.h#L168) | Declared, never defined | Logging an unsupported type fails **at compile time**, not at runtime |
| 6 | **C++20 concepts and `requires`** | `LogInteger` [L211](logging.h#L211), `CharPointer` [L216](logging.h#L216), `LogFormattable` [L223](logging.h#L223), `requires std::is_enum_v<T>` | Named compile-time predicates on types, used to constrain templates | Picks the right codec for every integer, enum or pointer without writing out each type. The constraints don't overlap, so overload resolution is never ambiguous. Error messages are clearer than with SFINAE |
| 7 | **`requires` expression** | `requires(LogWriter& w, const T& t) { formatLog(w, t); }` [L224](logging.h#L224) | Checks at compile time whether an expression is valid | Detects that a user type has a `formatLog()` and opts it in automatically, with no registration step |
| 8 | **`consteval`** (C++20) | `FormatString` constructor [L379](logging.h#L379) | A function that **must** run at compile time | Counts the `%` placeholders during compilation. A mismatch is a compile error, and there is zero runtime cost |
| 9 | **Calling a non-`constexpr` function to force a compile error** | `log_format_error_...()` [L357](logging.h#L357) | Constant evaluation cannot call it, so the compiler reports it | The function's name serves as a readable error message (the same trick `std::format` uses) |
| 10 | **`std::type_identity_t` (non-deduced context)** | `FormatString<std::type_identity_t<Args>...>` [L639](logging.h#L639) | Stops the compiler from deducing `Args` from that parameter | `Args` is deduced only from the real arguments, and the format string is then checked against them. Without this, deduction would conflict |
| 11 | **Reference-to-array parameter** | `const char (&s)[N]` [L379](logging.h#L379), [L315](logging.h#L315) | Takes an array without decaying it to a pointer | Keeps the length `N` at compile time, for counting placeholders and for `strnlen(s, N)` |
| 12 | **Type erasure through template-generated function pointers** | `&Decoder<Args...>::run` stored in `RecordHeader::decode` [L742](logging.h#L742) | The compiler generates one decoder per argument-type list, and its address is stored as data | The background thread calls the right decoder without knowing the types: no `switch`, no type-tag bytes, no virtual functions or vtables |
| 13 | **Recursive template instantiation** | `Decoder<A, Rest...>::run` calls `Decoder<Rest...>::run` [L461](logging.h#L461) | Compile-time recursion over a type list, ending at `Decoder<>` | Decodes each argument in order. It compiles to a flat sequence of calls |
| 14 | **Type traits** | `std::remove_cvref_t`, `is_trivially_copyable_v`, `is_enum_v`, `underlying_type_t`, `is_floating_point_v` | Standard compile-time queries and transformations on types | `const std::string&` and `std::string` map to the same codec. `memcpy` is only allowed for types where it is safe |
| 15 | **`if constexpr`** | `LOG_AT_LEVEL` [L968](logging.h#L968) | A branch evaluated at compile time; the discarded side is never run | Disabled log levels disappear completely, including evaluation of their arguments |
| 16 | **`static_assert`** | `sizeof(RecordHeader) == 32` [L413](logging.h#L413) | A compile-time check | Locks in the record layout the code depends on, so an accidental padding change can't slip through |
| 17 | **Argument-dependent lookup (ADL) as a customization point** | `formatLog(w, t)` [L345](logging.h#L345) | An unqualified call also searches the argument's namespace | Users add `formatLog` next to their own type and never modify `logging.h` (the same pattern `swap` and `std::hash` use) |
| 18 | **Inheritance for code reuse (mixin base)** | `LogCodec<T> : TrivialCopy<T>` [L176](logging.h#L176), `LogCodec<std::string> : LogCodec<std::string_view>` [L337](logging.h#L337) | A base struct supplies shared static functions | Writes `size`/`encode` once for every plain-data type. `std::string` reuses the `string_view` codec unchanged |

### 13.2 Memory and concurrency

| # | Technique | Where | What it is | Benefit here |
|---|---|---|---|---|
| 19 | **`std::atomic` with explicit memory orders** | `commit()` release [L524](logging.h#L524), `peek()` acquire [L538](logging.h#L538), relaxed own-index loads | Use the weakest ordering that is still correct, instead of the seq_cst default | On x86, a release store is a plain `mov`, while seq_cst is `xchg`, a full fence. That cost is saved on every message |
| 20 | **Acquire/release publication** | the producer writes the record, then release-stores `write_pos_`; the consumer acquire-loads it, then reads the record | A happens-before handoff between two threads | The consumer is guaranteed to see the complete record without any lock |
| 21 | **Single-writer variables** | `dropped_.store(load() + 1)` [L505](logging.h#L505), `write_pos_`, `read_pos_` | Each variable has exactly one writing thread | Plain load + store replaces `fetch_add`, so no `lock` prefix is needed |
| 22 | **`alignas(64)` cache-line separation** | `write_pos_` [L566](logging.h#L566), `read_pos_` [L572](logging.h#L572), `Logger` member groups | Places data on separate 64-byte cache lines | Avoids **false sharing**: one core's writes don't invalidate the other core's cached line |
| 23 | **Cached copy of the other thread's index** | `cached_read_pos_` [L567](logging.h#L567), `cached_write_pos_` [L538](logging.h#L538) | Keeps a private, possibly stale copy and refreshes it only when necessary | In the common case the hot path never reads the other core's cache line |
| 24 | **Monotonic 64-bit positions + power-of-two masking** | `pos & mask_` [L496](logging.h#L496), `std::bit_ceil` [L473](logging.h#L473) | Positions only ever increase, and the buffer offset is `pos & (size-1)` | No modulo `div` (about 30 cycles), no shared counter, and full vs empty is a simple subtraction |
| 25 | **`thread_local` with constant initialization** | `static thread_local TlsEntry cache[4] = {}` [L710](logging.h#L710) | Per-thread storage; a trivial type with a constant initializer needs no init guard | Finding the thread's ring is one `%fs`-relative load |
| 26 | **Placement `new`** | `new(record) RecordHeader{...}` [L740](logging.h#L740) | Constructs an object in memory you already own | Writes the header directly into the ring, with no allocation and no copy |
| 27 | **`std::memcpy` for type punning / unaligned access** | every codec `encode`/`decode`, e.g. [L179](logging.h#L179) | Copies object bytes instead of casting pointers | Arguments are packed tightly at arbitrary alignment. `memcpy` is the well-defined way to do this, and compilers turn it into one `mov` |
| 28 | **`std::launder`** | `LogFormattable` decode [L345](logging.h#L345) | Tells the compiler that a pointer to bytes now refers to a `T` object | Makes reading a user struct back from raw bytes well-defined |
| 29 | **Brace (aggregate) initialization** | `RecordHeader{{size, kind, level, 0}, tsc, fmt, decode}` [L740](logging.h#L740) | Initializes the struct's fields directly | Compiles to plain stores with no constructor overhead |

### 13.3 Low-level performance tools

| # | Technique | Where | What it is | Benefit here |
|---|---|---|---|---|
| 30 | **Branch hints `__builtin_expect`** | `LIKELY` / `UNLIKELY` [macros.h:7](macros.h#L7) | Tells the compiler which side of a branch is the common one | Keeps the fast path as straight-line code, with the full/drop/slow-path branches moved out of line |
| 31 | **Intrinsics `__rdtsc`, `_mm_pause`** | [L415](logging.h#L415), [L423](logging.h#L423) | Single CPU instructions exposed as functions | A timestamp in a few ns with no syscall. `pause` makes the background thread's spin loop cheaper for its hyper-thread sibling |
| 32 | **`__builtin_prefetch`** | [L517](logging.h#L517) | Asks the CPU to fetch a cache line early | Starts fetching the next record's cache lines ahead of time. It made no measurable difference on this machine (see section 10) |
| 33 | **`std::to_chars`** (C++17) | `LogWriter::appendNumber` [L107](logging.h#L107) | Converts numbers to text with no locale, no allocation and no exceptions | Several times faster than `ostream <<` or `snprintf`; produces the shortest round-trip form for floats |
| 34 | **`std::string_view`** | codecs [L326](logging.h#L326), `LogWriter::append` | A non-owning (pointer, length) view of a string | Passes strings around without copies or allocations |
| 35 | **`noexcept`** | almost every function | A promise that the function never throws | No exception-handling paths in the hot code, and a clear contract that logging never throws |
| 36 | **OS memory control: `mmap`, `madvise(MADV_HUGEPAGE)`, `mlock`, pre-fault `memset`** | [L476-L480](logging.h#L476-L480) | Explicit control over how the ring's memory is mapped | No page faults or swapping on the hot path, and fewer TLB misses |

### 13.4 Resource management and API design

| # | Technique | Where | What it is | Benefit here |
|---|---|---|---|---|
| 37 | **RAII** | `std::unique_ptr<char[]>` [L156](logging.h#L156), `std::lock_guard` [L774](logging.h#L774), the `Logger` destructor drains and joins | Each resource is tied to an object's lifetime | Shutdown can't leak memory or lose messages, and the mutex can't be left locked |
| 38 | **Deleted copy/move operations** | `Logger(const Logger&) = delete` [L684](logging.h#L684) | Forbids copying | Rings and the background thread are owned exactly once; an accidental copy is a compile error |
| 39 | **Preprocessor: stringify + literal concatenation** | `LOG_STRINGIFY(__LINE__)`, `__FILE_NAME__ ":" "52" " " fmt` [L963-L976](logging.h#L963-L976) | `#x` turns a macro argument into a string; adjacent literals are merged by the compiler | `file:line` becomes part of the format literal, so the source location costs nothing at runtime |
| 40 | **`__VA_OPT__`** (C++20) | `LOG_*` macros [L966](logging.h#L966) | Inserts the comma only when there are variadic arguments | `LOG_INFO(l, "text")` and `LOG_INFO(l, "x:%", x)` both work, with no GNU `##__VA_ARGS__` extension |
| 41 | **`do { ... } while (0)` macro wrapper** | `LOG_AT_LEVEL` [L966](logging.h#L966), `ASSERT` [macros.h:13](macros.h#L13) | Makes a multi-statement macro act like one statement | Safe inside `if/else` without braces |

### 13.5 How they combine

- **Compile-time techniques** (1–18, 39–40): everything that can be known before the program runs is worked out by the compiler. That covers types, sizes, format checking, file:line and level filtering.
- **Concurrency techniques** (19–25): the necessary sharing between threads costs one plain store and almost no cache-line traffic.
- **Memory techniques** (26–29, 36): bytes go straight into pre-faulted memory, with no allocation and no undefined behavior.
- **Low-level tools** (30–35): the remaining instructions are cheap and predictable.

> Line links point at the current version of `logging.h`. They will shift as the file is edited.
