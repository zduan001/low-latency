#pragma once

// Low latency asynchronous logger. See logging_readme.md for the full design write-up.
//
// Hot path (the thread calling log()):
//   rdtsc  ->  reserve one variable-length record in this thread's own SPSC byte ring
//          ->  write a 32 byte header (timestamp, format-string pointer, decoder function pointer)
//          ->  memcpy the raw argument bytes
//          ->  one release store to publish
// No formatting, no locks, no syscalls, no heap allocations, no per-character work.
//
// Background thread: merges all per-thread rings in timestamp order, formats the text with
// std::to_chars into a 1MB buffer and write()s it to the file.

#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#if defined(__x86_64__) || defined(__i386__)
#include <x86intrin.h>
#endif

#include "macros.h"
#include "thread_utils.h"
#include "time_utils.h"

namespace Common {
  // ===================================================================================================
  // Configuration
  // ===================================================================================================

  enum class LogLevel : uint8_t {
    Debug = 0,
    Info = 1,
    Warn = 2,
    Error = 3,
    Raw = 4 // log(): no timestamp/level prefix and no automatic newline (the original behaviour).
  };

  struct LoggerConfig {
    /// Core to pin the background logger thread to. -1 = not pinned.
    /// In production pin it to a housekeeping core, NOT a trading core or its hyper-thread sibling.
    int core_id = -1;

    /// Size of the ring buffer each producer thread gets (rounded up to a power of two, min 64KB).
    /// It only has to absorb bursts while the background thread catches up.
    std::size_t per_thread_buffer_bytes = 4 * 1024 * 1024;

    /// true : background thread spins forever with `pause` (lowest log latency, burns a whole core).
    /// false: spins briefly, then yields, then sleeps 50us when idle.
    /// Either way the producer never waits on the background thread.
    bool busy_spin = false;

    /// Runtime level filter for logDebug()/logInfo()/... (log() is always written).
    LogLevel min_level = LogLevel::Debug;
  };

  // ===================================================================================================
  // LogWriter - background-thread text buffer. Also the interface for user-defined formatters.
  // ===================================================================================================

  class LogWriter final {
  public:
    explicit LogWriter(int fd) noexcept : fd_(fd), buf_(new char[Capacity]) {}

    void append(char c) noexcept {
      if (UNLIKELY(len_ == Capacity))
        flush();
      buf_[len_++] = c;
    }

    void append(const char *p, std::size_t n) noexcept {
      if (UNLIKELY(n > Capacity - len_)) {
        flush();
        if (n > Capacity) {
          writeAll(p, n);
          return;
        }
      }
      std::memcpy(buf_.get() + len_, p, n);
      len_ += n;
    }

    void append(std::string_view s) noexcept { append(s.data(), s.size()); }

    /// Integers and floating point values via std::to_chars (no locale, no virtual calls).
    /// Floating point uses the shortest representation that round-trips.
    template<typename T>
    void appendNumber(T value) noexcept {
      reserve(MaxNumberChars);
      const auto [ptr, ec] = std::to_chars(buf_.get() + len_, buf_.get() + Capacity, value);
      if (LIKELY(ec == std::errc()))
        len_ = static_cast<std::size_t>(ptr - buf_.get());
    }

    void appendHex(uintptr_t value) noexcept {
      reserve(MaxNumberChars);
      buf_[len_++] = '0';
      buf_[len_++] = 'x';
      const auto [ptr, ec] = std::to_chars(buf_.get() + len_, buf_.get() + Capacity, value, 16);
      if (LIKELY(ec == std::errc()))
        len_ = static_cast<std::size_t>(ptr - buf_.get());
    }

    void flush() noexcept {
      if (len_) {
        writeAll(buf_.get(), len_);
        len_ = 0;
      }
    }

    LogWriter(const LogWriter &) = delete;
    LogWriter &operator=(const LogWriter &) = delete;

  private:
    static constexpr std::size_t Capacity = 1 << 20;
    static constexpr std::size_t MaxNumberChars = 128;

    void reserve(std::size_t n) noexcept {
      if (UNLIKELY(Capacity - len_ < n))
        flush();
    }

    void writeAll(const char *p, std::size_t n) noexcept {
      while (n) {
        const auto written = ::write(fd_, p, n);
        if (written < 0) {
          if (errno == EINTR)
            continue;
          return; // disk full / closed fd: nothing sensible to do from the logger thread.
        }
        p += written;
        n -= static_cast<std::size_t>(written);
      }
    }

    int fd_;
    std::unique_ptr<char[]> buf_;
    std::size_t len_ = 0;
  };

  // ===================================================================================================
  // LogCodec<T> - how a type is copied on the hot path and formatted on the background thread.
  //   static size_t       size  (const T&)                    bytes encode() will write
  //   static std::byte*   encode(std::byte*, const T&)        hot path: raw copy, NO formatting
  //   static const byte*  decode(const std::byte*, LogWriter&) background thread: format as text
  // A type with no LogCodec fails to compile.
  // ===================================================================================================

  template<typename T>
  struct LogCodec;

  namespace log_detail {
    /// Longer strings are truncated. Keeps every record far smaller than the ring.
    constexpr std::size_t MaxStringBytes = 4096;

    template<typename T>
    struct TrivialCopy {
      static constexpr std::size_t size(const T &) noexcept { return sizeof(T); }

      static std::byte *encode(std::byte *p, const T &v) noexcept {
        std::memcpy(p, &v, sizeof(T));
        return p + sizeof(T);
      }

      static const std::byte *load(const std::byte *p, T &out) noexcept {
        std::memcpy(&out, p, sizeof(T));
        return p + sizeof(T);
      }
    };

    struct StringCodec {
      static std::size_t size(std::size_t len) noexcept {
        return sizeof(uint32_t) + (len < MaxStringBytes ? len : MaxStringBytes);
      }

      static std::byte *encode(std::byte *p, const char *s, std::size_t len) noexcept {
        const auto n = static_cast<uint32_t>(len < MaxStringBytes ? len : MaxStringBytes);
        std::memcpy(p, &n, sizeof(n));
        std::memcpy(p + sizeof(n), s, n);
        return p + sizeof(n) + n;
      }

      static const std::byte *decode(const std::byte *p, LogWriter &w) noexcept {
        uint32_t n;
        std::memcpy(&n, p, sizeof(n));
        w.append(reinterpret_cast<const char *>(p + sizeof(n)), n);
        return p + sizeof(n) + n;
      }
    };

    template<typename T>
    concept LogInteger = std::is_integral_v<T> && !std::is_same_v<T, bool> && !std::is_same_v<T, char> &&
                         !std::is_same_v<T, wchar_t> && !std::is_same_v<T, char8_t> &&
                         !std::is_same_v<T, char16_t> && !std::is_same_v<T, char32_t>;

    template<typename T>
    concept CharPointer = std::is_same_v<T, const char *> || std::is_same_v<T, char *>;
  }

  /// Opt-in for your own types (orders, market updates, ...): make the type trivially copyable and
  /// provide `void formatLog(Common::LogWriter&, const YourType&)` in the type's namespace.
  /// The hot path then memcpy's the object; formatLog() runs later on the background thread.
  template<typename T>
  concept LogFormattable = std::is_class_v<T> && std::is_trivially_copyable_v<T> &&
                           requires(LogWriter &w, const T &t) { formatLog(w, t); };

  template<>
  struct LogCodec<char> : log_detail::TrivialCopy<char> {
    static const std::byte *decode(const std::byte *p, LogWriter &w) noexcept {
      char c;
      p = load(p, c);
      w.append(c);
      return p;
    }
  };

  template<>
  struct LogCodec<bool> : log_detail::TrivialCopy<bool> {
    static const std::byte *decode(const std::byte *p, LogWriter &w) noexcept {
      bool b;
      p = load(p, b);
      w.append(b ? std::string_view("true") : std::string_view("false"));
      return p;
    }
  };

  /// int, long, uint64_t, int8_t (printed as a number, not a character), ...
  template<log_detail::LogInteger T>
  struct LogCodec<T> : log_detail::TrivialCopy<T> {
    static const std::byte *decode(const std::byte *p, LogWriter &w) noexcept {
      T v;
      p = log_detail::TrivialCopy<T>::load(p, v);
      w.appendNumber(v);
      return p;
    }
  };

  template<typename T> requires std::is_floating_point_v<T>
  struct LogCodec<T> : log_detail::TrivialCopy<T> {
    static const std::byte *decode(const std::byte *p, LogWriter &w) noexcept {
      T v;
      p = log_detail::TrivialCopy<T>::load(p, v);
      w.appendNumber(v);
      return p;
    }
  };

  /// Enums print their underlying integer value.
  template<typename T> requires std::is_enum_v<T>
  struct LogCodec<T> : log_detail::TrivialCopy<T> {
    static const std::byte *decode(const std::byte *p, LogWriter &w) noexcept {
      T v;
      p = log_detail::TrivialCopy<T>::load(p, v);
      w.appendNumber(static_cast<std::underlying_type_t<T>>(v));
      return p;
    }
  };

  /// Non-char pointers print as hex addresses.
  template<typename T> requires (std::is_pointer_v<T> && !log_detail::CharPointer<T>)
  struct LogCodec<T> : log_detail::TrivialCopy<T> {
    static const std::byte *decode(const std::byte *p, LogWriter &w) noexcept {
      T v;
      p = log_detail::TrivialCopy<T>::load(p, v);
      w.appendHex(reinterpret_cast<uintptr_t>(v));
      return p;
    }
  };

  template<>
  struct LogCodec<std::nullptr_t> {
    static constexpr std::size_t size(std::nullptr_t) noexcept { return 0; }
    static std::byte *encode(std::byte *p, std::nullptr_t) noexcept { return p; }
    static const std::byte *decode(const std::byte *p, LogWriter &w) noexcept {
      w.append(std::string_view("nullptr"));
      return p;
    }
  };

  /// const char* / char*: the characters are copied (the pointer may not outlive the call).
  template<log_detail::CharPointer T>
  struct LogCodec<T> {
    static std::size_t size(const char *s) noexcept {
      return log_detail::StringCodec::size(s ? std::strlen(s) : 6);
    }
    static std::byte *encode(std::byte *p, const char *s) noexcept {
      return s ? log_detail::StringCodec::encode(p, s, std::strlen(s)) : log_detail::StringCodec::encode(p, "(null)", 6);
    }
    static const std::byte *decode(const std::byte *p, LogWriter &w) noexcept {
      return log_detail::StringCodec::decode(p, w);
    }
  };

  /// String literals, __FUNCTION__, char buffers. The length bound N is known at compile time.
  template<std::size_t N>
  struct LogCodec<char[N]> {
    static std::size_t size(const char (&s)[N]) noexcept { return log_detail::StringCodec::size(strnlen(s, N)); }
    static std::byte *encode(std::byte *p, const char (&s)[N]) noexcept {
      return log_detail::StringCodec::encode(p, s, strnlen(s, N));
    }
    static const std::byte *decode(const std::byte *p, LogWriter &w) noexcept {
      return log_detail::StringCodec::decode(p, w);
    }
  };

  template<>
  struct LogCodec<std::string_view> {
    static std::size_t size(std::string_view s) noexcept { return log_detail::StringCodec::size(s.size()); }
    static std::byte *encode(std::byte *p, std::string_view s) noexcept {
      return log_detail::StringCodec::encode(p, s.data(), s.size());
    }
    static const std::byte *decode(const std::byte *p, LogWriter &w) noexcept {
      return log_detail::StringCodec::decode(p, w);
    }
  };

  template<>
  struct LogCodec<std::string> : LogCodec<std::string_view> {
  };

  template<LogFormattable T>
  struct LogCodec<T> : log_detail::TrivialCopy<T> {
    static const std::byte *decode(const std::byte *p, LogWriter &w) noexcept {
      alignas(T) std::byte storage[sizeof(T)];
      std::memcpy(storage, p, sizeof(T));
      formatLog(w, *std::launder(reinterpret_cast<const T *>(storage)));
      return p + sizeof(T);
    }
  };

  // ===================================================================================================
  // Compile-time checked format string. '%' is a placeholder, '%%' prints a literal '%'.
  // ===================================================================================================

  namespace log_detail {
    // Deliberately not constexpr: calling it during constant evaluation is a compile error whose
    // message contains this function's name.
    inline void log_format_error_number_of_placeholders_does_not_match_number_of_arguments() {}

    template<std::size_t N>
    consteval std::size_t countPlaceholders(const char (&s)[N]) {
      std::size_t count = 0;
      for (std::size_t i = 0; i + 1 < N && s[i]; ++i) {
        if (s[i] == '%') {
          if (s[i + 1] == '%')
            ++i;
          else
            ++count;
        }
      }
      return count;
    }
  }

  template<typename... Args>
  struct FormatString {
    const char *str;

    template<std::size_t N>
    consteval FormatString(const char (&s)[N]) : str(s) {
      if (log_detail::countPlaceholders(s) != sizeof...(Args))
        log_detail::log_format_error_number_of_placeholders_does_not_match_number_of_arguments();
    }
  };

  // ===================================================================================================
  // Record layout and the per-thread SPSC byte ring.
  // ===================================================================================================

  namespace log_detail {
    using DecodeFn = void (*)(LogWriter &, const char *fmt, const std::byte *payload);

    enum class RecordKind : uint8_t {
      Message = 0,
      Wrap = 1 // padding up to the end of the ring; the next record starts at offset 0.
    };

    /// First 8 bytes of every record (a Wrap marker is ONLY these 8 bytes).
    struct RecordPrefix {
      uint32_t size; // whole record in bytes, multiple of 8
      RecordKind kind;
      LogLevel level;
      uint16_t reserved;
    };

    struct RecordHeader {
      RecordPrefix prefix;
      uint64_t tsc;       // raw cycle counter, converted to wall-clock time on the logger thread
      const char *fmt;    // points at the string literal - never copied
      DecodeFn decode;    // knows the argument types for this call site
      // followed by the encoded arguments
    };
    static_assert(sizeof(RecordPrefix) == 8);
    static_assert(sizeof(RecordHeader) == 32);

    inline uint64_t rdtsc() noexcept {
#if defined(__x86_64__) || defined(__i386__)
      return __rdtsc();
#else
      return static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
#endif
    }

    inline void cpuRelax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
      _mm_pause();
#endif
    }

    /// Writes literal text up to the next unescaped '%' and returns a pointer to it (or to '\0').
    inline const char *emitLiteral(LogWriter &w, const char *s) noexcept {
      const char *run = s;
      while (*s) {
        if (*s == '%') {
          if (s[1] != '%')
            break;
          w.append(run, static_cast<std::size_t>(s + 1 - run)); // up to and including one '%'
          s += 2;
          run = s;
          continue;
        }
        ++s;
      }
      w.append(run, static_cast<std::size_t>(s - run));
      return s;
    }

    /// One instantiation per distinct argument-type list; its address is stored in each record.
    template<typename... Args>
    struct Decoder;

    template<>
    struct Decoder<> {
      static void run(LogWriter &w, const char *fmt, const std::byte *) noexcept { emitLiteral(w, fmt); }
    };

    template<typename A, typename... Rest>
    struct Decoder<A, Rest...> {
      static void run(LogWriter &w, const char *fmt, const std::byte *payload) noexcept {
        fmt = emitLiteral(w, fmt); // now at a '%' - guaranteed by the compile-time check
        payload = LogCodec<A>::decode(payload, w);
        Decoder<Rest...>::run(w, fmt + 1, payload);
      }
    };

    constexpr std::size_t CacheLine = 64;

    /// Single-producer / single-consumer ring of variable-length records.
    /// Positions are monotonically increasing byte counts; `pos & mask_` is the offset.
    class SpscByteRing final {
    public:
      explicit SpscByteRing(std::size_t bytes) noexcept {
        constexpr std::size_t MinBytes = 64 * 1024, MaxBytes = std::size_t{1} << 30;
        capacity_ = std::bit_ceil(bytes < MinBytes ? MinBytes : (bytes > MaxBytes ? MaxBytes : bytes));
        mask_ = capacity_ - 1;

        void *mem = ::mmap(nullptr, capacity_, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        ASSERT(mem != MAP_FAILED, "Logger: mmap of ring buffer failed. errno:" + std::string(std::strerror(errno)));
        ::madvise(mem, capacity_, MADV_HUGEPAGE); // best effort: fewer TLB misses
        std::memset(mem, 0, capacity_);           // pre-fault every page now, not on the hot path
        ::mlock(mem, capacity_);                  // best effort: never swapped out (needs RLIMIT_MEMLOCK)
        buf_ = static_cast<std::byte *>(mem);
        owner_ = std::this_thread::get_id();
      }

      ~SpscByteRing() { ::munmap(buf_, capacity_); }

      SpscByteRing(const SpscByteRing &) = delete;
      SpscByteRing &operator=(const SpscByteRing &) = delete;

      // ---------------- producer side ----------------

      /// Space for `n` contiguous bytes (n % 8 == 0), or nullptr if full (the message is dropped and
      /// counted). Must be followed by commit().
      std::byte *reserve(std::size_t n) noexcept {
        const uint64_t write = write_pos_.load(std::memory_order_relaxed);
        const std::size_t offset = write & mask_;
        const std::size_t room_to_end = capacity_ - offset;
        const std::size_t skip = UNLIKELY(room_to_end < n) ? room_to_end : 0;
        const uint64_t end = write + skip + n;

        // Only touch the consumer's cache line when our cached view says we might be full.
        if (UNLIKELY(end - cached_read_pos_ > capacity_)) {
          cached_read_pos_ = read_pos_.load(std::memory_order_acquire);
          if (end - cached_read_pos_ > capacity_ || n > capacity_) {
            dropped_.store(dropped_.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
            return nullptr;
          }
        }

        if (UNLIKELY(skip)) // room_to_end is a non-zero multiple of 8: a prefix always fits.
          new(buf_ + offset) RecordPrefix{static_cast<uint32_t>(skip), RecordKind::Wrap, LogLevel::Raw, 0};

        pending_end_ = end;

        // Ask for the next records' cache lines in exclusive (write) state now, so the next call
        // doesn't stall on fetching a line the consumer last touched one lap of the ring ago.
        __builtin_prefetch(buf_ + ((end + PrefetchAhead) & mask_), 1, 3);
        __builtin_prefetch(buf_ + ((end + PrefetchAhead + CacheLine) & mask_), 1, 3);

        return buf_ + ((write + skip) & mask_);
      }

      /// Publishes the reserved record (and the wrap marker, if any) with a single release store.
      void commit() noexcept { write_pos_.store(pending_end_, std::memory_order_release); }

      uint64_t dropped() const noexcept { return dropped_.load(std::memory_order_relaxed); }

      std::thread::id owner() const noexcept { return owner_; }

      void setOwner(std::thread::id id) noexcept { owner_ = id; }

      // ---------------- consumer side ----------------

      /// Oldest committed record, or nullptr if empty.
      const RecordHeader *peek() noexcept {
        for (;;) {
          if (read_pos_local_ == cached_write_pos_) {
            cached_write_pos_ = write_pos_.load(std::memory_order_acquire);
            if (read_pos_local_ == cached_write_pos_)
              return nullptr;
          }
          const auto *prefix = reinterpret_cast<const RecordPrefix *>(buf_ + (read_pos_local_ & mask_));
          if (prefix->kind == RecordKind::Wrap) {
            read_pos_local_ += prefix->size;
            continue;
          }
          return reinterpret_cast<const RecordHeader *>(prefix);
        }
      }

      void pop(const RecordHeader *record) noexcept {
        read_pos_local_ += record->prefix.size;
        read_pos_.store(read_pos_local_, std::memory_order_release);
      }

    private:
      static constexpr std::size_t PrefetchAhead = 2 * CacheLine;

      // Read-only after construction.
      std::byte *buf_ = nullptr;
      std::size_t capacity_ = 0;
      std::size_t mask_ = 0;
      std::thread::id owner_;

      // Producer's cache line.
      alignas(CacheLine) std::atomic<uint64_t> write_pos_{0};
      uint64_t cached_read_pos_ = 0;
      uint64_t pending_end_ = 0;
      std::atomic<uint64_t> dropped_{0};

      // Consumer's cache line.
      alignas(CacheLine) std::atomic<uint64_t> read_pos_{0};
      uint64_t read_pos_local_ = 0;
      uint64_t cached_write_pos_ = 0;
    };

    /// Converts rdtsc ticks to wall-clock nanoseconds. Only used on the logger thread.
    struct TscClock {
      uint64_t base_tsc = 0;
      int64_t base_ns = 0;
      double ns_per_tick = 1.0;

      void calibrate() noexcept {
        base_tsc = rdtsc();
        base_ns = getCurrentNanos();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        refine();
      }

      /// Re-measures the tick rate over the whole time since calibrate(), so it gets more precise.
      void refine() noexcept {
        const auto tsc = rdtsc();
        const auto ns = getCurrentNanos();
        if (tsc > base_tsc && ns > base_ns)
          ns_per_tick = static_cast<double>(ns - base_ns) / static_cast<double>(tsc - base_tsc);
      }

      int64_t toNanos(uint64_t tsc) const noexcept {
        return base_ns + static_cast<int64_t>(static_cast<double>(static_cast<int64_t>(tsc - base_tsc)) * ns_per_tick);
      }
    };
  }

  // ===================================================================================================
  // Logger
  // ===================================================================================================

  class Logger final {
  public:
    explicit Logger(const std::string &file_name, LoggerConfig config = {})
        : file_name_(file_name), config_(config), id_(nextLoggerId()),
          min_level_(static_cast<uint8_t>(config.min_level)),
          fd_(::open(file_name.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644)), writer_(fd_) {
      ASSERT(fd_ >= 0, "Could not open log file:" + file_name);
      clock_.calibrate();
      logger_thread_ = createAndStartThread(config_.core_id, "Common/Logger " + file_name_, [this]() { run(); });
      ASSERT(logger_thread_ != nullptr, "Failed to start Logger thread.");
    }

    ~Logger() {
      std::string time_str;
      std::cerr << Common::getCurrentTimeStr(&time_str) << " Flushing and closing Logger for " << file_name_ << std::endl;

      running_.store(false, std::memory_order_release); // the logger thread drains everything, then exits
      logger_thread_->join();
      delete logger_thread_;

      ::close(fd_);
      for (std::size_t i = 0; i < ring_count_.load(std::memory_order_acquire); ++i)
        delete rings_[i].load(std::memory_order_relaxed);

      std::cerr << Common::getCurrentTimeStr(&time_str) << " Logger for " << file_name_ << " exiting." << std::endl;
    }

    /// Same as the original API: `%` is replaced by the next argument, `%%` prints `%`.
    /// No prefix and no newline are added. The format must be a string literal; the number of
    /// placeholders is checked at compile time.
    template<typename... Args>
    void log(FormatString<std::type_identity_t<Args>...> fmt, const Args &... args) noexcept {
      write(LogLevel::Raw, fmt.str, args...);
    }

    /// Leveled logging: "YYYY-MM-DD HH:MM:SS.nnnnnnnnn LEVEL <message>\n".
    /// Prefer the LOG_INFO(...) etc. macros, which add file:line at compile time for free.
    template<typename... Args>
    void logDebug(FormatString<std::type_identity_t<Args>...> fmt, const Args &... args) noexcept {
      writeIfEnabled(LogLevel::Debug, fmt.str, args...);
    }

    template<typename... Args>
    void logInfo(FormatString<std::type_identity_t<Args>...> fmt, const Args &... args) noexcept {
      writeIfEnabled(LogLevel::Info, fmt.str, args...);
    }

    template<typename... Args>
    void logWarn(FormatString<std::type_identity_t<Args>...> fmt, const Args &... args) noexcept {
      writeIfEnabled(LogLevel::Warn, fmt.str, args...);
    }

    template<typename... Args>
    void logError(FormatString<std::type_identity_t<Args>...> fmt, const Args &... args) noexcept {
      writeIfEnabled(LogLevel::Error, fmt.str, args...);
    }

    /// Call once at the start of every thread that will log (before the latency-critical loop).
    /// The first log() from a thread otherwise pays for creating that thread's ring buffer
    /// (mmap + pre-faulting + mlock of per_thread_buffer_bytes: milliseconds, not nanoseconds).
    /// Returns false if MaxProducerThreads threads are already registered.
    bool registerThread() noexcept { return localRing() != nullptr; }

    void setMinLevel(LogLevel level) noexcept { min_level_.store(static_cast<uint8_t>(level), std::memory_order_relaxed); }

    /// Messages lost because a producer's ring was full (or too many producer threads).
    uint64_t droppedCount() const noexcept {
      uint64_t total = dropped_no_ring_.load(std::memory_order_relaxed);
      for (std::size_t i = 0; i < ring_count_.load(std::memory_order_acquire); ++i)
        total += rings_[i].load(std::memory_order_acquire)->dropped();
      return total;
    }

    // Deleted default, copy & move constructors and assignment-operators.
    Logger() = delete;

    Logger(const Logger &) = delete;

    Logger(const Logger &&) = delete;

    Logger &operator=(const Logger &) = delete;

    Logger &operator=(const Logger &&) = delete;

  private:
    static constexpr std::size_t MaxProducerThreads = 64;
    static constexpr std::size_t TlsCacheSize = 4;
    static constexpr std::size_t MaxRecordsPerBatch = 1024;
    static constexpr int IdlePauseBatch = 32;

    struct TlsEntry {
      uint64_t logger_id;
      log_detail::SpscByteRing *ring;
    };

    static uint64_t nextLoggerId() noexcept {
      static std::atomic<uint64_t> next_id{1};
      return next_id.fetch_add(1, std::memory_order_relaxed);
    }

    /// Per-thread cache of (logger, ring). Constant-initialised so access is a plain %fs-relative load.
    static TlsEntry *tlsCache() noexcept {
      static thread_local TlsEntry cache[TlsCacheSize] = {};
      return cache;
    }

    // =============================== hot path ===============================

    template<typename... Args>
    void writeIfEnabled(LogLevel level, const char *fmt, const Args &... args) noexcept {
      if (UNLIKELY(static_cast<uint8_t>(level) < min_level_.load(std::memory_order_relaxed)))
        return;
      write(level, fmt, args...);
    }

    template<typename... Args>
    void write(LogLevel level, const char *fmt, const Args &... args) noexcept {
      const uint64_t tsc = log_detail::rdtsc(); // timestamp of the event, taken first

      auto *ring = localRing();
      if (UNLIKELY(ring == nullptr)) {
        dropped_no_ring_.fetch_add(1, std::memory_order_relaxed);
        return;
      }

      const std::size_t payload = (std::size_t{0} + ... + LogCodec<std::remove_cvref_t<Args>>::size(args));
      const std::size_t total = (sizeof(log_detail::RecordHeader) + payload + 7) & ~std::size_t{7};

      std::byte *record = ring->reserve(total);
      if (UNLIKELY(record == nullptr))
        return; // full: drop (counted) rather than block the trading thread

      new(record) log_detail::RecordHeader{
          {static_cast<uint32_t>(total), log_detail::RecordKind::Message, level, 0},
          tsc, fmt, &log_detail::Decoder<std::remove_cvref_t<Args>...>::run};

      [[maybe_unused]] std::byte *cursor = record + sizeof(log_detail::RecordHeader);
      ((cursor = LogCodec<std::remove_cvref_t<Args>>::encode(cursor, args)), ...);

      ring->commit();
    }

    log_detail::SpscByteRing *localRing() noexcept {
      auto *cache = tlsCache();
      if (LIKELY(cache[0].logger_id == id_))
        return cache[0].ring;
      return localRingSlow(cache);
    }

    // =============================== slow path: first log() from a thread ===============================

    log_detail::SpscByteRing *localRingSlow(TlsEntry *cache) noexcept {
      for (std::size_t i = 1; i < TlsCacheSize; ++i) {
        if (cache[i].logger_id == id_) {
          std::swap(cache[0], cache[i]);
          return cache[0].ring;
        }
      }
      auto *ring = findOrCreateRing();
      for (std::size_t i = TlsCacheSize - 1; i > 0; --i)
        cache[i] = cache[i - 1];
      cache[0] = {id_, ring};
      return ring;
    }

    log_detail::SpscByteRing *findOrCreateRing() noexcept {
      std::lock_guard<std::mutex> lock(register_mutex_);
      const auto me = std::this_thread::get_id();
      const auto count = ring_count_.load(std::memory_order_relaxed);
      for (std::size_t i = 0; i < count; ++i) { // evicted from the TLS cache, or a dead thread's id reused
        auto *ring = rings_[i].load(std::memory_order_relaxed);
        if (ring->owner() == me)
          return ring;
      }
      if (count == MaxProducerThreads)
        return nullptr;
      auto *ring = new log_detail::SpscByteRing(config_.per_thread_buffer_bytes);
      rings_[count].store(ring, std::memory_order_relaxed);
      ring_count_.store(count + 1, std::memory_order_release);
      return ring;
    }

    // =============================== background thread ===============================

    void run() noexcept {
      uint64_t idle_rounds = 0;
      int64_t last_refine_ns = getCurrentNanos();

      for (;;) {
        // Read the flag BEFORE draining: if a drain then finds nothing, everything logged before
        // the destructor ran has been written.
        const bool stopping = !running_.load(std::memory_order_acquire);

        if (drainBatch()) {
          idle_rounds = 0;
          continue;
        }

        if (idle_rounds % 4096 == 0) { // first idle round and periodically after that
          writer_.flush();
          reportDrops();
          const auto now = getCurrentNanos();
          if (now - last_refine_ns > NANOS_TO_SECS) {
            clock_.refine();
            last_refine_ns = now;
          }
        }

        if (stopping)
          break;

        ++idle_rounds;
        // Don't re-poll the producers' write positions in a tight loop: each poll pulls their cache
        // line into shared state, and the producer's next commit then pays a cross-core
        // invalidation. Polling ~every microsecond costs the log latency nothing that matters.
        if (config_.busy_spin || idle_rounds < 2000)
          for (int i = 0; i < IdlePauseBatch; ++i)
            log_detail::cpuRelax();
        else if (idle_rounds < 2100)
          std::this_thread::yield();
        else
          std::this_thread::sleep_for(std::chrono::microseconds(50));
      }

      reportDrops();
      writer_.flush();
    }

    /// Writes up to MaxRecordsPerBatch records, always taking the oldest head across all threads'
    /// rings, so the file is in timestamp order. Returns the number written.
    std::size_t drainBatch() noexcept {
      const auto count = ring_count_.load(std::memory_order_acquire);
      std::size_t written = 0;

      for (; written < MaxRecordsPerBatch; ++written) {
        const log_detail::RecordHeader *best = nullptr;
        log_detail::SpscByteRing *best_ring = nullptr;

        for (std::size_t i = 0; i < count; ++i) {
          auto *ring = rings_[i].load(std::memory_order_relaxed);
          const auto *head = ring->peek();
          if (head && (!best || static_cast<int64_t>(head->tsc - best->tsc) < 0)) {
            best = head;
            best_ring = ring;
          }
        }
        if (!best)
          break;

        formatRecord(best);
        best_ring->pop(best);
      }
      return written;
    }

    void formatRecord(const log_detail::RecordHeader *record) noexcept {
      const auto level = record->prefix.level;
      if (level != LogLevel::Raw) {
        appendTimestamp(record->tsc);
        appendLevel(level);
      }
      record->decode(writer_, record->fmt, reinterpret_cast<const std::byte *>(record) + sizeof(log_detail::RecordHeader));
      if (level != LogLevel::Raw)
        writer_.append('\n');
    }

    void appendTimestamp(uint64_t tsc) noexcept {
      const int64_t ns = clock_.toNanos(tsc);
      const int64_t sec = ns / NANOS_TO_SECS;
      auto nanos = static_cast<uint64_t>(ns % NANOS_TO_SECS);

      if (sec != cached_sec_) { // localtime_r + strftime only once per second
        const auto t = static_cast<time_t>(sec);
        tm tm_val{};
        localtime_r(&t, &tm_val);
        cached_sec_len_ = std::strftime(cached_sec_str_, sizeof(cached_sec_str_), "%Y-%m-%d %H:%M:%S", &tm_val);
        cached_sec_ = sec;
      }
      writer_.append(cached_sec_str_, cached_sec_len_);

      char frac[11];
      frac[0] = '.';
      for (int i = 9; i >= 1; --i) {
        frac[i] = static_cast<char>('0' + nanos % 10);
        nanos /= 10;
      }
      frac[10] = ' ';
      writer_.append(frac, sizeof(frac));
    }

    void appendLevel(LogLevel level) noexcept {
      switch (level) {
        case LogLevel::Debug: writer_.append(std::string_view("DEBUG ")); break;
        case LogLevel::Info:  writer_.append(std::string_view("INFO  ")); break;
        case LogLevel::Warn:  writer_.append(std::string_view("WARN  ")); break;
        case LogLevel::Error: writer_.append(std::string_view("ERROR ")); break;
        case LogLevel::Raw:   break;
      }
    }

    void reportDrops() noexcept {
      const auto total = droppedCount();
      if (UNLIKELY(total != reported_drops_)) {
        appendTimestamp(log_detail::rdtsc());
        writer_.append(std::string_view("WARN  [Logger] dropped "));
        writer_.appendNumber(total - reported_drops_);
        writer_.append(std::string_view(" messages because a producer buffer was full (total "));
        writer_.appendNumber(total);
        writer_.append(std::string_view(")\n"));
        reported_drops_ = total;
      }
    }

    // ---- read-only after construction (read by every producer on every call) ----
    const std::string file_name_;
    const LoggerConfig config_;
    const uint64_t id_;
    std::atomic<uint8_t> min_level_;

    // ---- producer registration (slow path only) ----
    alignas(log_detail::CacheLine) std::array<std::atomic<log_detail::SpscByteRing *>, MaxProducerThreads> rings_{};
    std::atomic<std::size_t> ring_count_{0};
    std::mutex register_mutex_;
    std::atomic<uint64_t> dropped_no_ring_{0};

    // ---- background thread only ----
    alignas(log_detail::CacheLine) int fd_;
    LogWriter writer_;
    log_detail::TscClock clock_;
    int64_t cached_sec_ = -1;
    char cached_sec_str_[32] = {};
    std::size_t cached_sec_len_ = 0;
    uint64_t reported_drops_ = 0;

    alignas(log_detail::CacheLine) std::atomic<bool> running_{true};
    std::thread *logger_thread_ = nullptr;
  };
}

// =====================================================================================================
// Leveled logging macros. file:line is pasted into the format literal at compile time (zero runtime
// cost). Calls below LOG_COMPILED_MIN_LEVEL (0=DEBUG .. 3=ERROR) are compiled out entirely, including
// the evaluation of their arguments.
// =====================================================================================================

#ifndef LOG_COMPILED_MIN_LEVEL
#define LOG_COMPILED_MIN_LEVEL 0
#endif

#ifdef __FILE_NAME__
#define LOG_SOURCE_FILE __FILE_NAME__
#else
#define LOG_SOURCE_FILE __FILE__
#endif

#define LOG_STRINGIFY_IMPL(x) #x
#define LOG_STRINGIFY(x) LOG_STRINGIFY_IMPL(x)

#define LOG_AT_LEVEL(logger, level_num, method, fmt, ...)                                              \
  do {                                                                                                 \
    if constexpr ((level_num) >= LOG_COMPILED_MIN_LEVEL) {                                             \
      (logger).method(LOG_SOURCE_FILE ":" LOG_STRINGIFY(__LINE__) " " fmt __VA_OPT__(,) __VA_ARGS__);  \
    }                                                                                                  \
  } while (0)

#define LOG_DEBUG(logger, fmt, ...) LOG_AT_LEVEL(logger, 0, logDebug, fmt __VA_OPT__(,) __VA_ARGS__)
#define LOG_INFO(logger, fmt, ...)  LOG_AT_LEVEL(logger, 1, logInfo,  fmt __VA_OPT__(,) __VA_ARGS__)
#define LOG_WARN(logger, fmt, ...)  LOG_AT_LEVEL(logger, 2, logWarn,  fmt __VA_OPT__(,) __VA_ARGS__)
#define LOG_ERROR(logger, fmt, ...) LOG_AT_LEVEL(logger, 3, logError, fmt __VA_OPT__(,) __VA_ARGS__)
