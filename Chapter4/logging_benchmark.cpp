// Measures the cost of a log call ON THE CALLING THREAD (what matters for a trading thread).
// Usage: ./logging_benchmark [num_messages]
#include "logging.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <vector>

int main(int argc, char **argv) {
  using namespace Common;

  const std::size_t n = argc > 1 ? std::strtoul(argv[1], nullptr, 10) : 100000;
  std::vector<uint64_t> cycles(n);

  {
    Logger logger("logging_benchmark.log");
    const std::string symbol = "ESZ6";
    logger.registerThread(); // create this thread's ring up front, off the measured path

    for (std::size_t i = 0; i < n; ++i) {
      const uint64_t start = log_detail::rdtsc();
      LOG_INFO(logger, "fill oid:% sym:% px:% qty:% side:%", i, symbol, 5012.25 + static_cast<double>(i % 7), 10u, 'B');
      cycles[i] = log_detail::rdtsc() - start;
      if (i % 64 == 0) // pace a little so we measure the fast path, not a permanently full buffer
        for (int spin = 0; spin < 2000; ++spin)
          log_detail::cpuRelax();
    }

    std::printf("dropped: %lu\n", static_cast<unsigned long>(logger.droppedCount()));
  }

  log_detail::TscClock clock;
  clock.calibrate();

  std::sort(cycles.begin(), cycles.end());
  auto pct = [&](double p) { return static_cast<double>(cycles[static_cast<std::size_t>(p * static_cast<double>(n - 1))]) * clock.ns_per_tick; };
  std::printf("log call cost (ns, calling thread) over %zu calls: p50=%.1f p90=%.1f p99=%.1f p99.9=%.1f max=%.1f\n",
              n, pct(0.50), pct(0.90), pct(0.99), pct(0.999), static_cast<double>(cycles.back()) * clock.ns_per_tick);
  return 0;
}
