#include "logging.h"

#include <cstdint>
#include <vector>

// A user-defined type can be logged by value: make it trivially copyable and give it a formatLog()
// overload in its own namespace. The hot path just memcpy's the 24 bytes; formatLog() runs on the
// background logger thread.
namespace Example {
  enum class Side : int8_t { BUY = 1, SELL = -1 };

  struct Order {
    uint64_t order_id;
    int64_t price;
    uint32_t qty;
    Side side;
  };

  inline void formatLog(Common::LogWriter &w, const Order &o) {
    w.append(std::string_view("Order{id:"));
    w.appendNumber(o.order_id);
    w.append(std::string_view(" px:"));
    w.appendNumber(o.price);
    w.append(std::string_view(" qty:"));
    w.appendNumber(o.qty);
    w.append(o.side == Side::BUY ? std::string_view(" BUY}") : std::string_view(" SELL}"));
  }
}

int main(int, char **) {
  using namespace Common;

  char c = 'd';
  int i = 3;
  unsigned long ul = 65;
  float f = 3.4;
  double d = 34.56;
  const char* s = "test C-string";
  std::string ss = "test string";

  Logger logger("logging_example.log");
  logger.registerThread();

  // 1. The original API - unchanged call sites, but now checked at compile time:
  //    logger.log("% %\n", i);   // <- does not compile: 2 placeholders, 1 argument
  logger.log("Logging a char:% an int:% and an unsigned:%\n", c, i, ul);
  logger.log("Logging a float:% and a double:%\n", f, d);
  logger.log("Logging a C-string:'%'\n", s);
  logger.log("Logging a string:'%'\n", ss);
  logger.log("Escaped percent: 100%% done\n");

  // 2. Leveled logging: timestamp, level and file:line are added automatically.
  LOG_INFO(logger, "Starting up, pid:%", getpid());
  LOG_WARN(logger, "Spread wide on % : bid:% ask:%", "ESZ6", 5012.25, 5013.75);
  LOG_ERROR(logger, "bool:% enum:% ptr:% null:%", true, Example::Side::SELL, static_cast<void *>(&i), nullptr);
  LOG_INFO(logger, "Sent %", Example::Order{42, 501225, 10, Example::Side::BUY});

  // 3. Any number of threads may log concurrently; each gets its own lock-free buffer and the
  //    output is merged in timestamp order.
  std::vector<std::thread> threads;
  for (int t = 0; t < 3; ++t) {
    threads.emplace_back([&logger, t]() {
      logger.registerThread(); // pay the one-time buffer setup before the "hot" part of the thread
      for (int n = 0; n < 3; ++n)
        LOG_DEBUG(logger, "worker:% message:%", t, n);
    });
  }
  for (auto &th : threads)
    th.join();

  return 0;
}
