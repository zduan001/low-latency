#pragma once

#include <cstring>
#include <iostream>
#include <string>

#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)

/// ASSERT is a macro (not a function) so that the message expression - which is usually a
/// std::string concatenation that heap-allocates - is only evaluated when the check FAILS.
/// As a function, the message was built on every call, even on the success path.
#define ASSERT(cond, msg)                                       \
  do {                                                          \
    if (UNLIKELY(!(cond))) {                                    \
      std::cerr << "ASSERT : " << (msg) << std::endl;           \
      exit(EXIT_FAILURE);                                       \
    }                                                           \
  } while (0)

[[noreturn]] inline void FATAL(const std::string &msg) noexcept {
  std::cerr << "FATAL : " << msg << std::endl;

  exit(EXIT_FAILURE);
}
