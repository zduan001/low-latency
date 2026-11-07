#pragma once

#include <iostream>
#include <atomic>
#include <thread>
#include <string>
#include <utility>
#include <unistd.h>

#include <sys/syscall.h>

namespace Common {
  /// Set affinity for current thread to be pinned to the provided core_id.
  inline auto setThreadCore(int core_id) noexcept {
    cpu_set_t cpuset;

    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    return (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) == 0);
  }

  /// Creates a thread instance, sets affinity on it, assigns it a name and
  /// passes the function to be run on that thread as well as the arguments to the function.
  ///
  /// Everything the new thread needs is captured BY VALUE. The previous version captured by
  /// reference ([&]), which left the thread reading `name` / `func` / `args` after this function
  /// had returned and those temporaries were destroyed (use-after-free hidden by the sleep below).
  template<typename T, typename... A>
  inline auto createAndStartThread(int core_id, const std::string &name, T &&func, A &&... args) noexcept {
    auto t = new std::thread([core_id, name, func = std::forward<T>(func), ... args = std::forward<A>(args)]() mutable {
      if (core_id >= 0 && !setThreadCore(core_id)) {
        std::cerr << "Failed to set core affinity for " << name << " " << pthread_self() << " to " << core_id << std::endl;
        exit(EXIT_FAILURE);
      }
      std::cerr << "Set core affinity for " << name << " " << pthread_self() << " to " << core_id << std::endl;

      func(std::move(args)...);
    });

    // No longer needed for memory safety (see above); kept so existing callers that expect the
    // thread to be up and pinned when this returns keep their behaviour.
    using namespace std::literals::chrono_literals;
    std::this_thread::sleep_for(1s);

    return t;
  }
}
