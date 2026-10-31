#pragma once

#include <iostream>
#include <atomic>
#include <thread>
#include <unistd.h>
#include <string>

#include <sys/syscall.h>

namespace Common {
    inline auto setThreadCore(int core_id) noexcept {
        cpu_set_t cpuset; // cpu bitset

        CPU_ZERO(&cpuset); // zeroes out bitset
        CPU_SET(core_id, &cpuset);

        return (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) == 0);
    }

    template<typename T, typename... A>
    inline auto createAndStartThread(int core_id, const std::string& name, T&& func, A&&... args) noexcept {
        auto t = new std::thread([core_id, 
                                  &name, 
                                  func = std::forward<T>(func), 
                                  ...args = std::forward<A>(args)]() mutable {
            if(core_id >= 0 && !setThreadCore(core_id)){
                std::cerr << "Failed to set core affinity" << std::endl;
                exit(EXIT_FAILURE);
            }
        
        
            std::cerr << "Set core affinity successfully" << std::endl;

            std::move(func)((std::move(args))...);
        });
        //   auto t = new std::thread([core_id, &name, func = std::forward<T>(func)](auto&& ...thread_args) mutable {
        //     if(core_id >= 0 && !setThreadCore(core_id)){
        //         cerr << "Failed to set core affinity" << endl;
        //         exit(EXIT_FAILURE);
        //     }

        //     cerr << "Set core affinity successfully" << endl;

        //     std::move(func)((std::move(thread_args))...);
        // }, std::forward<A>(args)...);

        return t;
    }

} // Common