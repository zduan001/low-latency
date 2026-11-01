#pragma once
#include <atomic>
#include <array>
#include <semaphore.h>

namespace tdsys {
    // 带信号量的多生产者单消费者队列，用于非低时延HFT辅助通道
    template<typename T, size_t SZ = 127>
    class MsgQueue {
    private:
        // 队列长度对齐为2的幂（HFT标准优化）：LEN = 1<<n，确保下标可通过位运算快速计算
        static constexpr size_t LEN = 1 << std::bit_width(SZ);
        // 掩码：替代取模运算，CPU单周期完成，降低延迟
        static constexpr size_t MASK = LEN - 1;
        // 版本号占用位数（16位）：用于解决ABA问题，高16位版本，低16位下标
        static constexpr size_t IB = 16;
        // 下标掩码：提取低16位下标
        static constexpr size_t IM = (1ULL << IB) - 1;

        // 队列元素包装：data存储数据，ok标记数据是否写入完成（多生产者场景必备）
        // 多生产者并发写入时，需确保数据完全写入后，消费者才能读取，避免数据错乱
        struct W {
            std::atomic<bool> ok{ false };  // 数据就绪标记，原子操作保证可见性
            T data{};                      // 存储实际消息数据，默认初始化
        };

        // 缓存行对齐（64字节）：h（头指针）、vt（带版本的尾指针）分别独占缓存行，杜绝伪共享
        // 伪共享会导致多核CPU缓存乒乓，增加延迟，这是HFT队列的关键优化点
        alignas(64) std::atomic<size_t> h{ 0 }, vt{ 0 };
        sem_t sem{};                          // 信号量：消费者阻塞等待，避免空转
        std::array<W, LEN> buf{};             // 静态环形缓冲区，无动态内存分配

    public:
        // 构造函数：初始化信号量，pshared=1表示跨进程共享（可适配多进程HFT系统），初始值0表示无消息
        MsgQueue() { sem_init(&sem, 1, 0); }
        // 析构函数：销毁信号量，避免资源泄漏（HFT系统需保证无资源泄漏，否则会导致系统不稳定）
        ~MsgQueue() { sem_destroy(&sem); }

        // 多生产者并发入队（无锁，CAS抢占写入权），支持右值引用，避免拷贝，降低延迟
        template<typename U>
        bool push(U&& u) {
            size_t v;  // 版本号临时变量
            while (true) {
                // 加载带版本的尾指针：acquire内存序，保证读取到最新的vt值，避免数据过期
                auto lvt = vt.load(std::memory_order_acquire);
                auto lt = lvt & IM;                          // 提取低16位下标
                auto n = (lt + 1) & MASK;                    // 计算下一个写入位置，位运算替代取模
                auto hh = h.load(std::memory_order_acquire) & IM; // 读取头指针下标，判断队列是否满
                if (n == hh) return false;                   // 队列满，入队失败（HFT场景下通常会重试，此处按原代码实现）

                // 版本号+1：高16位版本号自增，解决ABA问题，避免CAS误判
                v = (lvt >> IB) + 1;
                size_t nvt = (v << IB) | n;                  // 新的带版本尾指针

                // CAS抢占写入权：compare_exchange_strong是原子操作，失败则重试
                // acq_rel内存序：保证CAS成功后，之前的写入操作可见，后续的读取操作能看到最新数据
                if (vt.compare_exchange_strong(lvt, nvt, std::memory_order_acq_rel)) {
                    auto& w = buf[lt];
                    w.data = std::forward<U>(u);             // 右值引用，完美转发，避免拷贝开销
                    w.ok.store(true, std::memory_order_release); // 标记数据就绪，release保证写入可见
                    sem_post(&sem);                          // 唤醒消费者（非核心链路，允许系统调用）
                    return true;
                }
            }
        }

        // 单消费者出队（阻塞式），适用于非核心链路，允许轻微阻塞
        bool pop(T& out) {
            sem_wait(&sem);                                  // 阻塞等待消息，避免消费者空转，节约CPU
            auto lh = h.load(std::memory_order_relaxed);     // 读取头指针，relaxed无同步开销，仅保证原子性
            auto tail = vt.load(std::memory_order_acquire) & IM; // 读取尾指针下标，acquire保证可见性
            if (lh == tail) return false;                   // 队列空，出队失败（理论上不会出现，因sem信号量控制）
            auto& w = buf[lh];
            if (!w.ok.load(std::memory_order_acquire)) return false; // 确保数据已写入完成
            out = std::move(w.data);                        // 移动语义，零拷贝，降低延迟
            w.ok.store(false, std::memory_order_release);   // 重置就绪标记，允许下一次写入
            h.store((lh + 1) & MASK, std::memory_order_release); // 更新头指针，release保证可见性
            return true;
        }
    };
}



#pragma once
#include <atomic>
#include <array>

namespace tdsys {
    // 单生产者单消费者极致低时延无锁队列，HFT核心链路专用
    template<typename T, size_t SZ = 127>
    class SPSCQueue {
    private:
        // 队列长度对齐为2的幂，HFT标准设计，位运算替代取模
        static constexpr size_t LEN = 1 << std::bit_width(SZ);
        static constexpr size_t MASK = LEN - 1;

        // 头指针（消费者修改）、尾指针（生产者修改），分别缓存行对齐，杜绝伪共享
        // 单生产者仅修改t，单消费者仅修改h，无任何竞争，无需CAS
        alignas(64) std::atomic<size_t> h{ 0 }, t{ 0 };
        // 本地缓存sh、st：减少原子变量加载次数，原子操作比普通变量访问慢10倍以上，这是HFT关键优化
        alignas(64) size_t sh = 0, st = 0;
        std::array<T, LEN> buf{}; // 静态环形缓冲区，无动态内存分配，延迟可控

    public:
        // 消费者出队：单线程操作，无竞争，延迟极低
        bool pop(T& out) {
            auto lh = h.load(std::memory_order_relaxed); // 读取头指针，relaxed无同步开销
            // 本地缓存st判断，避免频繁加载原子变量t（原子加载开销高）
            if (lh != st && (st = t.load(std::memory_order_acquire)) != lh) {
                out = std::move(buf[lh]);                // 移动语义，零拷贝，降低延迟
                // 更新头指针：release内存序，保证出队后的数据修改可见
                h.store((lh + 1) & MASK, std::memory_order_release);
                return true;
            }
            return false; // 队列空，出队失败（HFT场景下通常会自旋重试，此处按原代码实现）
        }

        // 生产者入队：单线程操作，无竞争，无需CAS
        template<typename U>
        bool push(U&& u) {
            auto lt = t.load(std::memory_order_relaxed); // 读取尾指针，relaxed无同步开销
            auto n = (lt + 1) & MASK;                   // 计算下一个写入位置，位运算替代取模
            // 本地缓存sh判断，避免频繁加载原子变量h
            if (n != sh && (sh = h.load(std::memory_order_acquire)) != n) {
                buf[lt] = std::forward<U>(u);            // 完美转发，避免拷贝
                // 更新尾指针：release内存序，保证入队数据可见
                t.store((lt + 1) & MASK, std::memory_order_release);
                return true;
            }
            return false; // 队列满，入队失败（HFT场景下会重试，此处按原代码实现）
        }
    };
}

#pragma once
#include <atomic>
#include <array>

namespace tdsys {
    // 单生产者多消费者无锁队列，HFT行情广播、多策略并行消费专用
    template<typename T, size_t SZ = 127>
    class SPMCQueue {
    private:
        // 队列长度对齐为2的幂，HFT标准设计，位运算替代取模
        static constexpr size_t LEN = 1 << std::bit_width(SZ);
        static constexpr size_t MASK = LEN - 1;
        // 版本号占用16位，下标占用16位，解决多消费者CAS竞争的ABA问题
        static constexpr size_t IB = 16;
        static constexpr size_t IM = (1ULL << IB) - 1;

        // 带版本的头指针（多消费者CAS竞争修改）、尾指针（单生产者修改，无竞争）
        // 缓存行对齐，避免伪共享
        alignas(64) std::atomic<size_t> vh{ 0 }, t{ 0 };
        std::array<T, LEN> buf{}; // 静态环形缓冲区，无动态内存分配

    public:
        // 单生产者入队：无竞争，无需CAS，延迟极低
        template<typename U>
        bool push(U&& u) {
            auto lt = t.load(std::memory_order_relaxed); // 读取尾指针，relaxed无同步开销
            auto n = (lt + 1) & MASK;                   // 计算下一个写入位置
            // 读取带版本的头指针，判断队列是否满，acquire保证可见性
            auto hh = vh.load(std::memory_order_acquire) & IM;
            if (n == hh) return false;                   // 队列满，入队失败
            buf[lt] = std::forward<U>(u);                // 写入数据，无竞争
            // 更新尾指针，release保证数据写入后可见
            t.store(n, std::memory_order_release);
            return true;
        }

        // 多消费者出队：CAS竞争读取权，保证每个消息只被一个消费者读取
        bool pop(T& out) {
            while (true) {
                // 加载带版本的头指针，acquire保证读取到最新的版本和下标
                auto lvh = vh.load(std::memory_order_acquire);
                auto lh = lvh & IM;                      // 提取下标
                auto lt = t.load(std::memory_order_relaxed); // 读取尾指针，判断队列是否空
                if (lh == lt) return false;              // 队列空，出队失败

                // 版本号+1，生成新的带版本头指针，用于CAS竞争
                size_t nv = (lvh >> IB) + 1;
                size_t nvh = (nv << IB) | ((lh + 1) & MASK);

                // CAS竞争读取权：多消费者同时竞争，只有一个能成功
                // acq_rel保证CAS成功后，之前的读取操作可见，后续的写入操作可见
                if (vh.compare_exchange_strong(lvh, nvh, std::memory_order_acq_rel)) {
                    out = std::move(buf[lh]);            // 移动数据，零拷贝
                    return true;
                }
                // CAS失败，重试（HFT场景下通常会自旋重试，避免阻塞）
            }
        }
    };
}


#pragma once
#include <atomic>
#include <array>

namespace tdsys {
    // 多生产者单消费者无锁队列，HFT多策略报盘、多数据源汇聚专用
    template<typename T, size_t SZ = 127>
    class MPSCQueue {
    private:
        // 队列长度对齐为2的幂，HFT标准优化，位运算替代取模
        static constexpr size_t LEN = 1 << std::bit_width(SZ);
        static constexpr size_t MASK = LEN - 1;
        // 版本号16位，下标16位，解决多生产者CAS竞争的ABA问题
        static constexpr size_t IB = 16;
        static constexpr size_t IM = (1ULL << IB) - 1;

        // 队列元素包装：ok标记数据是否写入完成，多生产者场景必备
        struct W {
            std::atomic<bool> ok{ false };
            T data{};
        };

        // 头指针（消费者修改）、带版本的尾指针（多生产者CAS竞争修改），缓存行对齐
        alignas(64) std::atomic<size_t> h{ 0 }, vt{ 0 };
        std::array<W, LEN> buf{}; // 静态环形缓冲区，无动态内存分配

    public:
        // 单消费者出队：无竞争，延迟极低
        bool pop(T& out) {
            auto lh = h.load(std::memory_order_relaxed); // 读取头指针，relaxed无同步开销
            // 读取带版本的尾指针，acquire保证可见性
            auto tail = vt.load(std::memory_order_acquire) & IM;
            if (lh == tail) return false;               // 队列空，出队失败
            auto& w = buf[lh];
            if (!w.ok.load(std::memory_order_acquire)) return false; // 数据未就绪
            out = std::move(w.data);                    // 移动语义，零拷贝
            w.ok.store(false, std::memory_order_release); // 重置标记，允许下一次写入
            // 更新头指针，release保证可见性
            h.store((lh + 1) & MASK, std::memory_order_release);
            return true;
        }

        // 多生产者并发入队：CAS竞争写入权，无锁无阻塞
        template<typename U>
        bool push(U&& u) {
            size_t v;
            while (true) {
                // 加载带版本的尾指针，acquire保证读取到最新值
                auto lvt = vt.load(std::memory_order_acquire);
                auto lt = lvt & IM;                      // 提取下标
                auto n = (lt + 1) & MASK;                // 计算下一个写入位置
                // 读取头指针，判断队列是否满
                auto hh = h.load(std::memory_order_acquire) & IM;
                if (n == hh) return false;               // 队列满，入队失败

                // 版本号+1，生成新的带版本尾指针
                v = (lvt >> IB) + 1;
                size_t nvt = (v << IB) | n;

                // CAS竞争写入权：多生产者同时竞争，只有一个能成功
                if (vt.compare_exchange_strong(lvt, nvt, std::memory_order_acq_rel)) {
                    auto& w = buf[lt];
                    w.data = std::forward<U>(u);         // 完美转发，避免拷贝
                    w.ok.store(true, std::memory_order_release); // 标记数据就绪
                    return true;
                }
                // CAS失败，重试（HFT场景下自旋重试，无阻塞）
            }
        }
    };
}