#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <functional>
#include <sys/ucontext.h>
#include <ucontext.h>


class Fiber : public std::enable_shared_from_this<Fiber>{
friend class Scheduler;
public:
    typedef std::shared_ptr<Fiber> ptr;
    enum State{
        INIT,
        HOLD,
        EXEC,
        TERM,
        READY,
        EXCEPT
    };

private:
    Fiber();

public:
    Fiber(std::function<void()> cb , size_t stack_size = 0 , bool use_caller = false);
    ~Fiber();
    void reset(std::function<void()> cb);
    void swapIn();
    void swapOut();
    void call();
    void back();
    uint64_t getID() const {
        return m_id;
    }
    State getState() const {
        return m_state;
    }

    bool TryMarkQueued() {
        bool expected = false;
        return m_queued.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
    }
    void ClearQueued() { m_queued.store(false, std::memory_order_release); }
    bool IsQueued() const { return m_queued.load(std::memory_order_acquire); }

    uint64_t readyAtUs() const { return m_ready_at_us.load(std::memory_order_acquire); }
    void setReadyAtUs(uint64_t v) { m_ready_at_us.store(v, std::memory_order_release); }

public:
    static void SetThis(Fiber *f);
    static Fiber::ptr GetThis();
    static void YieldToHold();
    static void YieldToReady();
    static uint64_t TotalFibers();
    static void MainFunc();
    static void CallMainFunc();
    static uint64_t GetFiberID();


private:
    uint64_t m_id = 0;
    uint32_t m_stacksize = 0;
    State m_state = State::INIT;
    ucontext_t m_ctx;
    void *m_stack = nullptr;
    std::function<void()> m_cb;
    std::atomic<bool>     m_queued{false};
    std::atomic<uint64_t> m_ready_at_us{0};
};