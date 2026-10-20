#include "scheduler.h"

#include "assert.h"
#include "fiber/thread.h"
#include <chrono>
#include <butil/logging.h>
#include <mutex>
#include <unistd.h>

static thread_local Scheduler* t_scheduler = nullptr;
static thread_local Fiber* t_scheduler_fiber = nullptr;
static thread_local bool t_job_finished = false;

void Scheduler::setJobFinish(bool value){
    t_job_finished = value;
}

Scheduler::Scheduler(size_t threads, bool use_caller, const std::string& name)
    :m_name(name) {
    assert(threads > 0);
    if (use_caller){
        // 初始化一下 t_fiber ，作为调度协程
        Fiber::GetThis();
        --threads;
        assert(GetThis() == nullptr);
        t_scheduler = this;
        m_rootFiber.reset(new Fiber(std::bind(&Scheduler::run, this), 0, true));
        Thread::SetName(m_name);

        t_scheduler_fiber = m_rootFiber.get();
        m_rootThread = getThreadID();
        m_threadIds.push_back(m_rootThread);
    }else {
        m_rootThread = -1;
    }
    m_threadCount = threads;
}

Scheduler::Scheduler(const std::string &name){
    m_name = name;
    // 不采用 caller 模式
    m_rootThread = -1;
    m_threadCount = 0;
}

Scheduler::~Scheduler(){
    assert(m_stopping);
    if (GetThis() == this){
        t_scheduler = nullptr;
    }
}

Scheduler* Scheduler::GetThis() {
    return t_scheduler;
}

Fiber* Scheduler::GetMainFiber() {
    return t_scheduler_fiber;
}

int Scheduler::addThread(){
    MutexType::Lock lock(m_mutex);
    assert(m_name == "SQL_Scheduler");
    // LOG(INFO) << "Add A Thread\n";
    m_threads.emplace_back(new Thread(std::bind(&Scheduler::sql_run , this) , m_name + "_" + std::to_string(m_threadCount)));
    m_threadIds.push_back(m_threads[m_threadCount]->getID());

    int ret = m_threads[m_threadCount]->getID();
    m_threadCount++;

    return ret;
}

void Scheduler::start(){
    MutexType::Lock lock(m_mutex);
    if (!m_stopping){
        return;
    }
    std::cout << "Scheduler Start\n";
    m_stopping = false;
    assert(m_threads.empty());
    m_threads.resize(m_threadCount);
    for(size_t i = 0; i < m_threadCount; ++i) {
        if (m_name == "SQL_Scheduler"){
            m_threads[i].reset(new Thread(std::bind(&Scheduler::sql_run, this)
                            , m_name + "_" + std::to_string(i)));
        }else if (m_name == "PushPageScheduler"){
            m_threads[i].reset(new Thread(std::bind(&Scheduler::push_page_run, this)
                            , m_name + "_" + std::to_string(i)));
        }else {
            m_threads[i].reset(new Thread(std::bind(&Scheduler::run, this)
                            , m_name + "_" + std::to_string(i)));
        }
        m_threadIds.push_back(m_threads[i]->getID());
    }
    lock.unlock();
}

void Scheduler::stop(){
    std::cout << "Scheduler Stop\n";
    m_autoStop = true;
    if(m_rootFiber
        && m_threadCount == 0
        && (m_rootFiber->getState() == Fiber::TERM
            || m_rootFiber->getState() == Fiber::INIT)) {
        std::cout << "Scheduler Stopped";
        m_stopping = true;

        if(stopping()) {
            return;
        }
    }

    if (m_rootThread != -1){
        assert(GetThis() == this);
    }else {
        assert(GetThis() != this);
    }

    m_stopping = true;
    for(size_t i = 0; i < m_threadCount; ++i) {
        tickle();
    }

    if(m_rootFiber) {
        tickle();
    }

    if (m_rootFiber){
        if(!stopping()) {
            m_rootFiber->call();
        }
    }

    std::vector<Thread::ptr> thrs;
    {
        MutexType::Lock lock(m_mutex);
        thrs.swap(m_threads);
    }

    for (auto &i : thrs){
        i->join();
    }   
}

void Scheduler::setThis() {
    t_scheduler = this;
}

void Scheduler::tickle() {
}

bool Scheduler::stopping() {
    MutexType::Lock lock(m_mutex);
    return m_autoStop && m_stopping
        && m_fibers.empty() && m_activeThreadCount == 0;
}

void Scheduler::idle() {
    // std::cout << "idle" << "\n";
    while(!stopping()) {
        Fiber::YieldToHold();
    }
}


void Scheduler::switchTo(int thread) {
    assert(Scheduler::GetThis() != nullptr);
    if(Scheduler::GetThis() == this) {
        if(thread == -1 || thread == getThreadID()) {
            return;
        }
    }
    schedule(Fiber::GetThis(), thread);
    Fiber::YieldToHold();
}

void Scheduler::scheduleToSlice(Fiber::ptr fiber, size_t slice_id, int thread) {
    assert(fiber);
    schedule(fiber, thread);
}

void Scheduler::scheduleToSlice(std::function<void()> cb, size_t slice_id, int thread) {
    if(!cb) {
        return;
    }
    schedule(cb, thread);
}

void Scheduler::markPushReady(LLSN now_persist_lsn){
    LLSN observed = m_push_ready_lsn.load(std::memory_order_relaxed);
    while (observed < now_persist_lsn &&
           !m_push_ready_lsn.compare_exchange_weak(observed, now_persist_lsn,
                                                   std::memory_order_release,
                                                   std::memory_order_relaxed)) {
    }
}

void Scheduler::push_page_run(){
    setThis();
    if (getThreadID() != m_rootThread){
        t_scheduler_fiber = Fiber::GetThis().get();
    }
    Fiber::ptr cb_fiber;
    FiberAndThread ft;

    while (true){
        ft.reset();
        bool is_active = false;
        {
            MutexType::Lock lock(m_mutex);
            LLSN ready_lsn = m_push_ready_lsn.load(std::memory_order_acquire);
            auto it = m_fibers.begin();
            while (it != m_fibers.end()){
                // 如果这个任务不是指定我执行的，那我就跳过
                if(it->thread != -1 && it->thread != getThreadID()) {
                    ++it;
                    continue;
                }
                assert(it->fiber || it->thread);
                
                // 如果这个协程已经有别的线程在跑了
                if (it->fiber && it->fiber->getState() == Fiber::State::EXEC){
                    ++it;
                    continue;
                }

                if (!it->can_push_page && (it->wait_lsn == 0 || it->wait_lsn <= ready_lsn)){
                    it->can_push_page = true;
                }

                if (!it->can_push_page){
                    ++it;
                    // 还没准备好推送页面呢，等着吧
                    continue;
                }

                ft = *it;
                m_fibers.erase(it++);
                m_queueSize.fetch_sub(1, std::memory_order_relaxed);
                if (ft.fiber) ft.fiber->ClearQueued();   // 出队 = 释放"已排队"权
                ++m_activeThreadCount;
                is_active = true;
                break;
            }
        }

        if (ft.fiber && (ft.fiber->getState() != Fiber::State::TERM
                    && ft.fiber->getState() != Fiber::State::EXCEPT)){
            ft.fiber->swapIn();

            --m_activeThreadCount;
            if (ft.fiber->getState() == Fiber::State::READY){
                schedule(ft.fiber, getThreadID());
            } else if(ft.fiber->getState() != Fiber::TERM
                && ft.fiber->getState() != Fiber::EXCEPT) {
                ft.fiber->m_state = Fiber::HOLD;
            }
            ft.reset();
        }else if (ft.cb){
            if(cb_fiber) {
                cb_fiber->reset(ft.cb);
            } else {
                cb_fiber.reset(new Fiber(ft.cb));
            }
            ft.reset();
            cb_fiber->swapIn();
            --m_activeThreadCount;
            if(cb_fiber->getState() == Fiber::READY) {
                schedule(cb_fiber, getThreadID());
                cb_fiber.reset();
            } else if(cb_fiber->getState() == Fiber::EXCEPT
                    || cb_fiber->getState() == Fiber::TERM) {
                cb_fiber->reset(nullptr);
            } else {//if(cb_fiber->getState() != Fiber::TERM) {
                cb_fiber->m_state = Fiber::HOLD;
                cb_fiber.reset();
            }
        }else {
            if(is_active) {
                --m_activeThreadCount;
                continue;
            }
            usleep(10);
        }
    }
}

// run 函数太杂了，这里重新写个简化版本的，取 SQL 任务做
void Scheduler::sql_run(){
    setThis();
    if (getThreadID() != m_rootThread){
        t_scheduler_fiber = Fiber::GetThis().get();
    }
    // 对于那些 cb，需要用一个协程把它包起来，cb_fiber 就是干这个的
    Fiber::ptr cb_fiber;
    FiberAndThread ft;
    while(true){
        if (t_job_finished){
            break;
        }
        ft.reset();
        bool is_active = false;
        {
            MutexType::Lock lock(m_mutex);
            auto it = m_fibers.begin();
            while (it != m_fibers.end()){
                // 如果这个任务不是指定我执行的，那我就跳过
                if(it->thread != -1 && it->thread != getThreadID()) {
                    ++it;
                    // tickle_me = true;
                    continue;
                }
                assert(it->fiber || it->thread);
                
                // 如果这个协程已经有别的线程在跑了
                if (it->fiber && it->fiber->getState() == Fiber::State::EXEC){
                    ++it;
                    continue;
                }
                // 就绪时间存在 fiber 自己身上（唤醒方可直接拨到 0）；cb 任务仍用 delay_us
                if (it->fiber){
                    uint64_t ready_at = it->fiber->readyAtUs();
                    if (ready_at != 0) {
                        uint64_t current_time_us = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::high_resolution_clock::now().time_since_epoch()
                        ).count();
                        if (current_time_us <= ready_at) { ++it; continue; }
                        it->fiber->setReadyAtUs(0);
                    }
                } else if (it->delay_us != 0){
                    uint64_t current_time_us = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::high_resolution_clock::now().time_since_epoch()
                    ).count();
                    if (current_time_us <= it->delay_us) { ++it; continue; }
                    it->delay_us = 0;
                }

                ft = *it;
                m_fibers.erase(it++);
                m_queueSize.fetch_sub(1, std::memory_order_relaxed);
                if (ft.fiber) ft.fiber->ClearQueued();   // 出队 = 释放"已排队"权
                ++m_activeThreadCount;
                is_active = true;
                break;
            }
        }

        if (ft.fiber && (ft.fiber->getState() != Fiber::State::TERM
                    && ft.fiber->getState() != Fiber::State::EXCEPT)){
            ft.fiber->swapIn();

            --m_activeThreadCount;
            if (ft.fiber->getState() == Fiber::State::READY){
                schedule(ft.fiber, getThreadID());
            } else if(ft.fiber->getState() != Fiber::TERM
                && ft.fiber->getState() != Fiber::EXCEPT) {
                ft.fiber->m_state = Fiber::HOLD;
            }
            ft.reset();
        }else if (ft.cb){
            if(cb_fiber) {
                cb_fiber->reset(ft.cb);
            } else {
                cb_fiber.reset(new Fiber(ft.cb));
            }
            ft.reset();
            cb_fiber->swapIn();
            --m_activeThreadCount;
            if(cb_fiber->getState() == Fiber::READY) {
                schedule(cb_fiber, getThreadID());
                cb_fiber.reset();
            } else if(cb_fiber->getState() == Fiber::EXCEPT
                    || cb_fiber->getState() == Fiber::TERM) {
                cb_fiber->reset(nullptr);
            } else {//if(cb_fiber->getState() != Fiber::TERM) {
                cb_fiber->m_state = Fiber::HOLD;
                cb_fiber.reset();
            }
        }else {
            if(is_active) {
                --m_activeThreadCount;
                continue;
            }
            usleep(10);
        }
    }
    std::cout << "Thread : " << getThreadID() << " Quit\n";
}

void Scheduler::run(){
    setThis();
    if (getThreadID() != m_rootThread){
        t_scheduler_fiber = Fiber::GetThis().get();
    }

    // Fiber::ptr idle_fiber(new Fiber(std::bind(&Scheduler::idle, this)));
    // 对于那些 cb，需要用一个协程把它包起来，cb_fiber 就是干这个的
    Fiber::ptr cb_fiber;

    FiberAndThread ft;
    while(true){
        ft.reset();
        // bool tickle_me = false;
        bool is_active = false;

        // 首先检查是否有新的任务，或者超时的任务需要去完成，优先去跑这些任务
        {
            MutexType::Lock lock(m_mutex);
            auto it = m_fibers.begin();
            while (it != m_fibers.end()){
                // 如果这个任务不是指定我执行的，那我就跳过
                if(it->thread != -1 && it->thread != getThreadID()) {
                    ++it;
                    // tickle_me = true;
                    continue;
                }
                assert(it->fiber || it->thread);
                
                // 如果这个协程已经有别的线程在跑了
                if (it->fiber && it->fiber->getState() == Fiber::State::EXEC){
                    ++it;
                    continue;
                }
                // 就绪时间存在 fiber 自己身上（唤醒方可直接拨到 0）；cb 任务仍用 delay_us
                if (it->fiber){
                    uint64_t ready_at = it->fiber->readyAtUs();
                    if (ready_at != 0) {
                        uint64_t current_time_us = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::high_resolution_clock::now().time_since_epoch()
                        ).count();
                        if (current_time_us <= ready_at) { ++it; continue; }
                        it->fiber->setReadyAtUs(0);
                    }
                } else if (it->delay_us != 0){
                    uint64_t current_time_us = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::high_resolution_clock::now().time_since_epoch()
                    ).count();
                    if (current_time_us <= it->delay_us) { ++it; continue; }
                    it->delay_us = 0;
                }

                ft = *it;
                m_fibers.erase(it++);
                m_queueSize.fetch_sub(1, std::memory_order_relaxed);
                if (ft.fiber) ft.fiber->ClearQueued();   // 出队 = 释放"已排队"权
                ++m_activeThreadCount;
                is_active = true;
                break;
            }
        }

        assert(m_activeThreadCount <= m_threadCount);

        if (ft.fiber && (ft.fiber->getState() != Fiber::State::TERM
                    && ft.fiber->getState() != Fiber::State::EXCEPT)){
            
            // auto start_time = std::chrono::high_resolution_clock::now();
            ft.fiber->swapIn();
            // auto end_time = std::chrono::high_resolution_clock::now();
            // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            // auto ms = duration.count() / 1000;
            // auto us = duration.count() % 1000;

            --m_activeThreadCount;
            if (ft.fiber->getState() == Fiber::State::READY){
                schedule(ft.fiber, getThreadID());
            } else if(ft.fiber->getState() != Fiber::TERM
                && ft.fiber->getState() != Fiber::EXCEPT) {
                ft.fiber->m_state = Fiber::HOLD;
            }
            ft.reset();
        }else if (ft.cb){
            if(cb_fiber) {
                cb_fiber->reset(ft.cb);
            } else {
                cb_fiber.reset(new Fiber(ft.cb));
            }
            ft.reset();
            cb_fiber->swapIn();
            --m_activeThreadCount;
            if(cb_fiber->getState() == Fiber::READY) {
                schedule(cb_fiber, getThreadID());
                cb_fiber.reset();
            } else if(cb_fiber->getState() == Fiber::EXCEPT
                    || cb_fiber->getState() == Fiber::TERM) {
                cb_fiber->reset(nullptr);
            } else {//if(cb_fiber->getState() != Fiber::TERM) {
                cb_fiber->m_state = Fiber::HOLD;
                cb_fiber.reset();
            }
        }else {
            if(is_active) {
                --m_activeThreadCount;
                continue;
            }
            // 走到这里，说明没任务给他做了，不要一直跑，会占用锁的
            usleep(10);
            // if(idle_fiber->getState() == Fiber::TERM) {
            //     // std::cout << "FIBER TERM\n";
            //     break;
            // }

            // ++m_idleThreadCount;
            // // 进入到 idle_fiber 之后，立刻回来
            // idle_fiber->swapIn();
            // --m_idleThreadCount;
            // if(idle_fiber->getState() != Fiber::TERM
            //         && idle_fiber->getState() != Fiber::EXCEPT) {
            //     idle_fiber->m_state = Fiber::HOLD;
            // }
        }
    }
}
