---
title: ClickHouse线程池
date: 2022-04-20 16:10:28
tags: [ClickHouse]
categories:
     - OLAP
comments: true
---
### 线程池的一般原理
线程池（Thread Pool），简单理解就是系统一方面为了减少创建销毁线程的开销，另一方面避免
系统中线程数量膨胀导致的调度开销，而维护的一系列线程集合。其工作原理非常简单，线程池中维护一系列的worker线程和一个任务队列，这些worker线程不断的从任务队列里来取出任务并执行。客户端只需要通过接口向线程池中提交任务即可，线程池负责这些任务的调度与执行。接下来主要从ClickHouse中线程池的类关系，启动过程，worker工作线程，job提交几方面来讲述。

### 线程池的类关系
ClickHouse中的线程池实现定义在ThreadPool.文件中。类似于boost::threadpool。
几个主要类关系为下
```
/// ThreadPool with std::thread for threads.
using FreeThreadPool = ThreadPoolImpl<std::thread>;
class GlobalThreadPool : public FreeThreadPool, private boost::noncopyable
```
线程池中的任务定义为JobWithPriority，实现为ThreadPoolImpl内部的一个结构体。
```
using Job = std::function<void()>;
struct JobWithPriority
    {
        Job job;
        int priority;

        JobWithPriority(Job job_, int priority_)
            : job(job_), priority(priority_) {}

        bool operator< (const JobWithPriority & rhs) const
        {
            return priority < rhs.priority;
        }
    };
```
#### 线程池的启动
ThreadPoolImpl中有个成员变量std::list<Thread> threads来维护线程，
以及一个任务队列boost::heap::priority_queue<JobWithPriority> jobs;
调度方法实现在scheduleImpl中,如下，省略部分无关代码。首先判断线程队列中已有的线程数量
是否超过线程池设置的参数最大线程数量，如果没有超过，那么新启动一个新worker线程，并将job存到任务队列里。否则不会启动新的worker线程，只是将job放到任务队列。启动过程类似于Java线程池实现(ThreadPoolExecutor)[1]。
```
ReturnType ThreadPoolImpl<Thread>::scheduleImpl(Job job, int priority, std::optional<uint64_t> wait_microseconds)
{
        ....
        /// Check if there are enough threads to process job.
        if (threads.size() < std::min(max_threads, scheduled_jobs + 1))
        {
            try
            {
                threads.emplace_front();
            }
            catch (...)
            {
                /// Most likely this is a std::bad_alloc exception
                return on_error("cannot allocate thread slot");
            }

            try
            {
                //这一行代码创建线程了新的worker线程。Thread是模板类型std:thread
                threads.front() = Thread([this, it = threads.begin()] { worker(it); });
            }
            catch (...)
            {
                threads.pop_front();
                return on_error("cannot allocate thread");
            }
        }

        jobs.emplace(std::move(job), priority); //task入队列
        ++scheduled_jobs;
        new_job_or_shutdown.notify_one();
    }

    return ReturnType(true);
}
```
#### worker线程
每个worker线程,如下,省略部分非核心代码,以及异常判断。核心思想就是从tasks队列里按序
取出jobWithPriority对象，然后转换成job对象，可以理解为一个函数，然后执行这个函数，不段的重复这个过程。
```
void ThreadPoolImpl<Thread>::worker(typename std::list<Thread>::iterator thread_it)
{
    while (true)
    {
        /// This is inside the loop to also reset previous thread names set inside the jobs.
        setThreadName("ThreadPool");

        Job job;
        bool need_shutdown = false;

        {
            std::unique_lock lock(mutex);
            new_job_or_shutdown.wait(lock, [this] { return shutdown || !jobs.empty(); });
            need_shutdown = shutdown;

            if (!jobs.empty())
            {
                /// boost::priority_queue does not provide interface for getting non-const reference to an element
                /// to prevent us from modifying its priority. We have to use const_cast to force move semantics on JobWithPriority::job.
                job = std::move(const_cast<Job &>(jobs.top().job));
                jobs.pop();
            }
            else
            {
                /// shutdown is true, simply finish the thread.
                return;
            }

        }

        if (!need_shutdown)
        {
            try
            {
                ...
                job(); // 执行真正的任务
                /// job should be reset before decrementing scheduled_jobs to
                /// ensure that the Job destroyed before wait() returns.
                job = {};
            }
            catch (...)
            {
                /// job should be reset before decrementing scheduled_jobs to
                /// ensure that the Job destroyed before wait() returns.
                job = {};

                {
                    std::unique_lock lock(mutex);
                    if (!first_exception)
                        first_exception = std::current_exception(); // NOLINT
                    if (shutdown_on_exception)
                        shutdown = true;
                    --scheduled_jobs;
                }

                job_finished.notify_all();
                new_job_or_shutdown.notify_all();
                return;
            }
        }

        {
            std::unique_lock lock(mutex);
            --scheduled_jobs;

            if (threads.size() > scheduled_jobs + max_free_threads)
            {
                thread_it->detach();
                threads.erase(thread_it);
                job_finished.notify_all();
                return;
            }
        }

        job_finished.notify_all();
    }
}
```
#### job的提交
ClickHouse的线程实现为类ThreadFromGlobalPool，使用方法类似于std::thread,只不过添加了ThreadStatus for ClickHouse。ThreadFromGlobalPool的核心方法为他的构造函数。在创建ThreadFromGlobalPool对象时，同时也向GlobalThreadPool提交了job(方法scheduleorThrow中会调用上面讲到的scheduleImpl方法，从而将任务提交到线程池中去)
```
template <typename Function, typename... Args>
    explicit ThreadFromGlobalPool(Function && func, Args &&... args)
        : state(std::make_shared<Poco::Event>())
        , thread_id(std::make_shared<std::thread::id>())
    {
        /// NOTE: If this will throw an exception, the destructor won't be called.
        //scheduleOrThrow中的参数为lambda表达式
        GlobalThreadPool::instance().scheduleOrThrow([
            thread_id = thread_id,
            state = state,
            func = std::forward<Function>(func),
            args = std::make_tuple(std::forward<Args>(args)...)]() mutable /// mutable is needed to destroy capture
        {
            auto event = std::move(state);
            SCOPE_EXIT(event->set());

            thread_id = std::make_shared<std::thread::id>(std::this_thread::get_id());

            /// This moves are needed to destroy function and arguments before exit.
            /// It will guarantee that after ThreadFromGlobalPool::join all captured params are destroyed.
            auto function = std::move(func);
            auto arguments = std::move(args);

            /// Thread status holds raw pointer on query context, thus it always must be destroyed
            /// before sending signal that permits to join this thread.
            DB::ThreadStatus thread_status;
            std::apply(function, arguments);
        });

    }
```
#### 参考文献
[1] https://tech.meituan.com/2020/04/02/java-pooling-pratice-in-meituan.html
