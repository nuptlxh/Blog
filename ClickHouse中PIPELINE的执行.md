---
title: ClickHouse中PIPELINE的执行
date: 2022-04-24 16:10:28
tags: [ClickHouse]
categories:
     - [大数据]
comments: true
---
#### PIPELINE的执行过程
我们按照执行过程中的函数调用来讲解执行的细节。
以SELECT语句为例，
首先在TCPHandker.cpp中调用TCPHandler::processOrdinaryQueryWithProcessors方法。
(1)处构建了一个PullingAsyncPipelineExecutor对象executor，然后不断执行executor的pull方法，结果通过block返回。pull返回一个布尔值，返回false表示query结束。
```
void TCPHandler::processOrdinaryQueryWithProcessors()
{
    auto & pipeline = state.io.pipeline;
    ...
    {
        PullingAsyncPipelineExecutor executor(pipeline);                  //(1)
        ...
        Block block;
        while (executor.pull(block, interactive_delay / 1000))            //(2)
        {
            ...
            if (block)
            {
                if (!state.io.null_format)
                    sendData(block);
            }
        }
    ...
    sendProgress();
}
```
pull方法主要完成的任务是构造一个函数func(1)处，然后将这个函数提交到线程池中(3)处(关于如何提交job的细节可以看[上一篇文章](https://nuptlxh.github.io/2022/04/17/ClickHouse_threadpool/))。因此最终一定会在某个线程中执行threadfuntion函数。
```
bool PullingAsyncPipelineExecutor::pull(Chunk & chunk, uint64_t milliseconds)
{
    if (!data)
    {
        data = std::make_unique<Data>();
        data->executor = std::make_shared<PipelineExecutor>(pipeline.processors, pipeline.process_list_element);
        data->lazy_format = lazy_format.get();

        auto func = [&, thread_group = CurrentThread::getGroup()]()       //(1)
        {
            threadFunction(*data, thread_group, pipeline.getNumThreads());//(2)
        };

        data->thread = ThreadFromGlobalPool(std::move(func));             //(3)
    }

    ...
    return true;
}
```
threadfunction是一个静态方法，方法中会调用data.executor->execute(num_threads);
execute方法中会调用executeImpl方法(num_threads)方法，下面详细看executeImpl方法。
首先(2)处创建了一个Vector，其容器类型为[ThreadFromGloablPool](https://nuptlxh.github.io/2022/04/17/ClickHouse_threadpool/)，其功能类似于std::thread，
只不过添加了一些额外的一些线程状态信息。然后在for循环中(3)处，通过thread.emplace_back，在想vector中添加对象的同时，也会向线程池中提交job，因为[ClickHouse中在类ThreadFromGlobal的构造函数中就向线程池提交任务](https://nuptlxh.github.io/2022/04/17/ClickHouse_threadpool/)。而任务就是(3)处中emplace_back的参数lamda表达式。整个executeImple函数的核心逻辑为根据参数num_threads，来创建同样数量的job并提交到线程之中去，也意味着此PIPELINE由num_theads个线程来执行。可以看到，每个线程中的核心方法为executeSingleThread(thread_num)(4)处。

(1)处的initializeExecution(num_threads)方法其实是实现ClickHouse，pull 流水线的逻辑。数据库中常用的模型还有比如火山模型等等(TODO总结常见的流水线模型)。
```
void PipelineExecutor::executeImpl(size_t num_threads)
{
    ...
    initializeExecution(num_threads);                                     //(1)

    using ThreadsData = std::vector<ThreadFromGlobalPool>;                //(2)
    ThreadsData threads;
    threads.reserve(num_threads);

    bool finished_flag = false;
    ...

    if (num_threads > 1)
    {
        auto thread_group = CurrentThread::getGroup();

        for (size_t i = 0; i < num_threads; ++i)
        {    
            //(3)empalce_back函数接受的参数是lambda表达式会被转化为job。
            threads.emplace_back([this, thread_group, thread_num = i]
            {
                /// ThreadStatus thread_status;

                setThreadName("QueryPipelineEx");

                if (thread_group)
                    CurrentThread::attachTo(thread_group);

                try
                {
                    executeSingleThread(thread_num);                      //(4)
                }
                catch (...)
                {
                    /// In case of exception from executor itself, stop other threads.
                    finish();
                    tasks.getThreadContext(thread_num).setException(std::current_exception());
                }
            });
        }

      ...
}
```
pipeline的构造函数中会将pipeline的processor转化成DAG图，对应ClickHouse中类为ExecutingGraph。initializeExecution函数会调用ExecutingGraph的initializeExecution函数。initializeExecution函数中首先在DAG图中找到没有出边的节点作为启动节点并加入栈中，然后从这个节点updateNode。UpdateNode函数通过执行当前节点的
prepare方法去拉取数据，然后更新与当前节点相关联的edge(这里有个trick，prepare方法更新的是processor的端口状态，那么如何通过端口来找到与之关联的edge呢，其实端口对象中有一集结构体updateInfo，其中有一个字段id表示了其edge的地址(指针)详细参考[[1]](https://www.cnblogs.com/wyc2021/p/15648668.html))然后通过edge找到下一个node，执行下一个node的prepare方法，从而依次类推，最终将所有状态为Ready的node将入到队列中，加入到队列的node会被线程池中的线程去执行其node中的procesor中的work方法。
```
void ExecutingGraph::initializeExecution(Queue & queue)
{
    std::stack<uint64_t> stack;

    /// Add childless processors to stack.
    uint64_t num_processors = nodes.size();
    for (uint64_t proc = 0; proc < num_processors; ++proc)
    {
        if (nodes[proc]->direct_edges.empty())
        {
            stack.push(proc);
            /// do not lock mutex, as this function is executed in single thread
            nodes[proc]->status = ExecutingGraph::ExecStatus::Preparing;
        }
    }
    ...
    while (!stack.empty())
    {
        uint64_t proc = stack.top();
        stack.pop();
        updateNode(proc, queue, async_queue);
        ...
    }
}
```

```
bool ExecutingGraph::updateNode(uint64_t pid, Queue & queue, Queue & async_queue)
{
    std::stack<Edge *> updated_edges;        //需要更新的edge
    std::stack<uint64_t> updated_processors; //需要更新端口状态的processon
    updated_processors.push(pid);

    UpgradableMutex::ReadGuard read_lock(nodes_mutex);

    while (!updated_processors.empty() || !updated_edges.empty())
    {
        std::optional<std::unique_lock<std::mutex>> stack_top_lock;

        if (updated_processors.empty())
        {
            auto * edge = updated_edges.top();
            updated_edges.pop();

            /// Here we have ownership on edge, but node can be concurrently accessed.

            auto & node = *nodes[edge->to];     //通过edge找到与这条边相关联的node

            std::unique_lock lock(node.status_mutex);

            ExecutingGraph::ExecStatus status = node.status;

            //这里的updated_output_ports与updated_input_portss实际上并没有用，可能是为了后续可能的优化
            if (status != ExecutingGraph::ExecStatus::Finished)
            {
                if (edge->backward)
                    node.updated_output_ports.push_back(edge->output_port_number);
                else
                    node.updated_input_ports.push_back(edge->input_port_number);

                if (status == ExecutingGraph::ExecStatus::Idle)
                {
                    node.status = ExecutingGraph::ExecStatus::Preparing;
                    updated_processors.push(edge->to);//edge相关联的node入栈
                    stack_top_lock = std::move(lock);
                }
                else
                    nodes[edge->to]->processor->onUpdatePorts();
            }
        }

        if (!updated_processors.empty())
        {
            pid = updated_processors.top();
            updated_processors.pop();

            /// In this method we have ownership on node.
            auto & node = *nodes[pid];

            bool need_expand_pipeline = false;
            {
                std::unique_lock<std::mutex> lock(std::move(*stack_top_lock));

                try
                {
                    auto & processor = *node.processor;
                    IProcessor::Status last_status = node.last_processor_status;
                    //通过执行当前node中的prepare方法来拉取数据并更新与之相关的端口状态。
                    IProcessor::Status status = processor.prepare(node.updated_input_ports, node.updated_output_ports);
                    node.last_processor_status = status;

                    if (profile_processors)
                    {
                      ...
                    }
                }
                catch (...)
                {
                    node.exception = std::current_exception();
                    return false;
                }
                node.updated_input_ports.clear();
                node.updated_output_ports.clear();

                switch (node.last_processor_status)
                {
                    case IProcessor::Status::NeedData:
                    case IProcessor::Status::PortFull:
                    {
                        node.status = ExecutingGraph::ExecStatus::Idle;
                        break;
                    }
                    case IProcessor::Status::Finished:
                    {
                        node.status = ExecutingGraph::ExecStatus::Finished;
                        break;
                    }
                    case IProcessor::Status::Ready:
                    {
                        node.status = ExecutingGraph::ExecStatus::Executing;
                        queue.push(&node);// node状态为ready表示该节点对应的processor，中的数据准备好了，可以进行处理，
                        //放入queue里。当方法返回后，线程池中的线程会从这个队列里取出这个node然后执行其work方法。
                        break;
                    }
                    case IProcessor::Status::Async:
                    {
                        node.status = ExecutingGraph::ExecStatus::Executing;
                        async_queue.push(&node);
                        break;
                    }
                    case IProcessor::Status::ExpandPipeline:
                    {
                        need_expand_pipeline = true;
                        break;
                    }
                }

                if (!need_expand_pipeline)
                {
                    /// If you wonder why edges are pushed in reverse order,
                    /// it is because updated_edges is a stack, and we prefer to get from stack
                    /// input ports firstly, and then outputs, both in-order.
                    ///
                    /// Actually, there should be no difference in which order we process edges.
                    /// However, some tests are sensitive to it (e.g. something like SELECT 1 UNION ALL 2).
                    /// Let's not break this behaviour so far.

                    for (auto it = node.post_updated_output_ports.rbegin(); it != node.post_updated_output_ports.rend(); ++it)
                    {
                        auto * edge = static_cast<ExecutingGraph::Edge *>(*it);
                        updated_edges.push(edge);
                        edge->update_info.trigger();
                    }

                    for (auto it = node.post_updated_input_ports.rbegin(); it != node.post_updated_input_ports.rend(); ++it)
                    {
                        auto * edge = static_cast<ExecutingGraph::Edge *>(*it);
                        updated_edges.push(edge);
                        edge->update_info.trigger();
                    }

                    node.post_updated_input_ports.clear();
                    node.post_updated_output_ports.clear();
                }
            }

            if (need_expand_pipeline)
            {
                {
                    UpgradableMutex::WriteGuard lock(read_lock);
                    if (!expandPipeline(updated_processors, pid))
                        return false;
                }

                /// Add itself back to be prepared again.
                updated_processors.push(pid);
            }
        }
    }

    return true;
}
```

executeSingleThread方法中会调用executeStepImpl函数，(1)处context的类型为ExecutionThreadContext。(2)处的tryGetTask方法，其实就是pipeineExecutor中通过成员变量ExecutorTasks维护了一个vector<vector<Node>>这样一个二维任务队列。每个线程根据线程id来读取他自己的任务队列。tryGetTask方法就是将node从任务队列取出，然后通过threadContext来将当前正在处理的节点设置为node，那么context.hasTask方法通过当前node是否为空来判断是否有任务，有则执行(3)，context.executeTaskh会调用executeJob。
```
void PipelineExecutor::executeStepImpl(size_t thread_num, std::atomic_bool * yield_flag)
{

    auto & context = tasks.getThreadContext(thread_num);                //(1)
    bool yield = false;

    while (!tasks.isFinished() && !yield)
    {
        /// First, find any processor to execute.
        /// Just traverse graph and prepare any processor.
        while (!tasks.isFinished() && !context.hasTask())
            tasks.tryGetTask(context);                                  //(2)

        while (context.hasTask() && !yield)
        {
            if (tasks.isFinished())
                break;

            if (!context.executeTask())                                  //(3)
                cancel();

            if (tasks.isFinished())
                break;

            if (!checkTimeLimitSoft())
                break;

#ifndef NDEBUG
            Stopwatch processing_time_watch;
#endif

            /// Try to execute neighbour processor.
            {
                Queue queue;
                Queue async_queue;

                /// Prepare processor after execution.
                if (!graph->updateNode(context.getProcessorID(), queue, async_queue))
                    finish();

                /// Push other tasks to global queue.
                tasks.pushTasks(queue, async_queue, context);
            }


}
```
executeJob是一个静态方法，会执行pipeline中的processor的work方法，
```
static void executeJob(IProcessor * processor)
{
    try
    {
        processor->work();
    }
    catch (Exception & exception)
    {
        if (checkCanAddAdditionalInfoToException(exception))
            exception.addMessage("While executing " + processor->getName());
        throw;
    }
}
```
#### 参考文献
[1]https://www.cnblogs.com/wyc2021/p/15648668.html
