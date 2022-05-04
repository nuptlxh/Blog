---
title: ClickHouse中PIPELINE的执行
date: 2022-04-24 16:10:28
tags: [ClickHouse]
categories:
     - [大数据]
comments: true
---
#### 核心概念和数据结构
ClickHouse中Pipeline的表示，是用Port将不同的processor连接起来。processor接口中
核心两个方法prepare和work方法。prepare可以简单理解为拉取数据。如果经过prepare方法数据准备好后，再通过执行work方法来处理数据，可能需要把处理完的数据推送到它的outPort并更新outPort状态。还有个比较重要的事情，即两个相连接的端口是共享状态的。即如果processor在work方法执行完毕后，改变了它的outport状态，那么processorB的inport端口状态也改变了。因此通过这种端口共享状态的方式实现了processor之间的状态变化感知。
```
prcessorA ->outport->inport->procesorB
```
```
class IProcessor
{
  protected:
    InputPorts inputs;
    OutputPorts outputs;
  public:
    virtual Status prepare()
    {
        throw Exception("Method 'prepare' is not implemented for " + getName() + " processor", ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Method 'prepare' is responsible for all cheap ("instantaneous": O(1) of data volume, no wait) calculations.
     *
     * It may access input and output ports,
     *  indicate the need for work by another processor by returning NeedData or PortFull,
     *  or indicate the absence of work by returning Finished or Unneeded,
     *  it may pull data from input ports and push data to output ports.
     *
     * The method is not thread-safe and must be called from a single thread in one moment of time,
     *  even for different connected processors.
     *
     * Instead of all long work (CPU calculations or waiting) it should just prepare all required data and return Ready or Async.
     *
     * Thread safety and parallel execution:
     * - no methods (prepare, work, schedule) of single object can be executed in parallel;
     * - method 'work' can be executed in parallel for different objects, even for connected processors;
     * - method 'prepare' cannot be executed in parallel even for different objects,
     *   if they are connected (including indirectly) to each other by their ports;
     */
   virtual Status prepare()
   {
       throw Exception("Method 'prepare' is not implemented for " + getName() + " processor", ErrorCodes::NOT_IMPLEMENTED);
   }

   /// Optimization for prepare in case we know ports were updated.
   virtual Status prepare(const PortNumbers & /*updated_input_ports*/, const PortNumbers & /*updated_output_ports*/) { return prepare(); }

   /** You may call this method if 'prepare' returned Ready.
       * This method cannot access any ports. It should use only data that was prepared by 'prepare' method.
       *
       * Method work can be executed in parallel for different processors.
       */
     virtual void work()
     {
         throw Exception("Method 'work' is not implemented for " + getName() + " processor", ErrorCodes::NOT_IMPLEMENTED);
     }
}
```
ClickHouse在执行Pipeline之前会把Pipeline转化为ExecutingGraph，简单理解就是把pipeline中每个processor转化为node。pipeline中DAG图的每条连接在转化为ExecutingGraph后都会有两条边，分别为direct_edge 和 backward_edge。通过edge来关联每个node。其中的node中的direct_edges和back_edges容易让人迷惑。举个简单的例子，如下图，红色虚线表示原始的pipeline，数据流向表示的是数据从数据源中读出，经过各个tranform处理的数据流向。从图中可以看出原始的pipeline中的一个连接对应着两条边，比方说NodeA和NodeB的连接，那么边1是NodeA的direct_edge,边4是NodeB的backward_edge。每个edge结构体中有个to字段，表示的是与这条边相关联的Node。对于direct_edge来说，这个边对应的是节点的outPort，而backward_edge对应的是节点的inport。还需要注意的是edge中的input_port_number和output_port_number指的是这条边相关联的一个inport和一个outPort的序号。因此对应于原pipeline中一个连接的两条边，他们的inport_numer和output_port_number是一样的。

![](https://lxhblog.oss-cn-beijing.aliyuncs.com/bigdata/edge.png)

```
/// Edge represents connection between OutputPort and InputPort.
/// For every connection, two edges are created: direct and backward (it is specified by backward flag).
struct Edge
    {
        {
            update_info.update_list = update_list;
            update_info.id = this;
        }

        /// Processor id this edge points to.
        /// It is processor with output_port for direct edge or processor with input_port for backward.
        bool backward;
        /// Port numbers. They are same for direct and backward edges.
        uint64_t input_port_number;
        uint64_t output_port_number;

        /// Edge version is increased when port's state is changed (e.g. when data is pushed). See Port.h for details.
        /// To compare version with prev_version we can decide if neighbour processor need to be prepared.
        Port::UpdateInfo update_info;
    };


    struct Node
    {
        /// Processor and it's position in graph.
        IProcessor * processor = nullptr;
        uint64_t processors_id = 0;

        /// Direct edges are for output ports, back edges are for input ports.
        Edges direct_edges;
        Edges back_edges;

        /// Ports which have changed their state since last processor->prepare() call.
        /// They changed when neighbour processors interact with connected ports.
        /// They will be used as arguments for next processor->prepare() (and will be cleaned after that).
        IProcessor::PortNumbers updated_input_ports;
        IProcessor::PortNumbers updated_output_ports;

        /// Ports that have changed their state during last processor->prepare() call.
        /// We use this data to fill updated_input_ports and updated_output_ports for neighbour nodes.
        /// This containers are temporary, and used only after processor->prepare() is called.
        /// They could have been local variables, but we need persistent storage for Port::UpdateInfo.
        Port::UpdateInfo::UpdateList post_updated_input_ports;
        Port::UpdateInfo::UpdateList post_updated_output_ports;

    };
```
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
只不过添加了一些额外的一些线程状态信息。然后在for循环中(3)处，通过thread.emplace_back，在向vector中添加对象的同时，也会向线程池中提交job，因为[ClickHouse中在类ThreadFromGlobal的构造函数中就向线程池提交任务](https://nuptlxh.github.io/2022/04/17/ClickHouse_threadpool/)。而任务就是(3)处中emplace_back的参数lamda表达式。整个executeImple函数的核心逻辑为根据参数num_threads，来创建同样数量的job并提交到线程之中去，也意味着此PIPELINE由num_theads个线程来执行。可以看到，每个线程中的核心方法为executeSingleThread(thread_num)(4)处。

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
开篇讲过pipeline的构造函数中会将pipeline的processor转化成DAG图，对应ClickHouse中类为ExecutingGraph。initializeExecution函数会调用ExecutingGraph的initializeExecution函数。initializeExecution函数中首先在DAG图中找到没有direct_edge(出边)的节点作为启动节点并加入栈中，然后从这个节点执行updateNode。UpdateNode函数通过执行当前节点的
prepare方法去拉取数据，然后更新与当前节点相关联的edge(这里有个trick，prepare方法更新的是processor的端口状态，那么如何通过端口来找到与之关联的edge呢，其实端口对象中有一个结构体updateInfo，其中有一个字段id表示了其edge的地址(指针)详细参考[[1]](https://www.cnblogs.com/wyc2021/p/15648668.html))然后通过edge找到下一个node，执行下一个node的prepare方法，从而依次类推，最终将所有状态为Ready的node将入到队列中，加入到队列的node会被线程池中的线程去执行其node中的procesor中的work方法。
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
            ...

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
                    ...
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
              ...
            }
        }
    }

    return true;
}
```

executeSingleThread方法中会调用executeStepImpl函数，(1)处context的类型为ExecutionThreadContext。(2)处的tryGetTask方法，其实就是pipeineExecutor中通过成员变量ExecutorTasks维护了一个vector<vector<Node>>这样一个二维任务队列(前面讲到的updateNode方法中将状态为Ready的Node放到队列中去，最终会将这个队列中的task按照Round Robin方式放到这个二维任务队列中去)。每个线程根据线程id来读取他自己的任务队列。tryGetTask方法就是将node从任务队列取出，然后通过threadContext来将当前正在处理的节点设置为node，那么context.hasTask方法通过当前node是否为空来判断是否有任务，有则执行(3)，context.executeTaskh会调用executeJob。当work方法执行完毕后，如果有下游节点的话，那么通常数据会被推送到输出端口，这个时候通常情况下它的下游node因为输入端口有了数据，执行prepare方法很大可能状态会变为Ready。因此会当前node执行完work后会再执行updateNode方法来尝试执行下游节点。
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

                /// Prepare processor after execution.                 //(4)
                if (!graph->updateNode(context.getProcessorID(), queue, async_queue))
                    finish();

                /// Push other tasks to global queue.
                tasks.pushTasks(queue, async_queue, context);
            }


}
```
executeJob是一个静态方法，会最终执行pipeline中的processor的work方法，完成数据的处理。
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
最后用一张图来总结全文

![](https://lxhblog.oss-cn-beijing.aliyuncs.com/bigdata/pipeline%E6%89%A7%E8%A1%8C.png)

#### 参考文献
[1]https://www.cnblogs.com/wyc2021/p/15648668.html
