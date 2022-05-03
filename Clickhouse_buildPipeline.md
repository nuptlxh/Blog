---
title: ClickHouse中QueryPlan到PIPELINE的转换
date: 2022-05-03 11:54:23
tags: [ClickHouse,OLAP]
categories:
     - OLAP
comments: true
---
#### QueryPlane->PIPELINE
使用[官方教程](https://clickhouse.com/docs/zh/getting-started/tutorial)的例子, 构建相应的表并导入数据集tutorial.hits_v1和tutorial.visits_v1。执行查询示例。
```
SELECT
    StartURL AS URL,
    AVG(Duration) AS AvgDuration
FROM tutorial.visits_v1
WHERE StartDate BETWEEN '2014-03-23' AND '2014-03-30'
GROUP BY URL
ORDER BY AvgDuration DESC
LIMIT 10
```
使用explain查询sql的queryPlane以及使用EXPLAIN PIPELINE查询pipeline情况。

QueryPlan结果如下
```
┌─explain─────────────────────────────────────────────────────────────────────────────┐
│ Expression (Projection)                                                             │
│   Limit (preliminary LIMIT (without OFFSET))                                        │
│     Sorting (Sorting for ORDER BY)                                                  │
│       Expression (Before ORDER BY)                                                  │
│         Aggregating                                                                 │
│           Expression (Before GROUP BY)                                              │
│             SettingQuotaAndLimits (Set limits and quota after reading from storage) │
│               ReadFromMergeTree (tutorial.visits_v1)                                │
└─────────────────────────────────────────────────────────────────────────────────────┘
```
QueryPlan是一个树形结构，对应到代码中表示为如下，箭头表示A->B表示A是B的父节点。xxxStep表示的是类名字
```
ExpressionStep->LimitStep->SortingSetp->ExpressionStep->AggregatingStep->ExpressionStep->SettingQuotaAndLimitsStep->ReadFromMergeTreeStep
```

PIPELINE结果如下
```
┌─explain─────────────────────────────────────────┐
│ (Expression)                                    │
│ ExpressionTransform                             │
│   (Limit)                                       │
│   Limit                                         │
│     (Sorting)                                   │
│     MergeSortingTransform                       │
│       LimitsCheckingTransform                   │
│         PartialSortingTransform                 │
│           (Expression)                          │
│           ExpressionTransform                   │
│             (Aggregating)                       │
│             Resize 6 → 1                        │
│               AggregatingTransform × 6          │
│                 StrictResize 6 → 6              │
│                   (Expression)                  │
│                   ExpressionTransform × 6       │
│                     (SettingQuotaAndLimits)     │
│                       (ReadFromMergeTree)       │
│                       MergeTreeThread × 6 0 → 1 │
└─────────────────────────────────────────────────┘

```
上述结果的 x 表示有几个这个类型的processor，比如有6个MergeTreeThread来拉取数据，而
x->y中，x表示有几个inPort，y表示有几个outPort。其中括号内的内容表示对应的queryPlanStep。笔者对ClickHouse中这一查询进行debug，其中具体pipeline表达如下图。相同颜色的表示同一类型的processor。
![](https://lxhblog.oss-cn-beijing.aliyuncs.com/bigdata/pipeline.png)

构建pipeline的代码调用栈如下图。

![](https://lxhblog.oss-cn-beijing.aliyuncs.com/bigdata/buildPipeline.png)

buildQueryPipeline方法本质上对queryPlan树形数据结构进行后序遍历，不过这里没有使用递归实现，而是使用栈。考虑原因一个是如果流水线太深，则会有太多的调用栈，可能会出现栈溢出问题。其次，栈实现效率上面本身也因为少了函数调用的开销而更有效率。主要过程为首先将queryplan的root节点入栈(1)处。其中比较巧妙的地方在于(3)处，通过判断当前栈顶元素pipelines中有几个已经构建好的pipeline，如果当前节点的每个孩子节点都已经处理完毕了，也就是说每个孩子节点对应的子树都已经转换为了一个pipeline，那么通过当前节点的updatePipeline方法来将当前节点加入并将孩子节点的pipleline连接起来(4)处。举一个简单的例子,如下图所示。这个queryplan有四个节点，首先后续遍历会处理node2节点，进入(3)代码处，因为他的孩子节点数目为零等于pipelines的元素个数也为零。因此，(3)中会将node2节点中生成的pipeline赋值给lastpipeline，然后将node2从栈中弹出。那么有后续遍历的规则，退出当前栈顶节点为node1,(2)处lastpipeline不为空，将lastpipeline放入之node1的pipelines中。当代码运行到(3)时，nextchild为1，而孩子个数为2，因此将node3入栈，当下一次循环时候，node3因为没有孩子节点会跟node2节点一样进入(3),从而生成pipeline并赋值给lastpipeline并弹出node3.因此node1再次成为栈顶节点，且这次piplelines中有了2个流水线，分别为node2节点和node3节点生成的，成功进入到(3),得以执行updatePipeline方法将本节点加入到pipleline，并将生成的新pipeline传给root节点，依次类推，root节点最终返回生成的最后的lastpipeline。

![](https://lxhblog.oss-cn-beijing.aliyuncs.com/bigdata/tree.png)

```
QueryPipelineBuilderPtr QueryPlan::buildQueryPipeline(
    const QueryPlanOptimizationSettings & optimization_settings,
    const BuildQueryPipelineSettings & build_pipeline_settings)
{
    ...
    struct Frame
    {
        Node * node = {};
        QueryPipelineBuilders pipelines = {};
    };

    QueryPipelineBuilderPtr last_pipeline;

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});                                   //(1)

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (last_pipeline)                                             //(2)
        {
            frame.pipelines.emplace_back(std::move(last_pipeline));
            last_pipeline = nullptr; //-V1048
        }

        size_t next_child = frame.pipelines.size();                  
        if (next_child == frame.node->children.size())                 //(3)
        {
            bool limit_max_threads = frame.pipelines.empty();
            last_pipeline = frame.node->step->updatePipeline(std::move(frame.pipelines),                //(4) build_pipeline_settings);

            if (limit_max_threads && max_threads)
                last_pipeline->limitMaxThreads(max_threads);

            stack.pop();
        }
        else
            stack.push(Frame{.node = frame.node->children[next_child]});
    }

    ...
    return last_pipeline;
}
```
