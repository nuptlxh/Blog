### ClickHouse查询
### MergeTree存储引擎
#### 数据存储
以表hits_v1 为例，它的数据目录如下所示。
```
.
├── 197506_32_85_11
│   ├── checksums.txt
│   ├── columns.txt
│   ├── count.txt
│   ├── data.bin
│   ├── data.mrk3
│   ├── default_compression_codec.txt
│   ├── minmax_EventDate.idx
│   ├── partition.dat
│   └── primary.idx
├── 201403_1_31_2
│   ├── AdvEngineID.bin
│   ├── AdvEngineID.mrk2
│   ├── Age.bin
│   ├── Age.mrk2
│   ├── BrowserCountry.bin
│   ├── BrowserCountry.mrk2
│   ├── BrowserLanguage.bin
│   ├── BrowserLanguage.mrk2
... ...
│   ├── minmax_EventDate.idx
... ...
├── 202204_109_109_0
│   ├── checksums.txt
│   ├── columns.txt
│   ├── count.txt
│   ├── data.bin
│   ├── data.mrk3
│   ├── default_compression_codec.txt
│   ├── minmax_EventDate.idx
│   ├── partition.dat
│   └── primary.idx
├── detached
├── format_version.txt
└── temp.text
```
可以看到，上面显示了3个Datapart,DataPart197506_32_85_11和202204_109_109_0的数据组织方式是Compact,而201403_1_31_2的数拒组织方式是Wide。两种方式主要区别在于Compact方式的所有列数据存放在一个Data.bin文件中,而Wide方式中则是每一个列有一个columnsName.bin文件对应。
visits_v1: ClickHouse的每个表都会在其设置的数据目录下有个目录文件对应。

197506_32_85_11,201403_1_6_1,202204_109_109_0：分区目录，hits_v1的分区键为StartDate字段的年月(PARTITION BY toYYYYMM(StartDate))

分区目录的格式为partionKey_minBlock_maxBlock_level。level表示的是合并的次数。每个形如partionKey_minBlock_maxBlock_level的目录下的所有文件构成一个DataPart。每个datapart大小有个上限，并不能一直合并。

primary.idx：主键索引文件，用于存放稀疏索引的数据。通过查询条件与稀疏索引能够快速的过滤无用的数据，减少需要加载的数据量。

{column}.bin：列数据的存储文件，以列名+bin为文件名，默认设置采用 lz4 压缩格式。Wide模式下每一列都会有单独的文件。(还有compact模式，所有的列数据文件合并成一个data.bin)

{column}.mrk2：列数据的标记信息，记录了数据块在 bin 文件中的偏移量。标记文件首先与列数据的存储文件对齐，记录了某个压缩块在 bin 文件中的相对位置；其次与索引文件对齐，记录了稀疏索引对应数据在列存储文件中的位置.(compact模式下只有一个data.mrk3文件)

minmax_EventDate.idx: 分区键的minmax索引文件。
columns.txt：列名以及数据类型

count.txt：记录数据的总行数。
**注意**:可能会有读者有疑惑，mark存在的意义在哪，为什么不可以直接通过primary.idx直接索引到.bin数据文件。笔者认为，为了加快数据的查询效率，ClickHouse中的primary索引是常驻内存的，因此需要尽量较少主键索引的大小，而如果没有mark文件，那么势必主键索引中需要记录目前mark文件中有关.bin文件的偏移信息，会造成内存压力。
#### 主键索引
具体的以官方文档为例。
```
全部数据  :     [-------------------------------------------------------------------------]
CounterID:      [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
Date:           [1111111222222233331233211111222222333211111112122222223111112223311122333]
标记:            |      |      |      |      |      |      |      |      |      |      |
                a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
标记号:          0      1      2      3      4      5      6      7      8      9      10
```
如果指定查询如下：

1. CounterID in ('a', 'h')，服务器会读取标记号在 [0, 3) 和 [6, 8) 区间中的数据。
2. CounterID IN ('a', 'h') AND Date = 3，服务器会读取标记号在 [1, 3) 和 [7, 8) 区间中的数据。
3. Date = 3，服务器会读取标记号在 [1, 10] 区间中的数据。

主键索引与mark文件的生成
简单的解释就是：ClickHouse 会根据 index_granularity 的设置将数据分成多个 granule，每个 granule 中索引列的第一个记录将作为索引写入到 primary.idx；其他非索引列也会用相同的策略生成一条 mark 数据写入相应的*.mrk2 文件中，并与主键索引一一对应，并记录该条索引对应的记录列在column中的偏移(偏移是个抽象的概念，具体的.bin数据文件需要压缩存放，而压缩存放有具体为的一系列的数据块，可以理解为(块号:块内偏移)这个放到后序ClickHouse插入数据的文章中详细讲解)


#### 跳数索引
```
INDEX index_name expr TYPE type(...) GRANULARITY granularity_value
```
跳数索引可以理解为索引的索引。将mark文件中每隔granularity_value个值，进行索引。
可用的索引类型
1. 有minmax 存储指定表达式的极值
2. set(max_rows) 存储指定表达式的不重复值
3. ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)存储一个包含数据块中所有 n元短语（ngram） 的 布隆过滤器
4. tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)跟 ngrambf_v1 类似，但是存储的是token而不是ngrams
5. bloom_filter(bloom_filter([false_positive]) – 为指定的列存储布隆过滤器

### SELECT读数据
SELECT读数据主要分为两三部分，如下图所示。
1. 首先通过分区和一系列索引来排除不需要扫描的datapart和Mark(getAnalysisResult)
2. 将待扫描的DataPart划分为更细粒度的ThreadTask，并尽量将不同的磁盘负载分配到不同的线程中去以达到磁盘最大的并行化。(spreadMarkRangesAmongStreams)。
3. PIPELINE执行时，真正的调度线程拉取markRage(getTask)并从文件中读取数据(readRows)

![](https://lxhblog.oss-cn-beijing.aliyuncs.com/bigdata/select.png)


```
void ReadFromMergeTree::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
  auto result = getAnalysisResult();
  if (select.final())
    {
      ...
    }
    else if ((settings.optimize_read_in_order || settings.optimize_aggregation_in_order) && input_order_info)
    {
      ...
    }
    else  
    {
        pipe = spreadMarkRangesAmongStreams(
            std::move(result.parts_with_ranges),
            column_names_to_read);
    }
}
```
#### 分析需要扫描的mark
上一篇文章《ClickHouse的QueryPlan到Pipeline的翻译》中讲述了ClickHouse是如何从queryPlan转化为pipeline，而读数据的第一部分就是在构建pipeline时候做的(IsourceStep.updatePipeline)，调用栈如下。

![](https://lxhblog.oss-cn-beijing.aliyuncs.com/bigdata/buildquery.png)
下面主要详细讲解几个核心函数

getAnalysisResult->selectRangestoRead()。selectRangestoRead中会分别调用
MergeTreeDataSelectExecutor::filterPartsByVirtualColumns()；
virtualColumn的官网定义如下，一般情况下我们不会使用到它。
```
Virtual column is an integral table engine attribute that is defined in the engine source code.

You shouldn’t specify virtual columns in the CREATE TABLE query and you can’t see them in SHOW CREATE TABLE and DESCRIBE TABLE query results. Virtual columns are also read-only, so you can’t insert data into virtual columns.

To select data from a virtual column, you must specify its name in the SELECT query. SELECT * does not return values from virtual columns.

If you create a table with a column that has the same name as one of the table virtual columns, the virtual column becomes inaccessible. We do not recommend doing this. To help avoid conflicts, virtual column names are usually prefixed with an underscore.
```
virtual columns的常用值如下
```
_part  -- name of a part
_part_index -- sequential index of the part in the query result
_partition_id -- name of a partition
_part_uuid -- unique part identifier, if enabled `MergeTree` setting `assign_part_uuids` (Part movement between shards)
_partition_value -- values (tuple) of a `partition by` expression
_sample_factor -- sample_factor from the query
```

MergeTreeDataSelectExecutor::filterPartsByPartition()，会调用selectPartstoRead.
其中(1)处的part_values是filterPartsByVirtualColumns方法返回的结果，因此在遍历每一个part判断他的partition key是否满足要求之前，可以通过其名字是否在part_values中来筛选一下。代码(2)处是真正来判断该datapart的partion key是否满足要求。
```
void MergeTreeDataSelectExecutor::selectPartsToRead(
    MergeTreeData::DataPartsVector & parts,
    const std::optional<std::unordered_set<String>> & part_values,
    const std::optional<KeyCondition> & minmax_idx_condition,
    const DataTypes & minmax_columns_types,
    std::optional<PartitionPruner> & partition_pruner,
    const PartitionIdToMaxBlock * max_block_numbers_to_read,
    PartFilterCounters & counters)
{
    MergeTreeData::DataPartsVector prev_parts;
    std::swap(prev_parts, parts);
    for (const auto & part_or_projection : prev_parts)
    {
        const auto * part = part_or_projection->isProjectionPart() ? part_or_projection->getParentPart() : part_or_projection.get();
        if (part_values && part_values->find(part->name) == part_values->end())     //(1)
            continue;

        ...
        if (partition_pruner)
        {
            if (partition_pruner->canBePruned(*part)) //(2)
                continue;
        }


        parts.push_back(part_or_projection);
    }
}
```

MergeTreeDataSelectExecutor::filterPartsByPrimaryKeyAndSkipIndexes()

filterPartsByPrimaryKeyAndSkipIndexes方法整体上并不复杂，大部门代码是方法process_part的函数体(1)处。然后在(4)处创新一个新的线程池，并在(5)处向线程池中提交任务，也就是说会有num_threads个线程会执行process_part方法。

具体来看process_part方法。(2)处markRangesFromPKRange方法是通过主键筛选，(3)处的
useful_indices是跳数索引。
```
RangesInDataParts MergeTreeDataSelectExecutor::filterPartsByPrimaryKeyAndSkipIndexes(
    MergeTreeData::DataPartsVector && parts,
    StorageMetadataPtr metadata_snapshot,
    const SelectQueryInfo & query_info,
    const ContextPtr & context,
    const KeyCondition & key_condition,
    const MergeTreeReaderSettings & reader_settings,
    Poco::Logger * log,
    size_t num_streams,
    ReadFromMergeTree::IndexStats & index_stats,
    bool use_skip_indexes)
{
    ...
    /// Let's find what range to read from each part.
    {

        auto process_part = [&](size_t part_index)                    //(1)函数定义 process_part
        {
            auto & part = parts[part_index];

            RangesInDataPart ranges(part, part_index);

            size_t total_marks_count = part->index_granularity.getMarksCountWithoutFinal();

            if (metadata_snapshot->hasPrimaryKey())
                ranges.ranges = markRangesFromPKRange(part, metadata_snapshot, key_condition, settings, log); //(2)
            else if (total_marks_count)
                ranges.ranges = MarkRanges{MarkRange{0, total_marks_count}};

            sum_marks_pk.fetch_add(ranges.getMarksCount(), std::memory_order_relaxed);


            for (auto & index_and_condition : useful_indices)    //(3)
            {
                ...
            }

            ...
            parts_with_ranges[part_index] = std::move(ranges);

            }
        }; //函数结束

        size_t num_threads = std::min(size_t(num_streams), parts.size());

        if (num_threads <= 1)
        {
            ...
        }
        else
        {
            /// Parallel loading of data parts.
            ThreadPool pool(num_threads);                           //(4)

            for (size_t part_index = 0; part_index < parts.size(); ++part_index)
                pool.scheduleOrThrowOnError([&, part_index, thread_group = CurrentThread::getGroup()] //(5)
                {
                    if (thread_group)
                        CurrentThread::attachToIfDetached(thread_group);

                    process_part(part_index);                       //(6)
                });

            pool.wait();
        }

    }

    return parts_with_ranges;
}
```
具体来看markRangesFromPKRange方法,(1)处方法定义了判断一个MarkRange里是否可能含有满足条件的数据，可能则返回真，否则返回false。(2)处代码，首先将整个datapart的mark放入栈中，然后来判断全部的markRange有没有可能含有目标列。如果没有则直接排除掉(3)。如果可能含有目标列，那么继续将markRange划分，range范围包括step个mark，并将这些新range放入栈中。依次类推。示意图如下

![](https://lxhblog.oss-cn-beijing.aliyuncs.com/bigdata/step.png)

(4)处代码表示，最后筛选后的结果range都是一个mark，这个时候要判断，该目标mark与上一个符合要求的range之间的gap，如果gap小于参数min_marks_for_seek则，则将这个mark与上一个range合成一个range。示意图如下。

![](https://lxhblog.oss-cn-beijing.aliyuncs.com/bigdata/gap.png)
```
MarkRanges MergeTreeDataSelectExecutor::markRangesFromPKRange(
    const MergeTreeData::DataPartPtr & part,
    const StorageMetadataPtr & metadata_snapshot,
    const KeyCondition & key_condition,
    const Settings & settings,
    Poco::Logger * log)
{
    MarkRanges res;
    ....
    auto may_be_true_in_range = [&](MarkRange & range)                  //(1)
    {
        if (range.end == marks_count && !has_final_mark)
        {
            for (size_t i = 0; i < used_key_size; ++i)
            {
                create_field_ref(range.begin, i, index_left[i]);
                index_right[i] = POSITIVE_INFINITY;
            }
        }
        else
        {
            if (has_final_mark && range.end == marks_count)
                range.end -= 1; /// Remove final empty mark. It's useful only for primary key condition.

            for (size_t i = 0; i < used_key_size; ++i)
            {
                create_field_ref(range.begin, i, index_left[i]);
                create_field_ref(range.end, i, index_right[i]);
            }
        }
        return key_condition.mayBeTrueInRange(
            used_key_size, index_left.data(), index_right.data(), primary_key.data_types);
    };

    if (!key_condition.matchesExactContinuousRange())
    {

        std::vector<MarkRange> ranges_stack = { {0, marks_count} };
        size_t steps = 0;

        while (!ranges_stack.empty())                                  //(2)
        {
            MarkRange range = ranges_stack.back();
            ranges_stack.pop_back();

            if (!may_be_true_in_range(range))                          //(3)
                continue;

            if (range.end == range.begin + 1)
            {
                /// We saw a useful gap between neighboring marks. Either add it to the last range, or start a new range.
                if (res.empty() || range.begin - res.back().end > min_marks_for_seek)                                   //(4)
                    res.push_back(range);
                else
                    res.back().end = range.end;
            }
            else
            {
                /// Break the segment and put the result on the stack from right to left.
                size_t step = (range.end - range.begin - 1) / settings.merge_tree_coarse_index_granularity + 1;
                size_t end;

                for (end = range.end; end > range.begin + step; end -= step)
                    ranges_stack.emplace_back(end - step, end);

                ranges_stack.emplace_back(range.begin, end);
            }
        }

    }
    else
    {
        ...
    }

    return res;
}
```
#### 划分ThreadTask
spreadMarkRangesAmongStreams函数中主要通过构建多个MergeTreeThreadSelectProcessor并与同一个MergeTreeReadPool相关联。而MergeTreeReadPool的构造函数中会调用fillPerPartInfo和fillPerThreadInfo方法。fillPerPartInfo方法主要是统计了每个待读取的DataPart的相关信息，比如每个DataPart含有的总mark数。而fillPerThreadInfo方法中则是首先将所有的DataPart按照所在的disk名字进行排序，然后将这些的DataPart，进一步分成小的markranges。Mark作为ClickHouse中读取数据的最小单位，markrange记录了Datapart
中mark的范围[begin,end).
```
void MergeTreeReadPool::fillPerThreadInfo(
    size_t threads, size_t sum_marks, std::vector<size_t> per_part_sum_marks,
    const RangesInDataParts & parts, size_t min_marks_for_concurrent_read)
{
    threads_tasks.resize(threads);      //thread_taks类似于一个二维数组，存放每个线程tasks
    ...

    using PartsInfo = std::vector<PartInfo>;
    std::queue<PartsInfo> parts_queue;

    {
        // 根据DataPart所在Disk的名字排序
        std::map<String, std::vector<PartInfo>> parts_per_disk;

        for (size_t i = 0; i < parts.size(); ++i)
        {
            PartInfo part_info{parts[i], per_part_sum_marks[i], i};
            if (parts[i].data_part->isStoredOnDisk())
                parts_per_disk[parts[i].data_part->volume->getDisk()->getName()].push_back(std::move(part_info));
            else
                parts_per_disk[""].push_back(std::move(part_info));
        }

        for (auto & info : parts_per_disk)
            parts_queue.push(std::move(info.second));
    }

    const size_t min_marks_per_thread = (sum_marks - 1) / threads + 1;

    // 遍历每一个线程，为每一个线程分配任务
    for (size_t i = 0; i < threads && !parts_queue.empty(); ++i)
    {
        auto need_marks = min_marks_per_thread;

        while (need_marks > 0 && !parts_queue.empty())
        {
            auto & current_parts = parts_queue.front();
            RangesInDataPart & part = current_parts.back().part;
            size_t & marks_in_part = current_parts.back().sum_marks;
            const auto part_idx = current_parts.back().part_idx;

            /// Do not get too few rows from part.
            if (marks_in_part >= min_marks_for_concurrent_read &&
                need_marks < min_marks_for_concurrent_read)
                need_marks = min_marks_for_concurrent_read;

            /// Do not leave too few rows in part for next time.
            if (marks_in_part > need_marks &&
                marks_in_part - need_marks < min_marks_for_concurrent_read)
                need_marks = marks_in_part;

            MarkRanges ranges_to_get_from_part;
            size_t marks_in_ranges = need_marks;gett

            /// Get whole part to read if it is small enough.
            if (marks_in_part <= need_marks)
            {
                ranges_to_get_from_part = part.ranges;
                marks_in_ranges = marks_in_part;

                need_marks -= marks_in_part;
                current_parts.pop_back();
                if (current_parts.empty())
                    parts_queue.pop();
            }
            else
            {
                /// Loop through part ranges.
                while (need_marks > 0)
                {
                    if (part.ranges.empty())
                        throw Exception("Unexpected end of ranges while spreading marks among threads", ErrorCodes::LOGICAL_ERROR);

                    MarkRange & range = part.ranges.front();

                    const size_t marks_in_range = range.end - range.begin;
                    const size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);

                    ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
                    range.begin += marks_to_get_from_range;
                    marks_in_part -= marks_to_get_from_range;
                    need_marks -= marks_to_get_from_range;
                    if (range.begin == range.end)
                        part.ranges.pop_front();
                }
            }
            //
            threads_tasks[i].parts_and_ranges.push_back({ part_idx, ranges_to_get_from_part });
            threads_tasks[i].sum_marks_in_parts.push_back(marks_in_ranges);
            if (marks_in_ranges != 0)
                remaining_thread_tasks.insert(i);
        }

        //切换到分配下一个线程任务之前，切换disk。这样尽可能的是不同的磁盘负载到不同的线程中去，依次来最大化磁盘并行度。
        if (parts_queue.size() > 1)
        {
            parts_queue.push(std::move(parts_queue.front()));
            parts_queue.pop();
        }
    }
}
```
#### PIPELINE执行
在pipeline执行的时候，MergeTreeThreadSelectProcessor的work方法会调用到getTask方法向MergeTreeReadPool中请求Task
```
MergeTreeReadTaskPtr MergeTreeReadPool::getTask(size_t min_marks_to_read, size_t thread, const Names & ordered_names)
{
    ...
    auto thread_idx = thread;
    if (!tasks_remaining_for_this_thread)
    {
      ... //如果本线程的task做完，则尝试窃取其他线程的任务                                                  
    }

    ...
    /// Do not leave too little rows in part for next time.
    // 如果此次获取到的range后，剩下的mark比较少，那么就一次行读整个DataPart，提高效率。
    if (marks_in_part > need_marks &&
        marks_in_part - need_marks < min_marks_to_read)
        need_marks = marks_in_part;

    MarkRanges ranges_to_get_from_part;

    /// Get whole part to read if it is small enough.
    //DataPart本身含有的mark总数就比较少，也一次性的读取整个DataPart
    if (marks_in_part <= need_marks)
    {
        const auto marks_to_get_from_range = marks_in_part;
        ranges_to_get_from_part = thread_task.ranges;

        marks_in_part -= marks_to_get_from_range;

        thread_tasks.parts_and_ranges.pop_back();
        thread_tasks.sum_marks_in_parts.pop_back();

        if (thread_tasks.sum_marks_in_parts.empty())
            remaining_thread_tasks.erase(thread_idx);
    }
    else
    {   

        /// Loop through part ranges.
        // 遍历这个DataPart的range，找到足够数量的mark然后返回。
        while (need_marks > 0 && !thread_task.ranges.empty())
        {
            auto & range = thread_task.ranges.front();

            const size_t marks_in_range = range.end - range.begin;
            const size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);

            ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
            range.begin += marks_to_get_from_range;
            if (range.begin == range.end)
                thread_task.ranges.pop_front();

            marks_in_part -= marks_to_get_from_range;
            need_marks -= marks_to_get_from_range;
        }
    }

    return std::make_unique<MergeTreeReadTask>(
        part.data_part, ranges_to_get_from_part, part.part_index_in_query, ordered_names,
        per_part_column_name_set[part_idx], per_part_columns[part_idx], per_part_pre_columns[part_idx],
        prewhere_info && prewhere_info->remove_prewhere_column, per_part_should_reorder[part_idx], std::move(curr_task_size_predictor));
}
```
MergeTreeThreadSelectProcessor的work在执行完getTask方法后，会根据返回的结果去读取数据。
代码调用如下。

![](https://lxhblog.oss-cn-beijing.aliyuncs.com/bigdata/readRow.png)

因为clickHouse有谓词下推的优化，MergeTreeRangeReader::read读取的逻辑上是首先根据PreWhere信息(如果有的话)去读取prewhere列(1)处，然后读取其他需要的列(2)处，最后根据preWhere信息去除掉不满足要求的列(5)处。而其中真正读取数据时(1,2,4)处，会根据DataPart类型对应于MergeTreeReaderCompact、MergeTreeReaderWide以及mergeTreeReaderInMemory三种Reader来最终将数据读到内存。除了mergeTreeReaderInMemory，其他两个读取数据主要涉及编解码，比较底层，有兴趣的同学可以阅读源码MergeTreeReaderWide/Compact/InMemroy::readRows。

```
MergeTreeRangeReader::ReadResult MergeTreeRangeReader::read(size_t max_rows, MarkRanges & ranges)
{
    ...
    if (prev_reader)
    {
        read_result = prev_reader->read(max_rows, ranges);                  //(1)
        ...
        Columns columns = continueReadingChain(read_result, num_read_rows); //(2)
        ...

        for (auto & column : columns)                                       //(3)
            read_result.columns.emplace_back(std::move(column));
    }
    else
    {
        ...
        read_result = startReadingChain(max_rows, ranges);                  //(4)
        ...
    }

    ...
    executePrewhereActionsAndFilterColumns(read_result);                    //(5)
    return read_result;
}
```
