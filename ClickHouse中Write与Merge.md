## ClickHouse中write与merge
### write过程
写过程中对应的Processor是MergeTreeSink。
继承关系为
```
MergeTreeSink->SinkToStorage->ExceptionKeepingTransform->IProcessor
```
其中主要的方法实现在MergeTreeSink::consume()方法中。consume方法的逻辑首先是(1)处将Chunk转化成Block，Block可以看做是Chunk的封装，都是column数据的容器。然后(2)处通过
将整个Block的数据按照分区键来分为多个block，每个block中的数据属于同一个partition。
(3)处通过遍历每个block，然后在(4)处将每个block的数据写入临时文件，也可以理解为临时DataPart。(5)处将每个分区的DataPart放入容器partitions。然后我们看(6)处的finishDelayedChunk方法。
```
void MergeTreeSink::consume(Chunk chunk)
{
    auto block = getHeader().cloneWithColumns(chunk.detachColumns()); //(1)
    ...
    auto part_blocks = storage.writer.splitBlockIntoParts(block,  max_parts_per_block, metadata_snapshot, context);//(2)

    ...

    for (auto & current_block : part_blocks)                        //(3)
    {
        ...
        auto temp_part = storage.writer.writeTempPart(current_block, metadata_snapshot, context);//(4)

        ...

        partitions.emplace_back(MergeTreeSink::DelayedChunk::Partition //(5)
        {
            .temp_part = std::move(temp_part),
            .elapsed_ns = elapsed_ns,
            .block_dedup_token = std::move(block_dedup_token)
        });
    }

    finishDelayedChunk();                                           //(6)
    delayed_chunk = std::make_unique<MergeTreeSink::DelayedChunk>();
    delayed_chunk->partitions = std::move(partitions);
}
```
finishDelayedChunk方法主要是将各个临时datapart刷到磁盘，然后使用renameTempPartAndAdd将临时DataPart改为正式的名字，最后触发后台merge操作。
```
void MergeTreeSink::finishDelayedChunk()
{
    if (!delayed_chunk)
        return;

    for (auto & partition : delayed_chunk->partitions)
    {
        partition.temp_part.finalize();          //(1)

        auto & part = partition.temp_part.part;

        /// Part can be deduplicated, so increment counters and add to part log only if it's really added
        //(2)
        if (storage.renameTempPartAndAdd(part, context->getCurrentTransaction().get(), &storage.increment, nullptr, storage.getDeduplicationLog(), partition.block_dedup_token))
        {
            PartLog::addNewPart(storage.getContext(), part, partition.elapsed_ns);

            /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
            storage.background_operations_assignee.trigger(); //(3)
        }
    }

    delayed_chunk.reset();
}
```
### merge过程
在介绍merge过程中，首先介绍两个线程池。
BackgroundSchedulePool和MergeTreeBackgroundExecutor。因为merge操作是异步的，相关的任务会在个线程池中实现。
#### BackgroundSchedulePool
可以看到BackgroundSchedulePoo中的线程为ThreadFromGlobalPool，所以其实任务都是在全局的线程池中执行的。在本系列的第一篇文章中讲过ClickHouse中的全局线程池。
```
class BackgroundSchedulePool
{
public:
    ...
private:
    using Threads = std::vector<ThreadFromGlobalPool>;

    void threadFunction();                                //worker函数
    void delayExecutionThreadFunction();

    Threads threads;                                      //线程队列
    Poco::NotificationQueue queue;                        //任务队列

};

void BackgroundSchedulePool::threadFunction()
{
    ...
    while (!shutdown)
    {
        ...
        if (Poco::AutoPtr<Poco::Notification> notification = queue.waitDequeueNotification(wait_timeout_ms))
        {
            TaskNotification & task_notification = static_cast<TaskNotification &>(*notification);
            task_notification.execute();
        }
    }
}

```
#### MergeTreeBackgroundExecutor
MergeTreeBackgroundExecutor有两个任务队列，pending和active,pending表示待执行的tasks，而active表示正在执行的tasks。MergeTreeBackgroundExecutor被实现为coroutine,原注释为
```
Executor for a background MergeTree related operations such as merges, mutations, fetches an so on.
 *  It can execute only successors of ExecutableTask interface.
 *  Which is a self-written coroutine. It suspends, when returns true from executeStep() method.
```
任务队列的实现为类MergeMutateRuntimeQueue，可以理解为一个优先级队列，因为在执行merge的时候，ClickHouse的策略认为应该先merge小DataPart来提高系统性能。
```
template <class Queue>
class MergeTreeBackgroundExecutor final : public shared_ptr_helper<MergeTreeBackgroundExecutor<Queue>>
{
public:
    ...
    bool trySchedule(ExecutableTaskPtr task);
private:

    void routine(TaskRuntimeDataPtr item);
    void threadFunction();                                //worker函数       

    Queue pending{};                                      //任务队列
    boost::circular_buffer<TaskRuntimeDataPtr> active{0}; //任务队列

    ThreadPool pool;                                      //线程池
};
```
#### 调用关系
上面讲到在方法finishDelayedChunk的最后通过调用storage.background_operations_assignee.trigger()触发merge。trigger方法中通过
BackgroundSchedulePool::TaskHolder(holder是在BackgroundJobsAssignee::start方法中初始化的)来向BackgroundSchedulePool提交任务。任务函数如下，merge的任务类型为DataProcessing。因此最后一定会有某个线程执行了threadFunc函数。
```
void BackgroundJobsAssignee::threadFunc()
try
{
    bool succeed = false;
    switch (type)
    {
        case Type::DataProcessing:
            succeed = data.scheduleDataProcessingJob(*this);
            break;
        case Type::Moving:
            succeed = data.scheduleDataMovingJob(*this);
            break;
    }

    if (!succeed)
        postpone();
}
```
具体来看scheduleDataProcessingJob函数
```
bool StorageMergeTree::scheduleDataProcessingJob(BackgroundJobsAssignee & assignee) //-V657
{
    if (shutdown_called)
        return false;

    ...
    auto metadata_snapshot = getInMemoryMetadataPtr();
    std::shared_ptr<MergeMutateSelectedEntry> merge_entry, mutate_entry;
    bool were_some_mutations_skipped = false;

    auto share_lock = lockForShare(RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);

    MergeTreeTransactionHolder transaction_for_merge;
    MergeTreeTransactionPtr txn;
    if (transactions_enabled.load(std::memory_order_relaxed))
    {
        /// TODO Transactions: avoid beginning transaction if there is nothing to merge.
        txn = TransactionLog::instance().beginTransaction();
        transaction_for_merge = MergeTreeTransactionHolder{txn, /* autocommit = */ true};
    }
    ...
    {
        ...
        merge_entry = selectPartsToMerge(metadata_snapshot, false, {}, false, nullptr, share_lock, lock, txn);
        ...

    }

    ...

    if (merge_entry)
    {
        auto task = std::make_shared<MergePlainMergeTreeTask>(*this, metadata_snapshot, false, Names{}, merge_entry, share_lock, common_assignee_trigger);
        task->setCurrentTransaction(std::move(transaction_for_merge), std::move(txn));
        assignee.scheduleMergeMutateTask(task);
        return true;
    }

    ....
}
```
selectPartsToMerge
```
std::shared_ptr<MergeMutateSelectedEntry> StorageMergeTree::selectPartsToMerge(
    const StorageMetadataPtr & metadata_snapshot,
    bool aggressive,
    const String & partition_id,
    bool final,
    String * out_disable_reason,
    TableLockHolder & /* table_lock_holder */,
    std::unique_lock<std::mutex> & lock,
    const MergeTreeTransactionPtr & txn,
    bool optimize_skip_merged_partitions,
    SelectPartsDecision * select_decision_out)
{
    auto data_settings = getSettings();

    auto future_part = std::make_shared<FutureMergedMutatedPart>();

    if (storage_settings.get()->assign_part_uuids)
        future_part->uuid = UUIDHelpers::generateV4();

    auto can_merge = [this, &lock](const DataPartPtr & left, const DataPartPtr & right, const MergeTreeTransaction * tx, String *) -> bool
    {
        if (tx)
        {
            /// Cannot merge parts if some of them are not visible in current snapshot
            /// TODO Transactions: We can use simplified visibility rules (without CSN lookup) here
            if (left && !left->version.isVisible(tx->getSnapshot(), Tx::EmptyTID))
                return false;
            if (right && !right->version.isVisible(tx->getSnapshot(), Tx::EmptyTID))
                return false;

            /// Do not try to merge parts that are locked for removal (merge will probably fail)
            if (left && left->version.isRemovalTIDLocked())
                return false;
            if (right && right->version.isRemovalTIDLocked())
                return false;
        }

        /// This predicate is checked for the first part of each range.
        /// (left = nullptr, right = "first part of partition")
        if (!left)
            return !currently_merging_mutating_parts.count(right);
        return !currently_merging_mutating_parts.count(left) && !currently_merging_mutating_parts.count(right)
            && getCurrentMutationVersion(left, lock) == getCurrentMutationVersion(right, lock) && partsContainSameProjections(left, right);
    };

    SelectPartsDecision select_decision = SelectPartsDecision::CANNOT_SELECT;

    if (partition_id.empty())
    {
        UInt64 max_source_parts_size = merger_mutator.getMaxSourcePartsSizeForMerge();
        bool merge_with_ttl_allowed = getTotalMergesWithTTLInMergeList() < data_settings->max_number_of_merges_with_ttl_in_pool;

        /// TTL requirements is much more strict than for regular merge, so
        /// if regular not possible, than merge with ttl is not also not
        /// possible.
        if (max_source_parts_size > 0)
        {
            select_decision = merger_mutator.selectPartsToMerge(
                future_part,
                aggressive,
                max_source_parts_size,
                can_merge,
                merge_with_ttl_allowed,
                txn,
                out_disable_reason);
        }
        else if (out_disable_reason)
            *out_disable_reason = "Current value of max_source_parts_size is zero";
    }
    else
    {
        ...
    }
    ...
    merging_tagger = std::make_unique<CurrentlyMergingPartsTagger>(future_part, MergeTreeDataMergerMutator::estimateNeededDiskSpace(future_part->parts), *this, metadata_snapshot, false);
    return std::make_shared<MergeMutateSelectedEntry>(future_part, std::move(merging_tagger), MutationCommands::create());
}
```
merger_mutator.selectPartsToMerge为方法，逻辑主要为遍历目前的所以可见的DataPart(事务)，这里需要注意的是，ClickHouse在内存中以索引的形式维护这些DataPart信息，因此这些读出来的DataPart是有序的，排序根据(partition_id, min_block, max_block, level, mutation)。

结合merger_mutator.selectPartsToMerge方法和 can_merge方法总结
能够Merge的DataPart需要满足如下条件：
1. 首先能够merge的DataPart必须是同一个分区，且是连续的。
2. 使用事务时候，DataPart是同时可见的
3. 待更正的mutation版本是一致的。

因为每次可以Merge的DataPart数量是有限制的，因此还需要在所有可以合并的DataPart中选择最合适的Range来合并。实现在如下方法中，是一种启发时算法，有兴趣的同学可以研究一下。
```
PartsRange select(
        const PartsRanges & parts_ranges,
        size_t max_total_size_to_merge)
```



```
MergeTreeDataMergerMutator::selectPartsToMerge
```
这里大约概括下选择parts的策略，里面还有很多细节。感兴趣的同学可以去研读代码

#### merge的执行
