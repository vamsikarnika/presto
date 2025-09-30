/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.hudi.split;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.util.AsyncQueue;
import com.facebook.presto.hudi.HudiColumnHandle;
import com.facebook.presto.hudi.HudiPartition;
import com.facebook.presto.hudi.HudiTableLayoutHandle;
import com.facebook.presto.hudi.query.HudiDirectoryLister;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.Lazy;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.facebook.airlift.concurrent.MoreFutures.addExceptionCallback;
import static com.facebook.presto.hive.metastore.MetastoreUtil.extractPartitionValues;
import static com.facebook.presto.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hudi.HudiErrorCode.HUDI_INVALID_METADATA;
import static com.facebook.presto.hudi.HudiMetadata.fromDataColumns;
import static com.facebook.presto.hudi.HudiSessionProperties.getSplitGeneratorParallelism;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * A runnable to load Hudi splits asynchronously in the background
 */
public class HudiBackgroundSplitLoader
        implements Runnable
{
    private static final Logger log = Logger.get(HudiBackgroundSplitLoader.class);

    private final ConnectorSession session;
    private final HudiTableLayoutHandle layout;
    private final HudiDirectoryLister hudiDirectoryLister;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final Lazy<Map<String, Partition>> lazyPartitions;
    private final int splitGeneratorNumThreads;
    private final ExecutorService splitGeneratorExecutorService;
    private final boolean enableMetadataTable;
    private final Consumer<Throwable> errorListener;

    public HudiBackgroundSplitLoader(
            ConnectorSession session,
            ExecutorService splitGeneratorExecutorService,
            HudiTableLayoutHandle layout,
            HudiDirectoryLister hudiDirectoryLister,
            AsyncQueue<ConnectorSplit> asyncQueue,
            Lazy<Map<String, Partition>> lazyPartitions,
            boolean enableMetadataTable,
            Consumer<Throwable> errorListener)
    {
        this.session = requireNonNull(session, "session is null");
        this.layout = requireNonNull(layout, "layout is null");
        this.hudiDirectoryLister = requireNonNull(hudiDirectoryLister, "hudiDirectoryLister is null");
        this.asyncQueue = requireNonNull(asyncQueue, "asyncQueue is null");
        this.lazyPartitions = requireNonNull(lazyPartitions, "partitions is null");

        this.splitGeneratorNumThreads = getSplitGeneratorParallelism(session);
        this.splitGeneratorExecutorService = requireNonNull(splitGeneratorExecutorService, "splitGeneratorExecutorService is null");
        this.enableMetadataTable = enableMetadataTable;
        this.errorListener = errorListener;
    }

    @Override
    public void run()
    {
        try {
            if (enableMetadataTable) {
                generateSplits(true);
                return;
            }

            // Fallback to partition pruning generator
            generateSplits(false);
        }
        catch (Exception e) {
            errorListener.accept(e);
        }
    }

    private void generateSplits(boolean useIndex)
    {
        // Attempt to apply partition pruning using partition stats index
        Deque<HudiPartition> partitionQueue = getPartitions();
        if (partitionQueue.isEmpty()) {
            asyncQueue.finish();
            return;
        }

        List<HudiPartitionSplitGenerator> splitGenerators = new ArrayList<>();
        List<ListenableFuture<Void>> futures = new ArrayList<>();

        int splitGeneratorParallelism = Math.max(1, Math.min(splitGeneratorNumThreads, partitionQueue.size()));
        Executor splitGeneratorExecutor = new BoundedExecutor(splitGeneratorExecutorService, splitGeneratorParallelism);

        for (int i = 0; i < splitGeneratorParallelism; i++) {
            HudiPartitionSplitGenerator generator = new HudiPartitionSplitGenerator(
                    session, layout, hudiDirectoryLister, asyncQueue, partitionQueue, useIndex);
            splitGenerators.add(generator);
            ListenableFuture<Void> future = Futures.submit(generator, splitGeneratorExecutor);
            addExceptionCallback(future, errorListener);
            futures.add(future);
        }

        // Signal all generators to stop once partition queue is drained
        splitGenerators.forEach(HudiPartitionSplitGenerator::stopRunning);

        log.info("Wait for partition pruning split generation to finish on table %s.%s", layout.getTable().getSchemaName(), layout.getTable().getTableName());
        try {
            Futures.whenAllComplete(futures)
                    .run(asyncQueue::finish, directExecutor())
                    .get();
            log.info("Partition pruning split generation finished on table %s.%s", layout.getTable().getSchemaName(), layout.getTable().getTableName());
        }
        catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new PrestoException(HUDI_CANNOT_OPEN_SPLIT, "Error generating Hudi split", e);
        }
    }

    private Deque<HudiPartition> getPartitions()
    {
        return lazyPartitions.get().keySet().stream()
                .map(partitionName -> getHudiPartition(layout, partitionName))
                .collect(Collectors.toCollection(ConcurrentLinkedDeque::new));
    }

    private HudiPartition getHudiPartition(HudiTableLayoutHandle tableLayout, String partitionName)
    {
        String databaseName = tableLayout.getTable().getSchemaName();
        String tableName = tableLayout.getTable().getTableName();
        List<HudiColumnHandle> partitionColumns = tableLayout.getPartitionColumns();
        Path tablePath = new Path(layout.getTable().getPath());
        if (partitionColumns.isEmpty()) {
            // non-partitioned tableLayout
            Table metastoreTable = Optional.ofNullable(layout.getTable().getTable())
                    .orElseThrow(() -> new PrestoException(HUDI_INVALID_METADATA, format("Table %s.%s expected but not found", databaseName, tableName)));
            Path partitionPath = new Path(metastoreTable.getStorage().getLocation());
            String relativePartitionPath = FSUtils.getRelativePartitionPath(new StoragePath(tablePath.toUri()), new StoragePath(partitionPath.toUri()));
            return new HudiPartition(partitionName, ImmutableList.of(), ImmutableMap.of(), metastoreTable.getStorage(), tableLayout.getDataColumns(), relativePartitionPath);
        }
        else {
            // partitioned tableLayout
            List<String> partitionValues = extractPartitionValues(partitionName);
            checkArgument(partitionColumns.size() == partitionValues.size(),
                    format("Invalid partition name %s for partition columns %s", partitionName, partitionColumns));
            Partition partition = Optional.ofNullable(lazyPartitions.get().get(partitionName))
                    .orElseThrow(() -> new PrestoException(HUDI_INVALID_METADATA, format("Partition %s expected but not found", partitionName)));
            Map<String, String> keyValues = zipPartitionKeyValues(partitionColumns, partitionValues);
            Path partitionPath = new Path(partition.getStorage().getLocation());
            String relativePartitionPath = FSUtils.getRelativePartitionPath(new StoragePath(tablePath.toUri()), new StoragePath(partitionPath.toUri()));
            return new HudiPartition(partitionName, partitionValues, keyValues, partition.getStorage(), fromDataColumns(partition.getColumns()), relativePartitionPath);
        }
    }

    private Map<String, String> zipPartitionKeyValues(List<HudiColumnHandle> partitionColumns, List<String> partitionValues)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        Streams.forEachPair(partitionColumns.stream(), partitionValues.stream(),
                (column, value) -> builder.put(column.getName(), value));
        return builder.build();
    }
}
