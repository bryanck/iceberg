/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.ListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.TableScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RewriteTaskSelector extends AbstractStreamOperator<CombinedScanTask>
    implements OneInputStreamOperator<Long, CombinedScanTask> {

  private static final Logger LOG = LoggerFactory.getLogger(RewriteTaskSelector.class);

  private final TableLoader tableLoader;
  private final long maxFileSizeToCompact;

  private transient Table table;
  private transient long targetSizeInBytes;
  private transient int splitLookback;
  private transient long splitOpenFileCost;

  RewriteTaskSelector(TableLoader tableLoader, long maxFileSizeToCompact) {
    this.tableLoader = tableLoader;
    this.maxFileSizeToCompact = maxFileSizeToCompact;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    this.tableLoader.open();
    this.table = tableLoader.loadTable();

    // TODO see the BaseRewriteDataFilesAction, consider the read split-size.
    this.targetSizeInBytes = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    this.splitLookback = PropertyUtil.propertyAsInt(
        table.properties(),
        TableProperties.SPLIT_LOOKBACK,
        TableProperties.SPLIT_LOOKBACK_DEFAULT);
    this.splitOpenFileCost = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.SPLIT_OPEN_FILE_COST,
        TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {

  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {

  }

  @Override
  public void processElement(StreamRecord<Long> element) {
    table.refresh();

    long latestSnapshotId = element.getValue();
    for (CombinedScanTask combinedScanTask : planCombinedScanTask(latestSnapshotId)) {
      output.collect(new StreamRecord<>(combinedScanTask));
    }
  }

  @Override
  public void dispose() throws Exception {
    // Release all resources here.
    tableLoader.close();
  }

  private Iterable<CombinedScanTask> planCombinedScanTask(long latestSnapshotId) {
    List<CombinedScanTask> results = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> fileScanTask = table.newScan()
        .caseSensitive(false)
        .ignoreResiduals()
        .useSnapshot(latestSnapshotId)
        .planFiles()) {

      // Remove all files whose file size is greater than maxFileSizeToCompact.
      Iterable<FileScanTask> smallFileScanTasks = Iterables
          .filter(fileScanTask,
              task -> task.file().fileSizeInBytes() < maxFileSizeToCompact);

      // Group all FileScanTasks by partition.
      Map<StructLikeWrapper, Collection<FileScanTask>> tasksGroupedByPartition =
          groupTasksByPartition(smallFileScanTasks);

      for (Map.Entry<StructLikeWrapper, Collection<FileScanTask>> e : tasksGroupedByPartition.entrySet()) {
        if (e.getValue().size() <= 1) {
          // Skip to rewrite small files if there's no enough files in this partition.
          continue;
        }

        // Split the big file if possible. If we've removed files that is greater than maxFileSizeToCompact, do we need
        // to split here ? (TODO)
        CloseableIterable<FileScanTask> splitTasks =
            TableScanUtil.splitFiles(CloseableIterable.withNoopClose(e.getValue()), targetSizeInBytes);

        // Plan those FileScanTasks into CombinedScanTask.
        try (CloseableIterable<CombinedScanTask> combinedScanTasks = TableScanUtil.planTasks(splitTasks,
            targetSizeInBytes, splitLookback, splitOpenFileCost)) {

          for (CombinedScanTask combinedScanTask : combinedScanTasks) {
            if (combinedScanTask.files().size() > 1) {
              results.add(combinedScanTask);
            }
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to iterate the file scan tasks from iceberg table: ", e);
      throw new UncheckedIOException(e);
    }

    return results;
  }

  private Map<StructLikeWrapper, Collection<FileScanTask>> groupTasksByPartition(Iterable<FileScanTask> tasks) {
    ListMultimap<StructLikeWrapper, FileScanTask> tasksGroupedByPartition = Multimaps.newListMultimap(
        Maps.newHashMap(), Lists::newArrayList);

    Types.StructType partitionSpecType = table.spec().partitionType();
    for (FileScanTask task : tasks) {
      StructLikeWrapper structLike = StructLikeWrapper
          .forType(partitionSpecType)
          .set(task.file().partition());

      tasksGroupedByPartition.put(structLike, task);
    }

    return tasksGroupedByPartition.asMap();
  }
}
