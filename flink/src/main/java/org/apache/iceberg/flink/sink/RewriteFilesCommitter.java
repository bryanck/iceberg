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

import java.util.List;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.RewriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

class RewriteFilesCommitter extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<RewriteResult, Void> {

  private final TableLoader tableLoader;
  private final int thresholdToCommitTxn;
  private final List<RewriteResult> rewriteResults = Lists.newArrayList();

  private transient String flinkJobId;
  private transient Table table;

  RewriteFilesCommitter(TableLoader tableLoader, int thresholdToCommitTxn) {
    this.tableLoader = tableLoader;
    this.thresholdToCommitTxn = thresholdToCommitTxn;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.flinkJobId = getContainingTask().getEnvironment().getJobID().toString();

    this.tableLoader.open();
    this.table = tableLoader.loadTable();
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
  }

  @Override
  public void processElement(StreamRecord<RewriteResult> element) throws Exception {
    rewriteResults.add(element.getValue());

    if (rewriteResults.size() >= thresholdToCommitTxn) {
      commitRewriteTxn(rewriteResults, flinkJobId);
    }
  }

  private void commitRewriteTxn(List<RewriteResult> pendingResults, String newFlinkJobId) {
    RewriteResult mergedResult = RewriteResult.builder()
        .addAll(pendingResults)
        .build();

    Preconditions.checkState(mergedResult.deleteFilesToDelete().length == 0,
        "Cannot rewrite files with delete files to delete");
    Preconditions.checkState(mergedResult.dataFilesToAdd().length == 0,
        "Cannot rewrite files with delete files to add");

    // Commit the rewrite transaction.
    RewriteFiles rewriteFiles = table.newRewrite();
    rewriteFiles.rewriteFiles(
        Sets.newHashSet(mergedResult.dataFilesToDelete()),
        Sets.newHashSet(mergedResult.dataFilesToAdd()));

    commitOperation(rewriteFiles,
        mergedResult.dataFilesToDelete().length,
        mergedResult.dataFilesToAdd().length,
        mergedResult.deleteFilesToDelete().length,
        mergedResult.deleteFilesToAdd().length,
        "Rewrite files txn");
  }

  private void commitOperation(SnapshotUpdate<?> operation,
                               int dataFilesToDelete,
                               int dataFilesToAdd,
                               int deleteFilesToDelete,
                               int deleteFilesToAdd,
                               String description) {
    LOG.info("Committing {} with {} dataFilesToDelete, {} dataFilesToAdd, {} deleteFilesToDelete {} ,deleteFilesToAdd" +
        " to table {}", description, dataFilesToDelete, dataFilesToAdd, deleteFilesToDelete, deleteFilesToAdd, table);

    long start = System.currentTimeMillis();
    operation.commit(); // abort is automatically called if this fails.
    long duration = System.currentTimeMillis() - start;
    LOG.info("Committed in {} ms", duration);
  }

  @Override
  public void dispose() throws Exception {
    if (tableLoader != null) {
      tableLoader.close();
    }
  }
}
