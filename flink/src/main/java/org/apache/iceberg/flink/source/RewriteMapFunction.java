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

package org.apache.iceberg.flink.source;

import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.RewriteResult;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RewriteMapFunction extends RichMapFunction<CombinedScanTask, RewriteResult> {
  private static final Logger LOG = LoggerFactory.getLogger(RewriteMapFunction.class);

  private int subTaskId;
  private int attemptId;

  private final Schema schema;
  private final String nameMapping;
  private final FileIO io;
  private final boolean caseSensitive;
  private final EncryptionManager encryptionManager;
  private final TaskWriterFactory<RowData> taskWriterFactory;

  public RewriteMapFunction(Schema schema, String nameMapping, FileIO io, boolean caseSensitive,
                            EncryptionManager encryptionManager, TaskWriterFactory<RowData> taskWriterFactory) {
    this.schema = schema;
    this.nameMapping = nameMapping;
    this.io = io;
    this.caseSensitive = caseSensitive;
    this.encryptionManager = encryptionManager;
    this.taskWriterFactory = taskWriterFactory;
  }

  @Override
  public void open(Configuration parameters) {
    this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    this.attemptId = getRuntimeContext().getAttemptNumber();
    // Initialize the task writer factory.
    this.taskWriterFactory.initialize(subTaskId, attemptId);
  }

  @Override
  public RewriteResult map(CombinedScanTask task) throws Exception {
    Preconditions.checkArgument(task.files().stream().mapToInt(t -> t.deletes().size()).sum() == 0,
        "Cannot rewrite with delete files now, it will be supported in iceberg format v2.");

    // Initialize the task writer.
    TaskWriter<RowData> writer = taskWriterFactory.create();
    try (RowDataIterator iterator = new RowDataIterator(task, io, encryptionManager, schema,
        schema, nameMapping, caseSensitive)) {
      while (iterator.hasNext()) {
        RowData rowData = iterator.next();
        writer.write(rowData);
      }

      return RewriteResult.builder()
          .addDataFilesToDelete(task.files().stream().map(FileScanTask::file).collect(Collectors.toList()))
          .addDataFilesToAdd(writer.dataFiles())
          .build();
    } catch (Throwable originalThrowable) {
      try {
        LOG.error("Aborting commit for  (subTaskId {}, attemptId {})", subTaskId, attemptId);
        writer.abort();
        LOG.error("Aborted commit for  (subTaskId {}, attemptId {})", subTaskId, attemptId);
      } catch (Throwable inner) {
        if (originalThrowable != inner) {
          originalThrowable.addSuppressed(inner);
          LOG.warn("Suppressing exception in catch: {}", inner.getMessage(), inner);
        }
      }

      if (originalThrowable instanceof Exception) {
        throw originalThrowable;
      } else {
        throw new RuntimeException(originalThrowable);
      }
    }
  }
}
