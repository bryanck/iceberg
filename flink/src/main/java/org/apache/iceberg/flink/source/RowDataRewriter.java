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

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.RewriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.PropertyUtil;

import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;

public class RowDataRewriter {
  private final Schema schema;
  private final FileFormat format;
  private final String nameMapping;
  private final FileIO io;
  private final boolean caseSensitive;
  private final EncryptionManager encryptionManager;
  private final TaskWriterFactory<RowData> taskWriterFactory;
  private final String tableName;

  public RowDataRewriter(Table table, boolean caseSensitive, FileIO io, EncryptionManager encryptionManager) {
    this.schema = table.schema();
    this.caseSensitive = caseSensitive;
    this.io = io;
    this.encryptionManager = encryptionManager;
    this.nameMapping = PropertyUtil.propertyAsString(table.properties(), DEFAULT_NAME_MAPPING, null);
    this.tableName = table.name();

    String formatString = PropertyUtil.propertyAsString(table.properties(), TableProperties.DEFAULT_FILE_FORMAT,
        TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    this.format = FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
    RowType flinkSchema = FlinkSchemaUtil.convert(table.schema());
    this.taskWriterFactory = new RowDataTaskWriterFactory(
        table.schema(),
        flinkSchema,
        table.spec(),
        table.locationProvider(),
        io,
        encryptionManager,
        Long.MAX_VALUE,
        format,
        table.properties(),
        null);
  }

  public List<DataFile> rewriteDataForTasks(DataStream<CombinedScanTask> dataStream, int parallelism) throws Exception {
    RewriteMapFunction map = new RewriteMapFunction(schema, nameMapping, io, caseSensitive,
        encryptionManager, taskWriterFactory);
    DataStream<RewriteResult> ds = dataStream.map(map).setParallelism(parallelism);

    List<DataFile> dataFiles = Lists.newArrayList();
    ds.executeAndCollect("")
        .forEachRemaining(writeResult -> Collections.addAll(dataFiles, writeResult.dataFilesToAdd()));

    return dataFiles;
  }
}
