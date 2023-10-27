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
package org.apache.iceberg.connect.events;

import java.util.List;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.StructType;

/**
 * A control event payload for events sent by a worker that contains the table data that has been
 * written and is ready to commit.
 */
public class CommitResponsePayload implements Payload {

  private UUID commitId;
  private TableReference tableReference;
  private List<DataFile> dataFiles;
  private List<DeleteFile> deleteFiles;
  private final Schema avroSchema;

  // Used by Avro reflection to instantiate this class when reading events
  public CommitResponsePayload(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public CommitResponsePayload(
      StructType partitionType,
      UUID commitId,
      TableReference tableReference,
      List<DataFile> dataFiles,
      List<DeleteFile> deleteFiles) {
    this.commitId = commitId;
    this.tableReference = tableReference;
    this.dataFiles = dataFiles;
    this.deleteFiles = deleteFiles;

    StructType dataFileStruct = DataFile.getType(partitionType);
    Schema dataFileSchema =
        AvroSchemaUtil.convert(
            dataFileStruct,
            ImmutableMap.of(
                dataFileStruct,
                "org.apache.iceberg.GenericDataFile",
                partitionType,
                PartitionData.class.getName()));

    Schema deleteFileSchema =
        AvroSchemaUtil.convert(
            dataFileStruct,
            ImmutableMap.of(
                dataFileStruct,
                "org.apache.iceberg.GenericDeleteFile",
                partitionType,
                PartitionData.class.getName()));

    this.avroSchema =
        SchemaBuilder.builder()
            .record(getClass().getName())
            .fields()
            .name("commitId")
            .prop(AvroSchemaUtil.FIELD_ID_PROP, 1300)
            .type(UUID_SCHEMA)
            .noDefault()
            .name("tableRef")
            .prop(AvroSchemaUtil.FIELD_ID_PROP, 1301)
            .type(TableReference.AVRO_SCHEMA)
            .noDefault()
            .name("dataFiles")
            .prop(AvroSchemaUtil.FIELD_ID_PROP, 1302)
            .type()
            .nullable()
            .array()
            .items(dataFileSchema)
            .noDefault()
            .name("deleteFiles")
            .prop(AvroSchemaUtil.FIELD_ID_PROP, 1303)
            .type()
            .nullable()
            .array()
            .items(deleteFileSchema)
            .noDefault()
            .endRecord();
  }

  public UUID commitId() {
    return commitId;
  }

  public TableReference tableReference() {
    return tableReference;
  }

  public List<DataFile> dataFiles() {
    return dataFiles;
  }

  public List<DeleteFile> deleteFiles() {
    return deleteFiles;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.commitId = (UUID) v;
        return;
      case 1:
        this.tableReference = (TableReference) v;
        return;
      case 2:
        this.dataFiles = (List<DataFile>) v;
        return;
      case 3:
        this.deleteFiles = (List<DeleteFile>) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return commitId;
      case 1:
        return tableReference;
      case 2:
        return dataFiles;
      case 3:
        return deleteFiles;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
