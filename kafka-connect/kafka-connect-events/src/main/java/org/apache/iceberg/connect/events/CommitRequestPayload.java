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

import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.iceberg.avro.AvroSchemaUtil;

/**
 * A control event payload for events sent by a coordinator to request workers to send back the
 * table data that has been written and is ready to commit.
 */
public class CommitRequestPayload implements Payload {

  private UUID commitId;
  private final Schema avroSchema;

  public static final Schema AVRO_SCHEMA =
      SchemaBuilder.builder()
          .record(CommitRequestPayload.class.getName())
          .fields()
          .name("commitId")
          .prop(AvroSchemaUtil.FIELD_ID_PROP, 1200)
          .type(UUID_SCHEMA)
          .noDefault()
          .endRecord();

  // Used by Avro reflection to instantiate this class when reading events
  public CommitRequestPayload(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public CommitRequestPayload(UUID commitId) {
    this.commitId = commitId;
    this.avroSchema = AVRO_SCHEMA;
  }

  public UUID commitId() {
    return commitId;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.commitId = (UUID) v;
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
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
