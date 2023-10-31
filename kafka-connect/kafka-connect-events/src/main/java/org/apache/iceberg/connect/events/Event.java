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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroEncoderUtil;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.avro.DecoderResolver;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.types.Types.UUIDType;

/**
 * Class representing all events produced to the control topic. Different event types have different
 * payloads.
 */
public class Event implements Element {

  private UUID id;
  private EventType type;
  private Long timestamp;
  private String groupId;
  private Payload payload;
  private final StructType icebergSchema;
  private final Schema avroSchema;

  public static byte[] encode(Event event) {
    try {
      return AvroEncoderUtil.encode(event, event.getSchema());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static Event decode(byte[] bytes) {
    try {
      Event event = AvroEncoderUtil.decode(bytes);
      // clear the cache to avoid memory leak
      DecoderResolver.clearCache();
      return event;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // Used by Avro reflection to instantiate this class when reading events
  public Event(Schema avroSchema) {
    this.avroSchema = avroSchema;
    this.icebergSchema = AvroSchemaUtil.convert(avroSchema).asStructType();
  }

  public Event(String groupId, EventType type, Payload payload) {
    this.id = UUID.randomUUID();
    this.type = type;
    this.timestamp = System.currentTimeMillis();
    this.groupId = groupId;
    this.payload = payload;

    this.icebergSchema =
        StructType.of(
            NestedField.required(10_500, "id", UUIDType.get()),
            NestedField.required(10_501, "type", IntegerType.get()),
            NestedField.required(10_502, "timestamp", TimestampType.withZone()),
            NestedField.required(10_503, "group_id", StringType.get()),
            NestedField.required(10_504, "payload", payload.writeSchema()));

    Map<Integer, String> typeMap = Maps.newHashMap(AvroUtil.FIELD_ID_TO_CLASS);
    typeMap.put(10_504, payload.getClass().getName());

    this.avroSchema =
        AvroSchemaUtil.convert(
            icebergSchema,
            (fieldId, struct) ->
                struct.equals(icebergSchema) ? getClass().getName() : typeMap.get(fieldId));
  }

  public UUID id() {
    return id;
  }

  public EventType type() {
    return type;
  }

  public Long timestamp() {
    return timestamp;
  }

  public Payload payload() {
    return payload;
  }

  public String groupId() {
    return groupId;
  }

  @Override
  public StructType writeSchema() {
    return icebergSchema;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.id = (UUID) v;
        return;
      case 1:
        this.type = v == null ? null : EventType.values()[(Integer) v];
        return;
      case 2:
        this.timestamp = (Long) v;
        return;
      case 3:
        this.groupId = v == null ? null : v.toString();
        return;
      case 4:
        this.payload = (Payload) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return id;
      case 1:
        return type == null ? null : type.id();
      case 2:
        return timestamp;
      case 3:
        return groupId;
      case 4:
        return payload;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
