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
import org.apache.avro.Schema;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.avro.AvroEncoderUtil;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.avro.DecoderResolver;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;

/** Class for Avro-related utility methods. */
class AvroUtil {
  static final Map<Integer, String> FIELD_ID_TO_CLASS =
      ImmutableMap.of(
          10_102,
          TopicPartitionOffset.class.getName(),
          DataFile.PARTITION_ID,
          PartitionData.class.getName(),
          10_301,
          TableReference.class.getName(),
          10_303,
          "org.apache.iceberg.GenericDataFile",
          10_305,
          "org.apache.iceberg.GenericDeleteFile",
          10_401,
          TableReference.class.getName());

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

  static Schema convert(Types.StructType icebergSchema) {
    return AvroSchemaUtil.convert(
        icebergSchema, (fieldId, struct) -> FIELD_ID_TO_CLASS.get(fieldId));
  }

  private AvroUtil() {}
}
