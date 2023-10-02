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

import java.nio.ByteBuffer;
import java.util.Collections;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericDataFile;
import org.apache.iceberg.GenericDeleteFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;

public class EventTestUtil {
  public static DataFile createDataFile() {
    PartitionData partitionData =
        new PartitionData(StructType.of(NestedField.required(999, "type", StringType.get())));
    Metrics metrics =
        new Metrics(
            1L,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap());

    return new GenericDataFile(
        1,
        "path",
        FileFormat.PARQUET,
        partitionData,
        1L,
        metrics,
        ByteBuffer.wrap(new byte[] {0}),
        null,
        null,
        1);
  }

  public static DeleteFile createDeleteFile() {
    PartitionData partitionData =
        new PartitionData(StructType.of(NestedField.required(999, "type", StringType.get())));
    Metrics metrics =
        new Metrics(
            1L,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap());

    return new GenericDeleteFile(
        1,
        FileContent.EQUALITY_DELETES,
        "path",
        FileFormat.PARQUET,
        partitionData,
        1L,
        metrics,
        new int[] {1},
        1,
        Collections.singletonList(1L),
        ByteBuffer.wrap(new byte[] {0}));
  }

  private EventTestUtil() {}
}
