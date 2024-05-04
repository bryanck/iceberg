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
package org.apache.iceberg.connect.channel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.IcebergWriter;
import org.apache.iceberg.connect.data.IcebergWriterFactory;
import org.apache.iceberg.connect.data.WriterResult;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

public class WorkerTest extends ChannelTestBase {

  private static final String TABLE_NAME = "db.tbl";
  private static final String FIELD_NAME = "fld";

  @Test
  public void testStaticRoute() {
    when(config.catalogName()).thenReturn("catalog");
    when(config.tables()).thenReturn(ImmutableList.of(TABLE_NAME));
    Map<String, Object> value = ImmutableMap.of(FIELD_NAME, "val");
    workerTest(value);
  }

  @Test
  public void testDynamicRoute() {
    when(config.catalogName()).thenReturn("catalog");
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    Map<String, Object> value = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    workerTest(value);
  }

  private void workerTest(Map<String, Object> value) {
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(SRC_TOPIC_NAME, 0)));

    WriterResult writeResult =
        new WriterResult(
            TableIdentifier.parse(TABLE_NAME),
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(),
            StructType.of());
    IcebergWriter writer = mock(IcebergWriter.class);
    when(writer.complete()).thenReturn(ImmutableList.of(writeResult));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(writer);

    Worker worker = new Worker(config, clientFactory, writerFactory, context);
    worker.start();

    // init consumer after subscribe()
    initConsumer();

    // save a record
    SinkRecord rec = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, value, 0L);
    worker.save(ImmutableList.of(rec));

    UUID commitId = UUID.randomUUID();
    Event commitRequest = new Event(config.connectGroupId(), new StartCommit(commitId));
    byte[] bytes = AvroUtil.encode(commitRequest);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", bytes));

    worker.process();

    assertThat(producer.history()).hasSize(2);

    Event event = AvroUtil.decode(producer.history().get(0).value());
    assertThat(event.payload().type()).isEqualTo(PayloadType.DATA_WRITTEN);
    DataWritten responsePayload = (DataWritten) event.payload();
    assertThat(responsePayload.commitId()).isEqualTo(commitId);

    event = AvroUtil.decode(producer.history().get(1).value());
    assertThat(event.type()).isEqualTo(PayloadType.DATA_COMPLETE);
    DataComplete readyPayload = (DataComplete) event.payload();
    assertThat(readyPayload.commitId()).isEqualTo(commitId);
    assertThat(readyPayload.assignments()).hasSize(1);
    // offset should be one more than the record offset
    assertThat(readyPayload.assignments().get(0).offset()).isEqualTo(1L);
  }
}
