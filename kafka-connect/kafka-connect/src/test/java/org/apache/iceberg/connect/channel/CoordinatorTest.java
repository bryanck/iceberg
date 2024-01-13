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
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class CoordinatorTest extends ChannelTestBase {

  @Test
  public void testCommitAppend() {
    OffsetDateTime ts = EventTestUtil.now();
    UUID commitId =
        coordinatorTest(ImmutableList.of(EventTestUtil.createDataFile()), ImmutableList.of(), ts);

    assertThat(producer.history()).hasSize(3);
    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    verify(appendOp).appendFile(notNull());
    verify(appendOp).commit();

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(appendOp, times(3)).set(captor.capture(), notNull());
    assertThat(captor.getAllValues().get(0)).startsWith("kafka.connect.offsets.");
    assertThat(captor.getAllValues().get(1)).isEqualTo("kafka.connect.commit-id");
    assertThat(captor.getAllValues().get(2)).isEqualTo("kafka.connect.vtts");

    verify(deltaOp, times(0)).commit();
  }

  @Test
  public void testCommitDelta() {
    OffsetDateTime ts = EventTestUtil.now();
    UUID commitId =
        coordinatorTest(
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(EventTestUtil.createDeleteFile()),
            ts);

    assertThat(producer.history()).hasSize(3);
    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    verify(appendOp, times(0)).commit();

    verify(deltaOp).addRows(notNull());
    verify(deltaOp).addDeletes(notNull());
    verify(deltaOp).commit();

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(deltaOp, times(3)).set(captor.capture(), notNull());
    assertThat(captor.getAllValues().get(0)).startsWith("kafka.connect.offsets.");
    assertThat(captor.getAllValues().get(1)).isEqualTo("kafka.connect.commit-id");
    assertThat(captor.getAllValues().get(2)).isEqualTo("kafka.connect.vtts");
  }

  @Test
  public void testCommitNoFiles() {
    OffsetDateTime ts = EventTestUtil.now();
    UUID commitId = coordinatorTest(ImmutableList.of(), ImmutableList.of(), ts);

    assertThat(producer.history()).hasSize(2);
    assertCommitComplete(1, commitId, ts);

    verify(appendOp, times(0)).commit();
    verify(deltaOp, times(0)).commit();
  }

  @Test
  public void testCommitError() {
    doThrow(RuntimeException.class).when(appendOp).commit();

    OffsetDateTime ts = EventTestUtil.now();
    coordinatorTest(ImmutableList.of(EventTestUtil.createDataFile()), ImmutableList.of(), ts);

    // no commit messages sent
    assertThat(producer.history()).hasSize(1);
  }

  private void assertCommitTable(int idx, UUID commitId, OffsetDateTime ts) {
    byte[] bytes = producer.history().get(idx).value();
    Event commitToTable = AvroUtil.decode(bytes);
    assertThat(commitToTable.type()).isEqualTo(PayloadType.COMMIT_TO_TABLE);
    CommitToTable payload = (CommitToTable) commitToTable.payload();
    assertThat(payload.commitId()).isEqualTo(commitId);
    assertThat(payload.tableReference().identifier().toString()).isEqualTo("db.tbl");
    assertThat(payload.validThroughTs()).isEqualTo(ts);
  }

  private void assertCommitComplete(int idx, UUID commitId, OffsetDateTime ts) {
    byte[] bytes = producer.history().get(idx).value();
    Event commitComplete = AvroUtil.decode(bytes);
    assertThat(commitComplete.type()).isEqualTo(PayloadType.COMMIT_COMPLETE);
    CommitComplete payload = (CommitComplete) commitComplete.payload();
    assertThat(payload.commitId()).isEqualTo(commitId);
    assertThat(payload.validThroughTs()).isEqualTo(ts);
  }

  private UUID coordinatorTest(
      List<DataFile> dataFiles, List<DeleteFile> deleteFiles, OffsetDateTime ts) {
    when(config.catalogName()).thenReturn("catalog");
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    Coordinator coordinator = new Coordinator(catalog, config, ImmutableList.of(), clientFactory);
    coordinator.start();

    // init consumer after subscribe()
    initConsumer();

    coordinator.process();

    assertThat(producer.transactionCommitted()).isTrue();
    assertThat(producer.history()).hasSize(1);
    verify(appendOp, times(0)).commit();
    verify(deltaOp, times(0)).commit();

    byte[] bytes = producer.history().get(0).value();
    Event commitRequest = AvroUtil.decode(bytes);
    assertThat(commitRequest.type()).isEqualTo(PayloadType.START_COMMIT);

    UUID commitId = ((StartCommit) commitRequest.payload()).commitId();

    Event commitResponse =
        new Event(
            config.controlGroupId(),
            new DataWritten(
                StructType.of(),
                commitId,
                new TableReference("catalog", ImmutableList.of("db"), "tbl"),
                dataFiles,
                deleteFiles));
    bytes = AvroUtil.encode(commitResponse);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", bytes));

    Event commitReady =
        new Event(
            config.controlGroupId(),
            new DataComplete(
                commitId, ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts))));
    bytes = AvroUtil.encode(commitReady);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2, "key", bytes));

    when(config.commitIntervalMs()).thenReturn(0);

    coordinator.process();

    return commitId;
  }
}
