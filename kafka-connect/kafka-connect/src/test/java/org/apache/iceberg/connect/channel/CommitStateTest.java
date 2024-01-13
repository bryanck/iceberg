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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.Payload;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class CommitStateTest {
  @Test
  public void testIsCommitReady() {
    TopicPartitionOffset tp = mock(TopicPartitionOffset.class);

    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    DataComplete payload1 = mock(DataComplete.class);
    when(payload1.commitId()).thenReturn(commitState.currentCommitId());
    when(payload1.assignments()).thenReturn(ImmutableList.of(tp, tp));

    DataComplete payload2 = mock(DataComplete.class);
    when(payload2.commitId()).thenReturn(commitState.currentCommitId());
    when(payload2.assignments()).thenReturn(ImmutableList.of(tp));

    DataComplete payload3 = mock(DataComplete.class);
    when(payload3.commitId()).thenReturn(UUID.randomUUID());
    when(payload3.assignments()).thenReturn(ImmutableList.of(tp));

    commitState.addDataComplete(wrapInEnvelope(payload1));
    commitState.addDataComplete(wrapInEnvelope(payload2));
    commitState.addDataComplete(wrapInEnvelope(payload3));

    assertThat(commitState.isCommitReady(3)).isTrue();
    assertThat(commitState.isCommitReady(4)).isFalse();
  }

  @Test
  public void testGetValidThroughTs() {
    DataComplete payload1 = mock(DataComplete.class);
    TopicPartitionOffset tp1 = mock(TopicPartitionOffset.class);
    when(tp1.timestamp()).thenReturn(EventTestUtil.fromEpochMillis(3L));
    TopicPartitionOffset tp2 = mock(TopicPartitionOffset.class);
    when(tp2.timestamp()).thenReturn(EventTestUtil.fromEpochMillis(2L));
    when(payload1.assignments()).thenReturn(ImmutableList.of(tp1, tp2));

    DataComplete payload2 = mock(DataComplete.class);
    TopicPartitionOffset tp3 = mock(TopicPartitionOffset.class);
    when(tp3.timestamp()).thenReturn(EventTestUtil.fromEpochMillis(1L));
    when(payload2.assignments()).thenReturn(ImmutableList.of(tp3));

    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    commitState.addDataComplete(wrapInEnvelope(payload1));
    commitState.addDataComplete(wrapInEnvelope(payload2));

    assertThat(commitState.validThroughTs(false)).isEqualTo(EventTestUtil.fromEpochMillis(1L));
    assertThat(commitState.validThroughTs(true)).isNull();

    // null timestamp for one, so should not set a vtts
    DataComplete payload3 = mock(DataComplete.class);
    TopicPartitionOffset tp4 = mock(TopicPartitionOffset.class);
    when(tp4.timestamp()).thenReturn(null);
    when(payload3.assignments()).thenReturn(ImmutableList.of(tp4));

    commitState.addDataComplete(wrapInEnvelope(payload3));

    assertThat(commitState.validThroughTs(false)).isNull();
    assertThat(commitState.validThroughTs(true)).isNull();
  }

  private Envelope wrapInEnvelope(Payload payload) {
    Event event = mock(Event.class);
    when(event.payload()).thenReturn(payload);
    return new Envelope(event, 0, 0);
  }
}
