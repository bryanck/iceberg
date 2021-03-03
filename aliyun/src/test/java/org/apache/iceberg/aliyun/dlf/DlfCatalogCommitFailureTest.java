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

package org.apache.iceberg.aliyun.dlf;

import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import java.io.File;
import java.util.ConcurrentModificationException;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.aliyun.oss.OSSURI;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

public class DlfCatalogCommitFailureTest extends DlfTestBase {

  @Test
  public void testFailedCommit() {
    Table table = setupTable();
    DlfTableOperations ops = (DlfTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();
    TableMetadata metadataV2 = updateTable(table, ops);

    DlfTableOperations spyOps = Mockito.spy(ops);
    failCommitAndThrowException(spyOps);

    AssertHelpers.assertThrows("We should wrap the error to CommitFailedException if the " +
            "commit actually doesn't succeed", CommitFailedException.class, "unexpected exception",
        () -> spyOps.commit(metadataV2, metadataV1));
    Mockito.verify(spyOps, Mockito.times(1)).refresh();

    ops.refresh();
    Assert.assertEquals("Current metadata should not have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata should still exist", metadataFileExists(metadataV2));
    Assert.assertEquals("No new metadata files should exist", 2, metadataFileCount(ops.current()));
  }

  @Test
  public void testConcurrentModificationExceptionDoesNotCheckCommitStatus() {
    Table table = setupTable();
    DlfTableOperations ops = (DlfTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();
    TableMetadata metadataV2 = updateTable(table, ops);

    DlfTableOperations spyOps = Mockito.spy(ops);
    failCommitAndThrowException(spyOps, new ConcurrentModificationException());

    try {
      spyOps.commit(metadataV2, metadataV1);
    } catch (CommitFailedException e) {
      Assert.assertTrue("Exception message should mention concurrent exception",
          e.getMessage().contains("DLF detected concurrent update"));
      Assert.assertTrue("Cause should be concurrent modification exception",
          e.getCause() instanceof ConcurrentModificationException);
    }
    Mockito.verify(spyOps, Mockito.times(0)).refresh();

    ops.refresh();
    Assert.assertEquals("Current metadata should not have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata should still exist", metadataFileExists(metadataV2));
    Assert.assertEquals("No new metadata files should exist", 2, metadataFileCount(ops.current()));
  }

  @Test
  public void testCommitThrowsExceptionWhileSucceeded() {
    Table table = setupTable();
    DlfTableOperations ops = (DlfTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();
    TableMetadata metadataV2 = updateTable(table, ops);

    DlfTableOperations spyOps = Mockito.spy(ops);

    // Simulate a communication error after a successful commit
    commitAndThrowException(ops, spyOps);

    // Shouldn't throw because the commit actually succeeds even though persistTable throws an exception
    spyOps.commit(metadataV2, metadataV1);

    ops.refresh();
    Assert.assertNotEquals("Current metadata should have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata file should still exist", metadataFileExists(ops.current()));
    Assert.assertEquals("Commit should have been successful and new metadata file should be made",
        3, metadataFileCount(ops.current()));
  }

  @Test
  public void testFailedCommitThrowsUnknownException() {
    Table table = setupTable();
    DlfTableOperations ops = (DlfTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();
    TableMetadata metadataV2 = updateTable(table, ops);

    DlfTableOperations spyOps = Mockito.spy(ops);
    failCommitAndThrowException(spyOps);
    breakFallbackCatalogCommitCheck(spyOps);

    AssertHelpers.assertThrows("Should throw CommitStateUnknownException since the catalog check was blocked",
        CommitStateUnknownException.class, "Datacenter on fire",
        () -> spyOps.commit(metadataV2, metadataV1));

    ops.refresh();

    Assert.assertEquals("Current metadata should not have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata file should still exist", metadataFileExists(ops.current()));
    Assert.assertEquals("Client could not determine outcome so new metadata file should also exist",
        3, metadataFileCount(ops.current()));
  }

  /**
   * Pretends we threw an exception while persisting, the commit succeeded, the lock expired,
   * and a second committer placed a commit on top of ours before the first committer was able to check
   * if their commit succeeded or not
   * <p>
   * Timeline:
   * Client 1 commits which throws an exception but suceeded
   * Client 1's lock expires while waiting to do the recheck for commit success
   * Client 2 acquires a lock, commits successfully on top of client 1's commit and release lock
   * Client 1 check's to see if their commit was successful
   * <p>
   * This tests to make sure a disconnected client 1 doesn't think their commit failed just because it isn't the
   * current one during the recheck phase.
   */
  @Test
  public void testExceptionThrownInConcurrentCommit() {
    Table table = setupTable();
    DlfTableOperations ops = (DlfTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();
    TableMetadata metadataV2 = updateTable(table, ops);

    DlfTableOperations spyOps = Mockito.spy(ops);
    concurrentCommitAndThrowException(ops, spyOps, table);

    /*
    This commit and our concurrent commit should succeed even though this commit throws an exception
    after the persist operation succeeds
     */
    spyOps.commit(metadataV2, metadataV1);

    ops.refresh();
    Assert.assertNotEquals("Current metadata should have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata file should still exist", metadataFileExists(ops.current()));
    Assert.assertEquals("The column addition from the concurrent commit should have been successful",
        2, ops.current().schema().columns().size());
  }

  @SuppressWarnings("unchecked")
  private void concurrentCommitAndThrowException(DlfTableOperations realOps,
                                                 DlfTableOperations spyOperations,
                                                 Table table) {
    // Simulate a communication error after a successful commit
    Mockito.doAnswer(i -> {
      Object[] args = i.getArguments();

      Schema schema = (Schema) args[0];
      PartitionSpec spec = (PartitionSpec) args[1];
      Long lockId = (Long) args[2];
      com.aliyun.datalake20200710.models.Table dlfTable = (com.aliyun.datalake20200710.models.Table) args[3];
      Map<String, String> mapProperties = (Map<String, String>) args[4];

      realOps.persistDLFTable(schema, spec, lockId, dlfTable, mapProperties);

      // new metadata location is stored in map property, and used for locking
      String newMetadataLocation = mapProperties.get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);

      // Simulate lock expiration or removal, use commit status null to avoid deleting data
      realOps.cleanupMetadataAndUnlock(null, newMetadataLocation);

      table.refresh();
      table.updateSchema().addColumn("newCol", Types.IntegerType.get()).commit();
      throw new RuntimeException("Datacenter on fire");
    }).when(spyOperations).persistDLFTable(Matchers.any(),
        Matchers.any(),
        Matchers.anyLong(),
        Matchers.any(),
        Matchers.anyMap());
  }

  private Table setupTable() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    return dlfCatalog.loadTable(TableIdentifier.of(namespace, tableName));
  }

  private TableMetadata updateTable(Table table, DlfTableOperations ops) {
    table.updateSchema()
        .addColumn("n", Types.IntegerType.get())
        .commit();

    ops.refresh();
    TableMetadata metadataV2 = ops.current();
    Assert.assertEquals(2, metadataV2.schema().columns().size());
    return metadataV2;
  }

  @SuppressWarnings("unchecked")
  private void commitAndThrowException(DlfTableOperations realOps, DlfTableOperations spyOps) {
    Mockito.doAnswer(i -> {
      Object[] args = i.getArguments();

      Schema schema = (Schema) args[0];
      PartitionSpec spec = (PartitionSpec) args[1];
      Long lockId = (Long) args[2];
      com.aliyun.datalake20200710.models.Table table = (com.aliyun.datalake20200710.models.Table) args[3];
      Map<String, String> parameters = (Map<String, String>) args[4];

      realOps.persistDLFTable(
          schema,
          spec,
          lockId,
          table,
          parameters);
      throw new RuntimeException("Datacenter on fire");
    }).when(spyOps).persistDLFTable(
        Matchers.any(),
        Matchers.any(),
        Matchers.anyLong(),
        Matchers.any(),
        Matchers.anyMap()
    );
  }

  private void failCommitAndThrowException(DlfTableOperations spyOps) {
    failCommitAndThrowException(spyOps, new RuntimeException("Datacenter on fire"));
  }

  private void failCommitAndThrowException(DlfTableOperations spyOps, Exception exceptionToThrow) {
    Mockito.doThrow(exceptionToThrow)
        .when(spyOps).persistDLFTable(
        Matchers.any(),
        Matchers.any(),
        Matchers.anyLong(),
        Matchers.any(),
        Matchers.anyMap());
  }

  private void breakFallbackCatalogCommitCheck(DlfTableOperations spyOperations) {
    Mockito.when(spyOperations.refresh())
        .thenThrow(new RuntimeException("Still on fire")); // Failure on commit check
  }

  private boolean metadataFileExists(TableMetadata metadata) {
    OSSURI ossuri = new OSSURI(metadata.metadataFileLocation());
    return ossClient.doesObjectExist(ossuri.bucket(), ossuri.key());
  }

  private int metadataFileCount(TableMetadata metadata) {
    OSSURI ossuri = new OSSURI(metadata.metadataFileLocation());
    int count = 0;

    ListObjectsRequest request = new ListObjectsRequest(ossuri.bucket())
        .withPrefix(new File(ossuri.key()).getParent());
    for (OSSObjectSummary s : ossClient.listObjects(request).getObjectSummaries()) {
      if (s.getKey().endsWith("metadata.json")) {
        count += 1;
      }
    }

    return count;
  }
}
