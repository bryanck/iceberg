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

import com.aliyun.datalake20200710.models.FieldSchema;
import com.aliyun.datalake20200710.models.GetTableRequest;
import com.aliyun.datalake20200710.models.GetTableResponse;
import com.aliyun.datalake20200710.models.Table;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import java.util.List;
import java.util.Locale;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aliyun.oss.OSSURI;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.junit.Assert;
import org.junit.Test;

public class TestDlfCatalogTable extends DlfTestBase {

  @Test
  public void testCreateTable() throws Exception {
    String namespace = createNamespace();
    String tableName = getRandomName();
    dlfCatalog.createTable(TableIdentifier.of(namespace, tableName), schema, partitionSpec);

    GetTableResponse response = dlfClient.getTable(new GetTableRequest()
        .setCatalogId(aliyunProperties.dlfCatalogId())
        .setDatabaseName(namespace)
        .setTableName(tableName));
    Table table = response.getBody().getTable();
    Assert.assertEquals(namespace, table.getDatabaseName());
    Assert.assertEquals(tableName, table.getTableName());
    Assert.assertEquals(BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH),
        table.getParameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP));
    Assert.assertTrue(table.getParameters().containsKey(BaseMetastoreTableOperations.METADATA_LOCATION_PROP));
    Assert.assertEquals(String.format("%s/%s.db/%s", warehousePath, namespace, tableName), table.getSd().getLocation());
    assertFieldSchemaList(IcebergToDlfConverter.toFieldSchemaList(schema), table.getSd().getCols());
    assertFieldSchemaList(IcebergToDlfConverter.toFieldSchemaList(partitionSpec), table.getPartitionKeys());

    // verify metadata file exists in OSS
    String metaLocation = table.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
    OSSURI ossuri = new OSSURI(metaLocation);
    Assert.assertTrue(ossClient.doesObjectExist(ossuri.bucket(), ossuri.key()));

    org.apache.iceberg.Table iTable = dlfCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    Assert.assertEquals(partitionSpec, iTable.spec());
    Assert.assertEquals(schema.asStruct(), iTable.schema().asStruct());
  }

  private void assertFieldSchemaList(List<FieldSchema> expected, List<FieldSchema> actual) {
    if (expected == null || actual == null) {
      Assert.assertEquals("Both should be null.", expected, actual);
    }
    Assert.assertEquals("Should have the same length", expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      FieldSchema schema1 = expected.get(i);
      FieldSchema schema2 = actual.get(i);
      if (schema1 == null || schema2 == null) {
        Assert.assertEquals("Both should be null", schema1, schema2);
      }
      Assert.assertEquals("Should have the same name", schema1.getName(), schema2.getName());
      Assert.assertEquals("Should have the same comment", schema1.getComment(), schema2.getComment());
      Assert.assertEquals("Should have the same type", schema1.getType(), schema2.getType());
      Assert.assertEquals("Should have the same parameters", schema1.getParameters(), schema2.getParameters());
    }
  }

  @Test
  public void testCreateTableDuplicate() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    AssertHelpers.assertThrows("should not create table with the same name",
        AlreadyExistsException.class,
        "Table already exists",
        () -> dlfCatalog.createTable(TableIdentifier.of(namespace, tableName), schema, partitionSpec));
  }

  @Test
  public void testListTable() {
    String namespace = createNamespace();
    Assert.assertTrue("list namespace should have nothing before table creation",
        dlfCatalog.listTables(Namespace.of(namespace)).isEmpty());

    String tableName = createTable(namespace);
    List<TableIdentifier> tables = dlfCatalog.listTables(Namespace.of(namespace));
    Assert.assertEquals(1, tables.size());
    Assert.assertEquals(TableIdentifier.of(namespace, tableName), tables.get(0));
  }

  @Test
  public void testTableExists() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    Assert.assertTrue(dlfCatalog.tableExists(TableIdentifier.of(namespace, tableName)));
  }

  @Test
  public void testUpdateTable() throws Exception {
    String namespace = createNamespace();
    String tableName = getRandomName();
    // current should be null
    TableOperations ops = dlfCatalog.newTableOps(TableIdentifier.of(namespace, tableName));
    TableMetadata current = ops.current();
    Assert.assertNull(current);
    // create table, refresh should update
    createTable(namespace, tableName);
    current = ops.refresh();
    Assert.assertEquals(schema.asStruct(), current.schema().asStruct());
    Assert.assertEquals(partitionSpec, current.spec());
    org.apache.iceberg.Table table = dlfCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    Assert.assertTrue("initial table history should be empty", table.history().isEmpty());

    // Commit new version, should create a new snapshot.
    table = dlfCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    DataFile dataFile = DataFiles.builder(partitionSpec)
        .withPath("/path/to/data-a.parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
    table.newAppend().appendFile(dataFile).commit();
    table = dlfCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    Assert.assertEquals("Commit should create a new table version", 1, table.history().size());

    // Check table in DLF
    GetTableResponse response = dlfClient.getTable(new GetTableRequest()
        .setCatalogId(aliyunProperties.dlfCatalogId())
        .setDatabaseName(namespace)
        .setTableName(tableName));
    Assert.assertEquals("external table type is set after update", "EXTERNAL_TABLE",
        response.getBody().getTable().getTableType());
  }

  @Test
  public void testRenameTable() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    org.apache.iceberg.Table table = dlfCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    // Rename table
    String newTableName = String.format("%s_2", tableName);
    dlfCatalog.renameTable(TableIdentifier.of(namespace, tableName), TableIdentifier.of(namespace, newTableName));
    org.apache.iceberg.Table renamedTable = dlfCatalog.loadTable(TableIdentifier.of(namespace, newTableName));
    Assert.assertEquals(table.location(), renamedTable.location());
    Assert.assertEquals(table.schema().asStruct(), renamedTable.schema().asStruct());
    Assert.assertEquals(table.spec(), renamedTable.spec());
    Assert.assertEquals(table.currentSnapshot(), renamedTable.currentSnapshot());
  }

  @Test
  public void testRenameTable_NoSuchTable() {
    String namespace = createNamespace();
    String tableName = getRandomName();
    AssertHelpers.assertThrows("Table does not exist",
        NoSuchTableException.class,
        "Table does not exist: ",
        () -> dlfCatalog.loadTable(TableIdentifier.of(namespace, tableName)));
    // Rename table.
    String newTableName = String.format("%s_2", tableName);
    AssertHelpers.assertThrows("Table does not exist",
        NoSuchTableException.class,
        "because the table does not exist in DLF",
        () -> dlfCatalog.renameTable(
            TableIdentifier.of(namespace, tableName),
            TableIdentifier.of(namespace, newTableName)
        ));
    // Create the table.
    createTable(namespace, tableName);
    org.apache.iceberg.Table table = dlfCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    // Rename it again.
    dlfCatalog.renameTable(TableIdentifier.of(namespace, tableName), TableIdentifier.of(namespace, newTableName));
    // Load table for assert purpose.
    org.apache.iceberg.Table renamedTable = dlfCatalog.loadTable(TableIdentifier.of(namespace, newTableName));
    Assert.assertEquals(table.location(), renamedTable.location());
    Assert.assertEquals(table.schema().asStruct(), renamedTable.schema().asStruct());
    Assert.assertEquals(table.spec(), renamedTable.spec());
    Assert.assertEquals(table.currentSnapshot(), renamedTable.currentSnapshot());
  }

  @Test
  public void testRenameTable_AlreadyExists() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);

    TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
    org.apache.iceberg.Table table = dlfCatalog.loadTable(identifier);

    // Create a new table in DLF, so that rename to that table will fail
    String newTableName = tableName + "_2";
    dlfCatalog.createTable(TableIdentifier.of(namespace, newTableName), schema, partitionSpec);
    AssertHelpers.assertThrows("should fail to rename to an existing table",
        AlreadyExistsException.class,
        "already exists",
        () -> dlfCatalog.renameTable(
            TableIdentifier.of(namespace, tableName),
            TableIdentifier.of(namespace, newTableName)
        ));

    // Old table can still be read with same metadata.
    org.apache.iceberg.Table oldTable = dlfCatalog.loadTable(identifier);
    Assert.assertEquals(table.location(), oldTable.location());
    Assert.assertEquals(table.schema().asStruct(), oldTable.schema().asStruct());
    Assert.assertEquals(table.spec(), oldTable.spec());
    Assert.assertEquals(table.currentSnapshot(), oldTable.currentSnapshot());
  }

  @Test
  public void testDeleteTableWithoutPurge() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);

    dlfCatalog.dropTable(TableIdentifier.of(namespace, tableName), false);
    AssertHelpers.assertThrows("should not have table",
        NoSuchTableException.class,
        "Table does not exist",
        () -> dlfCatalog.loadTable(TableIdentifier.of(namespace, tableName)));

    String warehouseLocation = dlfCatalog.defaultWarehouseLocation(TableIdentifier.of(namespace, tableName));
    OSSURI ossuri = new OSSURI(warehouseLocation);

    boolean hasMetaFile = false;
    for (OSSObjectSummary s : ossClient.listObjects(new ListObjectsRequest(ossuri.bucket())
        .withPrefix(ossuri.key()))
        .getObjectSummaries()) {
      if (s.getKey().contains(".json")) {
        hasMetaFile = true;
        break;
      }
    }

    Assert.assertTrue("metadata json file exists after delete without purge", hasMetaFile);
  }

  @Test
  public void testDeleteTableWithPurge() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    dlfCatalog.dropTable(TableIdentifier.of(namespace, tableName));
    AssertHelpers.assertThrows("should not have table",
        NoSuchTableException.class,
        "Table does not exist",
        () -> dlfCatalog.loadTable(TableIdentifier.of(namespace, tableName)));

    String warehouseLocation = dlfCatalog.defaultWarehouseLocation(TableIdentifier.of(namespace, tableName));
    OSSURI ossuri = new OSSURI(warehouseLocation);

    for (OSSObjectSummary s : ossClient.listObjects(new ListObjectsRequest(ossuri.bucket())
        .withPrefix(ossuri.key())).getObjectSummaries()) {
      // might have directory markers left.
      Assert.assertEquals(0, s.getSize());
    }
  }
}
