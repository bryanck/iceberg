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

import com.aliyun.datalake20200710.models.CreateTableRequest;
import com.aliyun.datalake20200710.models.Database;
import com.aliyun.datalake20200710.models.GetDatabaseRequest;
import com.aliyun.datalake20200710.models.TableInput;
import com.aliyun.tea.TeaException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

public class TestDlfCatalogNamespace extends DlfTestBase {

  @Test
  public void testCreateNamespace() throws Exception {
    String namespace = getRandomName();
    namespaces.add(namespace);
    AssertHelpers.assertThrows("namespace does not exist before create",
        TeaException.class,
        "not found",
        () -> dlfClient.getDatabase(new GetDatabaseRequest()
            .setCatalogId(aliyunProperties.dlfCatalogId())
            .setName(namespace)));

    dlfCatalog.createNamespace(Namespace.of(namespace));
    Database database = dlfClient.getDatabase(new GetDatabaseRequest()
        .setCatalogId(aliyunProperties.dlfCatalogId())
        .setName(namespace))
        .getBody()
        .getDatabase();
    Assert.assertEquals("namespace must equal database name", namespace, database.getName());
  }

  @Test
  public void testCreateDuplicate() {
    String namespace = createNamespace();
    AssertHelpers.assertThrows("should not create namespace with the same name",
        AlreadyExistsException.class,
        "it already exists in DLF",
        () -> dlfCatalog.createNamespace(Namespace.of(namespace))
    );
  }

  @Test
  public void testNamespaceExists() {
    String namespace = createNamespace();
    Assert.assertTrue(dlfCatalog.namespaceExists(Namespace.of(namespace)));
  }

  @Test
  public void testListNamespace() {
    String namespace = createNamespace();
    List<Namespace> namespaceList = dlfCatalog.listNamespaces();
    Assert.assertTrue(namespaceList.size() > 0);
    Assert.assertTrue(namespaceList.contains(Namespace.of(namespace)));

    namespaceList = dlfCatalog.listNamespaces(Namespace.of(namespace));
    Assert.assertTrue(namespaceList.isEmpty());
  }

  @Test
  public void testListNamespace_multiPages() {
    Set<String> addedNamespaces = Sets.newHashSet();
    for (int i = 0; i < 25; i++) {
      addedNamespaces.add(createNamespace());
    }

    List<Namespace> namespaceList = dlfCatalog.listNamespaces();
    Assert.assertTrue(namespaceList.size() >= addedNamespaces.size());
    for (String addedNamespace : addedNamespaces) {
      Assert.assertTrue(namespaceList.contains(Namespace.of(addedNamespace)));
    }
  }

  @Test
  public void testNamespaceProperties() throws Exception {
    String namespace = createNamespace();
    // set properties
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key", "val");
    properties.put("key2", "val2");
    dlfCatalog.setProperties(Namespace.of(namespace), properties);
    Database database = dlfClient.getDatabase(new GetDatabaseRequest()
        .setCatalogId(aliyunProperties.dlfCatalogId())
        .setName(namespace))
        .getBody()
        .getDatabase();
    Assert.assertTrue(database.getParameters().containsKey("key"));
    Assert.assertEquals("val", database.getParameters().get("key"));
    Assert.assertTrue(database.getParameters().containsKey("key2"));
    Assert.assertEquals("val2", database.getParameters().get("key2"));
    // remove properties
    dlfCatalog.removeProperties(Namespace.of(namespace), Sets.newHashSet("key"));
    database = dlfClient.getDatabase(new GetDatabaseRequest()
        .setCatalogId(aliyunProperties.dlfCatalogId())
        .setName(namespace))
        .getBody().getDatabase();
    Assert.assertFalse(database.getParameters().containsKey("key"));
    Assert.assertTrue(database.getParameters().containsKey("key2"));
    // add back
    properties = Maps.newHashMap();
    properties.put("key", "val");
    dlfCatalog.setProperties(Namespace.of(namespace), properties);
    database = dlfClient.getDatabase(new GetDatabaseRequest()
        .setCatalogId(aliyunProperties.dlfCatalogId())
        .setName(namespace))
        .getBody()
        .getDatabase();
    Assert.assertTrue(database.getParameters().containsKey("key"));
    Assert.assertEquals("val", database.getParameters().get("key"));
    Assert.assertTrue(database.getParameters().containsKey("key2"));
    Assert.assertEquals("val2", database.getParameters().get("key2"));
  }

  @Test
  public void testDropNamespace() {
    String namespace = createNamespace();
    dlfCatalog.dropNamespace(Namespace.of(namespace));
    namespaces.remove(namespace);
    AssertHelpers.assertThrows("namespace should not exist after deletion",
        TeaException.class,
        "not found",
        () -> dlfCatalog.dropNamespace(Namespace.of(namespace)));
  }

  @Test
  public void testDropNamespaceNonEmpty_containsIcebergTable() {
    String namespace = createNamespace();
    createTable(namespace);
    AssertHelpers.assertThrows("namespace should not be dropped when still has Iceberg table",
        NamespaceNotEmptyException.class,
        "still contains iceberg tables",
        () -> dlfCatalog.dropNamespace(Namespace.of(namespace)));
  }

  @Test
  public void testDropNamespaceNonEmpty_containsNonIcebergTable() throws Exception {
    String namespace = createNamespace();

    String tableName = getRandomName();
    TableInput tableInput = new TableInput()
        .setDatabaseName(namespace)
        .setTableName(tableName)
        .setParameters(ImmutableMap.of());

    dlfClient.createTable(new CreateTableRequest()
        .setCatalogId(aliyunProperties.dlfCatalogId())
        .setDatabaseName(namespace)
        .setTableInput(tableInput));

    AssertHelpers.assertThrows("namespace should not be dropped when still has non-iceberg table",
        NamespaceNotEmptyException.class,
        "still contains non-iceberg tables",
        () -> dlfCatalog.dropNamespace(Namespace.of(namespace)));
  }
}
