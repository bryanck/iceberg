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

import com.aliyun.datalake20200710.Client;
import com.aliyun.datalake20200710.models.DeleteDatabaseRequest;
import com.aliyun.oss.OSS;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.tea.TeaException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aliyun.AliyunClientFactory;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.aliyun.AliyunTestUtility;
import org.apache.iceberg.aliyun.oss.OSSFileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"VisibilityModifier", "HideUtilityClassConstructor"})
public class DlfTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(DlfTestBase.class);

  private static final String testBucketName = AliyunTestUtility.testBucketName();
  private static final String catalogName = "dlf";
  private static final String testPathPrefix = getRandomName();
  static final String warehousePath = String.format("oss://%s/%s", testBucketName, testPathPrefix);
  static final List<String> namespaces = Lists.newArrayList();

  private static final AliyunClientFactory clientFactory = AliyunClientFactory.load(AliyunTestUtility.toProps());
  static final AliyunProperties aliyunProperties = new AliyunProperties(AliyunTestUtility.toProps());
  static final Client dlfClient = clientFactory.dlfClient();
  static final OSS ossClient = clientFactory.oss();
  static final DlfCatalog dlfCatalog = new DlfCatalog();

  static final Schema schema = new Schema(
      Types.NestedField.required(1, "c1", Types.StringType.get(), "c1")
  );
  static final PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).build();

  @BeforeClass
  public static void beforeClass() {
    LOG.info("Initialize the data lake format catalog.");
    OSSFileIO fileIO = new OSSFileIO(clientFactory::oss);

    dlfCatalog.initialize(catalogName, warehousePath, aliyunProperties, dlfClient, fileIO);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    // Clean all created namespaces.
    for (String namespace : namespaces) {
      try {
        dlfClient.deleteDatabase(new DeleteDatabaseRequest()
            .setCascade(true)
            .setCatalogId(aliyunProperties.dlfCatalogId())
            .setName(namespace));
      } catch (TeaException e) {
        if (!Objects.equals(DlfCatalog.NO_SUCH_OBJECT, e.getCode())) {
          throw e;
        }
      }
    }

    // Clean all objects from buckets.
    ObjectListing objectListing = ossClient.listObjects(
        new ListObjectsRequest(testBucketName)
            .withPrefix(testPathPrefix)
    );
    for (OSSObjectSummary s : objectListing.getObjectSummaries()) {
      ossClient.deleteObject(testBucketName, s.getKey());
    }
  }

  static String getRandomName() {
    String uuid = UUID.randomUUID().toString().replace("-", "");
    char ch = uuid.charAt(0);
    if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')) {
      return uuid;
    }
    return String.format("a%s", uuid.substring(1));
  }

  static String createNamespace() {
    String namespace = getRandomName();
    namespaces.add(namespace);
    dlfCatalog.createNamespace(Namespace.of(namespace));
    return namespace;
  }

  static String createTable(String namespace) {
    String tableName = getRandomName();
    return createTable(namespace, tableName);
  }

  static String createTable(String namespace, String tableName) {
    dlfCatalog.createTable(TableIdentifier.of(namespace, tableName), schema, partitionSpec);
    return tableName;
  }
}
