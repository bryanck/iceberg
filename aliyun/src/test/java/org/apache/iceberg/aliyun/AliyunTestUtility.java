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

package org.apache.iceberg.aliyun;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class AliyunTestUtility {
  private AliyunTestUtility() {
  }

  private static final String ALIYUN_TEST_BUCKET = "ALIYUN_TEST_BUCKET";
  private static final String ALIYUN_TEST_ACCESS_KEY_ID = "ALIYUN_TEST_ACCESS_KEY_ID";
  private static final String ALIYUN_TEST_ACCESS_KEY_SECRET = "ALIYUN_TEST_ACCESS_KEY_SECRET";
  private static final String ALIYUN_TEST_OSS_ENDPOINT = "ALIYUN_TEST_OSS_ENDPOINT";
  private static final String ALIYUN_TEST_OSS_KEY_PREFIX = "ALIYUN_TEST_OSS_KEY_PREFIX";
  private static final String ALIYUN_TEST_OSS_TEST_RULE_CLASS = "ALIYUN_TEST_OSS_TEST_RULE_CLASS";
  private static final String ALIYUN_TEST_DLF_CATALOG_ID = "ALIYUN_TEST_DLF_CATALOG_ID";
  private static final String ALIYUN_TEST_DLF_END_POINT = "ALIYUN_TEST_DLF_END_POINT";
  private static final String ALIYUN_TEST_DLF_REGION_ID = "ALIYUN_TEST_DLF_REGION_ID";

  /**
   * Set the environment variable ALIYUN_TEST_BUCKET for a default bucket to use for testing
   *
   * @return bucket name
   */
  public static String testBucketName() {
    return System.getenv(ALIYUN_TEST_BUCKET);
  }

  public static String testAccessKeyId() {
    return System.getenv(ALIYUN_TEST_ACCESS_KEY_ID);
  }

  public static String testAccessKeySecret() {
    return System.getenv(ALIYUN_TEST_ACCESS_KEY_SECRET);
  }

  public static String testOssEndpoint() {
    return System.getenv(ALIYUN_TEST_OSS_ENDPOINT);
  }

  public static String testOssKeyPrefix() {
    return System.getenv(ALIYUN_TEST_OSS_KEY_PREFIX);
  }

  public static String testOssTestRuleClass() {
    return System.getenv(ALIYUN_TEST_OSS_TEST_RULE_CLASS);
  }

  public static String testDlfCatalogId() {
    return System.getenv(ALIYUN_TEST_DLF_CATALOG_ID);
  }

  public static String testDlfEndpoint() {
    return System.getenv(ALIYUN_TEST_DLF_END_POINT);
  }

  public static String testDlfRegionId() {
    return System.getenv(ALIYUN_TEST_DLF_REGION_ID);
  }

  public static Map<String, String> toProps() {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    builder.put(AliyunProperties.OSS_ENDPOINT, testOssEndpoint());
    builder.put(AliyunProperties.ACCESS_KEY_ID, testAccessKeyId());
    builder.put(AliyunProperties.ACCESS_KEY_SECRET, testAccessKeySecret());
    builder.put(AliyunProperties.DLF_CATALOG_ID, testDlfCatalogId());
    builder.put(AliyunProperties.DLF_ENDPOINT, testDlfEndpoint());
    builder.put(AliyunProperties.DLF_REGION_ID, testDlfRegionId());

    return builder.build();
  }
}
