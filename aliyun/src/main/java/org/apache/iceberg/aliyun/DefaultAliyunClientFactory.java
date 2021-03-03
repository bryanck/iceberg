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

import com.aliyun.datalake20200710.Client;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.teaopenapi.models.Config;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class DefaultAliyunClientFactory implements AliyunClientFactory {

  private static final String DLF_AUTH_TYPE = "access_key";

  private AliyunProperties aliyunProperties;

  @Override
  public OSS oss() {
    Preconditions.checkNotNull(aliyunProperties,
        "Cannot create aliyun oss client before initializing the AliyunClientFactory.");

    return new OSSClientBuilder().build(
        aliyunProperties.ossEndpoint(),
        aliyunProperties.accessKeyId(),
        aliyunProperties.accessKeySecret());
  }

  @Override
  public Client dlfClient() {
    Preconditions.checkNotNull(aliyunProperties,
        "Cannot create aliyun DLF client before initializing the AliyunClientFactory.");

    Config authConfig = new Config();
    authConfig.setAccessKeyId(aliyunProperties.accessKeyId());
    authConfig.setAccessKeySecret(aliyunProperties.accessKeySecret());
    authConfig.setType(DLF_AUTH_TYPE);
    authConfig.setEndpoint(aliyunProperties.dlfEndpoint());
    authConfig.setRegionId(aliyunProperties.dlfRegionId());

    try {
      return new Client(authConfig);
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize the DLF client", e);
    }
  }

  @Override
  public void initialize(Map<String, String> properties) {
    this.aliyunProperties = new AliyunProperties(properties);
  }

  @Override
  public AliyunProperties aliyunProperties() {
    return aliyunProperties;
  }
}
