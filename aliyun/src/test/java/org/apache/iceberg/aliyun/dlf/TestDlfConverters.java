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

import java.util.stream.IntStream;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.junit.Test;

public class TestDlfConverters {
  private static final Joiner JOIN = Joiner.on("");

  @Test
  public void testValidNamespace() {
    IcebergToDlfConverter.validateNamespace(Namespace.of("a"));
    IcebergToDlfConverter.validateNamespace(Namespace.of("z"));
    IcebergToDlfConverter.validateNamespace(Namespace.of("A"));
    IcebergToDlfConverter.validateNamespace(Namespace.of("Z"));
    IcebergToDlfConverter.validateNamespace(Namespace.of("a_z_A_Z_1_128"));

    String name128 = JOIN.join(IntStream.range(0, 128).mapToObj(i -> "a").toArray());
    IcebergToDlfConverter.validateNamespace(Namespace.of(name128));
  }

  @Test
  public void testInvalidNamespace() {
    AssertHelpers.assertThrows("Cannot pass a empty string as namespace",
        ValidationException.class,
        "Cannot convert namespace",
        () -> IcebergToDlfConverter.validateNamespace(Namespace.of("")));

    String name129 = JOIN.join(IntStream.range(0, 129).mapToObj(i -> "a").toArray());
    AssertHelpers.assertThrows("Cannot exceed 128 characters",
        ValidationException.class,
        "Cannot convert namespace",
        () -> IcebergToDlfConverter.validateNamespace(Namespace.of(name129)));

    AssertHelpers.assertThrows("Cannot start with a '_'",
        ValidationException.class,
        "Cannot convert namespace",
        () -> IcebergToDlfConverter.validateNamespace(Namespace.of("_")));

    AssertHelpers.assertThrows("Cannot start with a number for database name",
        ValidationException.class,
        "Cannot convert namespace",
        () -> IcebergToDlfConverter.validateNamespace(Namespace.of("1")));
  }

  @Test
  public void testValidTable() {
    IcebergToDlfConverter.validateTableName("a");
    IcebergToDlfConverter.validateTableName("z");
    IcebergToDlfConverter.validateTableName("A");
    IcebergToDlfConverter.validateTableName("Z");
    IcebergToDlfConverter.validateTableName("a_z_A_Z_1_128");

    String name128 = JOIN.join(IntStream.range(0, 128).mapToObj(i -> "a").toArray());
    IcebergToDlfConverter.validateTableName(name128);
  }

  @Test
  public void testInvalidTable() {
    AssertHelpers.assertThrows("Cannot pass a empty string as table name",
        ValidationException.class,
        "Cannot use",
        () -> IcebergToDlfConverter.validateTableName(""));

    String name129 = JOIN.join(IntStream.range(0, 129).mapToObj(i -> "a").toArray());
    AssertHelpers.assertThrows("Cannot exceed 128 characters",
        ValidationException.class,
        "Cannot use",
        () -> IcebergToDlfConverter.validateTableName(name129));

    AssertHelpers.assertThrows("Cannot start with a '_'",
        ValidationException.class,
        "Cannot use",
        () -> IcebergToDlfConverter.validateTableName("_"));

    AssertHelpers.assertThrows("Cannot start with a number for table name",
        ValidationException.class,
        "Cannot use",
        () -> IcebergToDlfConverter.validateTableName("1"));
  }
}
