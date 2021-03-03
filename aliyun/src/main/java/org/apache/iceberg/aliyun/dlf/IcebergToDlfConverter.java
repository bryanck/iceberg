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

import com.aliyun.datalake20200710.models.DatabaseInput;
import com.aliyun.datalake20200710.models.FieldSchema;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

class IcebergToDlfConverter {

  private static final String DLF_PATTERN_STR = "^[a-zA-Z][a-zA-Z0-9_]{0,127}$";
  private static final Pattern DLF_DB_PATTERN = Pattern.compile(DLF_PATTERN_STR);
  private static final Pattern DLF_TABLE_PATTERN = Pattern.compile(DLF_PATTERN_STR);

  private IcebergToDlfConverter() {
  }

  static boolean isValidNamespace(Namespace namespace) {
    if (namespace.levels().length != 1) {
      return false;
    }
    String dbName = namespace.level(0);
    return dbName != null && DLF_DB_PATTERN.matcher(dbName).find();
  }

  static void validateNamespace(Namespace namespace) {
    ValidationException.check(isValidNamespace(namespace), "Cannot convert namespace %s to DLF database name, " +
        "because it must be matched '%s'", namespace, DLF_PATTERN_STR);
  }

  /**
   * Validate and get DLF database name from iceberg {@link TableIdentifier}.
   *
   * @param tableIdentifier table identifier
   * @return table name.
   */
  static String getDatabaseName(TableIdentifier tableIdentifier) {
    return toDatabaseName(tableIdentifier.namespace());
  }

  /**
   * Validate and convert Iceberg namespace to DLF database name
   *
   * @param namespace Iceberg namespace
   * @return database name
   */
  static String toDatabaseName(Namespace namespace) {
    validateNamespace(namespace);
    return namespace.level(0);
  }

  static boolean isValidTableName(String tableName) {
    return tableName != null && DLF_TABLE_PATTERN.matcher(tableName).find();
  }

  static void validateTableName(String tableName) {
    ValidationException.check(isValidTableName(tableName), "Cannot use %s as DLF table name, " +
        "because it must be matched '%s'", tableName, DLF_PATTERN_STR);
  }

  /**
   * Validate and get DLF table name from Iceberg TableIdentifier
   *
   * @param tableIdentifier table identifier
   * @return table name
   */
  static String getTableName(TableIdentifier tableIdentifier) {
    validateTableName(tableIdentifier.name());
    return tableIdentifier.name();
  }

  static DatabaseInput toDatabaseInput(Namespace namespace, Map<String, String> metadata) {
    return new DatabaseInput()
        .setName(toDatabaseName(namespace))
        .setParameters(metadata);
  }

  static List<FieldSchema> toFieldSchemaList(Schema schema) {
    List<FieldSchema> fields = Lists.newArrayList();
    for (Types.NestedField field : schema.asStruct().fields()) {
      fields.add(IcebergToDlfConverter.toFieldSchema(field));
    }
    return fields;
  }

  static List<FieldSchema> toFieldSchemaList(PartitionSpec spec) {
    Schema schema = spec.schema();
    List<FieldSchema> partitionKeys = Lists.newArrayList();
    if (spec != null && !spec.isUnpartitioned()) {
      for (PartitionField field : spec.fields()) {
        Types.NestedField nestedField = schema.findField(field.sourceId());
        Preconditions.checkNotNull(nestedField,
            "Cannot find field with field id %s in schema %s", field.sourceId(), schema);
        partitionKeys.add(IcebergToDlfConverter.toFieldSchema(nestedField));
      }
    }

    return partitionKeys;
  }

  static FieldSchema toFieldSchema(Types.NestedField field) {
    return new FieldSchema()
        .setName(field.name())
        .setType(toDlfDataType(field.type()))
        .setComment(field.doc());
  }

  /**
   * See https://help.aliyun.com/document_detail/197150.htm#FieldSchema
   */
  static String toDlfDataType(Type iType) {
    switch (iType.typeId()) {
      case BOOLEAN:
        return "boolean";
      case INTEGER:
        return "int";
      case LONG:
        return "bigint";
      case FLOAT:
        return "float";
      case DOUBLE:
        return "double";
      case DATE:
        return "date";
      case TIME:
        return "time";
      case TIMESTAMP:
        return "timestamp";
      case STRING:
        return "string";
      case UUID:
        return "uuid";
      case FIXED:
        return "char";
      case BINARY:
        return "binary";
      case DECIMAL:
        return "decimal";
      case STRUCT:
        return "struct";
      case LIST:
        return "array";
      case MAP:
        return "map";
      default:
        throw new IllegalArgumentException();
    }
  }
}
