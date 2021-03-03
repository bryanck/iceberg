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

import com.aliyun.datalake20200710.models.Database;
import com.aliyun.datalake20200710.models.Table;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;

class DlfToIcebergConverter {

  private DlfToIcebergConverter() {
  }

  static Namespace toNamespace(Database database) {
    return Namespace.of(database.getName());
  }


  static TableIdentifier toTableId(Table table) {
    return TableIdentifier.of(table.getDatabaseName(), table.getTableName());
  }

  /**
   * Check whether the DLF table is an iceberg table or not.
   *
   * @param table DLF table.
   * @return true if it's an iceberg table.
   */
  static boolean isDlfIcebergTable(Table table) {
    return table.getParameters() != null &&
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(
            table.getParameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP)
        );
  }

  /**
   * Validate the DLF table is Iceberg table by checking its parameters
   *
   * @param table    DLF table
   * @param fullName full table name for logging
   */
  static void validateTable(Table table, String fullName) {
    String tableType = table.getParameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP);
    ValidationException.check(tableType != null && tableType.equalsIgnoreCase(
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE),
        "Input DLF table is not an iceberg table: %s (type=%s)", fullName, tableType);
  }
}
