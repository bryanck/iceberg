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
package org.apache.iceberg.connect.events;

import static java.util.stream.Collectors.toList;

import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

/** Element representing a table identifier, with namespace and name. */
public class FullTableName implements Element {

  private List<String> namespace;
  private String name;
  private final Schema avroSchema;

  public static final Schema AVRO_SCHEMA =
      SchemaBuilder.builder()
          .record(FullTableName.class.getName())
          .fields()
          .name("namespace")
          .prop(AvroSchemaUtil.FIELD_ID_PROP, 1600)
          .type()
          .array()
          .items()
          .stringType()
          .noDefault()
          .name("name")
          .prop(AvroSchemaUtil.FIELD_ID_PROP, 1601)
          .type()
          .stringType()
          .noDefault()
          .endRecord();

  public static FullTableName of(TableIdentifier tableIdentifier) {
    return new FullTableName(
        Arrays.asList(tableIdentifier.namespace().levels()), tableIdentifier.name());
  }

  // Used by Avro reflection to instantiate this class when reading events
  public FullTableName(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public FullTableName(List<String> namespace, String name) {
    this.namespace = namespace;
    this.name = name;
    this.avroSchema = AVRO_SCHEMA;
  }

  public TableIdentifier toIdentifier() {
    Namespace icebergNamespace = Namespace.of(namespace.toArray(new String[0]));
    return TableIdentifier.of(icebergNamespace, name);
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.namespace =
            v == null ? null : ((List<Utf8>) v).stream().map(Utf8::toString).collect(toList());
        return;
      case 1:
        this.name = v == null ? null : v.toString();
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return namespace;
      case 1:
        return name;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
