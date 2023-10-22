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
package org.apache.iceberg.connect.data;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.PrimitiveType;

public class SchemaUpdate {

  public static class AddColumn extends SchemaUpdate {
    private final String parentName;
    private final String name;
    private final Type type;

    public AddColumn(String parentName, String name, Type type) {
      this.parentName = parentName;
      this.name = name;
      this.type = type;
    }

    public String parentName() {
      return parentName;
    }

    public String name() {
      return name;
    }

    public Type type() {
      return type;
    }
  }

  public static class UpdateType extends SchemaUpdate {
    private final String name;
    private final PrimitiveType type;

    public UpdateType(String name, PrimitiveType type) {
      this.name = name;
      this.type = type;
    }

    public String name() {
      return name;
    }

    public PrimitiveType type() {
      return type;
    }
  }

  public static class MakeOptional extends SchemaUpdate {
    private final String name;

    public MakeOptional(String name) {
      this.name = name;
    }

    public String name() {
      return name;
    }
  }
}