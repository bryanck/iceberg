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
package org.apache.iceberg.io;

public class FileInfo {

  public enum Type {
    FILE,
    DIRECTORY,
    OTHER
  }

  private final String location;
  private final long size;
  private final long createdAtMillis;
  private final Type type;

  /** @deprecated will be removed in 1.5.0, the type should always be specified */
  @Deprecated
  public FileInfo(String location, long size, long createdAtMillis) {
    this(location, size, createdAtMillis, Type.FILE);
  }

  public FileInfo(String location, long size, long createdAtMillis, Type type) {
    this.location = location;
    this.size = size;
    this.createdAtMillis = createdAtMillis;
    this.type = type;
  }

  public String location() {
    return location;
  }

  public long size() {
    return size;
  }

  public long createdAtMillis() {
    return createdAtMillis;
  }

  public Type type() {
    return type;
  }
}
