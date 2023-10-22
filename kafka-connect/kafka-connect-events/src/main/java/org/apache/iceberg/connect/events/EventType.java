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

/** Control event types. */
public enum EventType {
  /** Maps to payload of type {@link CommitRequestPayload} */
  COMMIT_REQUEST(0),
  /** Maps to payload of type {@link CommitResponsePayload} */
  COMMIT_RESPONSE(1),
  /** Maps to payload of type {@link CommitReadyPayload} */
  COMMIT_READY(2),
  /** Maps to payload of type {@link CommitTablePayload} */
  COMMIT_TABLE(3),
  /** Maps to payload of type {@link CommitCompletePayload} */
  COMMIT_COMPLETE(4);

  private final int id;

  EventType(int id) {
    this.id = id;
  }

  public int id() {
    return id;
  }
}