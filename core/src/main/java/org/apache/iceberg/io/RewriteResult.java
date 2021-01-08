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

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class RewriteResult implements Serializable {
  private DataFile[] dataFilesToDelete;
  private DataFile[] dataFilesToAdd;

  private DeleteFile[] deleteFilesToDelete;
  private DeleteFile[] deleteFilesToAdd;

  private RewriteResult(Set<DataFile> dataFilesToDelete,
                        Set<DataFile> dataFilesToAdd,
                        Set<DeleteFile> deleteFilesToDelete,
                        Set<DeleteFile> deleteFilesToAdd) {
    this.dataFilesToDelete = dataFilesToDelete.toArray(new DataFile[0]);
    this.dataFilesToAdd = dataFilesToAdd.toArray(new DataFile[0]);

    this.deleteFilesToDelete = deleteFilesToDelete.toArray(new DeleteFile[0]);
    this.deleteFilesToAdd = deleteFilesToAdd.toArray(new DeleteFile[0]);
  }

  public DataFile[] dataFilesToDelete() {
    return dataFilesToDelete;
  }

  public DataFile[] dataFilesToAdd() {
    return dataFilesToAdd;
  }

  public DeleteFile[] deleteFilesToDelete() {
    return deleteFilesToDelete;
  }

  public DeleteFile[] deleteFilesToAdd() {
    return deleteFilesToAdd;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final Set<DataFile> dataFilesToDelete;
    private final Set<DataFile> dataFilesToAdd;
    private final Set<DeleteFile> deleteFilesToDelete;
    private final Set<DeleteFile> deleteFilesToAdd;

    private Builder() {
      this.dataFilesToDelete = Sets.newHashSet();
      this.dataFilesToAdd = Sets.newHashSet();
      this.deleteFilesToDelete = Sets.newHashSet();
      this.deleteFilesToAdd = Sets.newHashSet();
    }

    public Builder add(RewriteResult result) {
      addDataFilesToDelete(result.dataFilesToDelete);
      addDataFilesToAdd(result.dataFilesToAdd);
      addDeleteFilesToDelete(result.deleteFilesToDelete);
      addDeleteFilesToAdd(result.deleteFilesToAdd);

      return this;
    }

    public Builder addAll(Iterable<RewriteResult> results) {
      results.forEach(this::add);
      return this;
    }

    public Builder addDataFilesToDelete(Iterable<DataFile> files) {
      Iterables.addAll(dataFilesToDelete, files);
      return this;
    }

    public Builder addDataFilesToDelete(DataFile... files) {
      Collections.addAll(dataFilesToDelete, files);
      return this;
    }

    public Builder addDataFilesToAdd(Iterable<DataFile> files) {
      Iterables.addAll(dataFilesToAdd, files);
      return this;
    }

    public Builder addDataFilesToAdd(DataFile... files) {
      Collections.addAll(dataFilesToAdd, files);
      return this;
    }

    public Builder addDeleteFilesToDelete(Iterable<DeleteFile> files) {
      Iterables.addAll(deleteFilesToDelete, files);
      return this;
    }

    public Builder addDeleteFilesToDelete(DeleteFile... files) {
      Collections.addAll(deleteFilesToDelete, files);
      return this;
    }

    public Builder addDeleteFilesToAdd(Iterable<DeleteFile> files) {
      Iterables.addAll(deleteFilesToAdd, files);
      return this;
    }

    public Builder addDeleteFilesToAdd(DeleteFile... files) {
      Collections.addAll(deleteFilesToAdd, files);
      return this;
    }

    public RewriteResult build() {
      return new RewriteResult(dataFilesToDelete, dataFilesToAdd, deleteFilesToDelete, deleteFilesToAdd);
    }
  }
}
