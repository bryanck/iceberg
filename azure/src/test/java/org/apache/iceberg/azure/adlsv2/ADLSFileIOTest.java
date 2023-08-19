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
package org.apache.iceberg.azure.adlsv2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.Response;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.PathItem;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Iterator;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class ADLSFileIOTest {
  @Test
  public void testFileOperations() {
    String location = "abfs://container@account.dfs.core.windows.net/path/to/file";

    DataLakeFileClient fileClient = mock(DataLakeFileClient.class);

    DataLakeFileSystemClient client = mock(DataLakeFileSystemClient.class);
    doReturn(fileClient).when(client).getFileClient(any());

    ADLSFileIO io = spy(new ADLSFileIO());
    io.initialize(ImmutableMap.of());
    doReturn(client).when(io).client(any(ADLSLocation.class));

    InputFile in = io.newInputFile(location);
    verify(fileClient, times(0)).openInputStream(any());

    io.newOutputFile(location);
    verify(fileClient, times(0)).getOutputStream(any());

    io.deleteFile(in);
    verify(fileClient).delete();
  }

  @Test
  public void testListPrefixOperations() {
    String prefix = "abfs://container@account.dfs.core.windows.net/dir";

    OffsetDateTime now = OffsetDateTime.now();
    PathItem dir =
        new PathItem("tag", now, 0L, "group", true, "dir", "owner", "permissions", now, null);
    PathItem file =
        new PathItem(
            "tag", now, 123L, "group", false, "dir/file", "owner", "permissions", now, null);

    PagedIterable<PathItem> response = mock(PagedIterable.class);
    when(response.stream()).thenReturn(ImmutableList.of(dir, file).stream());

    DataLakeFileSystemClient client = mock(DataLakeFileSystemClient.class);
    when(client.listPaths(any(), any())).thenReturn(response);

    ADLSFileIO io = spy(new ADLSFileIO());
    io.initialize(ImmutableMap.of());
    doReturn(client).when(io).client(any(ADLSLocation.class));

    Iterator<FileInfo> result = io.listPrefix(prefix).iterator();

    verify(client).listPaths(any(), any());

    // assert that only files were returned and not directories
    FileInfo fileInfo = result.next();
    assertThat(fileInfo.location()).isEqualTo("dir/file");
    assertThat(fileInfo.size()).isEqualTo(123L);
    assertThat(fileInfo.createdAtMillis()).isEqualTo(now.toInstant().toEpochMilli());

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  public void testDeletePrefixOperations() {
    String prefix = "abfs://container@account.dfs.core.windows.net/dir";

    Response<Void> response = mock(Response.class);

    DataLakeFileSystemClient client = mock(DataLakeFileSystemClient.class);
    when(client.deleteDirectoryWithResponse(any(), anyBoolean(), any(), any(), any()))
        .thenReturn(response);

    ADLSFileIO io = spy(new ADLSFileIO());
    io.initialize(ImmutableMap.of());
    doReturn(client).when(io).client(any(ADLSLocation.class));

    io.deletePrefix(prefix);

    // assert that recursive delete was called for the directory
    verify(client).deleteDirectoryWithResponse(eq("dir"), eq(true), any(), any(), any());
  }

  @Test
  public void testBulkDeleteFiles() {
    String location1 = "abfs://container@account.dfs.core.windows.net/path/to/file1";
    String location2 = "abfs://container@account.dfs.core.windows.net/path/to/file2";

    DataLakeFileClient fileClient = mock(DataLakeFileClient.class);

    DataLakeFileSystemClient client = mock(DataLakeFileSystemClient.class);
    doReturn(fileClient).when(client).getFileClient(any());

    ADLSFileIO io = spy(new ADLSFileIO());
    doReturn(client).when(io).client(any(ADLSLocation.class));
    io.initialize(ImmutableMap.of());

    io.deleteFiles(ImmutableList.of(location1, location2));
    verify(io, times(2)).deleteFile(anyString());
  }

  @Test
  public void testGetClient() {
    String location = "abfs://container@account.dfs.core.windows.net/path/to/file";

    DataLakeFileSystemClient client = mock(DataLakeFileSystemClient.class);

    ADLSFileIO io = spy(new ADLSFileIO());
    io.initialize(ImmutableMap.of());
    doReturn(client).when(io).client(any(ADLSLocation.class));

    io.client(location);
    verify(io).client(any(ADLSLocation.class));
  }

  @Test
  public void testKryoSerialization() throws IOException {
    FileIO testFileIO = new ADLSFileIO();

    // gcs fileIO should be serializable when properties are passed as immutable map
    testFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.KryoHelpers.roundTripSerialize(testFileIO);

    assertThat(testFileIO.properties()).isEqualTo(roundTripSerializedFileIO.properties());
  }

  @Test
  public void testJavaSerialization() throws IOException, ClassNotFoundException {
    FileIO testFileIO = new ADLSFileIO();

    // gcs fileIO should be serializable when properties are passed as immutable map
    testFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.roundTripSerialize(testFileIO);

    assertThat(testFileIO.properties()).isEqualTo(roundTripSerializedFileIO.properties());
  }
}
