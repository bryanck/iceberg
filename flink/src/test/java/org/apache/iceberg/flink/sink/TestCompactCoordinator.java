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

package org.apache.iceberg.flink.sink;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestTableLoader;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.flink.sink.ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION;

@RunWith(Parameterized.class)
public class TestCompactCoordinator extends TableTestBase {
  private String tablePath;
  private File flinkManifestFolder;

  private final FileFormat format;

  @Parameterized.Parameters(name = "FileFormat = {0}, FormatVersion={1}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {"avro", 1},
        new Object[] {"avro", 2},
        new Object[] {"parquet", 1},
        new Object[] {"parquet", 2},
        new Object[] {"orc", 1},
    };
  }

  public TestCompactCoordinator(String format, int formatVersion) {
    super(formatVersion);
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
  }

  @Before
  public void setupTable() throws IOException {
    flinkManifestFolder = temp.newFolder();

    this.tableDir = temp.newFolder();
    this.metadataDir = new File(tableDir, "metadata");
    Assert.assertTrue(tableDir.delete());

    tablePath = tableDir.getAbsolutePath();

    // Construct the iceberg table.
    table = create(SimpleDataUtil.SCHEMA, PartitionSpec.unpartitioned());

    table.updateProperties()
        .set(DEFAULT_FILE_FORMAT, format.name())
        .set(FLINK_MANIFEST_LOCATION, flinkManifestFolder.getAbsolutePath())
        .commit();
  }

  @Test
  public void testRestoreState() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;

    JobID jobID = new JobID();
    DataFile dataFile1;
    OperatorSubtaskState state;
    try (OneInputStreamOperatorTestHarness<WriteResult, CombinedScanTask> harness = createOperator(jobID)) {
      harness.setup();
      harness.open();

      // Process the 1th element.
      RowData row1 = SimpleDataUtil.createRowData(1, "AAAA");
      dataFile1 = writeDataFile("data-1", ImmutableList.of(row1));
      harness.processElement(of(dataFile1), ++timestamp);

      // Snapshot checkpoint#1.
      state = harness.snapshot(++checkpointId, ++timestamp);

      assertFlinkManifests(1);
      assertOutputFiles(harness, ImmutableList.of());
    }

    try (OneInputStreamOperatorTestHarness<WriteResult, CombinedScanTask> harness = createOperator(jobID)) {
      harness.setup();
      harness.initializeState(state);
      harness.open();

      assertFlinkManifests(1);
      assertOutputFiles(harness, ImmutableList.of(dataFile1));

      // Process the 2th element.
      RowData row2 = SimpleDataUtil.createRowData(2, "BBBB");
      DataFile dataFile2 = writeDataFile("data-2", ImmutableList.of(row2));
      harness.processElement(of(dataFile2), ++timestamp);

      // Snapshot checkpoint#2
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(2);
      assertOutputFiles(harness, ImmutableList.of(dataFile1));

      // Complete checkpoint#2
      harness.notifyOfCompletedCheckpoint(checkpointId);

      Assert.assertFalse(new File(dataFile1.path().toString()).exists());
      Assert.assertTrue(new File(dataFile2.path().toString()).exists());
      assertFlinkManifests(1);
      assertOutputFiles(harness, ImmutableList.of(dataFile1, dataFile2));

      // Snapshot checkpoint#3
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(1);

      // Complete checkpoint#3
      harness.notifyOfCompletedCheckpoint(checkpointId);

      Assert.assertFalse(new File(dataFile1.path().toString()).exists());
      Assert.assertFalse(new File(dataFile2.path().toString()).exists());
      assertFlinkManifests(0);
      assertOutputFiles(harness, ImmutableList.of(dataFile1, dataFile2));
    }
  }

  @Test
  public void testPlanTasks() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;

    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<WriteResult, CombinedScanTask> harness = createOperator(jobID)) {
      harness.setup();
      harness.open();

      RowData row1 = SimpleDataUtil.createRowData(1, "AAAA");
      DataFile dataFile1 = writeDataFile("data-1", ImmutableList.of(row1));

      harness.processElement(of(dataFile1), ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);

      assertOutputFiles(harness, ImmutableList.of(dataFile1));
    }
  }

  private void assertOutputFiles(OneInputStreamOperatorTestHarness<WriteResult, CombinedScanTask> harness,
                                 List<DataFile> expectedFiles) {
    List<CombinedScanTask> tasks = harness.extractOutputValues();
    Assert.assertEquals(expectedFiles.size(), harness.extractOutputValues().size());

    for (int i = 0; i < tasks.size(); i++) {
      CombinedScanTask task = tasks.get(i);
      DataFile expectedFile = expectedFiles.get(i);

      List<FileScanTask> scanTasks = Lists.newArrayList(task.files());
      Assert.assertEquals(1, scanTasks.size());
      FileScanTask scanTask = scanTasks.get(0);

      Assert.assertNotNull(scanTask);
      Assert.assertEquals(expectedFile.path(), scanTask.file().path());
    }
  }

  private List<Path> assertFlinkManifests(int expectedCount) throws IOException {
    List<Path> manifests = Files.list(flinkManifestFolder.toPath())
        .filter(p -> !p.toString().endsWith(".crc"))
        .filter(p -> p.toString().contains("-compactor"))
        .collect(Collectors.toList());
    Assert.assertEquals(String.format("Expected %s flink manifests, but the list is: %s", expectedCount, manifests),
        expectedCount, manifests.size());
    return manifests;
  }

  private DataFile writeDataFile(String filename, List<RowData> rows) throws IOException {
    return SimpleDataUtil.writeFile(table.schema(), table.spec(), tablePath, format.addExtension(filename), rows);
  }

  private static WriteResult of(DataFile dataFile) {
    return WriteResult.builder().addDataFiles(dataFile).build();
  }

  private OneInputStreamOperatorTestHarness<WriteResult, CombinedScanTask> createOperator(JobID jobID)
      throws Exception {
    TestOperatorFactory factory = TestOperatorFactory.of(tablePath);
    return new OneInputStreamOperatorTestHarness<>(factory, createEnvironment(jobID));
  }

  private static MockEnvironment createEnvironment(JobID jobID) {
    return new MockEnvironmentBuilder()
        .setTaskName("test task")
        .setManagedMemorySize(32 * 1024)
        .setInputSplitProvider(new MockInputSplitProvider())
        .setBufferSize(256)
        .setTaskConfiguration(new org.apache.flink.configuration.Configuration())
        .setExecutionConfig(new ExecutionConfig())
        .setMaxParallelism(16)
        .setJobID(jobID)
        .build();
  }

  private static class TestOperatorFactory extends AbstractStreamOperatorFactory<CombinedScanTask>
      implements OneInputStreamOperatorFactory<WriteResult, CombinedScanTask> {
    private final String tablePath;

    private TestOperatorFactory(String tablePath) {
      this.tablePath = tablePath;
    }

    private static TestOperatorFactory of(String tablePath) {
      return new TestOperatorFactory(tablePath);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<CombinedScanTask>> T createStreamOperator(
        StreamOperatorParameters<CombinedScanTask> param) {
      CompactCoordinator coordinator = new CompactCoordinator(TestTableLoader.of(tablePath));
      coordinator.setup(param.getContainingTask(), param.getStreamConfig(), param.getOutput());
      return (T) coordinator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return CompactCoordinator.class;
    }
  }
}
