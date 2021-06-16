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

package org.apache.iceberg.spark.extensions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.PARQUET_VECTORIZATION_ENABLED;

public class BKMiscTests extends SparkRowLevelOperationsTestBase {
    private final String sourceName;
    private final String targetName;
    private final String partitionedTargetName;

    @Parameterized.Parameters(
            name = "catalogName = {0}, implementation = {1}, config = {2}, format = {3}, vectorized = {4}")
    public static Object[][] parameters() {
        return new Object[][]{
                {"spark_catalog", SparkSessionCatalog.class.getName(),
                        ImmutableMap.of(
                                "type", "hive",
                                "default-namespace", "default",
                                "clients", "1",
                                "parquet-enabled", "false",
                                "cache-enabled", "false" // Spark will delete tables using v1, leaving the cache out of sync
                        ),
                        "parquet",
                        false
                }
        };
    }

    public BKMiscTests(String catalogName, String implementation, Map<String, String> config,
                       String fileFormat, Boolean vectorized) {
        super(catalogName, implementation, config, fileFormat, vectorized);
        this.sourceName = tableName("source");
        this.targetName = tableName("target");
        this.partitionedTargetName = tableName("target_p");
    }

    @BeforeClass
    public static void setupSparkConf() {
        spark.conf().set("spark.sql.shuffle.partitions", "4");
    }

    protected Map<String, String> extraTableProperties() {
        return ImmutableMap.of(TableProperties.MERGE_MODE, TableProperties.MERGE_MODE_DEFAULT);
    }

    @Before
    public void createTables() {
        createAndInitUnPartitionedTable(targetName);
        createAndInitPartitionedTable(partitionedTargetName);
        createAndInitPartitionedTable(sourceName);
    }

    @After
    public void removeTables() {
        sql("DROP TABLE IF EXISTS %s", targetName);
        sql("DROP TABLE IF EXISTS %s", partitionedTargetName);
        sql("DROP TABLE IF EXISTS %s", sourceName);
    }

    @Test
    public void testConcurrentDelete() {
        // This test succeeds because the DELETE and INSERT affect different files
        concurrentDelete(targetName);
    }

    @Test
    public void testConcurrentDeletePartitioned() {
        // This test succeeds because the DELETE and INSERT affect different files
        concurrentDelete(partitionedTargetName);
    }

    private void concurrentDelete(String testTarget) {
        append(testTarget, new BKEmployee(1, "emp-id-one", 2021), new BKEmployee(6, "emp-id-6", 2021));

        try {

            System.out.println("************ START");
            spark.read().format("iceberg").load(testTarget + ".snapshots").show(100, false);
            spark.read().format("iceberg").load(testTarget).show(100, false);

            Future<Void> f1 = CompletableFuture.runAsync(() -> sql("insert into " + testTarget + " values (9,'emp-id-9',2022)"));
            Future<Void> f2 = CompletableFuture.runAsync(() -> sql("delete from " + testTarget + " where id=1"));

            f1.get();
            f2.get();

            spark.read().format("iceberg").load(testTarget + ".snapshots").show(100, false);
            spark.read().format("iceberg").load(testTarget).show(100, false);
            System.out.println("************ END");

        } catch (Exception x) {
            x.printStackTrace();
        }

        assertEquals("Should have expected rows",
                ImmutableList.of(row(6, "emp-id-6", 2021), row(9, "emp-id-9", 2022)),
                sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", testTarget));
    }

    @Test
    public void testConcurrentMerge() {
        // This test fails because MERGE INTO detects a conflict and throws an exception
        concurrentMerge(targetName);
    }

    @Test
    public void testConcurrentMergePartitioned() {
        // This test fails because MERGE INTO detects a conflict and throws an exception
        // TODO: merging into separate partition but still fails??
        concurrentMerge(partitionedTargetName);
    }

    private void concurrentMerge(String testTarget) {

        append(testTarget, new BKEmployee(1, "emp-id-one", 2021), new BKEmployee(6, "emp-id-6", 2021));
        append(sourceName, new BKEmployee(2, "emp-id-2", 2021), new BKEmployee(1, "emp-id-1", 2021), new BKEmployee(6, "emp-id-6", 2021));

        String sqlText = "MERGE INTO %s AS target " +
                "USING (select * from %s where year = 2021) AS source " +
                "ON target.id = source.id " +
                "WHEN MATCHED AND target.id = 1 THEN UPDATE SET * " +
                "WHEN MATCHED AND target.id = 6 THEN DELETE " +
                "WHEN NOT MATCHED AND source.id = 2 THEN INSERT * ";

        try {

            System.out.println("************ START");
            spark.read().format("iceberg").load(testTarget + ".snapshots").show(100, false);
            spark.read().format("iceberg").load(testTarget).show(100, false);

            Future<Void> f1 = CompletableFuture.runAsync(() -> sql("insert into " + testTarget + " values (9,'emp-id-9',2022)"));
            Future<Void> f2 = CompletableFuture.runAsync(() -> sql(sqlText, testTarget, sourceName));

            f1.get();
            f2.get();

            spark.read().format("iceberg").load(testTarget + ".snapshots").show(100, false);
            spark.read().format("iceberg").load(testTarget).show(100, false);
            System.out.println("************ END");

        } catch (Exception x) {
            x.printStackTrace();
        }

        assertEquals("Should have expected rows",
                ImmutableList.of(row(1, "emp-id-1", 2021), row(2, "emp-id-2", 2021), row(9, "emp-id-9", 2022)),
                sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", testTarget));
    }

    @Test
    public void testConcurrentOverwrite() {
        // This test fails because INSERT INTO is overwritten by the INSERT OVERWRITE operation
        concurrentOverwrite(targetName);
    }

    @Test
    public void testConcurrentOverwritePartitioned() {
        // This test succeeds because INSERT INTO is a separate partition
        concurrentOverwrite(partitionedTargetName);
    }

    private void concurrentOverwrite(String testTarget) {
        append(testTarget, new BKEmployee(1, "emp-id-one", 2021), new BKEmployee(6, "emp-id-6", 2021));
        append(sourceName, new BKEmployee(2, "emp-id-2", 2021), new BKEmployee(1, "emp-id-1", 2021), new BKEmployee(6, "emp-id-6", 2021));

        String sqlText = "INSERT OVERWRITE %s select * from %s where year = 2021";

        try {

            System.out.println("************ START");
            spark.read().format("iceberg").load(testTarget + ".snapshots").show(100, false);
            spark.read().format("iceberg").load(testTarget).show(100, false);

            Future<Void> f1 = CompletableFuture.runAsync(() -> sql("insert into " + testTarget + " values (9,'emp-id-9',2022)"));
            Future<Void> f2 = CompletableFuture.runAsync(() -> sql(sqlText, testTarget, sourceName));

            f1.get();
            f2.get();

            spark.read().format("iceberg").load(testTarget + ".snapshots").show(100, false);
            spark.read().format("iceberg").load(testTarget).show(100, false);
            System.out.println("************ END");

            assertEquals("Should have expected rows",
                    ImmutableList.of(row(1, "emp-id-1", 2021), row(2, "emp-id-2", 2021), row(6, "emp-id-6", 2021), row(9, "emp-id-9", 2022)),
                    sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", testTarget));

        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    protected void createAndInitUnPartitionedTable(String tabName) {
        sql("CREATE TABLE %s (id INT, dep STRING, year INT) USING iceberg", tabName);
        initTable(tabName);
    }

    protected void createAndInitPartitionedTable(String tabName) {
        sql("CREATE TABLE %s (id INT, dep STRING, year INT) USING iceberg PARTITIONED BY (year)", tabName);
        initTable(tabName);
    }

    private void initTable(String tabName) {
        sql("ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')", tabName, DEFAULT_FILE_FORMAT, fileFormat);

        switch (fileFormat) {
            case "parquet":
                sql("ALTER TABLE %s SET TBLPROPERTIES('%s' '%b')", tabName, PARQUET_VECTORIZATION_ENABLED, vectorized);
                break;
            case "orc":
                Assert.assertTrue(vectorized);
                break;
            case "avro":
                Assert.assertFalse(vectorized);
                break;
        }

        Map<String, String> props = extraTableProperties();
        props.forEach((prop, value) -> {
            sql("ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')", tabName, prop, value);
        });
    }

    protected void append(String tabName, BKEmployee... employees) {
        try {
            List<BKEmployee> input = Arrays.asList(employees);
            Dataset<Row> inputDF = spark.createDataFrame(input, BKEmployee.class);
            inputDF.coalesce(1).writeTo(tabName).append();
        } catch (NoSuchTableException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public static class BKEmployee {
        private Integer id;
        private String dep;
        private Integer year;

        public BKEmployee() {
        }

        public BKEmployee(Integer id, String dep, Integer year) {
            this.id = id;
            this.dep = dep;
            this.year = year;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getDep() {
            return dep;
        }

        public void setDep(String dep) {
            this.dep = dep;
        }

        public Integer getYear() {
            return year;
        }

        public void setYear(Integer year) {
            this.year = year;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            } else if (other == null || getClass() != other.getClass()) {
                return false;
            }

            BKEmployee employee = (BKEmployee) other;
            return Objects.equals(id, employee.id) && Objects.equals(dep, employee.dep) && Objects.equals(year, employee.year);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, dep, year);
        }
    }
}