/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.sources.helpers;

import static org.apache.hudi.DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL;
import static org.apache.hudi.utilities.sources.HoodieIncrSource.Config.HOODIE_SRC_BASE_PATH;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.sources.SnapshotLoadQuerySplitter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestQueryRunner {

  private QueryRunner queryRunnerSpy;
  private SparkSession sparkSession;
  private Dataset<Row> snapshot;
  private String sourcePath = "path/to/source";

  @BeforeEach
  public void setup() {
    sparkSession = mock(SparkSession.class, RETURNS_DEEP_STUBS);
    snapshot = createEmptyDataset();
    when(sparkSession.read().format("org.apache.hudi")
        .option(anyString(), anyString())
        .load(eq(sourcePath))).thenReturn(snapshot);

    TypedProperties properties = new TypedProperties();
    properties.put(HOODIE_SRC_BASE_PATH, sourcePath);
    queryRunnerSpy = spy(new QueryRunner(sparkSession, properties));
  }

  @Test
  public void testRunSnapshotQueryWithSnapshotLoadQuerySplitter() {
    QueryInfo queryInfo = new QueryInfo(
        QUERY_TYPE_SNAPSHOT_OPT_VAL(), "commit0", "commit3",
        "commit10", "_hoodie_commit_time",
        "s3.object.key", "s3.object.size");
    QueryInfo snapshotQueryInfo = new QueryInfo(
        QUERY_TYPE_SNAPSHOT_OPT_VAL(), "commit1", "commit3",
        "commit10", "_hoodie_commit_time",
        "s3.object.key", "s3.object.size");
    SnapshotLoadQuerySplitter splitter = mock(SnapshotLoadQuerySplitter.class);
    when(splitter.getNextCheckpoint(eq(snapshot), eq(queryInfo))).thenReturn(snapshotQueryInfo);
    queryRunnerSpy.run(queryInfo, Option.of(splitter));
    verify(queryRunnerSpy).applySnapshotQueryFilters(eq(snapshot), eq(snapshotQueryInfo));

    queryRunnerSpy.run(queryInfo, Option.empty());
    verify(queryRunnerSpy).applySnapshotQueryFilters(eq(snapshot), eq(queryInfo));
  }

  private Dataset<Row> createEmptyDataset() {
    List<StructField> fields = new ArrayList<>();
    fields.add(DataTypes.createStructField(HoodieRecord.COMMIT_TIME_METADATA_FIELD, DataTypes.StringType, true));
    StructType schema = DataTypes.createStructType(fields);
    return sparkSession.createDataFrame(Collections.emptyList(), schema);
  }
}
