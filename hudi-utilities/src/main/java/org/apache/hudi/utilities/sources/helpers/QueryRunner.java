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

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import org.apache.hudi.utilities.sources.SnapshotLoadQuerySplitter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Collections;
import java.util.List;

import static org.apache.hudi.utilities.sources.HoodieIncrSource.Config.HOODIE_SRC_BASE_PATH;

/**
 * This class is currently used only by s3 and gcs incr sources that supports size based batching
 * This class will fetch comitted files from the current commit to support size based batching.
 */
public class QueryRunner {
  private final SparkSession sparkSession;
  private final TypedProperties props;
  private final String sourcePath;

  private static final Logger LOG = LogManager.getLogger(QueryRunner.class);

  public QueryRunner(SparkSession sparkSession, TypedProperties props) {
    this.sparkSession = sparkSession;
    this.props = props;
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(HOODIE_SRC_BASE_PATH));
    this.sourcePath = props.getString(HOODIE_SRC_BASE_PATH);
  }

  public Dataset<Row> run(QueryInfo queryInfo, Option<SnapshotLoadQuerySplitter> snapshotLoadQuerySplitterOption) {
    Dataset<Row> dataset = null;
    if (queryInfo.isIncremental()) {
      dataset = runIncrementalQuery(queryInfo);
    } else if (queryInfo.isSnapshot()) {
      dataset = runSnapshotQuery(queryInfo, snapshotLoadQuerySplitterOption);
    } else {
      throw new HoodieException("Unknown query type " + queryInfo.getQueryType());
    }
    return dataset;
  }

  public static Dataset<Row> applyOrdering(Dataset<Row> dataset, List<String> orderByColumns) {
    if (orderByColumns != null && !orderByColumns.isEmpty()) {
      LOG.debug("Applying ordering " + orderByColumns);
      return dataset.orderBy(orderByColumns.stream().map(functions::col).toArray(Column[]::new));
    }
    return dataset;
  }

  public Dataset<Row> runIncrementalQuery(QueryInfo queryInfo) {
    LOG.info("Running incremental query");
    return sparkSession.read().format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE().key(), queryInfo.getQueryType())
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME().key(), queryInfo.getPreviousInstant())
        .option(DataSourceReadOptions.END_INSTANTTIME().key(), queryInfo.getEndInstant()).load(sourcePath);
  }

  public Dataset<Row> runSnapshotQuery(QueryInfo queryInfo, Option<SnapshotLoadQuerySplitter> snapshotLoadQuerySplitterOption) {
    LOG.info("Running snapshot query");
    Dataset<Row> snapshot = sparkSession.read().format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE().key(), queryInfo.getQueryType()).load(sourcePath);
    QueryInfo snapshotQueryInfo = snapshotLoadQuerySplitterOption
        .map(snapshotLoadQuerySplitter -> snapshotLoadQuerySplitter.getNextCheckpoint(snapshot, queryInfo))
        .orElse(queryInfo);
    return applySnapshotQueryFilters(snapshot, snapshotQueryInfo);
  }

  public Dataset<Row> applySnapshotQueryFilters(Dataset<Row> snapshot, QueryInfo snapshotQueryInfo) {
    return snapshot
        // add filtering so that only interested records are returned.
        .filter(String.format("%s >= '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD,
            snapshotQueryInfo.getStartInstant()))
        .filter(String.format("%s <= '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD,
            snapshotQueryInfo.getEndInstant()));
  }
}
