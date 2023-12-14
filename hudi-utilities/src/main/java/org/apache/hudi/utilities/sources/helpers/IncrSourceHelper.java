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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.sources.HoodieIncrSource;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;

import java.util.Objects;

import static org.apache.hudi.utilities.sources.HoodieIncrSource.Config.DEFAULT_READ_LATEST_INSTANT_ON_MISSING_CKPT;
import static org.apache.hudi.utilities.sources.HoodieIncrSource.Config.MISSING_CHECKPOINT_STRATEGY;
import static org.apache.hudi.utilities.sources.HoodieIncrSource.Config.READ_LATEST_INSTANT_ON_MISSING_CKPT;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class IncrSourceHelper {

  private static final Logger LOG = LogManager.getLogger(IncrSourceHelper.class);
  public static final String DEFAULT_BEGIN_TIMESTAMP = HoodieTimeline.INIT_INSTANT_TS;
  private static final String CUMULATIVE_COLUMN_NAME = "cumulativeSize";

  /**
   * Kafka reset offset strategies.
   */
  public enum MissingCheckpointStrategy {
    // read from latest commit in hoodie source table
    READ_LATEST,
    // read everything upto latest commit
    READ_UPTO_LATEST_COMMIT
  }

  /**
   * Get a timestamp which is the next value in a descending sequence.
   *
   * @param timestamp Timestamp
   */
  private static String getStrictlyLowerTimestamp(String timestamp) {
    long ts = Long.parseLong(timestamp);
    ValidationUtils.checkArgument(ts > 0, "Timestamp must be positive");
    long lower = ts - 1;
    return "" + lower;
  }

  /**
   * Find begin and end instants to be set for the next fetch.
   *
   * @param jssc                      Java Spark Context
   * @param srcBasePath               Base path of Hudi source table
   * @param numInstantsPerFetch       Max Instants per fetch
   * @param beginInstant              Last Checkpoint String
   * @param missingCheckpointStrategy when begin instant is missing, allow reading based on missing checkpoint strategy
   * @param orderColumn               Column to order by (used for size based incr source)
   * @param keyColumn                 Key column (used for size based incr source)
   * @param limitColumn               Limit column (used for size based incr source)
   * @param sourceLimitBasedBatching  When sourceLimit based batching is used, we need to fetch the current commit as well,
   *                                  this flag is used to indicate that.
   * @param lastCheckpointKey         Last checkpoint key (used in the upgrade code path)
   * @return begin and end instants along with query type and other information.
   */
  public static QueryInfo generateQueryInfo(JavaSparkContext jssc, String srcBasePath,
                                            int numInstantsPerFetch, Option<String> beginInstant,
                                            MissingCheckpointStrategy missingCheckpointStrategy,
                                            String orderColumn, String keyColumn, String limitColumn,
                                            boolean sourceLimitBasedBatching,
                                            Option<String> lastCheckpointKey) {
    ValidationUtils.checkArgument(numInstantsPerFetch > 0,
        "Make sure the config hoodie.deltastreamer.source.hoodieincr.num_instants is set to a positive value");
    HoodieTableMetaClient srcMetaClient = HoodieTableMetaClient.builder().setConf(jssc.hadoopConfiguration()).setBasePath(srcBasePath).setLoadActiveTimelineOnLoad(true).build();

    // Find the earliest incomplete commit, deltacommit, or non-clustering replacecommit,
    // so that the incremental pulls should be strictly before this instant.
    // This is to guard around multi-writer scenarios where a commit starting later than
    // another commit from a concurrent writer can finish earlier, leaving an inflight commit
    // before a completed commit.
    final Option<HoodieInstant> firstIncompleteCommit = srcMetaClient.getCommitsTimeline()
        .filterInflightsAndRequested()
        .filter(instant ->
            !HoodieTimeline.REPLACE_COMMIT_ACTION.equals(instant.getAction())
                || !ClusteringUtils.getClusteringPlan(srcMetaClient, instant).isPresent())
        .firstInstant();
    final HoodieTimeline completedCommitTimeline =
        srcMetaClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
    final HoodieTimeline activeCommitTimeline = firstIncompleteCommit.map(
        commit -> completedCommitTimeline.findInstantsBefore(commit.getTimestamp())
    ).orElse(completedCommitTimeline);

    String beginInstantTime = beginInstant.orElseGet(() -> {
      if (missingCheckpointStrategy != null) {
        if (missingCheckpointStrategy == MissingCheckpointStrategy.READ_LATEST) {
          Option<HoodieInstant> lastInstant = activeCommitTimeline.lastInstant();
          return lastInstant.map(hoodieInstant -> getStrictlyLowerTimestamp(hoodieInstant.getTimestamp())).orElse(DEFAULT_BEGIN_TIMESTAMP);
        } else {
          return DEFAULT_BEGIN_TIMESTAMP;
        }
      } else {
        throw new IllegalArgumentException("Missing begin instant for incremental pull. For reading from latest "
            + "committed instant set hoodie.deltastreamer.source.hoodieincr.missing.checkpoint.strategy to a valid value");
      }
    });

    String previousInstantTime = DEFAULT_BEGIN_TIMESTAMP;
    if (!beginInstantTime.equals(DEFAULT_BEGIN_TIMESTAMP)) {
      Option<HoodieInstant> previousInstant = activeCommitTimeline.findInstantBefore(beginInstantTime);
      if (previousInstant.isPresent()) {
        previousInstantTime = previousInstant.get().getTimestamp();
      }
    }

    if (missingCheckpointStrategy == MissingCheckpointStrategy.READ_LATEST || !activeCommitTimeline.isBeforeTimelineStarts(beginInstantTime)) {
      Option<HoodieInstant> nthInstant;
      // When we are in the upgrade code path from non-sourcelimit-based batching to sourcelimit-based batching, we need to avoid fetching the commit
      // that is read already. Else we will have duplicates in append-only use case if we use "findInstantsAfterOrEquals".
      // As soon as we have a new format of checkpoint and a key we will move to the new code of fetching the current commit as well.
      if (sourceLimitBasedBatching && lastCheckpointKey.isPresent()) {
        nthInstant = Option.fromJavaOptional(activeCommitTimeline
            .findInstantsAfterOrEquals(beginInstantTime, numInstantsPerFetch).getInstants().reduce((x, y) -> y));
      } else {
        nthInstant = Option.fromJavaOptional(activeCommitTimeline
            .findInstantsAfter(beginInstantTime, numInstantsPerFetch).getInstants().reduce((x, y) -> y));
      }
      return new QueryInfo(DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL(), previousInstantTime,
          beginInstantTime, nthInstant.map(HoodieInstant::getTimestamp).orElse(beginInstantTime),
          orderColumn, keyColumn, limitColumn);
    } else {
      // when MissingCheckpointStrategy is set to read everything until latest, trigger snapshot query.
      Option<HoodieInstant> lastInstant = activeCommitTimeline.lastInstant();
      return new QueryInfo(DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL(),
          previousInstantTime, beginInstantTime, lastInstant.get().getTimestamp(),
          orderColumn, keyColumn, limitColumn);
    }
  }

  /**
   * Adjust the source dataset to size based batch based on last checkpoint key.
   *
   * @param sourceData  Source dataset
   * @param sourceLimit Max number of bytes to be read from source
   * @param queryInfo   Query Info
   * @return end instants along with filtered rows.
   */
  public static Pair<CloudObjectIncrCheckpoint, Option<Dataset<Row>>> filterAndGenerateCheckpointBasedOnSourceLimit(Dataset<Row> sourceData,
                                                                                                                    long sourceLimit, QueryInfo queryInfo,
                                                                                                                    CloudObjectIncrCheckpoint cloudObjectIncrCheckpoint) {
    if (sourceData.isEmpty()) {
      return Pair.of(new CloudObjectIncrCheckpoint(queryInfo.getEndInstant(), null), Option.empty());
    }
    // Let's persist the dataset to avoid triggering the dag repeatedly
    sourceData.persist(StorageLevel.MEMORY_AND_DISK());
    // Set ordering in query to enable batching
    Dataset<Row> orderedDf = QueryRunner.applyOrdering(sourceData, queryInfo.getOrderByColumns());
    Option<String> lastCheckpoint = Option.of(cloudObjectIncrCheckpoint.getCommit());
    Option<String> lastCheckpointKey = Option.ofNullable(cloudObjectIncrCheckpoint.getKey());
    Option<String> concatenatedKey = lastCheckpoint.flatMap(checkpoint -> lastCheckpointKey.map(key -> checkpoint + key));

    // Filter until last checkpoint key
    if (concatenatedKey.isPresent()) {
      orderedDf = orderedDf.withColumn("commit_key",
          functions.concat(functions.col(queryInfo.getOrderColumn()), functions.col(queryInfo.getKeyColumn())));
      // Apply incremental filter
      orderedDf = orderedDf.filter(functions.col("commit_key").gt(concatenatedKey.get())).drop("commit_key");
      // We could be just at the end of the commit, so return empty
      if (orderedDf.isEmpty()) {
        LOG.info("Empty ordered source, returning endpoint:" + queryInfo.getEndInstant());
        sourceData.unpersist();
        return Pair.of(new CloudObjectIncrCheckpoint(queryInfo.getEndInstant(), lastCheckpointKey.get()), Option.empty());
      }
    }

    // Limit based on sourceLimit
    WindowSpec windowSpec = Window.orderBy(col(queryInfo.getOrderColumn()), col(queryInfo.getKeyColumn()));
    // Add the 'cumulativeSize' column with running sum of 'limitColumn'
    Dataset<Row> aggregatedData = orderedDf.withColumn(CUMULATIVE_COLUMN_NAME,
        sum(col(queryInfo.getLimitColumn())).over(windowSpec));
    Dataset<Row> collectedRows = aggregatedData.filter(col(CUMULATIVE_COLUMN_NAME).leq(sourceLimit));

    Row row = null;
    if (collectedRows.isEmpty()) {
      // If the first element itself exceeds limits then return first element
      LOG.info("First object exceeding source limit: " + sourceLimit + " bytes");
      row = aggregatedData.select(queryInfo.getOrderColumn(), queryInfo.getKeyColumn(), CUMULATIVE_COLUMN_NAME).first();
      collectedRows = aggregatedData.limit(1);
    } else {
      // Get the last row and form composite key
      row = collectedRows.select(queryInfo.getOrderColumn(), queryInfo.getKeyColumn(), CUMULATIVE_COLUMN_NAME).orderBy(
          col(queryInfo.getOrderColumn()).desc(), col(queryInfo.getKeyColumn()).desc()).first();
    }
    LOG.info("Processed batch size: " + row.get(row.fieldIndex(CUMULATIVE_COLUMN_NAME)) + " bytes");
    sourceData.unpersist();
    return Pair.of(new CloudObjectIncrCheckpoint(row.getString(0), row.getString(1)), Option.of(collectedRows));
  }

  /**
   * Validate instant time seen in the incoming row.
   *
   * @param row          Input Row
   * @param instantTime  Hoodie Instant time of the row
   * @param sinceInstant begin instant of the batch
   * @param endInstant   end instant of the batch
   */
  public static void validateInstantTime(Row row, String instantTime, String sinceInstant, String endInstant) {
    Objects.requireNonNull(instantTime);
    ValidationUtils.checkArgument(HoodieTimeline.compareTimestamps(instantTime, HoodieTimeline.GREATER_THAN, sinceInstant),
        "Instant time(_hoodie_commit_time) in row (" + row + ") was : " + instantTime + "but expected to be between "
            + sinceInstant + "(excl) - " + endInstant + "(incl)");
    ValidationUtils.checkArgument(
        HoodieTimeline.compareTimestamps(instantTime, HoodieTimeline.LESSER_THAN_OR_EQUALS, endInstant),
        "Instant time(_hoodie_commit_time) in row (" + row + ") was : " + instantTime + "but expected to be between "
            + sinceInstant + "(excl) - " + endInstant + "(incl)");
  }

  /**
   * Determine the policy to choose if a checkpoint is missing (detected by the absence of a beginInstant),
   * during a run of a {@link HoodieIncrSource}.
   *
   * @param props the usual Hudi props object
   * @return
   */
  public static MissingCheckpointStrategy getMissingCheckpointStrategy(TypedProperties props) {
    boolean readLatestOnMissingCkpt = props.getBoolean(
        READ_LATEST_INSTANT_ON_MISSING_CKPT, DEFAULT_READ_LATEST_INSTANT_ON_MISSING_CKPT);

    if (readLatestOnMissingCkpt) {
      return MissingCheckpointStrategy.READ_LATEST;
    }

    if (props.containsKey(MISSING_CHECKPOINT_STRATEGY)) {
      return MissingCheckpointStrategy.valueOf(props.getString(MISSING_CHECKPOINT_STRATEGY));
    }

    return null;
  }
}
