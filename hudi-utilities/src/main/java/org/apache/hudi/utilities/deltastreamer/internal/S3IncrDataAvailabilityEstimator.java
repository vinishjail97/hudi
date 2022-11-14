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

package org.apache.hudi.utilities.deltastreamer.internal;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.sources.S3EventsHoodieIncrSource;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.utilities.sources.HoodieIncrSource.Config.HOODIE_SRC_BASE_PATH;

/**
 * Implements logic to schedule an S3 incr job based on number of pending commits to be ingested from the
 * base S3 Metadata table.
 *
 * NOTE: Eventually this class should be implemented by the {@link org.apache.hudi.utilities.sources.Source} class
 * that ingests data. To avoid changing the interface for
 */
public class S3IncrDataAvailabilityEstimator extends SourceDataAvailabilityEstimator {

  private static final Logger LOG = LogManager.getLogger(S3IncrDataAvailabilityEstimator.class);
  private static final String DEFAULT_BEGIN_TIMESTAMP = "000";

  private final Option<String> s3KeyPrefix;

  public S3IncrDataAvailabilityEstimator(JavaSparkContext jssc, TypedProperties properties) {
    super(jssc, properties);
    s3KeyPrefix = Option.ofNullable(properties.getString(S3EventsHoodieIncrSource.Config.S3_KEY_PREFIX, null));
  }

  @Override
  public Pair<SourceDataAvailabilityStatus, Long> getDataAvailabilityStatus(Option<String> lastCommittedCheckpointStr, Option<Long> averageRecordSizeInBytes, long sourceLimit) {
    S3MetadataTableInfo s3MetadataTableInfo = S3MetadataTableInfo.createOrGetInstance(jssc, properties);
    String lastCommittedCheckpoint = (lastCommittedCheckpointStr.isPresent()) ? lastCommittedCheckpointStr.get() : DEFAULT_BEGIN_TIMESTAMP;
    Pair<String, String> instantThresholds = s3MetadataTableInfo.getMinAndMaxInstantsForScheduling();
    Long aggrBytesPerIncrJob = s3MetadataTableInfo.getAggrBytesPerIncrJob(lastCommittedCheckpointStr, s3KeyPrefix);

    if (HoodieTimeline.compareTimestamps(lastCommittedCheckpoint, HoodieTimeline.LESSER_THAN_OR_EQUALS, instantThresholds.getLeft())) {
      LOG.info(String.format("Scheduling ingestion right away since lastCommittedCheckpoint %s <= lowerThresholdInstant %s", lastCommittedCheckpoint, instantThresholds.getLeft()));
      return Pair.of(SourceDataAvailabilityStatus.SCHEDULE_IMMEDIATELY, aggrBytesPerIncrJob);
    } else if (HoodieTimeline.compareTimestamps(lastCommittedCheckpoint, HoodieTimeline.LESSER_THAN, instantThresholds.getRight())) {
      LOG.info(String.format("Scheduling ingestion after min sync time since lastCommittedCheckpoint %s < upperThresholdInstant %s", lastCommittedCheckpoint, instantThresholds.getRight()));
      return Pair.of(SourceDataAvailabilityStatus.SCHEDULE_AFTER_MIN_SYNC_TIME, aggrBytesPerIncrJob);
    }
    return Pair.of(SourceDataAvailabilityStatus.SCHEDULE_DEFER, 0L);
  }

  static class S3MetadataTableInfo extends HoodieIncrSourceEstimator.HudiSourceTableInfo {
    public static final String S3_INCR_SOURCE_RATE_ESTIMATOR_KEY = HOODIE_SRC_BASE_PATH;
    private static final String S3_BUCKET_NAME_FIELD = "s3.bucket.name";
    private static final String S3_OBJECT_FIELD = "s3.object.";
    private static final String S3_KEY_FIELD = "key";
    private static final String S3_SIZE_FIELD = "size";
    // In the case of S3, we do not need to ingest in real-time, hence setting
    // a min sync interval of 5mins.
    private static final long MIN_SYNC_INTERVAL_MS = 300000;
    private static final Map<String, S3MetadataTableInfo> S3_METADATA_BASE_PATH = new HashMap<>();
    private final TypedProperties props;

    private final JavaSparkContext jssc;
    private final String s3MetadataBasePath;
    private final SparkSession sparkSession;
    private Dataset<Row> incrementalDataset;
    private long lastSyncTimeMs;

    private S3MetadataTableInfo(JavaSparkContext jssc, String s3MetadataBasePath, TypedProperties props) {
      super(jssc, s3MetadataBasePath, props);
      this.jssc = jssc;
      this.s3MetadataBasePath = s3MetadataBasePath;
      this.sparkSession = SparkSession.builder().config(jssc.getConf()).getOrCreate();
      this.props = props;
      lastSyncTimeMs = 0L;
    }

    static synchronized S3MetadataTableInfo createOrGetInstance(JavaSparkContext jssc, TypedProperties props) {
      String s3MetadataBasePath = props.getString(S3_INCR_SOURCE_RATE_ESTIMATOR_KEY);
      if (!S3_METADATA_BASE_PATH.containsKey(s3MetadataBasePath)) {
        S3MetadataTableInfo s3MetadataTableInfo = new S3MetadataTableInfo(jssc, s3MetadataBasePath, props);
        S3_METADATA_BASE_PATH.put(s3MetadataBasePath, s3MetadataTableInfo);
      }
      return S3_METADATA_BASE_PATH.get(s3MetadataBasePath);
    }

    /**
     * Query all S3 events rows from metadata table between first and latest commits in Active Timeline.
     * Only refresh the active timeline and make the incr query once in {syncIntervalMs} units of time.
     * @return All rows created between the earliest and latest commits in Active timeline.
     */
    private synchronized Dataset<Row> refreshIncrementalEventsInActiveTimeline(HoodieTimeline activeCommitTimeline) {
      if (lastSyncTimeMs > 0 && (System.currentTimeMillis() - lastSyncTimeMs) <= MIN_SYNC_INTERVAL_MS) {
        return incrementalDataset;
      }
      lastSyncTimeMs = System.currentTimeMillis();
      String beginInstantTime = (activeCommitTimeline.firstInstant().isPresent()) ? activeCommitTimeline.firstInstant().get().getTimestamp() : DEFAULT_BEGIN_TIMESTAMP;
      Option<HoodieInstant> lastInstant = activeCommitTimeline.lastInstant();

      Pair<String, String> instantEndpts = Pair.of(beginInstantTime, (lastInstant.isPresent()) ? lastInstant.get().getTimestamp() : DEFAULT_BEGIN_TIMESTAMP);

      incrementalDataset = sparkSession.read().format("org.apache.hudi")
          .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
          .option(DataSourceReadOptions.BEGIN_INSTANTTIME().key(), instantEndpts.getLeft())
          .option(DataSourceReadOptions.END_INSTANTTIME().key(), instantEndpts.getRight())
          .load(s3MetadataBasePath)
          .filter(S3_OBJECT_FIELD + S3_SIZE_FIELD + " > 0")
          .select(HoodieRecord.COMMIT_TIME_METADATA_FIELD, S3_BUCKET_NAME_FIELD, S3_OBJECT_FIELD + S3_KEY_FIELD, S3_OBJECT_FIELD + S3_SIZE_FIELD)
          .distinct();

      LOG.info(String.format("Refreshed the incr s3 metadata %s source rate, took %s ms to get all data from instant %s with source size %s",
          s3MetadataBasePath,
          (System.currentTimeMillis() - lastSyncTimeMs),
          instantEndpts.getLeft(),
          incrementalDataset.count()));

      return incrementalDataset;
    }

    /**
     * Run a one-time snapshot query on-demand to query all rows created at or after the given commit instant upto latest commit.
     * @param commit The given commit instant.
     * @return All rows created at or after the given commit instant upto latest commit.
     */
    private Dataset<Row> runSnapshotQuery(String commit) {
      long snapshotStarTimeMs = System.currentTimeMillis();
      Dataset<Row> source = sparkSession.read().format("org.apache.hudi")
          .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL()).load(s3MetadataBasePath)
          .filter(String.format("%s > '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD, commit))
          .filter(S3_OBJECT_FIELD + S3_SIZE_FIELD + " > 0")
          .select(HoodieRecord.COMMIT_TIME_METADATA_FIELD, S3_BUCKET_NAME_FIELD, S3_OBJECT_FIELD + S3_KEY_FIELD, S3_OBJECT_FIELD + S3_SIZE_FIELD)
          .distinct();

      LOG.info(String.format("Made a one-time snapshot query for the s3 metadata %s source rate, took %s ms to get all data from instant %s with source size %s",
          s3MetadataBasePath,
          (System.currentTimeMillis() - snapshotStarTimeMs),
          commit,
          source.count()));

      return source;
    }

    private Dataset<Row> getEventsRows(String startCommitInstant) {
      // Currently, we assume strategy is set to READ_UPTO_LATEST_COMMIT,
      // if checkpoint is missing from source table we have to issue snapshot query
      HoodieTableMetaClient srcMetaClient = HoodieTableMetaClient.builder().setConf(jssc.hadoopConfiguration()).setBasePath(s3MetadataBasePath).setLoadActiveTimelineOnLoad(true).build();
      HoodieTimeline activeCommitTimeline = srcMetaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants();

      if (activeCommitTimeline.isBeforeTimelineStarts(startCommitInstant)) {
        return runSnapshotQuery(startCommitInstant);
      } else {
        return refreshIncrementalEventsInActiveTimeline(activeCommitTimeline)
            .filter(String.format("%s > '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD, startCommitInstant));
      }
    }

    Long getAggrBytesPerIncrJob(Option<String> lastCommittedInstant, Option<String> keyPrefix) {
      long aggrBytes = 0L;
      String startCommitInstant = (lastCommittedInstant.isPresent()) ? lastCommittedInstant.get() : DEFAULT_BEGIN_TIMESTAMP;
      if (keyPrefix.isPresent()) {
        Row aggregateRow = getEventsRows(startCommitInstant)
            .filter(S3_KEY_FIELD + " like '" + keyPrefix.get() + "%'")
            .agg(org.apache.spark.sql.functions.sum(S3_SIZE_FIELD).cast("long")).first();

        if (!aggregateRow.isNullAt(0)) {
          aggrBytes = aggregateRow.getLong(0);
        }
      }

      LOG.info(String.format("Computed the total data waiting for ingest for the S3 metadata basepath: %s with prefix %s is %s bytes with starting commit (last committed commit) %s",
          s3MetadataBasePath,
          keyPrefix,
          aggrBytes,
          startCommitInstant));
      return aggrBytes;
    }

    public String getS3MetadataBasePath() {
      return s3MetadataBasePath;
    }
  }
}
