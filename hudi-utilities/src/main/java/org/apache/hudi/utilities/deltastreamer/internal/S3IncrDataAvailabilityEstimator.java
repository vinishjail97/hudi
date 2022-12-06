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
import org.apache.hudi.utilities.sources.HoodieIncrSource;
import org.apache.hudi.utilities.sources.S3EventsHoodieIncrSource;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;

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
public class S3IncrDataAvailabilityEstimator extends HoodieIncrSourceEstimator {

  private static final Logger LOG = LogManager.getLogger(S3IncrDataAvailabilityEstimator.class);

  private final Option<String> s3KeyPrefix;
  private final int numInstantsPerFetch;

  public S3IncrDataAvailabilityEstimator(JavaSparkContext jssc, TypedProperties properties) {
    super(jssc, properties);
    s3KeyPrefix = Option.ofNullable(properties.getString(S3EventsHoodieIncrSource.Config.S3_KEY_PREFIX, null));
    this.numInstantsPerFetch = properties.getInteger(HoodieIncrSource.Config.NUM_INSTANTS_PER_FETCH, HoodieIncrSource.Config.DEFAULT_NUM_INSTANTS_PER_FETCH);
  }

  @Override
  public IngestionStats getDataAvailabilityStatus(Option<String> lastCommittedCheckpointStr, Option<Long> averageRecordSizeInBytes, long sourceLimit) {
    S3MetadataTableInfo s3MetadataTableInfo = S3MetadataTableInfo.createOrGetInstance(jssc, properties);
    String lastCommittedCheckpoint = (lastCommittedCheckpointStr.isPresent()) ? lastCommittedCheckpointStr.get() : IncrSourceHelper.DEFAULT_BEGIN_TIMESTAMP;
    HoodieIncrSourceEstimator.HudiSourceTableInfo.TimelineStats stats = s3MetadataTableInfo.getMinAndMaxInstantsForScheduling(lastCommittedCheckpoint);
    Long aggrBytesPerIncrJob = s3MetadataTableInfo.getAggrBytesPerIncrJob(lastCommittedCheckpointStr, s3KeyPrefix);
    // Measure the approx source lag as the time taken to ingest the current data available in the source.
    Long sourceLagSecs = Long.valueOf(stats.numInstantsToIngest) / numInstantsPerFetch * minSyncTimeSecs;

    return new IngestionStats(
        getIngestionSchedulingStatus(lastCommittedCheckpoint, Pair.of(stats.minInstantToSchedule, stats.maxInstantToSchedule)), aggrBytesPerIncrJob, sourceLagSecs);

  }

  static class S3MetadataTableInfo extends HoodieIncrSourceEstimator.HudiSourceTableInfo {
    public static final String S3_INCR_SOURCE_RATE_ESTIMATOR_KEY = HOODIE_SRC_BASE_PATH;
    // In the case of S3, we do not need to ingest in real-time, hence setting
    // a min sync interval of 5mins.
    private static final long MIN_SYNC_INTERVAL_MS = 300000;
    private static final Map<String, S3MetadataTableInfo> S3_METADATA_BASE_PATH = new HashMap<>();
    private final TypedProperties props;

    private final JavaSparkContext jssc;
    private final String metadataTableBasePath;
    private final SparkSession sparkSession;
    private final FsIncrMetadataTableConfigProvider fsIncrMetadataTableConfigProvider;
    private Dataset<Row> incrementalDataset;
    private long lastSyncTimeMs;

    private S3MetadataTableInfo(JavaSparkContext jssc, String metadataTableBasePath, TypedProperties props) {
      super(jssc, metadataTableBasePath, props);
      this.jssc = jssc;
      this.metadataTableBasePath = metadataTableBasePath;
      this.sparkSession = SparkSession.builder().config(jssc.getConf()).getOrCreate();
      this.props = props;
      lastSyncTimeMs = 0L;
      this.fsIncrMetadataTableConfigProvider = FsIncrMetadataTableConfigProvider.getInstance(metadataTableBasePath);
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
      String beginInstantTime = (activeCommitTimeline.firstInstant().isPresent()) ? activeCommitTimeline.firstInstant().get().getTimestamp() : IncrSourceHelper.DEFAULT_BEGIN_TIMESTAMP;
      Option<HoodieInstant> lastInstant = activeCommitTimeline.lastInstant();

      Pair<String, String> instantEndpts = Pair.of(beginInstantTime, (lastInstant.isPresent()) ? lastInstant.get().getTimestamp() : IncrSourceHelper.DEFAULT_BEGIN_TIMESTAMP);

      incrementalDataset = sparkSession.read().format("org.apache.hudi")
          .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
          .option(DataSourceReadOptions.BEGIN_INSTANTTIME().key(), instantEndpts.getLeft())
          .option(DataSourceReadOptions.END_INSTANTTIME().key(), instantEndpts.getRight())
          .load(metadataTableBasePath)
          .filter(fsIncrMetadataTableConfigProvider.getSizeFieldName() + " > 0")
          .select(HoodieRecord.COMMIT_TIME_METADATA_FIELD, fsIncrMetadataTableConfigProvider.getBucketFieldName(),
              fsIncrMetadataTableConfigProvider.getKeyFieldName(), fsIncrMetadataTableConfigProvider.getSizeFieldName())
          .distinct();

      LOG.info(String.format("Refreshed the incr s3 metadata %s source rate, took %s ms to get all data from instant %s with source size %s",
          metadataTableBasePath,
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
          .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL()).load(metadataTableBasePath)
          .filter(String.format("%s > '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD, commit))
          .filter(fsIncrMetadataTableConfigProvider.getSizeFieldName() + " > 0")
          .select(HoodieRecord.COMMIT_TIME_METADATA_FIELD, fsIncrMetadataTableConfigProvider.getBucketFieldName(), fsIncrMetadataTableConfigProvider.getKeyFieldName(),
             fsIncrMetadataTableConfigProvider.getSizeFieldName())
          .distinct();

      LOG.info(String.format("Made a one-time snapshot query for the s3 metadata %s source rate, took %s ms to get all data from instant %s with source size %s",
          metadataTableBasePath,
          (System.currentTimeMillis() - snapshotStarTimeMs),
          commit,
          source.count()));

      return source;
    }

    private Dataset<Row> getEventsRows(String startCommitInstant) {
      // Currently, we assume strategy is set to READ_UPTO_LATEST_COMMIT,
      // if checkpoint is missing from source table we have to issue snapshot query
      HoodieTableMetaClient srcMetaClient = HoodieTableMetaClient.builder().setConf(jssc.hadoopConfiguration()).setBasePath(metadataTableBasePath).setLoadActiveTimelineOnLoad(true).build();
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
      String startCommitInstant = (lastCommittedInstant.isPresent()) ? lastCommittedInstant.get() : IncrSourceHelper.DEFAULT_BEGIN_TIMESTAMP;
      if (keyPrefix.isPresent()) {
        Row aggregateRow = getEventsRows(startCommitInstant)
            .filter(fsIncrMetadataTableConfigProvider.getAggregateKeyFieldName() + " like '" + keyPrefix.get() + "%'")
            .agg(org.apache.spark.sql.functions.sum(fsIncrMetadataTableConfigProvider.getAggregateSizeFieldName()).cast("long")).first();

        if (!aggregateRow.isNullAt(0)) {
          aggrBytes = aggregateRow.getLong(0);
        }
      }

      LOG.info(String.format("Computed the total data waiting for ingest for the S3 metadata basepath: %s with prefix %s is %s bytes with starting commit (last committed commit) %s",
          metadataTableBasePath,
          keyPrefix,
          aggrBytes,
          startCommitInstant));
      return aggrBytes;
    }

    public String getMetadataTableBasePath() {
      return metadataTableBasePath;
    }
  }

  interface FsIncrMetadataTableConfigProvider {

    String getBucketFieldName();

    String getKeyFieldName();

    String getSizeFieldName();

    String getAggregateKeyFieldName();

    String getAggregateSizeFieldName();

    static FsIncrMetadataTableConfigProvider getInstance(String tableBasePath) {
      switch (cloudProvider(tableBasePath)) {
        case "AWS":
          return new S3IncrMetadataTableConfigProvider();
        case "GCP":
          return new GcsIncrMetadataTableConfigProvider();
        default:
          throw new RuntimeException("cloudProvider could not be inferred from metadata table basePath: " + tableBasePath);
      }
    }

    static String cloudProvider(String tableBasePath) {
      if (tableBasePath.startsWith("s3://") || tableBasePath.startsWith("s3a://")) {
        return "AWS";
      } else if (tableBasePath.startsWith("gs://")) {
        return "GCP";
      }
      return "";
    }
  }

  private static class GcsIncrMetadataTableConfigProvider implements FsIncrMetadataTableConfigProvider {
    @Override
    public String getBucketFieldName() {
      return "bucket";
    }

    @Override
    public String getKeyFieldName() {
      return "name";
    }

    @Override
    public String getSizeFieldName() {
      return "size";
    }

    @Override
    public String getAggregateKeyFieldName() {
      return "name";
    }

    @Override
    public String getAggregateSizeFieldName() {
      return "size";
    }
  }

  private static class S3IncrMetadataTableConfigProvider implements FsIncrMetadataTableConfigProvider {
    @Override
    public String getBucketFieldName() {
      return "s3.bucket.name";
    }

    @Override
    public String getKeyFieldName() {
      return "s3.object.key";
    }

    @Override
    public String getSizeFieldName() {
      return "s3.object.size";
    }

    @Override
    public String getAggregateKeyFieldName() {
      return "key";
    }

    @Override
    public String getAggregateSizeFieldName() {
      return "size";
    }
  }

}
