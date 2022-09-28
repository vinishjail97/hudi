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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.async.HoodieAsyncService;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteCommitCallbackConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.OnehouseInternalDeltastreamerConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.IdentitySplitter;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.checkpointing.InitialCheckPointProvider;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.exception.HoodieDeltaStreamerException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.pulsar.shade.org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.slf4j.MDC;

import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.CHECKPOINT_KEY;
import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.CHECKPOINT_RESET_KEY;

/**
 * The deltastreamer ingestion job that supports the ingestion of one or more tables from one or more sources,
 * such as Kafka, S3 etc.
 *
 * If ingesting a single table, {@link OnehouseDeltaStreamer} leverages {@link HoodieDeltaStreamer}, and when
 * ingesting multiple tables, it leverages an instance of {@link org.apache.hudi.utilities.deltastreamer.DeltaSync} per table.
 *
 * RFC: https://app.clickup.com/18029943/v/dc/h67bq-7724/h67bq-15908
 */
public class OnehouseDeltaStreamer implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(OnehouseDeltaStreamer.class);

  protected final transient Config cfg;
  protected transient Option<HoodieAsyncService> syncService;

  public OnehouseDeltaStreamer(Config cfg, JavaSparkContext jssc) throws IOException {
    this.cfg = cfg;
    List<String> sourceTablePropertiesFiles = cfg.sourceTablePropsPaths;

    ValidationUtils.checkArgument(!sourceTablePropertiesFiles.isEmpty(), "At least configure 1 source for 1 table for ingestion.");
    ValidationUtils.checkArgument(((sourceTablePropertiesFiles.size() > 1 && cfg.syncOnce.equals(false))
            || sourceTablePropertiesFiles.size() == 1),
        "Sync once mode not supported for multiple table ingestion");

    Configuration hadoopConf = jssc.hadoopConfiguration();

    if (isSingleTableIngestion()) {
      String tablePropsRelativePath = (sourceTablePropertiesFiles.get(0).endsWith(".properties")) ? sourceTablePropertiesFiles.get(0) : sourceTablePropertiesFiles.get(0).concat(".properties");
      String tablePropsFilePath = String.format("%s/%s", cfg.propsDirRoot, tablePropsRelativePath);
      TypedProperties properties = combineProperties(tablePropsFilePath, cfg, hadoopConf);
      HoodieDeltaStreamer.Config tableConfig = composeTableConfigs(cfg, hadoopConf, properties, tablePropsFilePath, JobType.SINGLE_TABLE_DELTASTREAMER);
      this.syncService = Option.ofNullable(new HoodieDeltaStreamer.DeltaSyncService(tableConfig,
          jssc,
          FSUtils.getFs(tableConfig.targetBasePath, hadoopConf),
          hadoopConf,
          Option.of(properties)));
    } else {
      this.syncService = Option.ofNullable(new MultiTableSyncService(cfg, jssc, hadoopConf));
    }
  }

  public static OnehouseDeltaStreamer.Config getConfig(String[] args) {
    OnehouseDeltaStreamer.Config cfg = new OnehouseDeltaStreamer.Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    return cfg;
  }

  public void shutdownGracefully() {
    syncService.ifPresent(ds -> ds.shutdown(false));
  }

  /**
   * Main method to start syncing.
   *
   * @throws Exception
   */
  public void sync() throws Exception {

    if (!cfg.syncOnce) {
      LOG.info("Delta Streamer running in continuous mode");
      syncService.ifPresent(ds -> {
        ds.start(this::onDeltaSyncShutdown);
        try {
          ds.waitForShutdown();
        } catch (Exception e) {
          throw new HoodieException(e.getMessage(), e);
        }
      });
      LOG.info("Delta Sync shutting down");
    } else {
      LOG.info("Delta Streamer running only single round");
      try {
        syncService.ifPresent(ds -> {
          try {
            ((HoodieDeltaStreamer.DeltaSyncService) ds).getDeltaSync().syncOnce();
          } catch (IOException e) {
            throw new HoodieIOException(e.getMessage(), e);
          }
        });
      } catch (Exception ex) {
        LOG.error("Got error running delta sync once. Shutting down", ex);
        throw ex;
      } finally {
        this.onDeltaSyncShutdown(false);
        LOG.info("Shut down delta streamer");
      }
    }
  }

  private boolean isSingleTableIngestion() {
    return (cfg.sourceTablePropsPaths.size() == 1);
  }

  private boolean onDeltaSyncShutdown(boolean error) {
    LOG.info("DeltaSync shutdown. Closing write client. Error?" + error);
    syncService.ifPresent(ds -> {
      if (isSingleTableIngestion()) {
        ((HoodieDeltaStreamer.DeltaSyncService) syncService.get()).close();
      } else {
        ((MultiTableSyncService) syncService.get()).close();
      }
    });
    return true;
  }

  public static void main(String[] args) throws Exception {
    final OnehouseDeltaStreamer.Config cfg = getConfig(args);

    // Currently we run the table service inline (and not async) and hence
    // do not set the spark scheduler configs.
    JavaSparkContext jssc =
        UtilHelpers.buildSparkContext("onehouse-delta-streamer-" + cfg.jobUuid, cfg.sparkMaster);

    try {
      new OnehouseDeltaStreamer(cfg, jssc).sync();
    } finally {
      jssc.stop();
    }
  }

  public static class Config implements Serializable {
    public static final String DEFAULT_DFS_PROPS_ROOT = "file://" + System.getProperty("user.dir")
        + "/src/test/resources/delta-streamer-config/";
    private static final Long DEFAULT_MIN_SYNC_INTERVAL_AFTER_FAILURES_SECONDS = 300L; // 5mins
    private static final int MIN_SYNC_THREAD_POOL = 5;
    private static final int UNINITIALIZED = -1;

    @Parameter(names = {"--job-uuid"}, description = "The UUID for the multi table deltastreamer spark job.")
    public String jobUuid = "";

    @Parameter(names = {"--props-root"}, description = "root path to properties file on localfs or dfs, with configurations for "
        + "hoodie client, schema provider, key generator and data source for all individual tables. "
        + " Properties in this file can be overridden by \"--hoodie-conf\"")
    public String propsDirRoot = DEFAULT_DFS_PROPS_ROOT;

    @Parameter(names = {"--props-tables-source"}, description = "relative paths to the root path "
        + "of the table's source's properties file", splitter = IdentitySplitter.class)
    public List<String> sourceTablePropsPaths = new ArrayList<>();

    @Parameter(names = {"--min-sync-interval-post-failures-seconds"},
        description = "the min sync interval after a failed sync for a specific table")
    public Long minSyncIntervalPostFailuresSeconds = DEFAULT_MIN_SYNC_INTERVAL_AFTER_FAILURES_SECONDS;

    @Parameter(names = {"--sync-jobs-thread-pool"},
        description = "The size of the thread pool that runs the individual Ingestion jobs")
    public int syncJobsThreadPool = UNINITIALIZED;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--sync-once"}, description = "By default, the Delta Streamer runs in continuous mode. This flag will make it run "
        + "only once when ingesting a single table. Sync once is not supported when ingesting multiple tables."
        + " source-fetch -> Transform -> Hudi Write in loop.")
    public Boolean syncOnce = false;

    @Parameter(names = {"--spark-master"}, description = "spark master to use.")
    public String sparkMaster = "local[2]";

    @Parameter(names = {"--retry-on-source-failures"}, description = "Retry on any source failures")
    public Boolean retryOnSourceFailures = false;

    @Parameter(names = {"--retry-interval-seconds"}, description = "the retry interval for source failures if --retry-on-source-failures is enabled")
    public Integer retryIntervalSecs = 30;

    @Parameter(names = {"--retry-last-pending-inline-clustering", "-rc"}, description = "Retry last pending inline clustering plan before writing to sink.")
    public Boolean retryLastPendingInlineClusteringJob = true;

    @Parameter(names = {"--max-retry-count"}, description = "the max retry count if --retry-on-source-failures is enabled")
    public Integer maxRetryCount = 3;

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }

  public static class MultiTableSyncService extends HoodieAsyncService {

    private static final String PROPERTIES_FILE_EXTENSION = ".properties";
    private static final Integer INGESTION_SCHEDULING_FREQUENCY_MS = 15 * 1000; // 15 secs
    private static final Long MIN_TIME_EMIT_METRICS_MS = 5 * 60 * 1000L; // 5 mins

    private final transient Map<String, JobInfo> jobInfoMap;
    private final transient ExecutorService multiTableStreamThreadPool;

    /**
     * Multitable DeltaSync Config.
     */
    private final transient Config multiTableConfigs;
    private final transient JavaSparkContext jssc;
    private transient long lastTimeMetricsReportedMs;

    public MultiTableSyncService(Config multiTableConfigs, JavaSparkContext jssc, Configuration hadoopConfig) throws IOException {
      int totalExecutorResources = Integer.parseInt(jssc.getConf().get("spark.executor.cores", "0"))
          * Math.max(Integer.parseInt(jssc.getConf().get("spark.executor.instances", "0")),
          Integer.parseInt(jssc.getConf().get("spark.dynamicAllocation.maxExecutors", "0")));
      int numThreads = (multiTableConfigs.syncJobsThreadPool != Config.UNINITIALIZED) ? multiTableConfigs.syncJobsThreadPool
          : Math.max(totalExecutorResources, Config.MIN_SYNC_THREAD_POOL);
      LOG.info("The sync jobs will be scheduled concurrently across a thread pool of size " + numThreads);
      multiTableStreamThreadPool = Executors.newFixedThreadPool(numThreads);

      this.jobInfoMap = new HashMap<>();
      this.multiTableConfigs = multiTableConfigs;
      this.jssc = jssc;
      this.lastTimeMetricsReportedMs = 0L;

      Map<String, HoodieMultiTableCommitStatsManager.TableCommitStats> initialTableCommitStatsMap = new HashMap<>();
      multiTableConfigs.sourceTablePropsPaths.forEach(sourceTablePropsPath -> {
        sourceTablePropsPath = (sourceTablePropsPath.endsWith(PROPERTIES_FILE_EXTENSION)) ? sourceTablePropsPath : sourceTablePropsPath.concat(PROPERTIES_FILE_EXTENSION);

        String sourceTablePropsFilePath = String.format("%s/%s", multiTableConfigs.propsDirRoot, sourceTablePropsPath);
        try {
          final TypedProperties properties = buildProperties(sourceTablePropsFilePath, hadoopConfig);

          HoodieDeltaStreamer.Config tableConfig = composeTableConfigs(multiTableConfigs,
              hadoopConfig,
              properties,
              sourceTablePropsFilePath,
              JobType.MULTI_TABLE_DELTASTREAMER);

          LogContext.getInstance().withTableName(tableConfig.targetTableName);

          FileSystem fs = FSUtils.getFs(tableConfig.targetBasePath, hadoopConfig);
          HoodieDeltaStreamer.DeltaSyncService deltaSync = new HoodieDeltaStreamer.DeltaSyncService(tableConfig,
              jssc,
              fs,
              hadoopConfig,
              Option.of(properties));

          JobInfo jobInfo = new JobInfo(
              jssc,
              sourceTablePropsFilePath,
              tableConfig.targetTableName,
              tableConfig.targetBasePath,
              deltaSync,
              properties,
              multiTableConfigs);
          LOG.info("Hoodie properties: " + HoodieDeltaStreamer.toSortedTruncatedString(properties).replace("\n", " ; "));
          jobInfoMap.put(sourceTablePropsFilePath, jobInfo);

          Option<String> resumeCheckpointStr = getLastCommittedOffsets(fs, tableConfig.targetBasePath, tableConfig.payloadClassName);
          if (resumeCheckpointStr.isPresent()) {
            initialTableCommitStatsMap.put(jobInfo.getBasePath(),
                new HoodieMultiTableCommitStatsManager.TableCommitStats(resumeCheckpointStr, Option.empty()));
          }
        } catch (IOException exception) {
          String stackTrace = ExceptionUtils.getStackTrace(exception);
          LOG.error("Reading table config files failed: " + stackTrace);
          throw new HoodieException("Reading table config files failed ", exception);
        } finally {
          LogContext.clear();
        }
      });

      HoodieMultiTableCommitStatsManager.initializeCommitStatsMap(initialTableCommitStatsMap);
    }

    @Override
    protected Pair<CompletableFuture, ExecutorService> startService() {
      ExecutorService executor = Executors.newFixedThreadPool(1);
      return Pair.of(CompletableFuture.supplyAsync(() -> {
        try {
          while (!isShutdownRequested()) {
            try {
              // Mark eligible jobs for scheduling
              long currentTimeMs = System.currentTimeMillis();
              List<JobInfo> selectedJobs = jobInfoMap.values().stream()
                  .filter(jobInfo -> jobInfo.canSchedule(currentTimeMs))
                  .collect(Collectors.toList());

              LOG.info("Based on source data rates selected the following tables " + selectedJobs.stream().map(JobInfo::getSourceTablePath).collect(Collectors.toList()));

              selectedJobs.forEach(jobInfo -> {
                jobInfo.onSyncScheduled();
                CompletableFuture.supplyAsync(() -> {
                  LogContext.getInstance().withTableName(jobInfo.targetTableName);
                  try {
                    jobInfo.onSyncStarted();
                    HoodieDeltaStreamer.DeltaSyncService syncService = jobInfo.deltaSync;
                    syncService.getDeltaSync().syncOnce();
                  } catch (IOException e) {
                    throw new HoodieIOException(e.getMessage(), e);
                  } finally {
                    LogContext.clear();
                  }
                  return null;
                }, multiTableStreamThreadPool)
                    .whenCompleteAsync((response, throwable) -> {
                      LogContext.getInstance().withTableName(jobInfo.targetTableName);
                      if (throwable != null) {
                        jobInfo.onSyncFailure();
                        LOG.error("Failed to run job for table: " + jobInfo.getSourceTablePath(), throwable.getCause());
                        LOG.error("StackTrace: " + ExceptionUtils.getStackTrace(throwable));
                      } else {
                        jobInfo.onSyncSuccess();
                        LOG.info("Successfully ran job for table: " + jobInfo.getSourceTablePath());
                      }
                      LogContext.clear();
                    });
              });

              LOG.info("Sleeping for " + INGESTION_SCHEDULING_FREQUENCY_MS + " ms before checking the source topics for ingestion.");
              Thread.sleep(INGESTION_SCHEDULING_FREQUENCY_MS);

              // Update metrics
              if (lastTimeMetricsReportedMs == 0L
                  || (System.currentTimeMillis() - lastTimeMetricsReportedMs) > MIN_TIME_EMIT_METRICS_MS) {
                jobInfoMap.values().forEach(JobInfo::reportMetrics);
                lastTimeMetricsReportedMs = System.currentTimeMillis();
              }
            } catch (Exception e) {
              LOG.error("Shutting down delta-sync due to exception", e);
              throw new HoodieException(e.getMessage(), e);
            }
          }
        } finally {
          multiTableStreamThreadPool.shutdownNow();
          executor.shutdownNow();
        }
        return null;
      }, executor), executor);
    }

    /**
     * Close all resources.
     */
    public void close() {
      jobInfoMap.values().forEach(value -> value.deltaSync.close());
    }

    public static class JobInfo {

      /**
       * Source specific path for the job
       */
      private final String sourceTablePath;
      /**
       * Name of target hoodie table.
       */
      private final String targetTableName;
      /**
       * Target Table Path.
       */
      private final String basePath;

      /**
       * Bag of properties with source, hoodie client, key generator etc.
       */
      private final TypedProperties props;

      /**
       * Delta Sync.
       */
      private final HoodieDeltaStreamer.DeltaSyncService deltaSync;

      /**
       * Passed in configs
       */
      private final Config configs;

      /**
       * Estimates the amount of data available in source for ingestion
       */
      private final SourceDataAvailabilityEstimator sourceDataAvailabilityEstimator;

      /**
       * Is there an active or scheduled ingestion job for the table.
       */
      private final AtomicBoolean isTableSyncActive;

      /**
       * Number of consecutive failures of the ingest sync since the last successful sync.
       */
      private final AtomicInteger numberConsecutiveFailures;

      /**
       * Time in future that is the minimum time at which the sync should be scheduled due to sync failures.
       */
      private Long nextTimeScheduleMsecs;

      /**
       * Number of successful and failed syncs that get reported to the metrics reporter for the individual table.
       */
      private AtomicInteger numberSyncSuccesses;
      private AtomicInteger numberSyncFailures;

      /**
       * Amount of available data in bytes ready for ingest at the source.
       */
      private AtomicLong sourceBytesAvailableForIngest;

      /**
       * Tracks the (1) total time to sync one batch (round) of ingest data for a table after being scheduled and
       * (2) the actual time to sync once the thread was available from the pool.
       */
      private Long startSyncScheduledTimeMs;
      private Long startSyncTimeMs;

      /**
       * Tracks the last sync time in ms
       */
      private Long lastSyncCompletedTimeMs;

      private Integer minSyncTimeMs;
      private Long readSourceLimit;

      public JobInfo(JavaSparkContext jssc, String sourceTablePath, String targetTableName, String basePath,
                     HoodieDeltaStreamer.DeltaSyncService deltaSync, TypedProperties props, Config configs) {
        this.sourceTablePath = sourceTablePath;
        this.targetTableName = targetTableName;
        this.basePath = basePath;
        this.deltaSync = deltaSync;
        this.props = props;
        this.configs = configs;
        this.sourceDataAvailabilityEstimator = SourceDataAvailabilityFactory.createInstance(
            jssc,
            basePath,
            props
        );
        this.isTableSyncActive = new AtomicBoolean(false);
        this.numberConsecutiveFailures = new AtomicInteger(0);
        this.numberSyncSuccesses = new AtomicInteger(0);
        this.numberSyncFailures = new AtomicInteger(0);
        this.sourceBytesAvailableForIngest = new AtomicLong(0);
        this.lastSyncCompletedTimeMs = 0L;
        this.minSyncTimeMs = props.getInteger(OnehouseInternalDeltastreamerConfig.MIN_SYNC_INTERVAL_SECS.key(),
            OnehouseInternalDeltastreamerConfig.MIN_SYNC_INTERVAL_SECS.defaultValue()) * 1000;
        this.readSourceLimit = props.getLong(OnehouseInternalDeltastreamerConfig.READ_SOURCE_LIMIT.key(),
            OnehouseInternalDeltastreamerConfig.READ_SOURCE_LIMIT.defaultValue());
      }

      String getSourceTablePath() {
        return sourceTablePath;
      }

      String getBasePath() {
        return basePath;
      }

      TypedProperties getProps() {
        return props;
      }

      boolean canSchedule(long currentTimeMs) {
        // Send a heartbeat metrics event to track the active ingestion job for this table.
        deltaSync.getDeltaSync().getMetrics().updateDeltaStreamerHeartbeatTimestamp(currentTimeMs);

        // If an ingestion job is active, then do not schedule right away.
        if (isTableSyncActive.get()) {
          return false;
        }

        // If the ingestion job has been failing, schedule based on linear backoff.
        if (numberConsecutiveFailures.get() >= 1) {
          LOG.info("After " + numberConsecutiveFailures.get() + " consecutive failures, the table " + basePath
              + " will get scheduled at: " + nextTimeScheduleMsecs + " currentTimeMs " + currentTimeMs);
          if (currentTimeMs <= nextTimeScheduleMsecs) {
            return false;
          }
        }

        try {
          HoodieMultiTableCommitStatsManager.TableCommitStats commitStats = HoodieMultiTableCommitStatsManager.getCommitStatsMap().get(basePath);
          SourceDataAvailabilityEstimator.SourceDataAvailability sourceDataAvailability = sourceDataAvailabilityEstimator.getDataAvailability(
              (commitStats != null) ? commitStats.getLastCommittedCheckpoint() : Option.empty(),
              (commitStats != null) ? commitStats.getAvgRecordSizes() : Option.empty(),
              readSourceLimit);

          // If number of bytes available in source exceeds {@link OnehouseInternalDeltastreamerConfig.MIN_BYTES_INGESTION_SOURCE_PROP},
          // schedule right away.
          if (sourceDataAvailability.equals(SourceDataAvailabilityEstimator.SourceDataAvailability.MIN_INGEST_DATA_AVAILABLE)) {
            return true;
          }

          // If there is data in the source, schedule only if minSyncTimeMs has passed since the last ingest round.
          return sourceDataAvailability.equals(SourceDataAvailabilityEstimator.SourceDataAvailability.DATA_AVAILABLE) && checkSyncIntervalDone();
        } catch (Exception exception) {
          LOG.warn("Failed to detect data availability in source, falling back to using time based scheduling ", exception);
          return checkSyncIntervalDone();
        }
      }

      void onSyncScheduled() {
        isTableSyncActive.compareAndSet(false, true);
        startSyncScheduledTimeMs = System.currentTimeMillis();
      }

      void onSyncStarted() {
        startSyncTimeMs = System.currentTimeMillis();
        deltaSync.getDeltaSync().getMetrics().updateIsActivelyIngesting(1);
      }

      void onSyncSuccess() {
        numberConsecutiveFailures.set(0);
        numberSyncSuccesses.incrementAndGet();
        onSyncCompleted();
        // update metrics
        long currentTimeMs = System.currentTimeMillis();
        deltaSync.getDeltaSync().getMetrics().updateTotalSyncDurationMs(currentTimeMs - startSyncScheduledTimeMs);
        deltaSync.getDeltaSync().getMetrics().updateActualSyncDurationMs(currentTimeMs - startSyncTimeMs);
        deltaSync.getDeltaSync().getMetrics().updateIsActivelyIngesting(0);
      }

      void onSyncFailure() {
        numberSyncFailures.incrementAndGet();
        nextTimeScheduleMsecs = System.currentTimeMillis()
            + (numberConsecutiveFailures.incrementAndGet() * configs.minSyncIntervalPostFailuresSeconds * 1000);
        onSyncCompleted();
        // update metrics
        deltaSync.getDeltaSync().getMetrics().updateIsActivelyIngesting(0);
      }

      private void onSyncCompleted() {
        isTableSyncActive.compareAndSet(true, false);
        lastSyncCompletedTimeMs = System.currentTimeMillis();
      }

      // Check if its time to schedule the next ingestion round because minSyncTimeMs has passed since
      // the start of the last ingestion round
      private boolean checkSyncIntervalDone() {
        return startSyncScheduledTimeMs == null || (System.currentTimeMillis() - startSyncScheduledTimeMs >= minSyncTimeMs);
      }

      public void updateSourceBytesAvailableForIngest(long sourceBytes) {
        sourceBytesAvailableForIngest.set(sourceBytes);
      }

      public void reportMetrics() {
        deltaSync.getDeltaSync().getMetrics().updateNumSuccessfulSyncs(numberSyncSuccesses.getAndSet(0));
        deltaSync.getDeltaSync().getMetrics().updateNumFailedSyncs(numberSyncFailures.getAndSet(0));
        deltaSync.getDeltaSync().getMetrics().updateNumConsecutiveFailures(numberConsecutiveFailures.get());
        deltaSync.getDeltaSync().getMetrics().updateTotalSourceBytesAvailableForIngest(sourceBytesAvailableForIngest.get());
      }
    }

    private Option<String> getLastCommittedOffsets(FileSystem fs, String targetBasePath, String payloadClass) throws IOException {
      if (fs.exists(new Path(targetBasePath))) {
        HoodieTimeline commitTimelineOpt;
        HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setConf(new Configuration(fs.getConf())).setBasePath(targetBasePath).setPayloadClassName(payloadClass).build();
        switch (meta.getTableType()) {
          case COPY_ON_WRITE:
            commitTimelineOpt = meta.getActiveTimeline().getCommitTimeline().filterCompletedInstants();
            break;
          case MERGE_ON_READ:
            commitTimelineOpt = meta.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants();
            break;
          default:
            throw new HoodieException("Unsupported table type :" + meta.getTableType());
        }

        Option<String> resumeCheckpointStr = Option.empty();
        Option<HoodieInstant> lastCommit = commitTimelineOpt.lastInstant();
        if (lastCommit.isPresent()) {
          // if previous commit metadata did not have the checkpoint key, try traversing previous commits until we find one.
          Option<HoodieCommitMetadata> commitMetadataOption = getLatestCommitMetadataWithValidCheckpointInfo(commitTimelineOpt);
          if (commitMetadataOption.isPresent()) {
            HoodieCommitMetadata commitMetadata = commitMetadataOption.get();
            if (!StringUtils.isNullOrEmpty(commitMetadata.getMetadata(CHECKPOINT_KEY))) {
              //if previous checkpoint is an empty string, skip resume use Option.empty()
              resumeCheckpointStr = Option.of(commitMetadata.getMetadata(CHECKPOINT_KEY));
            } else if (HoodieTimeline.compareTimestamps(HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS,
                HoodieTimeline.LESSER_THAN, lastCommit.get().getTimestamp())) {
              throw new HoodieDeltaStreamerException(
                  "Unable to find previous checkpoint. Please double check if this table "
                      + "was indeed built via delta streamer. Last Commit :" + lastCommit + ", Instants :"
                      + commitTimelineOpt.getInstants().collect(Collectors.toList()) + ", CommitMetadata="
                      + commitMetadata.toJsonString());
            } else {
              LOG.warn("Unable to get latest commit offset for table " + targetBasePath);
            }
          }
        }
        return resumeCheckpointStr;
      }
      return Option.empty();
    }

    private TypedProperties buildProperties(String sourceTablePropsFilePath, Configuration hadoopConfig) {
      TypedProperties properties = combineProperties(sourceTablePropsFilePath, multiTableConfigs, hadoopConfig);
      // Configure the write commit callback that updates the checkpoints
      properties.setProperty(HoodieWriteCommitCallbackConfig.TURN_CALLBACK_ON.key(), String.valueOf(true));
      properties.setProperty(HoodieWriteCommitCallbackConfig.CALLBACK_CLASS_NAME.key(), HoodieMultiTableCommitStatsManager.class.getName());
      if (StringUtils.isNullOrEmpty(properties.getProperty(OnehouseInternalDeltastreamerConfig.MUTLI_WRITER_SOURCE_CHECKPOINT_ID.key()))) {
        properties.setProperty(HoodieWriteCommitCallbackConfig.CALLBACK_MUTLI_WRITER_CLASS_NAME.key(), HoodieMultiWriterCheckpointUpdateManager.class.getName());
      }

      // ToDo Currently multi table deltastreamer only supports inline table services (including clustering)
      // This will be fixed once we support async services for multi table
      HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.from(properties);
      if (clusteringConfig.isAsyncClusteringEnabled()) {
        properties.remove(HoodieClusteringConfig.ASYNC_CLUSTERING_ENABLE.key());
        properties.setProperty(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true");
        if (properties.containsKey(HoodieClusteringConfig.ASYNC_CLUSTERING_MAX_COMMITS)) {
          properties.setProperty(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(),
              properties.getProperty(HoodieClusteringConfig.ASYNC_CLUSTERING_MAX_COMMITS.key()));
          properties.remove(HoodieClusteringConfig.ASYNC_CLUSTERING_MAX_COMMITS.key());
        }
      }
      return properties;
    }

    // ToDo Convert to helper method and use it across Deltasync and here.
    private Option<HoodieCommitMetadata> getLatestCommitMetadataWithValidCheckpointInfo(HoodieTimeline timeline) {
      return (Option<HoodieCommitMetadata>) timeline.getReverseOrderedInstants().map(instant -> {
        try {
          HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
              .fromBytes(timeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
          if (!StringUtils.isNullOrEmpty(commitMetadata.getMetadata(CHECKPOINT_KEY)) || !StringUtils.isNullOrEmpty(commitMetadata.getMetadata(CHECKPOINT_RESET_KEY))) {
            return Option.of(commitMetadata);
          } else {
            return Option.empty();
          }
        } catch (IOException e) {
          throw new HoodieIOException("Failed to parse HoodieCommitMetadata for " + instant.toString(), e);
        }
      }).filter(Option::isPresent).findFirst().orElse(Option.empty());
    }
  }

  private static TypedProperties combineProperties(String sourceTablePropPath, Config cfg, Configuration hadoopConf) {
    HoodieConfig hoodieConfig = new HoodieConfig();

    hoodieConfig.setAll(UtilHelpers.readConfig(hadoopConf, new Path(sourceTablePropPath), cfg.configs).getProps());
    hoodieConfig.setDefaultValue(DataSourceWriteOptions.RECONCILE_SCHEMA());
    return hoodieConfig.getProps(true);
  }

  private static HoodieDeltaStreamer.Config composeTableConfigs(
      Config config,
      Configuration hadoopConf,
      TypedProperties properties,
      String sourceTablePropsFilePath,
      JobType jobType) throws IOException {
    // Compile the Config for HoodieDeltaStreamer using the properties
    HoodieWriteConfig configuredWriteConfig = HoodieWriteConfig.newBuilder()
        .withProps(properties)
        .build();
    OnehouseInternalDeltastreamerConfig onehouseInternalDeltastreamerConfig = OnehouseInternalDeltastreamerConfig.newBuilder()
        .withProps(properties)
        .build();

    HoodieDeltaStreamer.Config tableConfig = new HoodieDeltaStreamer.Config();
    tableConfig.targetBasePath = configuredWriteConfig.getBasePath();
    tableConfig.targetTableName = configuredWriteConfig.getTableName();
    tableConfig.tableType = configuredWriteConfig.getTableType().name();
    tableConfig.baseFileFormat = onehouseInternalDeltastreamerConfig.getTableFileFormat().name();
    tableConfig.propsFilePath = sourceTablePropsFilePath;
    tableConfig.sourceClassName = onehouseInternalDeltastreamerConfig.getSourceClassName();
    tableConfig.sourceLimit = onehouseInternalDeltastreamerConfig.getReadSourceLimit();
    tableConfig.sourceOrderingField = configuredWriteConfig.getPreCombineField();
    tableConfig.payloadClassName = configuredWriteConfig.getWritePayloadClass();
    tableConfig.schemaProviderClassName = onehouseInternalDeltastreamerConfig.getSchemaProviderClassName();
    tableConfig.transformerClassNames = onehouseInternalDeltastreamerConfig.getTransformerClassName();
    tableConfig.operation = WriteOperationType.valueOf(configuredWriteConfig.getStringOrDefault(
        DataSourceWriteOptions.OPERATION(), DataSourceWriteOptions.OPERATION().defaultValue()).toUpperCase());
    tableConfig.filterDupes = onehouseInternalDeltastreamerConfig.isFilterDupesEnabled();
    // currently multi table job runs in sync once mode and uses inline services
    tableConfig.continuousMode = !jobType.equals(JobType.MULTI_TABLE_DELTASTREAMER) && !config.syncOnce;
    tableConfig.minSyncIntervalSeconds = onehouseInternalDeltastreamerConfig.getMinSyncIntervalSecs();
    tableConfig.enableMetaSync = onehouseInternalDeltastreamerConfig.isMetaSyncEnabled();
    tableConfig.syncClientToolClassNames = onehouseInternalDeltastreamerConfig.getMetaSyncClasses();
    tableConfig.skipRddUnpersist = jobType.equals(JobType.MULTI_TABLE_DELTASTREAMER);

    tableConfig.checkpoint = onehouseInternalDeltastreamerConfig.getCheckpoint();
    tableConfig.initialCheckpointProvider = onehouseInternalDeltastreamerConfig.getInitialCheckpointProvider();
    if (tableConfig.initialCheckpointProvider != null && tableConfig.checkpoint == null) {
      InitialCheckPointProvider checkPointProvider =
          UtilHelpers.createInitialCheckpointProvider(tableConfig.initialCheckpointProvider, properties);
      checkPointProvider.init(hadoopConf);
      tableConfig.checkpoint = checkPointProvider.getCheckpoint();
    }

    // ToDo Move to table level configs??
    tableConfig.retryOnSourceFailures = config.retryOnSourceFailures;
    tableConfig.forceDisableCompaction = onehouseInternalDeltastreamerConfig.isCompactionDisabled();
    tableConfig.commitOnErrors = onehouseInternalDeltastreamerConfig.isAllowCommitOnErrors();
    tableConfig.retryLastPendingInlineClusteringJob = config.retryLastPendingInlineClusteringJob;
    tableConfig.retryIntervalSecs = config.retryIntervalSecs;
    tableConfig.maxRetryCount = config.maxRetryCount;

    tableConfig.configs = config.configs;
    return tableConfig;
  }

  private enum JobType {
    SINGLE_TABLE_DELTASTREAMER,
    MULTI_TABLE_DELTASTREAMER
  }

  private static class LogContext {
    private static final String TABLE_NAME_CONTEXT_KEY = "table";

    private static final LogContext LOG_CONTEXT = new LogContext();

    private LogContext() {
    }

    public static LogContext getInstance() {
      return LOG_CONTEXT;
    }

    public LogContext withTableName(String tableName) {
      if (!StringUtils.isNullOrEmpty(tableName)) {
        MDC.put(TABLE_NAME_CONTEXT_KEY, kvString(TABLE_NAME_CONTEXT_KEY, tableName));
      }
      return this;
    }

    public static void clear() {
      MDC.clear();
    }

    private static String kvString(String key, String value) {
      return "{" + key + "=" + value + "}";
    }
  }
}
