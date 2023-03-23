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

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
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
import org.apache.hudi.utilities.deltastreamer.DeltaSyncException;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.exception.HoodieDeltaStreamerException;
import org.apache.hudi.utilities.ingestion.HoodieIngestionException;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.ingestion.HoodieIngestionService;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.MDC;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.CHECKPOINT_KEY;
import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.CHECKPOINT_RESET_KEY;

/**
 * The deltastreamer ingestion job that supports the ingestion of one or more tables from one or more sources,
 * such as Kafka, S3 etc.
 * <p>
 * If ingesting a single table, {@link OnehouseDeltaStreamer} leverages {@link HoodieDeltaStreamer}, and when
 * ingesting multiple tables, it leverages an instance of {@link org.apache.hudi.utilities.deltastreamer.DeltaSync} per table.
 * <p>
 * RFC: https://app.clickup.com/18029943/v/dc/h67bq-7724/h67bq-15908
 */
public class OnehouseDeltaStreamer implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(OnehouseDeltaStreamer.class);

  protected final transient Config cfg;
  protected transient Option<HoodieIngestionService> syncService;

  public OnehouseDeltaStreamer(Config cfg, JavaSparkContext jssc) {
    this.cfg = cfg;
    Configuration hadoopConf = jssc.hadoopConfiguration();
    this.syncService = Option.of(new MultiTableSyncService(cfg, jssc, hadoopConf));
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
    syncService.ifPresent(HoodieIngestionService::close);
  }

  /**
   * Main method to start syncing.
   */
  public void sync() throws Exception {
    syncService.ifPresent(HoodieIngestionService::startIngestion);
  }

  public static void main(String[] args) throws Exception {
    final OnehouseDeltaStreamer.Config cfg = getConfig(args);

    // Currently we run the table service inline (and not async) and hence
    // do not set the spark scheduler configs.
    JavaSparkContext jssc = UtilHelpers.buildSparkContext("onehouse-delta-streamer-" + cfg.jobUuid, cfg.sparkMaster);

    try {
      new OnehouseDeltaStreamer(cfg, jssc).sync();
    } finally {
      jssc.stop();
    }
  }

  public static class Config implements Serializable {
    public static final String DEFAULT_DFS_PROPS_ROOT = "file://" + System.getProperty("user.dir") + "/src/test/resources/delta-streamer-config/";
    private static final Long DEFAULT_MIN_SYNC_INTERVAL_AFTER_FAILURES_SECONDS = 300L; // 5mins
    private static final int MIN_SYNC_THREAD_POOL = 5;
    private static final int UNINITIALIZED = -1;

    @Parameter(names = {"--job-uuid"}, description = "The UUID for the multi table deltastreamer spark job.")
    public String jobUuid = "";

    @Parameter(names = {"--table-props-file"}, description = "Full pathname of the config file that contains "
        + "the list of the pathname of the root configurations for each table that needs to be ingested. ")
    public String tablePropsFile = DEFAULT_DFS_PROPS_ROOT;

    @Parameter(names = {"--min-sync-interval-post-failures-seconds"}, description = "the min sync interval after a failed sync for a specific table")
    public Long minSyncIntervalPostFailuresSeconds = DEFAULT_MIN_SYNC_INTERVAL_AFTER_FAILURES_SECONDS;

    @Parameter(names = {"--sync-jobs-thread-pool"}, description = "The size of the thread pool that runs the individual Ingestion jobs")
    public int syncJobsThreadPool = UNINITIALIZED;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated", splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--sync-once"}, description = "By default, the Delta Streamer runs in continuous mode. This flag will make it run "
        + "only once when ingesting a single table. Sync once is not supported when ingesting multiple tables." + " source-fetch -> Transform -> Hudi Write in loop.")
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

    @Parameter(names = {"--onehouse-metric-conf"}, description = "Any configuration related to the metrics reported by OnehouseMetricsReporter"
        + "these metrics are not covered by HoodieDeltaStreamerMetrics, eg: failures in initialization or parsing configs for a table")
    public List<String> metricConfigs = new ArrayList<>();

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }

  public static class MultiTableSyncService extends HoodieIngestionService {

    private static final Integer INGESTION_SCHEDULING_FREQUENCY_MS = 15 * 1000; // 15 secs
    private static final Long MIN_TIME_EMIT_METRICS_MS = 5 * 60 * 1000L; // 5 mins
    private static final Logger LOG = LogManager.getLogger(MultiTableSyncService.class);

    private final transient JobManager jobManager;
    private final transient ExecutorService multiTableStreamThreadPool;
    private transient long lastTimeMetricsReportedMs;

    public MultiTableSyncService(Config multiTableConfigs, JavaSparkContext jssc, Configuration hadoopConfig) {
      super(HoodieIngestionConfig.newBuilder()
          .isContinuous(!multiTableConfigs.syncOnce)
          .withMinSyncInternalSeconds(INGESTION_SCHEDULING_FREQUENCY_MS / 1000).build());
      int totalExecutorResources = Integer.parseInt(jssc.getConf().get("spark.executor.cores", "0"))
          * Math.max(Integer.parseInt(jssc.getConf().get("spark.executor.instances", "0")), Integer.parseInt(jssc.getConf().get("spark.dynamicAllocation.maxExecutors", "0")));
      int numThreads = (multiTableConfigs.syncJobsThreadPool != Config.UNINITIALIZED) ? multiTableConfigs.syncJobsThreadPool : Math.max(totalExecutorResources, Config.MIN_SYNC_THREAD_POOL);
      LOG.info("The sync jobs will be scheduled concurrently across a thread pool of size " + numThreads);
      multiTableStreamThreadPool = Executors.newFixedThreadPool(numThreads);

      this.lastTimeMetricsReportedMs = 0L;
      jobManager = new JobManager(multiTableConfigs, hadoopConfig, UtilHelpers.getConfig(multiTableConfigs.metricConfigs).getProps(),
          (sourceTablePropsPath, jobManagerInstance) -> {
            try {
              JobInfo jobInfo = createJobInfo(multiTableConfigs, jssc, hadoopConfig, sourceTablePropsPath, jobManagerInstance);
              return Option.of(jobInfo);
            } catch (Exception e) {
              LOG.error("Failed to initialize jobInfo for the table " + sourceTablePropsPath, e);
              return Option.empty();
            }
          });
    }

    private JobInfo createJobInfo(Config multiTableConfigs, JavaSparkContext jssc, Configuration hadoopConfig, String sourceTablePropsFilePath, JobManager jobManager) {
      try {
        final TypedProperties properties = buildProperties(sourceTablePropsFilePath, multiTableConfigs, hadoopConfig);
        HoodieDeltaStreamer.Config tableConfig = composeTableConfigs(multiTableConfigs, hadoopConfig, properties, sourceTablePropsFilePath, JobType.MULTI_TABLE_DELTASTREAMER);

        LogContext.getInstance().withTableDetails(tableConfig.targetTableName, tableConfig.databaseName);
        LOG.info(String.format("Configuring the table / job info %s at propsFilePath %s", tableConfig.targetBasePath, sourceTablePropsFilePath));

        FileSystem fs = FSUtils.getFs(tableConfig.targetBasePath, hadoopConfig);
        HoodieDeltaStreamer.DeltaSyncService deltaSync = new HoodieDeltaStreamer.DeltaSyncService(tableConfig, jssc, fs, hadoopConfig, Option.of(properties));

        JobInfo jobInfo = new JobInfo(jssc, hadoopConfig, sourceTablePropsFilePath, tableConfig.targetBasePath, tableConfig.targetTableName, tableConfig.databaseName, deltaSync,
            properties, multiTableConfigs, jobManager);

        Option<String> resumeCheckpointStr = getLastCommittedOffsets(fs, tableConfig.targetBasePath, tableConfig.payloadClassName);
        HoodieMultiTableCommitStatsManager.getCommitStatsMap().put(jobInfo.getBasePath(), new HoodieMultiTableCommitStatsManager.TableCommitStats(resumeCheckpointStr, Option.empty()));
        return jobInfo;
      } catch (IOException exception) {
        LOG.error("Reading table config files failed: ", exception);
        throw new HoodieException("Reading table config files failed ", exception);
      } finally {
        LogContext.clear();
      }
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
              List<JobInfo> selectedJobs = jobManager.getActiveJobs().stream().filter(jobInfo -> {
                LogContext.getInstance().withTableDetails(jobInfo.tableName, jobInfo.databaseName);
                return jobInfo.canSchedule(currentTimeMs);
              }).collect(Collectors.toList());
              LogContext.clear();

              LOG.info("In this round, scheduling ingestion for the following tables " + selectedJobs.stream().map(JobInfo::getSourceTablePath).collect(Collectors.toList()));

              selectedJobs.forEach(jobInfo -> {
                jobInfo.onSyncScheduled();
                CompletableFuture.supplyAsync(() -> {
                  LogContext.getInstance().withTableDetails(jobInfo.tableName, jobInfo.databaseName);
                  try {
                    if (jobInfo.canStart()) {
                      jobInfo.onSyncStarted();
                      HoodieIngestionService syncService = jobInfo.ingestionService;
                      syncService.ingestOnce();
                      jobInfo.onSyncSuccess();
                      LOG.info("Successfully ran job for table: " + jobInfo.getSourceTablePath());
                    } else {
                      jobInfo.onSyncCompleted();
                      LOG.info("Skipped job run for table: " + jobInfo.getSourceTablePath());
                    }
                  } catch (Exception e) {
                    jobInfo.onSyncFailure(e);
                    LOG.error("Failed to run job for table: " + jobInfo.getSourceTablePath(), e);
                  } finally {
                    LogContext.clear();
                  }
                  return null;
                }, multiTableStreamThreadPool);
              });

              sleepBeforeNextIngestion(currentTimeMs);

              // Update metrics
              if (lastTimeMetricsReportedMs == 0L || (System.currentTimeMillis() - lastTimeMetricsReportedMs) > MIN_TIME_EMIT_METRICS_MS) {
                jobManager.getActiveJobs().forEach(JobInfo::reportMetrics);
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

    @Override
    protected void sleepBeforeNextIngestion(long ingestionStartEpochMillis) {
      LOG.info("Sleeping for " + INGESTION_SCHEDULING_FREQUENCY_MS + " milliseconds before checking the source topics for ingestion.");
      try {
        Thread.sleep(INGESTION_SCHEDULING_FREQUENCY_MS);
      } catch (InterruptedException e) {
        throw new HoodieIngestionException("Ingestion service (continuous mode) was interrupted during sleep.", e);
      }
    }

    @Override
    public void ingestOnce() {
      try {
        jobManager.getActiveJobs().forEach(jobInfo -> jobInfo.ingestionService.ingestOnce());
      } catch (Exception e) {
        throw new HoodieIngestionException(String.format("Ingestion via %s failed with exception.", this.getClass()), e);
      } finally {
        onIngestionCompletes(false);
      }
    }

    @Override
    protected boolean onIngestionCompletes(boolean error) {
      LOG.info(String.format("Shutting down %s. Closing write client. Error? %s", this.getClass().getSimpleName(), error));
      close();
      return true;
    }

    @Override
    public Option<HoodieIngestionMetrics> getMetrics() {
      return Option.empty();
    }

    private enum JobStatus {
      RUNNING, PAUSED, STOPPED;
    }

    private static class JobManager implements Serializable {
      private static final long JOB_MANAGER_REFRESH_INTERVAL = 1000;
      private final Config multiTableConfigs;
      private final Configuration hadoopConf;
      private final transient Timer timer;
      private Map<String, JobStatus> desiredJobStatuses;
      private final Map<String, JobStatus> currentJobStatuses;
      private final Map<String, JobInfo> jobInfoMap;
      private final SerializableBiFunction<String, JobManager, Option<JobInfo>> jobInfoCreator;
      private final transient OnehouseMetricsReporter metricsReporter;

      JobManager(final Config multiTableConfigs, final Configuration hadoopConf, final TypedProperties metricsReporterProps,
                 final SerializableBiFunction<String, JobManager, Option<JobInfo>> jobInfoCreator) {
        this.multiTableConfigs = multiTableConfigs;
        this.hadoopConf = hadoopConf;
        this.currentJobStatuses = new ConcurrentHashMap<>();
        this.jobInfoMap = new ConcurrentHashMap<>();
        this.jobInfoCreator = jobInfoCreator;
        this.metricsReporter = new OnehouseMetricsReporter(metricsReporterProps);
        resolve();
        timer = new Timer();
        timer.schedule(new TimerTask() {
          @Override
          public void run() {
            resolve();
          }
        }, JOB_MANAGER_REFRESH_INTERVAL, JOB_MANAGER_REFRESH_INTERVAL);
      }

      private synchronized void resolve() {
        try {
          desiredJobStatuses = UtilHelpers.readConfig(hadoopConf, new Path(multiTableConfigs.tablePropsFile), Collections.emptyList()).getProps().entrySet().stream()
              .collect(Collectors.toMap(entry -> entry.getKey().toString(), entry -> JobStatus.valueOf(entry.getValue().toString().toUpperCase())));
          // stop jobs that were removed form the desiredJobStatuses
          List<String> removed = currentJobStatuses.keySet().stream().filter(jobIdentifier -> {
            if (!desiredJobStatuses.containsKey(jobIdentifier)) {
              LOG.info("JobManager removing: " + jobIdentifier);
              JobInfo jobToStop = jobInfoMap.get(jobIdentifier);
              if (jobToStop != null) {
                try {
                  jobToStop.ingestionService.close();
                  jobInfoMap.remove(jobIdentifier);
                  return true;
                } catch (Exception ex) {
                  LOG.error(String.format("Failed to close DeltaSync for: %s, will retry in next resolve() iteration", jobIdentifier), ex);
                  return false;
                }
              }
            }
            return false;
          }).collect(Collectors.toList());
          // remove the stopped jobs from the current job statuses
          removed.forEach(currentJobStatuses::remove);

          List<String> jobsFailedToInitialize = new ArrayList<>();
          for (Map.Entry<String, JobStatus> desiredJobStatusEntry : desiredJobStatuses.entrySet()) {
            String jobIdentifier = desiredJobStatusEntry.getKey();
            JobStatus currentJobStatus = currentJobStatuses.get(jobIdentifier);
            // any job not in the desiredJobStatuses but not the currentJobStatuses should be created
            if (currentJobStatus == null) {
              LOG.info("JobManager adding: " + jobIdentifier);
              Option<JobInfo> jobInfo = jobInfoCreator.apply(jobIdentifier, this);
              metricsReporter.reportJobInitializationResult(jobIdentifier, jobInfo.isPresent());
              if (jobInfo.isPresent()) {
                jobInfoMap.putIfAbsent(jobIdentifier, jobInfo.get());
                setCurrentJobStatus(jobIdentifier, desiredJobStatusEntry.getValue());
              } else {
                jobsFailedToInitialize.add(jobIdentifier);
              }
            } else if (desiredJobStatusEntry.getValue() == JobStatus.PAUSED && !jobInfoMap.get(jobIdentifier).isRunning()) {
              // see which jobs need to be paused and whether they can be paused now
              setCurrentJobStatus(jobIdentifier, JobStatus.PAUSED);
            } else if (desiredJobStatusEntry.getValue() == JobStatus.RUNNING && currentJobStatus == JobStatus.PAUSED) {
              // make sure properties are updated when un-pausing a stream
              jobInfoMap.get(jobIdentifier).markPropertiesAsUpdated();
            }
          }
          metricsReporter.removeMetricsForMissingPaths(desiredJobStatuses.keySet());
          if (jobInfoMap.size() == 0) {
            throw new IllegalArgumentException("All the jobs failed during initialization " + jobsFailedToInitialize);
          }
        } catch (Exception ex) {
          LOG.error("JobManager unable to refresh state", ex);
        }
      }

      public JobStatus getDesiredStatusForJob(String jobName) {
        return desiredJobStatuses.getOrDefault(jobName, JobStatus.STOPPED);
      }

      public void setCurrentJobStatus(String jobName, JobStatus jobStatus) {
        currentJobStatuses.put(jobName, jobStatus);
      }

      public Collection<JobInfo> getActiveJobs() {
        return jobInfoMap.values();
      }

      public void close() {
        jobInfoMap.values().forEach(JobInfo::close);
        timer.cancel();
      }
    }

    /**
     * Close all resources.
     */
    public void close() {
      jobManager.close();
      if (!isShutdown()) {
        shutdown(false);
      }
    }

    public static class JobInfo {
      private static final long MAX_BACKOFF_MS = 30 * 60 * 1000; // 30 minutes
      private final JavaSparkContext jssc;
      private final Configuration hadoopConfig;

      /**
       * Source specific path for the job
       */
      private final String sourceTablePath;
      /**
       * Target Table Path.
       */
      private final String basePath;

      /**
       * Target Table name.
       */
      private final String tableName;

      /**
       * Target database name.
       */
      private final String databaseName;

      /**
       * Bag of properties with source, hoodie client, key generator etc.
       */
      private TypedProperties props;

      /**
       * Delta Sync.
       */
      private HoodieIngestionService ingestionService;

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
       * Fetches the desired ingestionSchedulingStatus for the job so we can stop scheduling ingest for individual tables instead of cancelling the whole job.
       */
      private final JobManager jobManager;

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
       * IngestionSchedulingStatus of the availability of data in the source as defined by
       * {@link SourceDataAvailabilityEstimator.IngestionSchedulingStatus}
       */
      private SourceDataAvailabilityEstimator.IngestionSchedulingStatus status;

      /**
       * Amount of available data in bytes ready for ingest at the source.
       */
      private AtomicLong sourceBytesAvailableForIngest;

      /**
       * Approximate lag in ingestion data from the source as measured by the {@link SourceDataAvailabilityEstimator}
       * It is the approx amount of time in secs expected to ingest the current data in source.
       */
      private Long sourceLagSecs;

      /**
       * Time in millis when the properties where last updated.
       */
      private final AtomicLong propertiesUpdated;

      /**
       * Time in millis when the delta sync service was last updated.
       */
      private final AtomicLong deltaServiceUpdated;

      /**
       * Type of last failure, will be set to null if there are no failures.
       */
      private final AtomicReference<DeltaSyncException.Type> lastFailureType;

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

      public JobInfo(JavaSparkContext jssc, Configuration hadoopConfig, String sourceTablePath, String basePath, String tableName, String databaseName,
                     HoodieIngestionService ingestionService, TypedProperties props, Config configs, JobManager jobManager) {
        this.jssc = jssc;
        this.hadoopConfig = hadoopConfig;
        long currentMillis = System.currentTimeMillis();
        this.propertiesUpdated = new AtomicLong(currentMillis);
        this.deltaServiceUpdated = new AtomicLong(currentMillis);
        this.sourceTablePath = sourceTablePath;
        this.tableName = tableName;
        this.databaseName = databaseName;
        this.basePath = basePath;
        this.ingestionService = ingestionService;
        this.props = props;
        this.configs = configs;
        this.sourceDataAvailabilityEstimator = SourceDataAvailabilityFactory.createInstance(jssc, basePath, props);
        this.isTableSyncActive = new AtomicBoolean(false);
        this.numberConsecutiveFailures = new AtomicInteger(0);
        this.numberSyncSuccesses = new AtomicInteger(0);
        this.numberSyncFailures = new AtomicInteger(0);
        this.status = SourceDataAvailabilityEstimator.IngestionSchedulingStatus.UNKNOWN;
        this.sourceBytesAvailableForIngest = new AtomicLong(0);
        this.sourceLagSecs = 0L;
        this.lastSyncCompletedTimeMs = 0L;
        this.minSyncTimeMs = props.getInteger(OnehouseInternalDeltastreamerConfig.MIN_SYNC_INTERVAL_SECS.key(), OnehouseInternalDeltastreamerConfig.MIN_SYNC_INTERVAL_SECS.defaultValue()) * 1000;
        this.readSourceLimit = props.getLong(OnehouseInternalDeltastreamerConfig.READ_SOURCE_LIMIT.key(), OnehouseInternalDeltastreamerConfig.READ_SOURCE_LIMIT.defaultValue());
        this.jobManager = jobManager;
        this.lastFailureType = new AtomicReference<>(null);
      }

      private void checkAndSetPropertyUpdates() {
        final long refreshTime = System.currentTimeMillis();
        final TypedProperties properties = buildProperties(sourceTablePath, configs, hadoopConfig);
        if (!this.props.equals(properties)) {
          LOG.info("Updating properties for: " + sourceTablePath);
          this.props = properties;
          propertiesUpdated.set(refreshTime);
        }
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

      JobStatus getDesiredJobStatus() {
        return jobManager.getDesiredStatusForJob(sourceTablePath);
      }

      boolean canSchedule(long currentTimeMs) {
        if (getDesiredJobStatus() != JobStatus.RUNNING) {
          return false;
        }

        // If an ingestion job is active, then do not schedule right away.
        if (isTableSyncActive.get()) {
          return false;
        }

        // If the ingestion job has been failing, schedule based on linear backoff.
        if (numberConsecutiveFailures.get() >= 1) {
          if (currentTimeMs <= nextTimeScheduleMsecs) {
            LOG.info(
                "After " + numberConsecutiveFailures.get() + " consecutive failures, the table " + basePath + " will be scheduled at: " + nextTimeScheduleMsecs + " currentTimeMs " + currentTimeMs);
            return false;
          }
        }

        try {
          HoodieMultiTableCommitStatsManager.TableCommitStats commitStats = HoodieMultiTableCommitStatsManager.getCommitStatsMap().get(basePath);
          SourceDataAvailabilityEstimator.IngestionStats estimatorStats = sourceDataAvailabilityEstimator
              .getDataAvailabilityStatus((commitStats != null) ? commitStats.getLastCommittedCheckpoint() : Option.empty(), (commitStats != null) ? commitStats.getAvgRecordSizes() : Option.empty(),
                  readSourceLimit);
          updateSourceDataAvailabilityMetrics(estimatorStats);

          if (estimatorStats.ingestionSchedulingStatus.equals(SourceDataAvailabilityEstimator.IngestionSchedulingStatus.SCHEDULE_IMMEDIATELY)) {
            return true;
          }
          // If there is data in the source, schedule only if minSyncTimeMs has passed since the last ingest round.
          return estimatorStats.ingestionSchedulingStatus.equals(SourceDataAvailabilityEstimator.IngestionSchedulingStatus.SCHEDULE_AFTER_MIN_SYNC_TIME) && checkSyncIntervalDone();
        } catch (Exception exception) {
          LOG.warn("Failed to estimate the data availability in source, falling back to using time based scheduling ", exception);
          return checkSyncIntervalDone();
        }
      }

      boolean canStart() {
        return getDesiredJobStatus() == JobStatus.RUNNING;
      }

      boolean isRunning() {
        return isTableSyncActive.get();
      }

      void onSyncScheduled() {
        isTableSyncActive.compareAndSet(false, true);
        startSyncScheduledTimeMs = System.currentTimeMillis();
      }

      void markPropertiesAsUpdated() {
        propertiesUpdated.set(System.currentTimeMillis());
      }

      void onSyncStarted() {
        // check if the delta sync settings/conf need to be refreshed
        jobManager.setCurrentJobStatus(sourceTablePath, JobStatus.RUNNING);
        if (propertiesUpdated.get() > deltaServiceUpdated.get()) {
          ingestionService.close();
          deltaServiceUpdated.set(System.currentTimeMillis());
          try {
            HoodieDeltaStreamer.Config tableConfig = composeTableConfigs(configs, hadoopConfig, props, sourceTablePath, JobType.MULTI_TABLE_DELTASTREAMER);

            FileSystem fs = FSUtils.getFs(tableConfig.targetBasePath, hadoopConfig);
            LOG.info("Creating new delta sync due to properties update for : " + sourceTablePath);
            this.ingestionService = new HoodieDeltaStreamer.DeltaSyncService(tableConfig, jssc, fs, hadoopConfig, Option.of(props));
          } catch (IOException exception) {
            throw new HoodieException("Reading table config files failed ", exception);
          }
        }
        startSyncTimeMs = System.currentTimeMillis();
        ingestionService.getMetrics().ifPresent(m -> ((OnehouseDeltaStreamerMetrics) m).updateIsActivelyIngesting(1));
      }

      void onSyncSuccess() {
        lastFailureType.set(null);
        numberConsecutiveFailures.set(0);
        numberSyncSuccesses.incrementAndGet();
        onSyncCompleted();
        // update metrics
        long currentTimeMs = System.currentTimeMillis();
        ingestionService.getMetrics().ifPresent(m -> ((OnehouseDeltaStreamerMetrics) m).updateTotalSyncDurationMs(currentTimeMs - startSyncScheduledTimeMs));
        ingestionService.getMetrics().ifPresent(m -> ((OnehouseDeltaStreamerMetrics) m).updateActualSyncDurationMs(currentTimeMs - startSyncTimeMs));
        ingestionService.getMetrics().ifPresent(m -> ((OnehouseDeltaStreamerMetrics) m).updateIsActivelyIngesting(0));
      }

      void onSyncFailure(Exception exception) {
        if (exception instanceof DeltaSyncException) {
          DeltaSyncException deltaSyncException = (DeltaSyncException) exception;
          lastFailureType.set(deltaSyncException.getType());
        } else {
          lastFailureType.set(DeltaSyncException.Type.UNKNOWN);
          LOG.warn("Uncategorized exception type in delta sync failure", exception);
        }
        numberSyncFailures.incrementAndGet();
        nextTimeScheduleMsecs = System.currentTimeMillis() + Math.min(MAX_BACKOFF_MS, numberConsecutiveFailures.incrementAndGet() * configs.minSyncIntervalPostFailuresSeconds * 1000);
        onSyncCompleted();
        // update metrics
        ingestionService.getMetrics().ifPresent(m -> ((OnehouseDeltaStreamerMetrics) m).updateIsActivelyIngesting(0));
      }

      private void onSyncCompleted() {
        isTableSyncActive.compareAndSet(true, false);
        lastSyncCompletedTimeMs = System.currentTimeMillis();
        if (getDesiredJobStatus() == JobStatus.PAUSED) {
          jobManager.setCurrentJobStatus(sourceTablePath, JobStatus.PAUSED);
        }
      }

      // Check if its time to schedule the next ingestion round because minSyncTimeMs has passed since
      // the start of the last ingestion round
      private boolean checkSyncIntervalDone() {
        return startSyncScheduledTimeMs == null || (System.currentTimeMillis() - startSyncScheduledTimeMs >= minSyncTimeMs);
      }

      private void updateSourceDataAvailabilityMetrics(SourceDataAvailabilityEstimator.IngestionStats sourceDataAvailability) {
        status = sourceDataAvailability.ingestionSchedulingStatus;
        sourceBytesAvailableForIngest.set(sourceDataAvailability.bytesAvailable);
        sourceLagSecs = sourceDataAvailability.sourceLagSecs;
      }

      public void reportMetrics() {
        ingestionService.getMetrics().ifPresent(m -> ((OnehouseDeltaStreamerMetrics) m).updateNumSuccessfulSyncs(numberSyncSuccesses.getAndSet(0)));
        ingestionService.getMetrics().ifPresent(m -> ((OnehouseDeltaStreamerMetrics) m).updateNumFailedSyncs(numberSyncFailures.getAndSet(0)));
        ingestionService.getMetrics().ifPresent(m -> ((OnehouseDeltaStreamerMetrics) m).updateNumConsecutiveFailures(numberConsecutiveFailures.get()));
        ingestionService.getMetrics().ifPresent(m -> ((OnehouseDeltaStreamerMetrics) m).updateFailureType(lastFailureType.get()));
        ingestionService.getMetrics().ifPresent(m -> ((OnehouseDeltaStreamerMetrics) m).updateTotalSourceBytesAvailableForIngest(sourceBytesAvailableForIngest.get()));
        ingestionService.getMetrics().ifPresent(m -> ((OnehouseDeltaStreamerMetrics) m).updateTotalSourceAvailabilityStatusForIngest(status.getValue()));
        ingestionService.getMetrics().ifPresent(m -> ((OnehouseDeltaStreamerMetrics) m).updateSourceIngestionLag(sourceLagSecs));
        // Send a heartbeat metrics event to track the active ingestion job for this table.
        ingestionService.getMetrics().ifPresent(m -> m.updateDeltaStreamerHeartbeatTimestamp(System.currentTimeMillis()));
      }

      public void close() {
        ingestionService.close();
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
            } else if (HoodieTimeline.compareTimestamps(HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS, HoodieTimeline.LESSER_THAN, lastCommit.get().getTimestamp())) {
              throw new HoodieDeltaStreamerException(
                  "Unable to find previous checkpoint. Please double check if this table " + "was indeed built via delta streamer. Last Commit :" + lastCommit + ", Instants :"
                      + commitTimelineOpt.getInstants().collect(Collectors.toList()) + ", CommitMetadata=" + commitMetadata.toJsonString());
            } else {
              LOG.warn("Unable to get latest commit offset for table " + targetBasePath);
            }
          }
        }
        return resumeCheckpointStr;
      }
      return Option.empty();
    }

    private static TypedProperties buildProperties(String sourceTablePropsFilePath, OnehouseDeltaStreamer.Config multiTableConfigs, Configuration hadoopConfig) {
      TypedProperties properties = combineProperties(sourceTablePropsFilePath, multiTableConfigs, hadoopConfig);
      // Configure the write commit callback that updates the checkpoints
      properties.setProperty(HoodieWriteCommitCallbackConfig.TURN_CALLBACK_ON.key(), String.valueOf(true));
      properties.setProperty(HoodieWriteCommitCallbackConfig.CALLBACK_CLASS_NAME.key(), HoodieMultiTableCommitStatsManager.class.getName());
      if (!StringUtils.isNullOrEmpty(properties.getProperty(OnehouseInternalDeltastreamerConfig.MUTLI_WRITER_SOURCE_CHECKPOINT_ID.key()))) {
        properties.setProperty(HoodieWriteCommitCallbackConfig.CALLBACK_MUTLI_WRITER_CLASS_NAME.key(), HoodieMultiWriterCheckpointUpdateManager.class.getName());
      }

      // ToDo Currently multi table deltastreamer only supports inline table services (including clustering)
      // This will be fixed once we support async services for multi table
      HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.from(properties);
      if (clusteringConfig.isAsyncClusteringEnabled()) {
        properties.remove(HoodieClusteringConfig.ASYNC_CLUSTERING_ENABLE.key());
        properties.setProperty(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true");
        if (properties.containsKey(HoodieClusteringConfig.ASYNC_CLUSTERING_MAX_COMMITS.key())) {
          properties.setProperty(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), properties.getProperty(HoodieClusteringConfig.ASYNC_CLUSTERING_MAX_COMMITS.key()));
          properties.remove(HoodieClusteringConfig.ASYNC_CLUSTERING_MAX_COMMITS.key());
        }
      }
      return properties;
    }

    // ToDo Convert to helper method and use it across Deltasync and here.
    private Option<HoodieCommitMetadata> getLatestCommitMetadataWithValidCheckpointInfo(HoodieTimeline timeline) {
      return (Option<HoodieCommitMetadata>) timeline.getReverseOrderedInstants().map(instant -> {
        try {
          HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
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

  private static HoodieDeltaStreamer.Config composeTableConfigs(Config config, Configuration hadoopConf, TypedProperties properties, String sourceTablePropsFilePath, JobType jobType)
      throws IOException {
    // Compile the Config for HoodieDeltaStreamer using the properties
    HoodieWriteConfig configuredWriteConfig = HoodieWriteConfig.newBuilder().withProps(properties).build();
    OnehouseInternalDeltastreamerConfig onehouseInternalDeltastreamerConfig = OnehouseInternalDeltastreamerConfig.newBuilder().withProps(properties).build();

    HoodieDeltaStreamer.Config tableConfig = new HoodieDeltaStreamer.Config();
    tableConfig.targetBasePath = configuredWriteConfig.getBasePath();
    tableConfig.targetTableName = configuredWriteConfig.getTableName();
    tableConfig.tableType = configuredWriteConfig.getTableType().name();
    tableConfig.baseFileFormat = onehouseInternalDeltastreamerConfig.getTableFileFormat().name();
    tableConfig.databaseName = onehouseInternalDeltastreamerConfig.getDatabaseName();
    tableConfig.propsFilePath = sourceTablePropsFilePath;
    tableConfig.sourceClassName = onehouseInternalDeltastreamerConfig.getSourceClassName();
    tableConfig.sourceLimit = onehouseInternalDeltastreamerConfig.getReadSourceLimit();
    tableConfig.sourceOrderingField = configuredWriteConfig.getPreCombineField();
    tableConfig.payloadClassName = configuredWriteConfig.getWritePayloadClass();
    tableConfig.schemaProviderClassName = onehouseInternalDeltastreamerConfig.getSchemaProviderClassName();
    tableConfig.transformerClassNames = onehouseInternalDeltastreamerConfig.getTransformerClassName();
    tableConfig.operation = WriteOperationType.valueOf(configuredWriteConfig.getStringOrDefault(DataSourceWriteOptions.OPERATION(), DataSourceWriteOptions.OPERATION().defaultValue()).toUpperCase());
    tableConfig.filterDupes = onehouseInternalDeltastreamerConfig.isFilterDupesEnabled();
    // currently multi table job runs in sync once mode and uses inline services
    tableConfig.continuousMode = !jobType.equals(JobType.MULTI_TABLE_DELTASTREAMER) && !config.syncOnce;
    tableConfig.minSyncIntervalSeconds = onehouseInternalDeltastreamerConfig.getMinSyncIntervalSecs();
    tableConfig.enableMetaSync = onehouseInternalDeltastreamerConfig.isMetaSyncEnabled();
    tableConfig.syncClientToolClassNames = onehouseInternalDeltastreamerConfig.getMetaSyncClasses();
    tableConfig.skipRddUnpersist = jobType.equals(JobType.MULTI_TABLE_DELTASTREAMER);
    tableConfig.ingestionMetricsClass = OnehouseDeltaStreamerMetrics.class.getCanonicalName();

    tableConfig.checkpoint = onehouseInternalDeltastreamerConfig.getCheckpoint();
    tableConfig.allowCommitOnNoCheckpointChange = onehouseInternalDeltastreamerConfig.isAllowCommitOnNoCheckpointChange();
    tableConfig.allowCommitOnNoData = onehouseInternalDeltastreamerConfig.isAllowCommitOnNoData();
    tableConfig.initialCheckpointProvider = onehouseInternalDeltastreamerConfig.getInitialCheckpointProvider();
    if (tableConfig.initialCheckpointProvider != null && tableConfig.checkpoint == null) {
      InitialCheckPointProvider checkPointProvider = UtilHelpers.createInitialCheckpointProvider(tableConfig.initialCheckpointProvider, properties);
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
    SINGLE_TABLE_DELTASTREAMER, MULTI_TABLE_DELTASTREAMER
  }

  private static class LogContext {
    private static final String DATABASE_TABLE_CONTEXT_KEY = "database.table";
    private static final String TABLE_SOURCE_PROPS_PATH = "table.source.props.path";

    private static final LogContext LOG_CONTEXT = new LogContext();

    private LogContext() {
    }

    public static LogContext getInstance() {
      return LOG_CONTEXT;
    }

    public LogContext withTableDetails(String tableName, String databaseName) {
      if (!StringUtils.isNullOrEmpty(tableName) && !StringUtils.isNullOrEmpty(databaseName)) {
        MDC.put(DATABASE_TABLE_CONTEXT_KEY, kvString(DATABASE_TABLE_CONTEXT_KEY, databaseName + "." + tableName));
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
