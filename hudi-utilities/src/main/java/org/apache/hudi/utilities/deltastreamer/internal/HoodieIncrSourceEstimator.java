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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.utilities.sources.HoodieIncrSource.Config.HOODIE_SRC_BASE_PATH;

/**
 * Implements logic to schedule an Hoodie incr job based on number of pending commits to be ingested from the
 * base Hoodie Table.
 *
 * NOTE: Eventually this class should be implemented by the {@link org.apache.hudi.utilities.sources.Source} class
 * that ingests data.
 */
public class HoodieIncrSourceEstimator extends SourceDataAvailabilityEstimator {

  private static final Logger LOG = LogManager.getLogger(HoodieIncrSourceEstimator.class);
  // If the last committed checkpoint (last ingested commit of the source hudi table) divided
  // by the total commits in the active timeline of the source hudi table is below this threshold,
  // then the ingestion of this derived table is scheduled immediately, else its scheduled
  // based on the min sync time.
  private static final int INSTANT_THRESHOLD_PERC = 30;

  public HoodieIncrSourceEstimator(JavaSparkContext jssc, TypedProperties properties) {
    super(jssc, properties);
  }

  @Override
  public Pair<IngestionSchedulingStatus, Long> getDataAvailabilityStatus(Option<String> lastCommittedCheckpointStr, Option<Long> averageRecordSizeInBytes, long sourceLimit) {
    HudiSourceTableInfo hudiSourceTableInfo = HudiSourceTableInfo.createOrGetInstance(jssc, properties);
    String lastCommittedCheckpoint = (lastCommittedCheckpointStr.isPresent()) ? lastCommittedCheckpointStr.get() : IncrSourceHelper.DEFAULT_BEGIN_TIMESTAMP;
    Pair<String, String> instantThresholds = hudiSourceTableInfo.getMinAndMaxInstantsForScheduling();

    // Currently we do not estimate the actual bytes for the source hudi table
    return Pair.of(getScheduleStatus(lastCommittedCheckpoint, instantThresholds), 0L);
  }

  /**
   * Computes the status for scheduling ingestion based on the instant (commit) thresholds and the last committed instant.
   * @param lastCommittedCheckpoint last committed instant.
   * @param instantThresholds instant (commit) thresholds within the active timeline.
   * @return The {@link IngestionSchedulingStatus}
   */
  public IngestionSchedulingStatus getScheduleStatus(String lastCommittedCheckpoint, Pair<String, String> instantThresholds) {
    // Currently we do not estimate the actual bytes for the source hudi table
    if (HoodieTimeline.compareTimestamps(lastCommittedCheckpoint, HoodieTimeline.LESSER_THAN_OR_EQUALS, instantThresholds.getLeft())) {
      LOG.info(String.format("Scheduling ingestion right away since lastCommittedCheckpoint %s <= lowerThresholdInstant %s", lastCommittedCheckpoint, instantThresholds.getLeft()));
      return IngestionSchedulingStatus.SCHEDULE_IMMEDIATELY;
    } else if (HoodieTimeline.compareTimestamps(lastCommittedCheckpoint, HoodieTimeline.LESSER_THAN, instantThresholds.getRight())) {
      LOG.info(String.format("Scheduling ingestion after min sync time since lastCommittedCheckpoint %s < upperThresholdInstant %s", lastCommittedCheckpoint, instantThresholds.getRight()));
      return IngestionSchedulingStatus.SCHEDULE_AFTER_MIN_SYNC_TIME;
    }
    return IngestionSchedulingStatus.SCHEDULE_DEFER;

  }

  public static class HudiSourceTableInfo {
    private static final long MIN_SYNC_INTERVAL_MS = 300000;
    private static final Map<String, HudiSourceTableInfo> HOODIE_SRC_TABLE_BASE_PATH_MAP = new HashMap<>();
    private final String hoodieSourceTableBasePath;
    private final JavaSparkContext jssc;
    private String minInstantToSchedule;
    private String maxInstantToSchedule;
    private long lastActiveTimelineSyncTimeMs;

    public HudiSourceTableInfo(JavaSparkContext jssc, String hoodieSourceTableBasePath, TypedProperties props) {
      this.jssc = jssc;
      this.hoodieSourceTableBasePath = hoodieSourceTableBasePath;
      lastActiveTimelineSyncTimeMs = 0L;
    }

    static synchronized HudiSourceTableInfo createOrGetInstance(JavaSparkContext jssc, TypedProperties props) {
      String hoodieSourceTableBasePath = props.getString(HOODIE_SRC_BASE_PATH);
      if (!HOODIE_SRC_TABLE_BASE_PATH_MAP.containsKey(hoodieSourceTableBasePath)) {
        HudiSourceTableInfo hudiSourceTableInfo = new HudiSourceTableInfo(jssc, hoodieSourceTableBasePath, props);
        HOODIE_SRC_TABLE_BASE_PATH_MAP.put(hoodieSourceTableBasePath, hudiSourceTableInfo);
      }
      return HOODIE_SRC_TABLE_BASE_PATH_MAP.get(hoodieSourceTableBasePath);
    }

    protected synchronized Pair<String, String> getMinAndMaxInstantsForScheduling() {
      if (lastActiveTimelineSyncTimeMs <= 0 || (System.currentTimeMillis() - lastActiveTimelineSyncTimeMs) > MIN_SYNC_INTERVAL_MS) {
        HoodieTableMetaClient srcMetaClient = HoodieTableMetaClient.builder().setConf(jssc.hadoopConfiguration()).setBasePath(hoodieSourceTableBasePath).setLoadActiveTimelineOnLoad(true).build();
        HoodieTimeline activeCommitTimeline = srcMetaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants();

        int minInstantToSchedulePos = (INSTANT_THRESHOLD_PERC * activeCommitTimeline.countInstants()) / 100;
        minInstantToSchedule = activeCommitTimeline.nthInstant(minInstantToSchedulePos).get().getTimestamp();
        maxInstantToSchedule = activeCommitTimeline.lastInstant().get().getTimestamp();

        LOG.info(String.format("Active Timeline refreshed with minThresholdInstant %s and maxThresholdInstant %s", this.minInstantToSchedule, maxInstantToSchedule));
        lastActiveTimelineSyncTimeMs = System.currentTimeMillis();
      }
      return Pair.of(minInstantToSchedule, maxInstantToSchedule);
    }
  }
}
