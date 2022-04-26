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

import org.apache.hudi.callback.HoodieWriteCommitCallback;
import org.apache.hudi.callback.common.HoodieWriteCommitCallbackMessage;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An implementation of {@link HoodieWriteCommitCallback} that keeps track of the
 * latest committed offsets and avg record sizes per table
 */
public class HoodieMultiTableCommitStatsManager implements HoodieWriteCommitCallback {

  private static final Logger LOG = LogManager.getLogger(HoodieMultiTableCommitStatsManager.class);
  private static final ConcurrentHashMap<String, TableCommitStats> COMMIT_STATS_MAP = new ConcurrentHashMap<>();

  public HoodieMultiTableCommitStatsManager(HoodieWriteConfig config) {
  }

  @Override
  public void call(HoodieWriteCommitCallbackMessage callbackMessage) {
    if (callbackMessage.getCommitActionType().isPresent()
        && (callbackMessage.getCommitActionType().get().equals(HoodieTimeline.COMMIT_ACTION)
        || callbackMessage.getCommitActionType().get().equals(HoodieTimeline.DELTA_COMMIT_ACTION))) {
      String lastCommittedCheckpoints = null;
      Long averageRecordSize = null;
      // First get the offsets
      if (callbackMessage.getExtraMetadata().isPresent()
          && callbackMessage.getExtraMetadata().get().containsKey(HoodieWriteConfig.DELTASTREAMER_CHECKPOINT_KEY)) {
        lastCommittedCheckpoints = callbackMessage.getExtraMetadata().get().get(HoodieWriteConfig.DELTASTREAMER_CHECKPOINT_KEY);
      }

      long totalWriteBytes = callbackMessage.getHoodieWriteStat().stream().mapToLong(HoodieWriteStat::getTotalWriteBytes).sum();
      long totalWritenRecords = callbackMessage.getHoodieWriteStat().stream().mapToLong(HoodieWriteStat::getNumWrites).sum();

      if (totalWritenRecords > 0L) {
        averageRecordSize = totalWriteBytes / totalWritenRecords;
      }
      TableCommitStats commitStats = new TableCommitStats(
          Option.ofNullable(lastCommittedCheckpoints),
          Option.ofNullable(averageRecordSize));
      COMMIT_STATS_MAP.put(callbackMessage.getTableName(), commitStats);
    }
  }

  public static void initializeCommitStatsMap(Map<String, TableCommitStats> initializeCommitStats) {
    if (COMMIT_STATS_MAP.isEmpty()) {
      COMMIT_STATS_MAP.putAll(initializeCommitStats);
    }
  }

  public static ConcurrentHashMap<String, TableCommitStats> getCommitStatsMap() {
    return COMMIT_STATS_MAP;
  }

  public static class TableCommitStats {
    private final Option<String> lastCommittedCheckpoint;
    private final Option<Long> avgRecordSizes;

    public TableCommitStats(Option<String> lastCommittedCheckpoint, Option<Long> avgRecordSizes) {
      this.lastCommittedCheckpoint = lastCommittedCheckpoint;
      this.avgRecordSizes = avgRecordSizes;
    }

    public Option<String> getLastCommittedCheckpoint() {
      return lastCommittedCheckpoint;
    }

    public Option<Long> getAvgRecordSizes() {
      return avgRecordSizes;
    }
  }
}
