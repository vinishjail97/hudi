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
import org.apache.hudi.common.util.Option;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.stream.Collectors;

public class SourceDataRateEstimatorAdapter {

  private static final Logger LOG = LogManager.getLogger(SourceDataRateEstimatorAdapter.class);

  private final long syncIntervalSeconds;
  private final Map<String, TypedProperties> multiTableProperties;
  private final Map<String, SourceDataRateEstimator> sourceDataRateEstimators;

  public SourceDataRateEstimatorAdapter(long syncIntervalSeconds,
                                        Map<String, TypedProperties> multiTableProperties) {
    this.syncIntervalSeconds = syncIntervalSeconds;
    this.multiTableProperties = multiTableProperties;
    // Currently only Kafka sources are supported
    // ToDo Move data rate estimator to {@link Source} and use reflection to instantiate
    // the source class from properties, and calling the method: computeLoad.
    sourceDataRateEstimators = multiTableProperties.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, tableProperties ->
            new KafkaSourceDataRateEstimator(syncIntervalSeconds, tableProperties.getValue())));
  }

  public Map<String, Long> computeAggregateLoad() {
    Map<String, HoodieMultiTableCommitStatsManager.TableCommitStats> commitStatsMap = HoodieMultiTableCommitStatsManager.getCommitStatsMap();
    return multiTableProperties.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, tableProperties -> {
          String key = tableProperties.getKey();
          HoodieMultiTableCommitStatsManager.TableCommitStats commitStats = commitStatsMap.get(key);
          return sourceDataRateEstimators.get(key).computeAvailableBytes(
              (commitStats != null) ? commitStats.getLastCommittedCheckpoint() : Option.empty(),
              (commitStats != null) ? commitStats.getAvgRecordSizes() : Option.empty());
        }));
  }
}
