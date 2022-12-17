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
import org.apache.hudi.config.OnehouseInternalDeltastreamerConfig;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * The derived classes of this class will implement the logic to compute
 * the total data that is available to be ingested at any
 * given time. The implementations will be specific to the source (e.g., kafka, s3 etc).
 * The checkpoint from the latest Hudi commit and the average record size as written
 * by Hudi is provided to the class to get more accurate data volume size that is
 * available for ingest.
 *
 * NOTE: This abstract/ interface will eventually be moved to/ within the
 * {@link org.apache.hudi.utilities.sources.Source} that implement the logic
 * for ingesting from custom sources.
 */
public abstract class SourceDataAvailabilityEstimator {

  public static final Long DEFAULT_AVAILABLE_SOURCE_BYTES = 1L;
  public static final Long DEFAULT_SOURCE_LAG_SECS = 1L;

  protected final JavaSparkContext jssc;
  protected final TypedProperties properties;
  protected final long minSourceBytesIngestion;
  protected final int minSyncTimeSecs;

  public SourceDataAvailabilityEstimator(JavaSparkContext jssc, TypedProperties properties) {
    this.jssc = jssc;
    this.properties = properties;
    this.minSourceBytesIngestion = properties.getLong(OnehouseInternalDeltastreamerConfig.MIN_BYTES_INGESTION_SOURCE_PROP.key(),
        OnehouseInternalDeltastreamerConfig.MIN_BYTES_INGESTION_SOURCE_PROP.defaultValue());
    this.minSyncTimeSecs = properties.getInteger(OnehouseInternalDeltastreamerConfig.MIN_SYNC_INTERVAL_SECS.key(), OnehouseInternalDeltastreamerConfig.MIN_SYNC_INTERVAL_SECS.defaultValue());
  }

  /**
   * Returns the ingestionSchedulingStatus of the source based on data available for ingest at any given time.
   * The following params are provided to ensure accurate numbers.
   *
   * @param lastCommittedCheckpointStr The checkpoint from the latest Hudi commit.
   * @param averageRecordSizeInBytes   The average record size as computed by Hudi writer.
   * @param sourceLimit                Max data that will be ingested in every round.
   * @return
   */
  abstract IngestionStats getDataAvailabilityStatus(Option<String> lastCommittedCheckpointStr, Option<Long> averageRecordSizeInBytes, long sourceLimit);

  /**
   * Reflects the ingestionSchedulingStatus for scheduling Ingestion for a specific table based on data availability in the source.
   */
  public enum IngestionSchedulingStatus {
    UNKNOWN(-1), // There is no data available in the source, differ scheduling until next time estimator is run.
    SCHEDULE_DEFER(0), // There is data available in the source, but schedule ingest only if min sync time has passed since the start of the last ingestion.
    SCHEDULE_AFTER_MIN_SYNC_TIME(1), // There is sufficient data available to schedule ingest immediately.
    SCHEDULE_IMMEDIATELY(2);

    private final int value;

    IngestionSchedulingStatus(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  public static class IngestionStats {
    public final IngestionSchedulingStatus ingestionSchedulingStatus;
    public final Long bytesAvailable;
    public final Long sourceLagSecs;

    public IngestionStats(IngestionSchedulingStatus ingestionSchedulingStatus, Long bytesAvailable, Long sourceLagSecs) {
      this.ingestionSchedulingStatus = ingestionSchedulingStatus;
      this.bytesAvailable = bytesAvailable;
      this.sourceLagSecs = sourceLagSecs;
    }
  }
}
