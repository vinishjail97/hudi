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

  protected final JavaSparkContext jssc;
  protected final TypedProperties properties;
  protected final long minSourceBytesIngestion;

  public SourceDataAvailabilityEstimator(JavaSparkContext jssc, TypedProperties properties) {
    this.jssc = jssc;
    this.properties = properties;
    this.minSourceBytesIngestion = properties.getLong(OnehouseInternalDeltastreamerConfig.MIN_BYTES_INGESTION_SOURCE_PROP.key(),
        OnehouseInternalDeltastreamerConfig.MIN_BYTES_INGESTION_SOURCE_PROP.defaultValue());
  }

  /**
   * Returns the status of the source based on data available for ingest at any given time.
   * The following params are provided to ensure accurate numbers.
   * @param lastCommittedCheckpointStr The checkpoint from the latest Hudi commit.
   * @param averageRecordSizeInBytes The average record size as computed by Hudi writer.
   * @param sourceLimit Max data that will be ingested in every round.
   * @return
   */
  abstract SourceDataAvailability getDataAvailability(Option<String> lastCommittedCheckpointStr, Option<Long> averageRecordSizeInBytes, long sourceLimit);

  /** Source Data Availability Status */
  public enum SourceDataAvailability {
    // There is sufficient data available to schedule ingest immediately.
    MIN_INGEST_DATA_AVAILABLE,
    // There is data available in the source, only schedule ingest if min sync time has passed since last ingestion.
    DATA_AVAILABLE,
    // There is no data available in the source.
    NO_DATA
  }
}