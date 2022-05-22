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

import org.apache.spark.api.java.JavaSparkContext;

/**
 * The derived classes of this class will implement the logic to compute
 * the total data volume in bytes that is available to be ingested at any
 * given time. The implementations will be specific to the source (e.g., kafka, s3 etc).
 * The checkpoint from the latest Hudi commit and the average record size as written
 * by Hudi is provided to the class to get more accurate data volume size that is
 * available for ingest.
 *
 * NOTE: This abstract/ interface will eventually be moved to/ within the
 * {@link org.apache.hudi.utilities.sources.Source} that implement the logic
 * for ingesting from custom sources.
 */
public abstract class SourceDataRateEstimator {

  protected final JavaSparkContext jssc;
  protected final long syncIntervalSeconds;
  protected final TypedProperties properties;

  public SourceDataRateEstimator(JavaSparkContext jssc, long syncIntervalSeconds, TypedProperties properties) {
    this.jssc = jssc;
    this.syncIntervalSeconds = syncIntervalSeconds;
    this.properties = properties;
  }

  /**
   * Computes the amount of bytes available for ingest at any given time.
   * The following params are provided to ensure accurate numbers.
   * @param lastCommittedCheckpointStr The checkpoint from the latest Hudi commit.
   * @param averageRecordSizeInBytes The average record size as computed by Hudi writer.
   * @return the amount of bytes available for ingest at any given time.
   */
  abstract Long computeAvailableBytes(Option<String> lastCommittedCheckpointStr, Option<Long> averageRecordSizeInBytes);
}