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
import org.apache.hudi.common.util.collection.Pair;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Always returns that the source has data available, so it will get scheduled regularly.
 */
public class DefaultSourceDataAvailabilityEstimator extends SourceDataAvailabilityEstimator {

  private static final Logger LOG = LogManager.getLogger(DefaultSourceDataAvailabilityEstimator.class);

  public DefaultSourceDataAvailabilityEstimator(JavaSparkContext jssc, TypedProperties properties) {
    super(jssc, properties);
  }

  @Override
  Pair<SourceDataAvailabilityStatus, Long> getDataAvailabilityStatus(Option<String> lastCommittedCheckpointStr, Option<Long> averageRecordSizeInBytes, long sourceLimit) {
    return Pair.of(SourceDataAvailabilityStatus.DATA_AVAILABLE, 0L);
  }
}
