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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import static org.apache.hudi.utilities.deltastreamer.internal.KafkaSourceDataAvailabilityEstimator.KafkaClusterInfo.KAFKA_SOURCE_RATE_ESTIMATOR_KEY;
import static org.apache.hudi.utilities.deltastreamer.internal.S3IncrDataAvailabilityEstimator.S3MetadataTableInfo.S3_INCR_SOURCE_RATE_ESTIMATOR_KEY;

public class SourceDataAvailabilityFactory {

  private static final Logger LOG = LogManager.getLogger(SourceDataAvailabilityFactory.class);

  public static SourceDataAvailabilityEstimator createInstance(JavaSparkContext jssc,
                                                               String basePath,
                                                               TypedProperties properties) {
    // ToDo Move data rate estimator to {@link Source} and use reflection to instantiate
    // the source class from properties, and calling the method: computeLoad.
    if (properties.containsKey(S3_INCR_SOURCE_RATE_ESTIMATOR_KEY)) {
      return new S3IncrDataAvailabilityEstimator(jssc, properties);
    } else if (properties.containsKey(KAFKA_SOURCE_RATE_ESTIMATOR_KEY)) {
      return new KafkaSourceDataAvailabilityEstimator(jssc, properties);
    }

    LOG.warn("Source rate availability estimator is not supported for this source " + basePath);
    return new DefaultSourceDataAvailabilityEstimator(jssc, properties);
  }
}
