/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.deltastreamer.internal;

import org.apache.hudi.common.config.TypedProperties;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Metrics reporter for reporting metrics which cannot be covered by org.apache.hudi.metrics.MetricsReporter
 * and HoodieDeltaStreamerMetrics.
 */
public class OnehouseMetricsReporter {
  private static final Logger LOG = LogManager.getLogger(OnehouseMetricsReporter.class);

  private static final String DEFAULT_K8_PUSH_GW_URL = "prometheus-stack-pushgw-prometheus-pushgateway.monitor.svc.cluster.local:9091";
  private static final String INIT_FAILURES_METRIC_NAME = "ohds_deltastreamer_numInitializationFailures";

  private static final CollectorRegistry REGISTRY = new CollectorRegistry();
  private static final Gauge NUM_INIT_FAILURES = Gauge.build()
      .name(INIT_FAILURES_METRIC_NAME)
      .help("Number of consecutive failures where table properties for the given job could not be initialized properly")
      .register(REGISTRY);

  private final PushGateway pg;

  public OnehouseMetricsReporter(TypedProperties props) {
    pg = new PushGateway(props.getString("onehouse.metrics.push.gateway.hostname.port", DEFAULT_K8_PUSH_GW_URL));
  }

  public void reportJobInitializationResult(String sourceTablePropsPath, boolean jobInitialized) {
    NUM_INIT_FAILURES.set(jobInitialized ? 0 : 1);
    try {
      pg.pushAdd(REGISTRY, sourceTablePropsPath);
    } catch (IOException e) {
      LOG.warn("Can't push monitoring information to pushGateway {" + sourceTablePropsPath + "}", e);
    }
  }
}
