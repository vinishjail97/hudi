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
import org.apache.hudi.config.metrics.HoodieMetricsPrometheusConfig;
import org.apache.hudi.metrics.HoodieGauge;
import org.apache.hudi.metrics.prometheus.PrometheusReporter;

import com.codahale.metrics.MetricRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Metrics reporter for reporting metrics which cannot be covered by org.apache.hudi.metrics.MetricsReporter
 * and HoodieDeltaStreamerMetrics.
 */
public class OnehouseMetricsReporter {
  private static final String INIT_FAILURES_METRIC_NAME = "ohds_deltastreamer_numInitializationFailures";

  private static final Map<String, MetricState> PATH_TO_METRIC_STATE = new HashMap<>();
  private static final int DEFAULT_PORT = 9091;
  private final int serverPort;

  public OnehouseMetricsReporter(TypedProperties props) {
    this.serverPort = props.getInteger(HoodieMetricsPrometheusConfig.PROMETHEUS_PORT_NUM.key(), DEFAULT_PORT);
  }

  public void reportJobInitializationResult(String sourceTablePropsPath, boolean jobInitialized) {
    getMetricStateForPath(sourceTablePropsPath).getGauge().setValue(jobInitialized ? 0 : 1);
  }

  /**
   * Removes metrics for any paths that are no longer managed by the job
   * @param sourceTablePropsPaths the set of paths managed by the job
   */
  public void removeMetricsForMissingPaths(Set<String> sourceTablePropsPaths) {
    PATH_TO_METRIC_STATE.forEach((path, metricState) -> {
      if (!sourceTablePropsPaths.contains(path)) {
        metricState.getReporter().stop();
      }
    });
  }

  private MetricState getMetricStateForPath(String path) {
    if (!PATH_TO_METRIC_STATE.containsKey(path)) {
      setMetricStateForPath(path, serverPort);
    }
    return PATH_TO_METRIC_STATE.get(path);
  }

  private static synchronized void setMetricStateForPath(String path, int serverPort) {
    PATH_TO_METRIC_STATE.computeIfAbsent(path, key -> {
      MetricRegistry metricRegistry = new MetricRegistry();
      PrometheusReporter reporter = new PrometheusReporter("job:" + key, serverPort, metricRegistry);
      HoodieGauge<Integer> gauge = new HoodieGauge<>(0);
      metricRegistry.register(INIT_FAILURES_METRIC_NAME, gauge);
      return new MetricState(reporter, gauge);
    });
  }

  private static class MetricState {
    private final PrometheusReporter reporter;
    private final HoodieGauge<Integer> gauge;

    MetricState(PrometheusReporter reporter, HoodieGauge<Integer> gauge) {
      this.reporter = reporter;
      this.gauge = gauge;
    }

    HoodieGauge<Integer> getGauge() {
      return gauge;
    }

    PrometheusReporter getReporter() {
      return reporter;
    }
  }
}
