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

package org.apache.hudi.metrics.prometheus;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.metrics.MetricUtils;
import org.apache.hudi.metrics.Metrics;
import org.apache.hudi.metrics.MetricsReporterType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestPushGateWayReporter {

  static final URL PROP_FILE_PROMETHEUS_URL = TestPushGateWayReporter.class.getClassLoader().getResource("prometheus.properties");
  static final URL PROP_FILE_DATADOG_URL = TestPushGateWayReporter.class.getClassLoader().getResource("datadog.properties");

  @Mock
  HoodieWriteConfig config;
  HoodieMetrics hoodieMetrics;
  Metrics metrics;

  @AfterEach
  void shutdownMetrics() {
    metrics.shutdown();
  }

  @Test
  public void testRegisterGauge() {
    addConfigs();
    hoodieMetrics = new HoodieMetrics(config);
    metrics = hoodieMetrics.getMetrics();

    assertDoesNotThrow(() -> {
      new HoodieMetrics(config);
    });

    metrics.registerGauge("pushGateWayReporter_metric", 123L);
    assertEquals("123", metrics.getRegistry().getGauges()
        .get("pushGateWayReporter_metric").getValue().toString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMultiReporter(boolean addDefaultReporter) throws IOException, InterruptedException, URISyntaxException {

    String propPrometheusPath = Objects.requireNonNull(PROP_FILE_PROMETHEUS_URL).toURI().getPath();
    String propDatadogPath = Objects.requireNonNull(PROP_FILE_DATADOG_URL).toURI().getPath();
    if (addDefaultReporter) {
      addConfigs();
    } else {
      when(config.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());
      when(config.isMetricsOn()).thenReturn(true);
    }
    when(config.getMetricReporterFileBasedConfigs()).thenReturn(propPrometheusPath + "," + propDatadogPath);
    hoodieMetrics = new HoodieMetrics(config);
    metrics = hoodieMetrics.getMetrics();
    Map<String, Long> metricsMap = new HashMap<>();
    Map<String, Long> labellessMetricMap = new HashMap<>();
    Map<String, String> labels = new HashMap<>();
    labels.put("group", "a");
    labels.put("job", "0");
    metricsMap.put("with_label_metric", 1L);
    labellessMetricMap.put("without_label_metric", 1L);
    metrics.registerGauges(metricsMap, Option.empty(), labels);
    metrics.registerGauges(labellessMetricMap, Option.empty());
    List<String> metricKeys = metrics.getRegistry().getGauges().keySet().stream().collect(Collectors.toList());
    assertEquals(0, MetricUtils.getLabelsAndMetricMap(metricKeys.stream()
        .filter(x -> x.contains("without_label_metric")).findFirst().get()).getValue().size());
    assertEquals(labels, MetricUtils.getLabelsAndMetricMap(metricKeys.stream()
        .filter(x -> x.contains("with_label_metric")).findFirst().get()).getValue());
  }

  private void addConfigs() {
    when(config.isMetricsOn()).thenReturn(true);
    when(config.getTableName()).thenReturn("foo");
    when(config.getMetricsReporterType()).thenReturn(MetricsReporterType.PROMETHEUS_PUSHGATEWAY);
    when(config.getPushGatewayHost()).thenReturn("localhost");
    when(config.getPushGatewayPort()).thenReturn(9091);
    when(config.getPushGatewayReportPeriodSeconds()).thenReturn(30);
    when(config.getPushGatewayDeleteOnShutdown()).thenReturn(true);
    when(config.getPushGatewayJobName()).thenReturn("foo");
    when(config.getBasePath()).thenReturn("s3://test" + UUID.randomUUID());
  }
}
