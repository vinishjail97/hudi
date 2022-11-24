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

package org.apache.hudi.metrics;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsDatadogConfig;
import org.apache.hudi.config.metrics.HoodieMetricsPrometheusConfig;
import org.apache.hudi.exception.HoodieException;

import com.codahale.metrics.MetricRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is the main class of the metrics system.
 */
public class Metrics {

  private static final Logger LOG = LogManager.getLogger(Metrics.class);

  private static final Map<String, Metrics> METRICS_INSTANCE_PER_BASEPATH = new HashMap<>();

  private final MetricRegistry registry;
  private final List<MetricsReporter> reporters;
  private final String commonMetricPrefix;
  private final String basePath;

  private Metrics(HoodieWriteConfig metricConfig) {
    registry = new MetricRegistry();
    commonMetricPrefix = metricConfig.getMetricReporterMetricsNamePrefix();
    reporters = new ArrayList<>();
    MetricsReporter defaultReporter = MetricsReporterFactory.createReporter(metricConfig, registry);
    if (defaultReporter != null) {
      reporters.add(defaultReporter);
    }
    reporters.addAll(addAdditionalMetricsExporters(metricConfig));
    if (reporters.size() == 0) {
      throw new RuntimeException("Cannot initialize Reporters.");
    }
    reporters.forEach(r -> r.start());
    basePath = metricConfig.getBasePath();

    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
  }

  private List<MetricsReporter> addAdditionalMetricsExporters(HoodieWriteConfig metricConfig) {
    List<MetricsReporter> reporterList = new ArrayList<>();
    if (!StringUtils.isNullOrEmpty(metricConfig.getMetricReporterFileBasedConfigs())) {
      Map<String, String> reporterLabelProps = getAdditionalMetricsExporterLabelProps(metricConfig);
      List<String> propPathList = StringUtils.split(metricConfig.getMetricReporterFileBasedConfigs(), ",");
      try (FileSystem fs = FSUtils.getFs(propPathList.get(0), new Configuration())) {
        for (String propPath: propPathList) {
          HoodieWriteConfig secondarySourceConfig = HoodieWriteConfig.newBuilder().fromInputStream(
              fs.open(new Path(propPath))).withProps(reporterLabelProps).withPath(metricConfig.getBasePath()).build();
          reporterList.add(MetricsReporterFactory.createReporter(secondarySourceConfig, registry));
        }
      } catch (IOException e) {
        LOG.error("Failed to add MetricsExporters", e);
        throw new HoodieException("failed to MetricsExporters", e);
      }
    }
    LOG.info("total additional metrics roporters added =" + reporterList.size());
    return reporterList;
  }

  private Map<String, String> getAdditionalMetricsExporterLabelProps(HoodieWriteConfig metricConfig) {
    Map<String, String> props = new HashMap<>();
    String defaultLabels = metricConfig.getMetricReporterDefaultLabels();
    if (!StringUtils.isNullOrEmpty(defaultLabels)) {
      props.put(HoodieMetricsPrometheusConfig.PUSHGATEWAY_LABELS.key(), defaultLabels);
      props.put(HoodieMetricsDatadogConfig.METRIC_TAG_VALUES.key(), defaultLabels);
    }
    return props;
  }

  private void registerHoodieCommonMetrics() {
    registerGauges(Registry.getAllMetrics(true, true), Option.of(commonMetricPrefix));
  }

  public static synchronized Metrics getInstance(HoodieWriteConfig metricConfig) {
    String basePath = metricConfig.getBasePath();
    if (METRICS_INSTANCE_PER_BASEPATH.containsKey(basePath)) {
      return METRICS_INSTANCE_PER_BASEPATH.get(basePath);
    }

    Metrics metrics = new Metrics(metricConfig);
    METRICS_INSTANCE_PER_BASEPATH.put(basePath, metrics);
    return metrics;
  }

  public synchronized void shutdown() {
    try {
      registerHoodieCommonMetrics();
      reporters.forEach(r -> r.report());
      LOG.info("Stopping the metrics reporter for base path: " + basePath);
      reporters.forEach(r -> r.stop());
      METRICS_INSTANCE_PER_BASEPATH.remove(basePath);
    } catch (Exception e) {
      LOG.warn("Error while closing reporter for base path: " + basePath, e);
    }
  }

  public synchronized void flush() {
    try {
      LOG.info("Reporting and flushing all metrics");
      registerHoodieCommonMetrics();
      reporters.forEach(r -> r.report());
      registry.getNames().forEach(this.registry::remove);
    } catch (Exception e) {
      LOG.error("Error while reporting and flushing metrics", e);
    }
  }

  public void registerGauges(Map<String, Long> metricsMap, Option<String> prefix) {
    String metricPrefix = prefix.isPresent() ? prefix.get() + "." : "";
    metricsMap.forEach((k, v) -> registerGauge(metricPrefix + k, v));
  }

  public void registerGauges(Map<String, Long> metricsMap, Option<String> prefix, Map<String, String> labels) {
    String metricPrefix = prefix.isPresent() ? prefix.get() + "." : "";
    metricsMap.forEach((k, v) -> registerGauge(metricPrefix + MetricUtils.getMetricWithLabel(k, labels), v));
  }

  public void registerGauge(String metricName, final long value) {
    try {
      HoodieGauge guage = (HoodieGauge) registry.gauge(metricName, () -> new HoodieGauge<>(value));
      guage.setValue(value);
    } catch (Exception e) {
      // Here we catch all exception, so the major upsert pipeline will not be affected if the
      // metrics system has some issues.
      LOG.error("Failed to send metrics: ", e);
    }
  }

  public MetricRegistry getRegistry() {
    return registry;
  }

  public static boolean isInitialized(String basePath) {
    return METRICS_INSTANCE_PER_BASEPATH.containsKey(basePath);
  }

  public static void shutdownAll() {
    List<Metrics> activeMetricsInstances = new ArrayList<>(METRICS_INSTANCE_PER_BASEPATH.values());
    activeMetricsInstances.forEach(Metrics::shutdown);
  }
}