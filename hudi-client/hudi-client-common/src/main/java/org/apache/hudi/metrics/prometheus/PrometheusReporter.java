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

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metrics.MetricsReporter;

import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.dropwizard.samplebuilder.DefaultSampleBuilder;
import io.prometheus.client.dropwizard.samplebuilder.SampleBuilder;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Implementation of Prometheus reporter, which connects to the Http server, and get metrics
 * from that server.
 */
public class PrometheusReporter extends MetricsReporter {
  private static final Pattern LABEL_PATTERN = Pattern.compile("\\s*,\\s*");

  private static final Logger LOG = LogManager.getLogger(PrometheusReporter.class);

  private static final Map<Integer, CollectorRegistry> PORT_TO_COLLECTOR_REGISTRY = new HashMap<>();
  private static final Map<Integer, HTTPServer> PORT_TO_SERVER = new HashMap<>();
  private final DropwizardExports metricExports;
  private final CollectorRegistry collectorRegistry;

  public PrometheusReporter(HoodieWriteConfig config, MetricRegistry registry) {
    this(config.getPushGatewayLabels(), config.getPrometheusPort(), registry);
  }

  public PrometheusReporter(String labelsAndValues, int serverPort, MetricRegistry registry) {
    if (!PORT_TO_SERVER.containsKey(serverPort) || !PORT_TO_COLLECTOR_REGISTRY.containsKey(serverPort)) {
      startHttpServer(serverPort);
    }
    List<String> labelNames = new ArrayList<>();
    List<String> labelValues = new ArrayList<>();
    if (!StringUtils.isNullOrEmpty(labelsAndValues)) {
      LABEL_PATTERN.splitAsStream(labelsAndValues.trim()).map(s -> s.split(":", 2))
          .forEach(parts -> {
            labelNames.add(parts[0]);
            labelValues.add(parts[1]);
          });
    }
    metricExports = new DropwizardExports(registry, new LabeledSampleBuilder(labelNames, labelValues));
    this.collectorRegistry = PORT_TO_COLLECTOR_REGISTRY.get(serverPort);
    metricExports.register(collectorRegistry);
  }

  private static synchronized void startHttpServer(int serverPort) {
    if (!PORT_TO_COLLECTOR_REGISTRY.containsKey(serverPort)) {
      PORT_TO_COLLECTOR_REGISTRY.put(serverPort, new CollectorRegistry());
    }
    if (!PORT_TO_SERVER.containsKey(serverPort)) {
      try {
        HTTPServer server = new HTTPServer(new InetSocketAddress(serverPort), PORT_TO_COLLECTOR_REGISTRY.get(serverPort));
        PORT_TO_SERVER.put(serverPort, server);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
      } catch (Exception e) {
        String msg = "Could not start PrometheusReporter HTTP server on port " + serverPort;
        LOG.error(msg, e);
        throw new HoodieException(msg, e);
      }
    }
  }

  @Override
  public void start() {
  }

  @Override
  public void report() {
  }

  @Override
  public void stop() {
    collectorRegistry.unregister(metricExports);
  }

  private static class LabeledSampleBuilder implements SampleBuilder {
    private final DefaultSampleBuilder defaultMetricSampleBuilder = new DefaultSampleBuilder();
    private final List<String> labelNames;
    private final List<String> labelValues;

    public LabeledSampleBuilder(List<String> labelNames, List<String> labelValues) {
      this.labelNames = labelNames;
      this.labelValues = labelValues;
    }

    @Override
    public Collector.MetricFamilySamples.Sample createSample(String dropwizardName, String nameSuffix, List<String> additionalLabelNames, List<String> additionalLabelValues, double value) {
      return defaultMetricSampleBuilder.createSample(
          dropwizardName,
          nameSuffix,
          labelNames,
          labelValues,
          value);
    }
  }
}
