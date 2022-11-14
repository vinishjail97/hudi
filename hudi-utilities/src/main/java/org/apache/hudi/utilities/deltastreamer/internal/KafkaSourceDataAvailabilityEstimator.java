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
import org.apache.hudi.config.OnehouseInternalDeltastreamerConfig;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implements logic to collect the total bytes yet to be ingested for a kafka topic in a
 * kafka cluster.
 *
 * NOTE: Eventually this class should be implemented by the {@link org.apache.hudi.utilities.sources.Source} class
 * that ingests data. To avoid changing the interface for
 */
public class KafkaSourceDataAvailabilityEstimator extends SourceDataAvailabilityEstimator {

  private static final Logger LOG = LogManager.getLogger(KafkaSourceDataAvailabilityEstimator.class);
  private static final Long DEFAULT_RECORD_SIZE = 1000L;

  private final Long maxBatchEvents;

  public KafkaSourceDataAvailabilityEstimator(JavaSparkContext jssc, TypedProperties properties) {
    super(jssc, properties);
    this.maxBatchEvents = properties.getLong(KafkaOffsetGen.Config.MAX_EVENTS_FROM_KAFKA_SOURCE_PROP.key(),
        KafkaOffsetGen.Config.MAX_EVENTS_FROM_KAFKA_SOURCE_PROP.defaultValue());
  }

  @Override
  Pair<SourceDataAvailabilityStatus, Long> getDataAvailabilityStatus(Option<String> lastCommittedCheckpointStr, Option<Long> averageRecordSizeInBytes, long sourceLimit) {
    long recordSizeBytes = (averageRecordSizeInBytes.isPresent() && averageRecordSizeInBytes.get() > 0L) ? averageRecordSizeInBytes.get() : DEFAULT_RECORD_SIZE;
    Long totalEventsAvailable = new KafkaTopicInfo(properties, lastCommittedCheckpointStr, recordSizeBytes, sourceLimit).eventsAvailableForTopic();

    // Either the aggr size of available kafka events is at least minSourceBytesIngestion
    // or if the number of events exceeds the max number of events per ingestion batch.
    Long estimatedBytesAvailableForIngestion = (totalEventsAvailable * recordSizeBytes);
    if (estimatedBytesAvailableForIngestion >= minSourceBytesIngestion || totalEventsAvailable > Math.min(sourceLimit, maxBatchEvents)) {
      return Pair.of(SourceDataAvailabilityStatus.SCHEDULE_IMMEDIATELY, estimatedBytesAvailableForIngestion);
    } else if (totalEventsAvailable > 0) {
      return Pair.of(SourceDataAvailabilityStatus.SCHEDULE_AFTER_MIN_SYNC_TIME, estimatedBytesAvailableForIngestion);
    }
    return Pair.of(SourceDataAvailabilityStatus.SCHEDULE_DEFER, 0L);
  }

  static class KafkaTopicInfo {

    private final KafkaClusterInfo clusterInfo;
    private final String topicName;
    private final Option<String> lastCommittedCheckpointStr;
    private final Long averageRecordSizeInBytes;

    KafkaTopicInfo(TypedProperties properties, Option<String> lastCommittedCheckpointStr, Long averageRecordSizeInBytes, long sourceLimit) {
      this.clusterInfo = KafkaClusterInfo.createOrGetInstance(properties);
      this.topicName = properties.getString(KafkaOffsetGen.Config.KAFKA_TOPIC_NAME.key());
      this.lastCommittedCheckpointStr = lastCommittedCheckpointStr;
      this.averageRecordSizeInBytes = averageRecordSizeInBytes;
    }

    /** The source status is computed as the difference between the latest offset per topic-partition and
    * the committed offset from the latest Hudi commit file. The load (num of events) is then summed across all
    * partitions of a topic.
    */
    Long eventsAvailableForTopic() {
      Map<Integer, Long> latestOffsets = clusterInfo.getLatestOffsetPerPartition(topicName);
      Map<Integer, Long> committedOffsets;
      if (lastCommittedCheckpointStr.isPresent() && !lastCommittedCheckpointStr.get().isEmpty()) {
        committedOffsets = KafkaOffsetGen.CheckpointUtils.strToOffsets(lastCommittedCheckpointStr.get())
            .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().partition(), Map.Entry::getValue));
      } else {
        committedOffsets = new HashMap<>();
      }
      long numUncommittedOffsets = 0L;
      for (Map.Entry<Integer, Long> entry : latestOffsets.entrySet()) {
        Long committedOffset = committedOffsets.getOrDefault(entry.getKey(), 0L);
        numUncommittedOffsets += (entry.getValue() - committedOffset) > 0 ? (entry.getValue() - committedOffset) : 0L;
      }

      LOG.info(String.format("Computed the total data waiting for ingest %s events %s bytes for the Kafka Topic: %s as with latestOffsets %s committedOffsets %s",
          numUncommittedOffsets, (numUncommittedOffsets * averageRecordSizeInBytes), topicName, latestOffsets, committedOffsets));

      return numUncommittedOffsets;
    }

    public String getTopicName() {
      return topicName;
    }
  }

  static class KafkaClusterInfo {
    public static final String KAFKA_SOURCE_RATE_ESTIMATOR_KEY = "bootstrap.servers";
    // In the case of Kafka, we do want to ingest in real-time, hence setting
    // a min sync interval of 10secs.
    private static final long MIN_SYNC_INTERVAL_MS = 15000;
    private static final Map<String, KafkaClusterInfo> CLUSTERS = new HashMap<>();
    private final TypedProperties props;
    private final Consumer consumer;


    private Map<TopicPartition, Long> lastOffsets = new HashMap<>();
    private long lastSyncTimeMs;

    private KafkaClusterInfo(TypedProperties props) {
      this.props = props;
      consumer = new KafkaConsumer(props);
      lastSyncTimeMs = 0L;
    }

    static synchronized KafkaClusterInfo createOrGetInstance(TypedProperties props) {
      String sourceId = props.getProperty(OnehouseInternalDeltastreamerConfig.DELTASTREAMER_SOURCE_ID.key());
      String bootstrapServers = props.getProperty(KAFKA_SOURCE_RATE_ESTIMATOR_KEY);
      // Use user-given source identifier if provided otherwise use bootstrapServers for caching offsets.
      String kafkaClusterIdentifier = sourceId != null ? sourceId : bootstrapServers;

      if (!CLUSTERS.containsKey(kafkaClusterIdentifier)) {
        KafkaClusterInfo clusterInfo = new KafkaClusterInfo(props);
        CLUSTERS.put(kafkaClusterIdentifier, clusterInfo);
      }
      return CLUSTERS.get(kafkaClusterIdentifier);
    }

    synchronized void refreshLatestOffsets() {
      if (lastSyncTimeMs > 0 && (System.currentTimeMillis() - lastSyncTimeMs) <= MIN_SYNC_INTERVAL_MS) {
        return;
      }
      lastSyncTimeMs = System.currentTimeMillis();

      Map<String, List<PartitionInfo>> topics = consumer.listTopics();
      List<TopicPartition> topicPartitions = topics.values().stream().flatMap(Collection::stream).map(partitionInfo ->
          new TopicPartition(partitionInfo.topic(), partitionInfo.partition())).collect(Collectors.toList());
      lastOffsets.putAll(consumer.endOffsets(topicPartitions));
    }

    Map<Integer, Long> getLatestOffsetPerPartition(String topicName) {
      refreshLatestOffsets();
      return lastOffsets.entrySet().stream().filter(offsets -> offsets.getKey().topic().equals(topicName)).collect(
          Collectors.toMap(x -> x.getKey().partition(), Map.Entry::getValue));
    }
  }
}
