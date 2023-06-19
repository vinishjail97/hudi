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

package org.apache.hudi.utilities.sources;

import java.util.Properties;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.deltastreamer.JsonQuarantineTableWriter;
import org.apache.hudi.utilities.deltastreamer.QuarantineTableWriterInterface;
import org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.Config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.config.HoodieQuarantineTableConfig.QUARANTINE_TABLE_BASE_PATH;
import static org.apache.hudi.config.HoodieQuarantineTableConfig.QUARANTINE_TARGET_TABLE;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_OFFSET_COLUMN;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_PARTITION_COLUMN;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_TIMESTAMP_COLUMN;
import static org.apache.hudi.utilities.schema.SchemaPostProcessor.Config.SCHEMA_POST_PROCESSOR_PROP;
import static org.apache.hudi.utilities.sources.JsonKafkaSource.Config.KAFKA_JSON_VALUE_DESERIALIZER_CLASS;
import static org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.Config.ENABLE_KAFKA_COMMIT_OFFSET;
import static org.apache.hudi.utilities.testutils.UtilitiesTestBase.Helpers.jsonifyRecords;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests against {@link JsonKafkaSource}.
 */
public class TestJsonKafkaSource extends BaseTestKafkaSource {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final HoodieTestDataGenerator DATA_GENERATOR = new HoodieTestDataGenerator(1L);
  static final URL SCHEMA_FILE_URL = TestJsonKafkaSource.class.getClassLoader().getResource("delta-streamer-config/source.avsc");

  @TempDir protected Path tmpDir;

  @BeforeEach
  public void init() throws Exception {
    String schemaFilePath = Objects.requireNonNull(SCHEMA_FILE_URL).toURI().getPath();
    TypedProperties props = new TypedProperties();
    props.put("hoodie.deltastreamer.schemaprovider.source.schema.file", schemaFilePath);
    schemaProvider = new FilebasedSchemaProvider(props, jsc());
  }

  @AfterAll
  public static void teardown() {
    DATA_GENERATOR.close();
  }

  @Override
  TypedProperties createPropsForKafkaSource(String topic, Long maxEventsToReadFromKafkaSource, String resetStrategy) {
    return createPropsForJsonKafkaSource(testUtils.brokerAddress(), topic, maxEventsToReadFromKafkaSource, resetStrategy);
  }

  static TypedProperties createPropsForJsonKafkaSource(String brokerAddress, String topic, Long maxEventsToReadFromKafkaSource, String resetStrategy) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.deltastreamer.source.kafka.topic", topic);
    props.setProperty("bootstrap.servers", brokerAddress);
    props.setProperty("auto.offset.reset", resetStrategy);
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.setProperty("hoodie.deltastreamer.kafka.source.maxEvents",
        maxEventsToReadFromKafkaSource != null ? String.valueOf(maxEventsToReadFromKafkaSource) :
            String.valueOf(Config.MAX_EVENTS_FROM_KAFKA_SOURCE_PROP.defaultValue()));
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    return props;
  }

  @Override
  SourceFormatAdapter createSource(TypedProperties props) {
    return new SourceFormatAdapter(new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics));
  }

  // test whether empty messages can be filtered
  @Test
  public void testJsonKafkaSourceFilterNullMsg() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testJsonKafkaSourceFilterNullMsg";
    testUtils.createTopic(topic, 2);
    TypedProperties props = createPropsForKafkaSource(topic, null, "earliest");

    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Option.empty(), kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());
    // Send  1000 non-null messages to Kafka
    sendMessagesToKafka(topic, 1000, 2);
    // Send  100 null messages to Kafka
    testUtils.sendMessages(topic, new String[100]);
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    // Verify that messages with null values are filtered
    assertEquals(1000, fetch1.getBatch().get().count());
  }

  @Test
  public void testJsonKafkaSourceWithJsonSchemaDeserializer() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testJsonKafkaSourceWithJsonSchemaDeserializer";
    testUtils.createTopic(topic, 2);
    TypedProperties props = createPropsForKafkaSource(topic, null, "earliest");
    props.put(KAFKA_JSON_VALUE_DESERIALIZER_CLASS.key(),
        "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer");
    props.put("schema.registry.url", "mock://127.0.0.1:8081");

    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Option.empty(),
        kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());
    // Send  1000 non-null messages to Kafka
    List<RawTripTestPayload> insertRecords = DATA_GENERATOR.generateInserts("000", 1000)
        .stream()
        .map(hr -> (RawTripTestPayload) hr.getData()).collect(Collectors.toList());
    sendMessagesToKafkaWithJsonSchemaSerializer(topic, 2, insertRecords);
    // send 200 null messages to Kafka
    List<RawTripTestPayload> nullInsertedRecords = Arrays.asList(new RawTripTestPayload[200]);
    sendMessagesToKafkaWithJsonSchemaSerializer(topic, 2, nullInsertedRecords);
    InputBatch<JavaRDD<GenericRecord>> fetch1 =
        kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    // Verify that messages with null values are filtered
    assertEquals(1000, fetch1.getBatch().get().count());
  }

  @Test
  public void testJsonKafkaSourceWithDefaultUpperCap() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testJsonKafkaSourceWithDefaultUpperCap";
    testUtils.createTopic(topic, 2);
    TypedProperties props = createPropsForKafkaSource(topic, Long.MAX_VALUE, "earliest");

    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    /*
    1. Extract without any checkpoint => get all the data, respecting default upper cap since both sourceLimit and
    maxEventsFromKafkaSourceProp are set to Long.MAX_VALUE
     */
    sendMessagesToKafka(topic, 1000, 2);
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(1000, fetch1.getBatch().get().count());

    // 2. Produce new data, extract new data based on sourceLimit
    sendMessagesToKafka(topic, 1000, 2);
    InputBatch<Dataset<Row>> fetch2 =
        kafkaSource.fetchNewDataInRowFormat(Option.of(fetch1.getCheckpointForNextBatch()), 1500);
    assertEquals(1000, fetch2.getBatch().get().count());
  }

  @Test
  public void testJsonKafkaSourceWithConfigurableUpperCap() {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testJsonKafkaSourceWithConfigurableUpperCap";
    testUtils.createTopic(topic, 2);
    TypedProperties props = createPropsForKafkaSource(topic, 500L, "earliest");

    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    sendMessagesToKafka(topic, 1000, 2);
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 900);
    assertEquals(900, fetch1.getBatch().get().count());

    // 2. Produce new data, extract new data based on upper cap
    sendMessagesToKafka(topic, 1000, 2);
    InputBatch<Dataset<Row>> fetch2 =
        kafkaSource.fetchNewDataInRowFormat(Option.of(fetch1.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(500, fetch2.getBatch().get().count());

    //fetch data respecting source limit where upper cap > sourceLimit
    InputBatch<JavaRDD<GenericRecord>> fetch3 =
        kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch1.getCheckpointForNextBatch()), 400);
    assertEquals(400, fetch3.getBatch().get().count());

    //fetch data respecting source limit where upper cap < sourceLimit
    InputBatch<JavaRDD<GenericRecord>> fetch4 =
        kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch2.getCheckpointForNextBatch()), 600);
    assertEquals(600, fetch4.getBatch().get().count());

    // 3. Extract with previous checkpoint => gives same data back (idempotent)
    InputBatch<JavaRDD<GenericRecord>> fetch5 =
        kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch1.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(fetch2.getBatch().get().count(), fetch5.getBatch().get().count());
    assertEquals(fetch2.getCheckpointForNextBatch(), fetch5.getCheckpointForNextBatch());

    // 4. Extract with latest checkpoint => no new data returned
    InputBatch<JavaRDD<GenericRecord>> fetch6 =
        kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch4.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(Option.empty(), fetch6.getBatch());
  }

  @Override
  void sendMessagesToKafka(String topic, int count, int numPartitions) {
    try {
      Tuple2<String, String>[] keyValues = new Tuple2[count];
      String[] records = jsonifyRecords(DATA_GENERATOR.generateInserts("000", count));
      for (int i = 0; i < count; i++) {
        keyValues[i] = new Tuple2<>(Integer.toString(i % numPartitions), records[i]);
      }
      testUtils.sendMessages(topic, keyValues);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  void sendJsonSafeMessagesToKafka(String topic, int count, int numPartitions) {
    try {
      Tuple2<String, String>[] keyValues = new Tuple2[count];
      String[] records = jsonifyRecords(DATA_GENERATOR.generateInserts("000", count));
      for (int i = 0; i < count; i++) {
        // Drop fields that don't translate to json properly
        Map node = OBJECT_MAPPER.readValue(records[i], Map.class);
        node.remove("height");
        node.remove("current_date");
        node.remove("nation");
        keyValues[i] = new Tuple2<>(Integer.toString(i % numPartitions), OBJECT_MAPPER.writeValueAsString(node));
      }
      testUtils.sendMessages(topic, keyValues);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void sendMessagesToKafkaWithJsonSchemaSerializer(String topic, int numPartitions,
                                                           List<RawTripTestPayload> insertRecords) {
    Properties config = getProducerPropertiesForJsonKafkaSchemaSerializer();
    try (Producer<String, RawTripTestPayload> producer = new KafkaProducer<>(config)) {
      for (int i = 0; i < insertRecords.size(); i++) {
        // use consistent keys to get even spread over partitions for test expectations
        producer.send(new ProducerRecord<>(topic, Integer.toString(i % numPartitions),
            insertRecords.get(i)));
      }
    }
  }

  private Properties getProducerPropertiesForJsonKafkaSchemaSerializer() {
    Properties props = new Properties();
    props.put("bootstrap.servers", testUtils.brokerAddress());
    props.put("value.serializer",
        "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");
    props.put("value.deserializer",
        "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer");
    // Key serializer is required.
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("schema.registry.url", "mock://127.0.0.1:8081");
    props.put("auto.register.schemas", "true");
    // wait for all in-sync replicas to ack sends
    props.put("acks", "all");
    return props;
  }

  @Test
  public void testErrorEventsForDataInRowFormat() throws IOException {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testErrorEventsRow";

    testUtils.createTopic(topic, 2);
    sendJsonSafeMessagesToKafka(topic, 1000, 2);
    testUtils.sendMessages(topic, new String[]{"error_event1", "error_event2"});

    TypedProperties props = createPropsForKafkaSource(topic, null, "earliest");
    props.put(ENABLE_KAFKA_COMMIT_OFFSET.key(), "true");
    props.put(QUARANTINE_TABLE_BASE_PATH.key(), tmpDir.toString() + "/qurantine_table_test/json_kafka_row_events");
    props.put(QUARANTINE_TARGET_TABLE.key(), "json_kafka_row_events");
    props.put("hoodie.base.path", tmpDir.toString() + "/json_kafka_row_events");
    props.put("hoodie.deltastreamer.quarantinetable.validate.targetschema.enable", "true");
    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    Option<QuarantineTableWriterInterface> quarantineTableWriterInterface = Option.of(new JsonQuarantineTableWriter(new HoodieDeltaStreamer.Config(),
        spark(), props, new HoodieSparkEngineContext(jsc()), fs()));
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource, quarantineTableWriterInterface, Option.of(props));
    String instantTime =  quarantineTableWriterInterface.get().startCommit();
    assertEquals(1000, kafkaSource.fetchNewDataInRowFormat(Option.empty(),Long.MAX_VALUE).getBatch().get().count());
    assertEquals(2,((JavaRDD)quarantineTableWriterInterface.get().getErrorEvents(instantTime, Option.empty()).get()).count());
  }

  @Test
  public void testErrorEventsForDataInAvroFormat() throws IOException {
    // topic setup.
    final String topic = TEST_TOPIC_PREFIX + "testErrorEventsAvro";

    testUtils.createTopic(topic, 2);
    sendMessagesToKafka(topic, 1000, 2);
    testUtils.sendMessages(topic, new String[]{"error_event1", "error_event2"});

    TypedProperties props = createPropsForKafkaSource(topic, null, "earliest");
    props.put(ENABLE_KAFKA_COMMIT_OFFSET.key(), "true");
    props.put(QUARANTINE_TABLE_BASE_PATH.key(), tmpDir.toString() + "/qurantine_table_test/json_kafka_events");
    props.put(QUARANTINE_TARGET_TABLE.key(), "json_kafka_events");
    props.put("hoodie.base.path", "/tmp/json_kafka_events");

    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    Option<QuarantineTableWriterInterface> quarantineTableWriterInterface = Option.of(new JsonQuarantineTableWriter(new HoodieDeltaStreamer.Config(),
        spark(), props, new HoodieSparkEngineContext(jsc()), fs()));
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource, quarantineTableWriterInterface, Option.of(props));
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(1000, fetch1.getBatch().get().count());
    String instantTime =  quarantineTableWriterInterface.get().startCommit();
    assertEquals(2, ((JavaRDD) quarantineTableWriterInterface.get().getErrorEvents(instantTime,Option.empty()).get()).count());
    quarantineTableWriterInterface.get().upsertAndCommit(instantTime,Option.empty());
    sendMessagesToKafka(topic, 1000, 2);
    testUtils.sendMessages(topic, new String[]{"error_event12", "error_event21", "error_events_31"});
    InputBatch<JavaRDD<GenericRecord>> fetch2 = kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch1.getCheckpointForNextBatch()),Long.MAX_VALUE);
    fetch2.getBatch().get().count();
    String instantTime2 =  quarantineTableWriterInterface.get().startCommit();
    assertEquals(3, ((JavaRDD)quarantineTableWriterInterface.get().getErrorEvents(instantTime2,Option.of(instantTime)).get()).count());
    quarantineTableWriterInterface.get().upsertAndCommit(instantTime2,Option.of(instantTime));
    // counts after 2 commits should match for unique events
    long count = spark().read().format("hudi").load(quarantineTableWriterInterface.get().getQuarantineTableWriteConfig().getBasePath()).count();
    assertEquals(5, count);
    testUtils.sendMessages(topic, new String[]{"error_event12", "error_event21", "error_events_31"});
    InputBatch<JavaRDD<GenericRecord>> fetch3 = kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch1.getCheckpointForNextBatch()),Long.MAX_VALUE);
    String instantTime3 =  quarantineTableWriterInterface.get().startCommit();

    quarantineTableWriterInterface.get().upsertAndCommit(instantTime3,Option.of(instantTime2));
    // duplicate event should get upserted
    assertEquals(5,spark().read().format("hudi").load(quarantineTableWriterInterface.get().getQuarantineTableWriteConfig().getBasePath()).count());
  }

  @Test
  public void testAppendKafkaOffset() {
    final String topic = TEST_TOPIC_PREFIX + "testKafkaOffsetAppend";
    testUtils.createTopic(topic, 2);
    sendMessagesToKafka(topic, 10, 2);

    TypedProperties props = createPropsForKafkaSource(topic, null, "earliest");
    Source jsonSource = new JsonKafkaSource(props, jsc(), spark(), schemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource, Option.empty(), Option.of(props));
    List<String> columns = Arrays.stream(kafkaSource.fetchNewDataInRowFormat(Option.empty(), Long.MAX_VALUE)
        .getBatch().get().columns()).collect(Collectors.toList());

    props.put(SCHEMA_POST_PROCESSOR_PROP, "org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor");
    SchemaProvider postSchemaProvider = UtilHelpers.wrapSchemaProviderWithPostProcessor(
        schemaProvider, props, jsc(), new ArrayList<>());
    jsonSource = new JsonKafkaSource(props, jsc(), spark(), postSchemaProvider, metrics);
    kafkaSource = new SourceFormatAdapter(jsonSource, Option.empty(), Option.of(props));
    List<String> withKafkaOffsetColumns = Arrays.stream(kafkaSource.fetchNewDataInRowFormat(Option.empty(), Long.MAX_VALUE)
        .getBatch().get().columns()).collect(Collectors.toList());

    assertEquals(3, withKafkaOffsetColumns.size() - columns.size());
    List<String> appendList = Arrays.asList(KAFKA_SOURCE_OFFSET_COLUMN, KAFKA_SOURCE_PARTITION_COLUMN, KAFKA_SOURCE_TIMESTAMP_COLUMN);
    assertEquals(appendList, withKafkaOffsetColumns.subList(withKafkaOffsetColumns.size() - 3, withKafkaOffsetColumns.size()));
  }

}
