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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.deltastreamer.DeltaSyncException;
import org.apache.hudi.utilities.functional.HoodieDeltaStreamerTestBase;
import org.apache.hudi.utilities.sources.HoodieIncrSource;
import org.apache.hudi.utilities.sources.JsonKafkaSource;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.transform.Transformer;

import com.codahale.metrics.Gauge;
import com.customer.CustomerExceptionThrowingTransformer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestOnehouseDeltaStreamer extends HoodieDeltaStreamerTestBase {
  private static final Logger LOG = LogManager.getLogger(TestOnehouseDeltaStreamer.class);
  private static final Integer KAFKA_NUM_RECORDS = 5;

  private final String sourceTablePropsKafka = UUID.randomUUID() + "-test-json-kafka-source.properties";
  private final String sourceTablePropsHudiIncr = UUID.randomUUID() + "-test-hudi-incr-source.properties";
  private final String sourceTablePropsInvalid = UUID.randomUUID() + "-invalid_job.properties";
  private final String sourceTableKafka = UUID.randomUUID() + "_test_json_kafka_table";
  private final String sourceTableHudiIncr = UUID.randomUUID() + "_hudi_incr_table";
  private final String topicNameForTest = UUID.randomUUID() + "-topic";

  private final ExecutorService executorService = Executors.newFixedThreadPool(1);
  private final HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

  @Override
  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
    executorService.shutdown();
  }

  @Test
  public void testOnehouseDeltaStreamer() throws Exception {
    // Source Props Paths.
    String sourceTablePropsPathKafka = TestHelpers.getConcatenatedPath(basePath, sourceTablePropsKafka);
    String sourceTablePropsHudiIncr = TestHelpers.getConcatenatedPath(basePath, this.sourceTablePropsHudiIncr);
    String invalidSourceTablePropsPath = TestHelpers.getConcatenatedPath(basePath, sourceTablePropsInvalid);
    List<String> multiSourceTablePropsPath = Arrays.asList(sourceTablePropsPathKafka, sourceTablePropsHudiIncr, invalidSourceTablePropsPath);

    // Table Base Paths.
    String tableBasePathKafka = TestHelpers.getConcatenatedPath(basePath, sourceTableKafka);
    String tableBasePathHudiIncr = TestHelpers.getConcatenatedPath(basePath, sourceTableHudiIncr);
    // Push data to Kafka.
    prepareDataForKafka(KAFKA_NUM_RECORDS, true, topicNameForTest);
    prepareJsonKafkaDFSSourceProps(sourceTablePropsPathKafka, tableBasePathKafka, topicNameForTest);
    // Push data to source hoodie table.
    String sourceHudiTablePath = basePath + "/source_hudi_incr";
    prepareHoodieIncrSourceProps(sourceTablePropsHudiIncr, tableBasePathHudiIncr);

    // Initialize OnehouseDeltaStreamer.
    OnehouseDeltaStreamer onehouseDeltaStreamer = new OnehouseDeltaStreamer(TestHelpers.makeConfig(multiSourceTablePropsPath), jsc);
    // Validate OnehouseDeltaStreamer start and shutdown with assertions.
    runOnehouseDeltaStreamer(onehouseDeltaStreamer);

    Thread.sleep(TimeUnit.SECONDS.toMillis(60));
    onehouseDeltaStreamer.shutdownGracefully();

    TestHelpers.assertRecordCount(5, tableBasePathKafka, sqlContext);
    TestHelpers.assertTimeline(2, 5, tableBasePathKafka, jsc.hadoopConfiguration());
    TestHelpers.assertRecordCount(20, tableBasePathHudiIncr, sqlContext);
    TestHelpers.assertTimeline(1, 5, tableBasePathHudiIncr, jsc.hadoopConfiguration());

    // Push more data to Kafka and source hoodie table.
    prepareDataForKafka(JSON_KAFKA_NUM_RECORDS, false, topicNameForTest);
    prepareDataForHoodieIncrSource(sourceHudiTablePath, "002");
    prepareDataForHoodieIncrSource(sourceHudiTablePath, "003");

    // Initialize OnehouseDeltaStreamer again with only kafka source.
    onehouseDeltaStreamer = new OnehouseDeltaStreamer(TestHelpers.makeConfig(Collections.singletonList(sourceTablePropsPathKafka)), jsc);
    runOnehouseDeltaStreamer(onehouseDeltaStreamer);

    // Add invalid table source to desired_job_status.properties.
    Thread.sleep(TimeUnit.SECONDS.toMillis(20));
    TestHelpers.updateDesiredJobStateProps(multiSourceTablePropsPath);

    // Pause kafka stream.
    Thread.sleep(TimeUnit.SECONDS.toMillis(20));
    TestHelpers.updateDesiredJobStateProps(Collections.singletonList(sourceTablePropsHudiIncr));

    // Clean and Restart kafka stream.
    Thread.sleep(TimeUnit.SECONDS.toMillis(60));
    TestHelpers.assertRecordCount(10, tableBasePathKafka, sqlContext);
    TestHelpers.assertTimeline(4, 5, tableBasePathKafka, jsc.hadoopConfiguration());
    TestHelpers.cleanAndRestartTableSource(tableBasePathKafka, multiSourceTablePropsPath);

    // Shutdown onehouseDeltaStreamer.
    Thread.sleep(TimeUnit.SECONDS.toMillis(90));
    onehouseDeltaStreamer.shutdownGracefully();

    TestHelpers.assertRecordCount(10, tableBasePathKafka, sqlContext);
    TestHelpers.assertTimeline(4, 5, tableBasePathKafka, jsc.hadoopConfiguration());
    TestHelpers.assertRecordCount(60, tableBasePathHudiIncr, sqlContext);
    TestHelpers.assertTimeline(3, 5, tableBasePathHudiIncr, jsc.hadoopConfiguration());
  }

  @Test
  public void transformerThrowsRuntimeError_internal() throws Exception {
    runKafkaErrorHandlingCase(Collections.singletonList(ExceptionThrowingTransformer.class.getName()), "source_uber.avsc", "target_uber.avsc", DeltaSyncException.Type.PLATFORM_TRANSFORM_EXECUTION);
  }

  @Test
  public void transformerThrowsRuntimeError_customer() throws Exception {
    runKafkaErrorHandlingCase(Collections.singletonList(CustomerExceptionThrowingTransformer.class.getName()), "source_uber.avsc", "target_uber.avsc", DeltaSyncException.Type.USER_TRANSFORM_EXECUTION
    );
  }

  @Test
  public void transformerHasInvalidSparkPlan() throws Exception {
    runKafkaErrorHandlingCase(Collections.singletonList(InvalidPlanTransformer.class.getName()), "source_uber.avsc", "target_uber.avsc", DeltaSyncException.Type.TRANSFORM_PLAN);
  }

  @Test
  public void dataDoesNotMatchTargetSchema() throws Exception {
    runKafkaErrorHandlingCase(null, "source_uber.avsc", "target.avsc", DeltaSyncException.Type.WRITE);
  }

  @Test
  public void dataDoesNotMatchTargetSchemaAfterTransform() throws Exception {
    runKafkaErrorHandlingCase(Collections.singletonList(IdentityTransformer.class.getName()), "source_uber.avsc", "target.avsc", DeltaSyncException.Type.SCHEMA_COMPATIBILITY);
  }

  @Test
  public void cannotReadSourceDataIntoSchema() throws Exception {
    runKafkaErrorHandlingCase(null, "source_evolved.avsc", "target_uber.avsc", DeltaSyncException.Type.SCHEMA_COMPATIBILITY);
  }

  @Test
  public void cannotReadSourceDataIntoSchemaWithTransform() throws Exception {
    runKafkaErrorHandlingCase(Collections.singletonList(IdentityTransformer.class.getName()), "source_evolved.avsc", "target_uber.avsc", DeltaSyncException.Type.SCHEMA_COMPATIBILITY);
  }

  private void runKafkaErrorHandlingCase(List<String> transformerClassNames, String sourceSchema, String targetSchema, DeltaSyncException.Type exceptionType)
      throws IOException, InterruptedException {
    // Source Props Paths.
    String sourceTablePropsPathKafka = TestHelpers.getConcatenatedPath(basePath, sourceTablePropsKafka);
    List<String> multiSourceTablePropsPath = Collections.singletonList(sourceTablePropsPathKafka);

    // Table Base Paths.
    String tableBasePathKafka = TestHelpers.getConcatenatedPath(basePath, sourceTableKafka);
    // Push data to Kafka.
    prepareDataForKafka(KAFKA_NUM_RECORDS, true, topicNameForTest);

    prepareJsonKafkaDFSSourceProps(sourceTablePropsPathKafka, tableBasePathKafka, topicNameForTest, transformerClassNames, sourceSchema,
        targetSchema);

    // Initialize OnehouseDeltaStreamer.
    OnehouseDeltaStreamer onehouseDeltaStreamer = new OnehouseDeltaStreamer(TestHelpers.makeConfig(multiSourceTablePropsPath), jsc);
    // Validate OnehouseDeltaStreamer start and shutdown with assertions.
    runOnehouseDeltaStreamer(onehouseDeltaStreamer);

    Thread.sleep(TimeUnit.SECONDS.toMillis(60));
    onehouseDeltaStreamer.shutdownGracefully();

    assertExpectedDeltaSyncFailureMetrics(tableBasePathKafka, exceptionType);
  }

  private void assertExpectedDeltaSyncFailureMetrics(String tableBasePath, DeltaSyncException.Type failureType) {
    Map<String, Gauge> gauges = TestMetricsReporter.getGaugesForTableBasePath(tableBasePath);
    for (DeltaSyncException.Type type : DeltaSyncException.Type.values()) {
      if (type == failureType) {
        assertEquals(1L, gauges.get("hoodie_trips.deltastreamer.failureType_" + type.name()).getValue());
      } else {
        assertEquals(0L, gauges.get("hoodie_trips.deltastreamer.failureType_" + type.name()).getValue());
      }
    }
  }

  private void runOnehouseDeltaStreamer(OnehouseDeltaStreamer onehouseDeltaStreamer) {
    executorService.execute(() -> {
      try {
        onehouseDeltaStreamer.sync();
      } catch (Exception e) {
        LOG.error("Failed to sync using OnehouseDeltaStreamer", e);
        throw new RuntimeException(e);
      }
    });
  }

  private void prepareHoodieIncrSourceProps(String sourceTablePropsPath, String tableBasePath) throws Exception {
    String sourceTablePath = basePath + "/source_hudi_incr";
    prepareDataForHoodieIncrSource(sourceTablePath, "001");

    // Properties used for testing delta-streamer with JsonKafka source
    TypedProperties props = new TypedProperties();
    props.setProperty("include", "base.properties");
    populateCommonProps(props, basePath);
    props.setProperty("hoodie.base.path", tableBasePath);
    props.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "driver");
    props.setProperty("hoodie.deltastreamer.source.hoodieincr.read_latest_on_missing_ckpt", "false");
    props.setProperty("hoodie.deltastreamer.source.hoodieincr.missing.checkpoint.strategy", "READ_UPTO_LATEST_COMMIT");
    props.setProperty("hoodie.datasource.write.operation", "INSERT");
    props.setProperty("hoodie.table.type", "COPY_ON_WRITE");
    props.setProperty("hoodie.deltastreamer.source.class.name", HoodieIncrSource.class.getName());
    props.setProperty("hoodie.deltastreamer.source.hoodieincr.path", sourceTablePath);
    props.setProperty("hoodie.table.name", "hoodie_trips");
    props.setProperty("hoodie.deltastreamer.min.sync.interval.secs", "5");
    props.setProperty("hoodie.deltastreamer.source.hoodieincr.num_instants", "1");
    UtilitiesTestBase.Helpers.savePropsToDFS(props, fs, sourceTablePropsPath);
  }

  private void prepareDataForHoodieIncrSource(String tableBasePath, String commitTime) throws IOException {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName("hoodie_trips")
        .setRecordKeyFields("_hoodie_record_key")
        .setPartitionFields("_hoodie_partition_path")
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(jsc.hadoopConfiguration(), tableBasePath);
    assertNotNull(metaClient);

    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder().forTable("hoodie_trips")
        .withPath(tableBasePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2);
    SparkRDDWriteClient writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), builder.build());
    writeClient.startCommitWithTime(commitTime);
    List<HoodieRecord> records = dataGen.generateInserts(commitTime, 20);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    List<WriteStatus> statuses = writeClient.insert(writeRecords, commitTime).collect();
    byte[] hoodieProps = new byte[1000];

    fs.open(new Path(tableBasePath + "/.hoodie/hoodie.properties")).read(hoodieProps);
    assertNoWriteErrors(statuses);
  }

  private void prepareJsonKafkaDFSSourceProps(String sourceTablePropsPath, String tableBasePath, String topicName) throws IOException {
    prepareJsonKafkaDFSSourceProps(sourceTablePropsPath, tableBasePath, topicName, null, "source_uber.avsc", "target_uber.avsc");
  }

  private void prepareJsonKafkaDFSSourceProps(String sourceTablePropsPath, String tableBasePath, String topicName, List<String> transformerClassNames,
                                              String sourceSchema, String targetSchema) throws IOException {
    // Properties used for testing delta-streamer with JsonKafka source
    TypedProperties props = new TypedProperties();
    populateAllCommonProps(props, basePath, testUtils.brokerAddress());
    props.setProperty("include", "base.properties");
    props.setProperty("hoodie.embed.timeline.server", "false");
    props.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "driver");
    props.setProperty("hoodie.deltastreamer.source.dfs.root", JSON_KAFKA_SOURCE_ROOT);
    props.setProperty("hoodie.deltastreamer.source.kafka.topic", topicName);
    props.setProperty(
        "hoodie.deltastreamer.schemaprovider.source.schema.file",
        basePath + "/" + sourceSchema);
    props.setProperty(
        "hoodie.deltastreamer.schemaprovider.target.schema.file",
        basePath + "/" + targetSchema);
    props.setProperty("auto.offset.reset", "earliest");

    props.setProperty("hoodie.base.path", tableBasePath);
    props.setProperty("hoodie.datasource.write.operation", "INSERT");
    props.setProperty("hoodie.table.type", "COPY_ON_WRITE");
    props.setProperty("hoodie.deltastreamer.source.class.name", JsonKafkaSource.class.getName());
    props.setProperty("hoodie.table.name", "hoodie_trips");
    props.setProperty("hoodie.deltastreamer.schema.provider.class.name", defaultSchemaProviderClassName);
    props.setProperty("hoodie.deltastreamer.source.estimator.class", "org.apache.hudi.utilities.deltastreamer.internal.KafkaSourceDataAvailabilityEstimator");
    props.setProperty("hoodie.deltastreamer.kafka.source.maxEvents", "3");
    props.setProperty("hoodie.deltastreamer.min.sync.interval.secs", "5");
    props.setProperty("hoodie.metrics.reporter.class", TestMetricsReporter.class.getName());
    props.setProperty("hoodie.metrics.on", "true");
    props.setProperty("hoodie.metrics.reporter.type", "INMEMORY");
    if (transformerClassNames != null && !transformerClassNames.isEmpty()) {
      props.setProperty("hoodie.deltastreamer.transformer.class.names", String.join(",", transformerClassNames));
    }
    UtilitiesTestBase.Helpers.savePropsToDFS(props, fs, sourceTablePropsPath);
  }

  private void prepareDataForKafka(int numRecords, boolean createTopic, String topicName) {
    if (createTopic) {
      try {
        testUtils.createTopic(topicName, 2);
      } catch (TopicExistsException e) {
        // no op
      }
    }
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.sendMessages(
        topicName,
        Helpers.jsonifyRecords(
            dataGenerator.generateInsertsAsPerSchema(
                "000", numRecords, HoodieTestDataGenerator.TRIP_SCHEMA)));
  }

  static class TestHelpers {

    static OnehouseDeltaStreamer.Config makeConfig(List<String> sourceTablePropsPath) throws IOException {
      String tablePropsFile = basePath + "/" + "desired_job_state.properties";
      UtilitiesTestBase.Helpers.saveStringsToDFS(sourceTablePropsPath.stream().map(s -> s + "=RUNNING").toArray(String[]::new), fs, tablePropsFile);
      OnehouseDeltaStreamer.Config config = new OnehouseDeltaStreamer.Config();
      config.syncOnce = false;
      config.tablePropsFile = tablePropsFile;
      return config;
    }

    static void cleanAndRestartTableSource(String tableBasePath, List<String> sourceTablePropPaths) {
      try {
        fs.delete(new Path(tableBasePath), true);
        TestHelpers.updateDesiredJobStateProps(sourceTablePropPaths);
      } catch (IOException e) {
        LOG.error("Failed to clean and re-start table", e);
        throw new RuntimeException(e);
      }
    }

    static void updateDesiredJobStateProps(List<String> sourceTablePropsPath) {
      String tablePropsFile = basePath + "/" + "desired_job_state.properties";
      try {
        Helpers.saveStringsToDFS(sourceTablePropsPath.stream().map(s -> s + "=RUNNING").toArray(String[]::new), fs, tablePropsFile);
      } catch (IOException e) {
        LOG.error("Failed to update desired_job_state.properties", e);
        throw new RuntimeException(e);
      }
    }

    static void assertRecordCount(long expected, String tablePath, SQLContext sqlContext) {
      sqlContext.clearCache();
      long recordCount = sqlContext.read().format("org.apache.hudi").load(tablePath).count();
      assertEquals(expected, recordCount);
    }

    static void assertTimeline(long commits, long deltaBetweenCommits, String tablePath, Configuration conf) throws ParseException {
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(conf).setBasePath(tablePath).build();
      List<HoodieInstant> instantList = metaClient.getActiveTimeline()
          .filterCompletedInstants()
          .filterCompletedAndCompactionInstants()
          .getInstants()
          .sorted()
          .collect(Collectors.toList());
      assertEquals(commits, instantList.size());
      for (int i = 1; i < instantList.size(); i++) {
        Duration duration = Duration.between(
            HoodieActiveTimeline.parseDateFromInstantTime(instantList.get(i - 1).getTimestamp()).toInstant(),
            HoodieActiveTimeline.parseDateFromInstantTime(instantList.get(i).getTimestamp()).toInstant()
        );
        assertTrue(duration.getSeconds() >= deltaBetweenCommits);
      }
    }

    static String getConcatenatedPath(String path1, String path2) {
      if (path1.endsWith("/")) {
        path1 = path1.substring(0, path1.length() - 1);
      }
      return String.format("%s/%s", path1, path2);
    }
  }

  /**
   * Throws an exception at runtime from the transformer to trigger a failure.
   */
  public static class ExceptionThrowingTransformer implements Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      return rowDataset.map(new ExceptionThrower(), rowDataset.exprEnc());
    }

    private static class ExceptionThrower implements MapFunction<Row, Row> {

      @Override
      public Row call(Row value) {
        throw new RuntimeException("Failure in internal transform");
      }
    }
  }

  /**
   * Identity transform to trigger delta sync flow that reads data into rows and converts back to avro.
   */
  public static class IdentityTransformer implements Transformer {

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      return rowDataset;
    }
  }

  /**
   * Creates an invalid spark plan so an error will be thrown when constructing the spark dag.
   */
  public static class InvalidPlanTransformer implements Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      return rowDataset.select("nonexistent_column");
    }
  }
}
