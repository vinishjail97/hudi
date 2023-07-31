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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.CloudDataFetcher;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;
import org.apache.hudi.utilities.sources.helpers.QueryRunner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestS3EventsHoodieIncrSource extends SparkClientFunctionalTestHarness {
  private static final Schema S3_METADATA_SCHEMA = SchemaTestUtil.getSchemaFromResource(
      TestS3EventsHoodieIncrSource.class, "/delta-streamer-config/s3-metadata.avsc", true);

  private ObjectMapper mapper = new ObjectMapper();

  private static final String MY_BUCKET = "some-bucket";
  private static final String PATH_ONE = "path/to/";
  private static final String PATH_ONE_WITHOUT_TRAILING_SLASH = "path/to";
  private static final List<String> PATH_ONE_FILES =
      Arrays.asList(
          PATH_ONE + "customers_2023.csv",
          PATH_ONE + "sales_2022.parquet",
          PATH_ONE + "inventory.gz",
          PATH_ONE + "report_2021.txt",
          PATH_ONE + "subpath1/Q2_2022/revenue.csv",
          PATH_ONE + "subpath1/Q2_2022/sales.parquet",
          PATH_ONE + "subpath1/Q2_2022/products.gz",
          PATH_ONE + "subpath2/Q3_2023/invoice.txt",
          PATH_ONE + "subpath2/Q3_2023/costs.csv",
          PATH_ONE + "subpath2/Q4_2021/expenses.parquet",
          PATH_ONE + "subpath2/Q4_2021/forecast.gz",
          PATH_ONE + "subpath3/Q4_2021/summary.txt",
          PATH_ONE + "subpath4/subpath3/Q4_2021/summary.txt");

  @Mock
  private SchemaProvider mockSchemaProvider;
  @Mock
  QueryRunner mockQueryRunner;
  @Mock
  CloudDataFetcher mockCloudDataFetcher;
  private JavaSparkContext jsc;
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws IOException {
    jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
    metaClient = getHoodieMetaClient(hadoopConf(), basePath());
  }

  @ParameterizedTest
  @MethodSource("generateFiltersForS3EventsHoodieIncrSource")
  public void testS3EventsHoodieIncrSourceFiltering(List<String> filePaths,
                                                    String keyPrefix,
                                                    String fileExtension,
                                                    String pathRegex,
                                                    Long objectSize,
                                                    String fileFormat,
                                                    String commitTime,
                                                    List<String> expectedFilePaths) {
    JavaRDD<String> testRdd = jsc.parallelize(getSampleS3ObjectKeys(objectSize, filePaths, commitTime), 2);
    Dataset<Row> inputDs = spark().read().json(testRdd);

    S3EventsHoodieIncrSource s3EventsHoodieIncrSource = new S3EventsHoodieIncrSource(
        createPropsForS3EventsHoodieIncrSource(basePath(), keyPrefix, fileExtension, pathRegex),
        jsc, spark(), mockSchemaProvider);
    List<String> resultKeys = s3EventsHoodieIncrSource.applyFilter(inputDs, fileFormat)
        .select("s3.object.key").collectAsList()
        .stream().map(row -> row.getString(0)).sorted().collect(Collectors.toList());
    assertEquals(expectedFilePaths, resultKeys);
  }

  private static Stream<Arguments> generateFiltersForS3EventsHoodieIncrSource() {
    return Stream.of(
        // s3.object.size is 0, expect no files
        Arguments.of(PATH_ONE_FILES, PATH_ONE, ".csv", ".*", 0L, ".csv", "1", Collections.emptyList()),
        // s3.key.prefix is PATH_ONE, match all csv files by file format and match all regex.
        Arguments.of(PATH_ONE_FILES, PATH_ONE, null, ".*", 100L, ".csv", "1",
            getSortedFilePathsByExtension(PATH_ONE_FILES, ".csv")),
        // s3.key.prefix is PATH_ONE and regex is null, match all gz files by file extension
        Arguments.of(PATH_ONE_FILES, PATH_ONE, ".gz", null, 100L, null, "1",
            getSortedFilePathsByExtension(PATH_ONE_FILES, ".gz")),
        // s3.key.prefix is PATH_ONE and regex is empty, match all parquet files by extension.
        Arguments.of(PATH_ONE_FILES, PATH_ONE, ".parquet", "", 100L, "", "1",
            getSortedFilePathsByExtension(PATH_ONE_FILES, ".parquet")),
        // s3.key.prefix is null, match all csv files by extension.
        Arguments.of(PATH_ONE_FILES, null, ".csv", null, 100L, null, "1",
            getSortedFilePathsByExtension(PATH_ONE_FILES, ".csv")),
        // s3.key.prefix is empty, match all gz files by extension.
        Arguments.of(PATH_ONE_FILES, "", ".gz", null, 100L, null, "1",
            getSortedFilePathsByExtension(PATH_ONE_FILES, ".gz")),
        // match all gz files in subpath1 by regex and extension
        Arguments.of(PATH_ONE_FILES, PATH_ONE, ".gz", "subpath1/.*\\.gz", 100L, null, "1",
            getSortedFilePathsByExtension(PATH_ONE_FILES, ".gz")
                .stream()
                .filter(path -> path.contains("subpath1")).sorted().collect(Collectors.toList())),
        // match all txt files by extension and prefix with no backslash
        Arguments.of(PATH_ONE_FILES, PATH_ONE_WITHOUT_TRAILING_SLASH, ".txt", ".*", 100L, null, "1",
            getSortedFilePathsByExtension(PATH_ONE_FILES, ".txt")),
        // match all txt files by extension but pick those starting with subpath3 only by regex.
        Arguments.of(PATH_ONE_FILES, PATH_ONE_WITHOUT_TRAILING_SLASH, ".txt", "subpath3/.*\\.txt", 100L, null, "1",
            Arrays.asList(PATH_ONE + "subpath3/Q4_2021/summary.txt")),
        // match all parquet files under under subpath1
        Arguments.of(PATH_ONE_FILES, PATH_ONE, ".parquet", "subpath1/.*\\.parquet", 100L, null, "1",
            getSortedFilePathsByExtension(PATH_ONE_FILES, ".parquet")
                .stream()
                .filter(path -> path.contains("subpath1"))
                .sorted().collect(Collectors.toList())),
        // match all parquet files under subpath1 and subpath2 by using "|" in regex
        Arguments.of(PATH_ONE_FILES, PATH_ONE, ".parquet",
            "subpath1/.*\\.parquet|subpath2/.*\\.parquet", 100L, null, "1",
            getSortedFilePathsByExtension(PATH_ONE_FILES, ".parquet")
                .stream()
                .filter(path -> path.contains("subpath1") || path.contains("subpath2"))
                .sorted().collect(Collectors.toList())),
        // conflicting extension and regex to not return any files
        Arguments.of(PATH_ONE_FILES, PATH_ONE, ".csv", "subpath1/.*\\.gz", 100L, null, "1",
            Collections.emptyList()));
  }

  @ParameterizedTest
  @MethodSource("generateS3RegexFilters")
  public void testGenerateS3RegexFilters(String keyPrefix, String pathRegex, String expectedFilter) {
    S3EventsHoodieIncrSource s3EventsHoodieIncrSource = new S3EventsHoodieIncrSource(
        createPropsForS3EventsHoodieIncrSource(basePath(), keyPrefix, ".unused", pathRegex),
        jsc, spark(), mockSchemaProvider);
    assertEquals(expectedFilter, s3EventsHoodieIncrSource.generateS3KeyRegexFilter(keyPrefix,
        pathRegex));
  }

  private static Stream<Arguments> generateS3RegexFilters() {
    return Stream.of(
        // Key prefix is null, path regex is ".*"
        Arguments.of(null, ".*", " and s3.object.key rlike '^.*'"),
        // key prefix is empty, path regex is ".*\\.parquet"
        Arguments.of("", ".*\\.parquet", " and s3.object.key rlike '^.*\\.parquet'"),
        // key prefix is "path/to/", path regex is ".*"
        Arguments.of("path/to/", ".*", " and regexp_replace(s3.object.key, '^path/to/', '') rlike '^.*'"),
        // key prefix is "path/to/another", path regex is ".*\\.parquet"
        Arguments.of("path/to/another", ".*\\.parquet", " and regexp_replace(s3.object.key, '^path/to/another/', '') rlike '^.*\\.parquet'"),
        // key prefix is "path/to/another", path regex is "subpath1/.*\\.gz"
        Arguments.of("path/to/another", "subpath1/.*\\.gz", " and regexp_replace(s3.object.key, '^path/to/another/', '') rlike '^subpath1/.*\\.gz'"),
        // key prefix is "path/to/another", path regex is "subpath1/.*|subpath2/.*"
        Arguments.of("path/to/another", "subpath1/.*|subpath2/.*", " and regexp_replace(s3.object.key, '^path/to/another/', '') rlike '^subpath1/.*|subpath2/.*'"));
  }

  static TypedProperties createPropsForS3EventsHoodieIncrSource(String basePath, String keyPrefix,
                                                                String fileExtension, String pathRegex) {
    TypedProperties props = new TypedProperties();
    if (basePath != null) {
      props.setProperty("hoodie.deltastreamer.source.hoodieincr.path", basePath);
    }
    if (keyPrefix != null) {
      props.setProperty("hoodie.deltastreamer.source.s3incr.key.prefix", keyPrefix);
    }
    if (fileExtension != null) {
      props.setProperty("hoodie.deltastreamer.source.s3incr.file.extensions", fileExtension);
    }
    if (pathRegex != null) {
      props.setProperty("hoodie.deltastreamer.source.cloud.data.select.path.regex", pathRegex);
    }
    return props;
  }

  private List<String> getSampleS3ObjectKeys(Long objectSize, List<String> filePaths, String commitTime) {
    return filePaths.stream().map(f -> {
      try {
        return generateS3EventMetadata(objectSize, MY_BUCKET, f, commitTime);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
  }

  private List<String> getSampleS3ObjectKeys(List<Triple<String, Long, String>> filePathSizeAndCommitTime) {
    return filePathSizeAndCommitTime.stream().map(f -> {
      try {
        return generateS3EventMetadata(f.getMiddle(), MY_BUCKET, f.getLeft(), f.getRight());
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
  }

  private Dataset<Row> generateDataset(List<Triple<String, Long, String>> filePathSizeAndCommitTime) {
    JavaRDD<String> testRdd = jsc.parallelize(getSampleS3ObjectKeys(filePathSizeAndCommitTime), 2);
    Dataset<Row> inputDs = spark().read().json(testRdd);
    return inputDs;
  }

  private static List<String> getSortedFilePathsByExtension(List<String> filePaths, String extension) {
    return filePaths.stream()
        .filter(f -> f.endsWith(extension)).sorted().collect(Collectors.toList());
  }

  /**
   * Generates simple Json structure like below
   * <p>
   * s3 : {
   * object : {
   * size:
   * key:
   * }
   * bucket: {
   * name:
   * }
   */
  private String generateS3EventMetadata(Long objectSize, String bucketName, String objectKey, String commitTime)
      throws JsonProcessingException {
    Map<String, Object> objectMetadata = new HashMap<>();
    objectMetadata.put("size", objectSize);
    objectMetadata.put("key", objectKey);
    Map<String, String> bucketMetadata = new HashMap<>();
    bucketMetadata.put("name", bucketName);
    Map<String, Object> s3Metadata = new HashMap<>();
    s3Metadata.put("object", objectMetadata);
    s3Metadata.put("bucket", bucketMetadata);
    Map<String, Object> eventMetadata = new HashMap<>();
    eventMetadata.put("s3", s3Metadata);
    eventMetadata.put("_hoodie_commit_time", commitTime);
    return mapper.writeValueAsString(eventMetadata);
  }

  private HoodieRecord generateS3EventMetadata(String commitTime, String bucketName, String objectKey, Long objectSize) {
    String partitionPath = bucketName;
    Schema schema = S3_METADATA_SCHEMA;
    GenericRecord rec = new GenericData.Record(schema);
    Schema.Field s3Field = schema.getField("s3");
    Schema s3Schema = s3Field.schema().getTypes().get(1); // Assuming the record schema is the second type
    // Create a generic record for the "s3" field
    GenericRecord s3Record = new GenericData.Record(s3Schema);

    Schema.Field s3BucketField = s3Schema.getField("bucket");
    Schema s3Bucket = s3BucketField.schema().getTypes().get(1); // Assuming the record schema is the second type
    GenericRecord s3BucketRec = new GenericData.Record(s3Bucket);
    s3BucketRec.put("name", bucketName);


    Schema.Field s3ObjectField = s3Schema.getField("object");
    Schema s3Object = s3ObjectField.schema().getTypes().get(1); // Assuming the record schema is the second type
    GenericRecord s3ObjectRec = new GenericData.Record(s3Object);
    s3ObjectRec.put("key", objectKey);
    s3ObjectRec.put("size", objectSize);

    s3Record.put("bucket", s3BucketRec);
    s3Record.put("object", s3ObjectRec);
    rec.put("s3", s3Record);
    rec.put("_hoodie_commit_time", commitTime);

    HoodieAvroPayload payload = new HoodieAvroPayload(Option.of(rec));
    return new HoodieAvroRecord(new HoodieKey(objectKey, partitionPath), payload);
  }

  private TypedProperties setProps(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy) {
    Properties properties = new Properties();
    properties.setProperty("hoodie.deltastreamer.source.hoodieincr.path", basePath());
    properties.setProperty("hoodie.deltastreamer.source.hoodieincr.missing.checkpoint.strategy",
        missingCheckpointStrategy.name());
    properties.setProperty("hoodie.deltastreamer.source.hoodieincr.file.format", "json");
    return new TypedProperties(properties);
  }

  private HoodieWriteConfig.Builder getConfigBuilder(String basePath, HoodieTableMetaClient metaClient) {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(S3_METADATA_SCHEMA.toString())
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withFinalizeWriteParallelism(2).withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .forTable(metaClient.getTableConfig().getTableName());
  }

  private HoodieWriteConfig getWriteConfig() {
    return getConfigBuilder(basePath(), metaClient)
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(2, 3).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(1).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .build();
  }

  private Pair<String, List<HoodieRecord>> writeS3MetadataRecords(String commitTime) throws IOException {
    HoodieWriteConfig writeConfig = getWriteConfig();
    SparkRDDWriteClient writeClient = getHoodieWriteClient(writeConfig);

    writeClient.startCommitWithTime(commitTime);
    List<HoodieRecord> s3MetadataRecords = Arrays.asList(
        generateS3EventMetadata(commitTime, "bucket-1", "data-file-1.json", 1L)
    );
    JavaRDD<WriteStatus> result = writeClient.upsert(jsc().parallelize(s3MetadataRecords, 1), commitTime);

    List<WriteStatus> statuses = result.collect();
    assertNoWriteErrors(statuses);

    return Pair.of(commitTime, s3MetadataRecords);
  }

  @Test
  public void testEmptyCheckpoint() throws IOException {
    String commitTimeForWrites = "1";
    String commitTimeForReads = commitTimeForWrites;

    Pair<String, List<HoodieRecord>> inserts = writeS3MetadataRecords(commitTimeForWrites);

    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(commitTimeForReads), 0L, inserts.getKey());
  }

  @Test
  public void testOneFileInCommit() throws IOException {
    String commitTimeForWrites = "commit2";
    String commitTimeForReads = "commit1";

    Pair<String, List<HoodieRecord>> inserts = writeS3MetadataRecords(commitTimeForWrites);

    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    filePathSizeAndCommitTime.add(Triple.of("path/to/file1.json", 100L, "commit1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file2.json", 150L, "commit1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file3.json", 200L, "commit1"));

    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    when(mockQueryRunner.run(Mockito.any())).thenReturn(inputDs);
    when(mockCloudDataFetcher.getCloudObjectDataDF(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Option.empty());

    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(commitTimeForReads), 100L, "commit1#path/to/file1.json");
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("commit1#path/to/file1.json"), 200L, "commit1#path/to/file2.json");
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("commit1#path/to/file2.json"), 200L, "commit1#path/to/file3.json");
  }

  @Test
  public void testTwoFilesAndContinueInSameCommit() throws IOException {
    String commitTimeForWrites = "commit2";
    String commitTimeForReads = "commit1";

    Pair<String, List<HoodieRecord>> inserts = writeS3MetadataRecords(commitTimeForWrites);

    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    filePathSizeAndCommitTime.add(Triple.of("path/to/file1.json", 100L, "commit1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file2.json", 150L, "commit1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file3.json", 200L, "commit1"));

    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    when(mockQueryRunner.run(Mockito.any())).thenReturn(inputDs);
    when(mockCloudDataFetcher.getCloudObjectDataDF(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Option.empty());

    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of(commitTimeForReads), 250L, "commit1#path/to/file2.json");
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("commit1#path/to/file2.json"), 250L, "commit1#path/to/file3.json");

  }

  @Test
  public void testTwoFilesAndContinueAcrossCommits() throws IOException {
    String commitTimeForWrites = "commit2";
    String commitTimeForReads = "commit1";

    Pair<String, List<HoodieRecord>> inserts = writeS3MetadataRecords(commitTimeForWrites);

    List<Triple<String, Long, String>> filePathSizeAndCommitTime = new ArrayList<>();
    // Add file paths and sizes to the list
    filePathSizeAndCommitTime.add(Triple.of("path/to/file1.json", 100L, "commit1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file3.json", 200L, "commit1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file2.json", 150L, "commit1"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file4.json", 50L, "commit2"));
    filePathSizeAndCommitTime.add(Triple.of("path/to/file5.json", 150L, "commit2"));

    Dataset<Row> inputDs = generateDataset(filePathSizeAndCommitTime);

    when(mockQueryRunner.run(Mockito.any())).thenReturn(inputDs);
    when(mockCloudDataFetcher.getCloudObjectDataDF(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Option.empty());

    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("commit1"), 100L, "commit1#path/to/file1.json");
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("commit1#path/to/file1.json"), 100L, "commit1#path/to/file2.json");
    readAndAssert(READ_UPTO_LATEST_COMMIT, Option.of("commit1#path/to/file2.json"), 1000L, "commit2#path/to/file5.json");
  }

  private void readAndAssert(IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy,
                             Option<String> checkpointToPull, long sourceLimit, String expectedCheckpoint) {
    TypedProperties typedProperties = setProps(missingCheckpointStrategy);

    S3EventsHoodieIncrSource incrSource = new S3EventsHoodieIncrSource(typedProperties, jsc(),
        spark(), mockSchemaProvider, mockQueryRunner, mockCloudDataFetcher);

    Pair<Option<Dataset<Row>>, String> dataAndCheckpoint = incrSource.fetchNextBatch(checkpointToPull, sourceLimit);

    Option<Dataset<Row>> datasetOpt = dataAndCheckpoint.getLeft();
    String nextCheckPoint = dataAndCheckpoint.getRight();

    Assertions.assertNotNull(nextCheckPoint);
    Assertions.assertEquals(expectedCheckpoint, nextCheckPoint);
  }
}
