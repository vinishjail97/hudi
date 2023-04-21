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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestS3EventsHoodieIncrSource extends SparkClientFunctionalTestHarness {
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
  private JavaSparkContext jsc;

  @BeforeEach
  public void setUp() {
    jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
  }

  @ParameterizedTest
  @MethodSource("generateFiltersForS3EventsHoodieIncrSource")
  public void testS3EventsHoodieIncrSourceFiltering(List<String> filePaths,
                                                    String keyPrefix,
                                                    String fileExtension,
                                                    String pathRegex,
                                                    Long objectSize,
                                                    String fileFormat,
                                                    List<String> expectedFilePaths) {
    JavaRDD<String> testRdd = jsc.parallelize(getSampleS3ObjectKeys(objectSize, filePaths), 2);
    Dataset<Row> inputDs = spark().read().json(testRdd);

    S3EventsHoodieIncrSource s3EventsHoodieIncrSource = new S3EventsHoodieIncrSource(
        createPropsForS3EventsHoodieIncrSource(keyPrefix, fileExtension, pathRegex),
        jsc, spark(), mockSchemaProvider);
    List<String> resultKeys = s3EventsHoodieIncrSource.applyFilter(inputDs, fileFormat)
        .select("s3.object.key").collectAsList()
        .stream().map(row -> row.getString(0)).sorted().collect(Collectors.toList());
    assertEquals(expectedFilePaths, resultKeys);
  }

  private static Stream<Arguments> generateFiltersForS3EventsHoodieIncrSource() {
    return Stream.of(
         // s3.object.size is 0, expect no files
         Arguments.of(PATH_ONE_FILES, PATH_ONE, ".csv", ".*", 0L, ".csv", Collections.emptyList()),
         // s3.key.prefix is PATH_ONE, match all csv files by file format and match all regex.
         Arguments.of(PATH_ONE_FILES, PATH_ONE, null, ".*", 100L, ".csv",
             getSortedFilePathsByExtension(PATH_ONE_FILES, ".csv")),
         // s3.key.prefix is PATH_ONE and regex is null, match all gz files by file extension
         Arguments.of(PATH_ONE_FILES, PATH_ONE, ".gz", null, 100L, null,
             getSortedFilePathsByExtension(PATH_ONE_FILES, ".gz")),
         // s3.key.prefix is PATH_ONE and regex is empty, match all parquet files by extension.
         Arguments.of(PATH_ONE_FILES, PATH_ONE, ".parquet", "", 100L, "",
             getSortedFilePathsByExtension(PATH_ONE_FILES, ".parquet")),
         // s3.key.prefix is null, match all csv files by extension.
         Arguments.of(PATH_ONE_FILES, null, ".csv", null, 100L, null,
                getSortedFilePathsByExtension(PATH_ONE_FILES, ".csv")),
         // s3.key.prefix is empty, match all gz files by extension.
         Arguments.of(PATH_ONE_FILES, "", ".gz", null, 100L, null,
                getSortedFilePathsByExtension(PATH_ONE_FILES, ".gz")),
         // match all gz files in subpath1 by regex and extension
         Arguments.of(PATH_ONE_FILES, PATH_ONE, ".gz", "subpath1/.*\\.gz", 100L, null,
                    getSortedFilePathsByExtension(PATH_ONE_FILES, ".gz")
                        .stream()
                        .filter(path -> path.contains("subpath1")).sorted().collect(Collectors.toList())),
         // match all txt files by extension and prefix with no backslash
         Arguments.of(PATH_ONE_FILES, PATH_ONE_WITHOUT_TRAILING_SLASH, ".txt", ".*", 100L, null,
                    getSortedFilePathsByExtension(PATH_ONE_FILES, ".txt")),
         // match all txt files by extension but pick those starting with subpath3 only by regex.
         Arguments.of(PATH_ONE_FILES, PATH_ONE_WITHOUT_TRAILING_SLASH, ".txt", "subpath3/.*\\.txt", 100L, null,
                        Arrays.asList(PATH_ONE + "subpath3/Q4_2021/summary.txt")),
         // match all parquet files under under subpath1
         Arguments.of(PATH_ONE_FILES, PATH_ONE, ".parquet", "subpath1/.*\\.parquet", 100L, null,
                        getSortedFilePathsByExtension(PATH_ONE_FILES, ".parquet")
                            .stream()
                            .filter(path -> path.contains("subpath1"))
                            .sorted().collect(Collectors.toList())),
         // match all parquet files under subpath1 and subpath2 by using "|" in regex
         Arguments.of(PATH_ONE_FILES, PATH_ONE, ".parquet",
             "subpath1/.*\\.parquet|subpath2/.*\\.parquet", 100L, null,
                        getSortedFilePathsByExtension(PATH_ONE_FILES, ".parquet")
                            .stream()
                            .filter(path -> path.contains("subpath1") || path.contains("subpath2"))
                            .sorted().collect(Collectors.toList())),
         // conflicting extension and regex to not return any files
         Arguments.of(PATH_ONE_FILES, PATH_ONE, ".csv", "subpath1/.*\\.gz", 100L, null,
                Collections.emptyList()));
  }

  @ParameterizedTest
  @MethodSource("generateS3RegexFilters")
  public void testGenerateS3RegexFilters(String keyPrefix, String pathRegex, String expectedFilter) {
    S3EventsHoodieIncrSource s3EventsHoodieIncrSource = new S3EventsHoodieIncrSource(
        createPropsForS3EventsHoodieIncrSource(keyPrefix, ".unused", pathRegex),
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

  static TypedProperties createPropsForS3EventsHoodieIncrSource(String keyPrefix,
                                                        String fileExtension, String pathRegex) {
    TypedProperties props = new TypedProperties();
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

  private List<String> getSampleS3ObjectKeys(Long objectSize, List<String> filePaths) {
    return filePaths.stream().map(f -> {
      try {
        return generateS3EventMetadata(objectSize, MY_BUCKET, f);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
  }

  private static List<String> getSortedFilePathsByExtension(List<String> filePaths, String extension) {
    return filePaths.stream()
        .filter(f -> f.endsWith(extension)).sorted().collect(Collectors.toList());
  }

  /**
   * Generates simple Json structure like below
   *
   * s3 : {
   *  object : {
   *    size:
   *    key:
   * }
   * bucket: {
   *   name:
   * }
   */
  private String generateS3EventMetadata(Long objectSize, String bucketName, String objectKey)
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
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(eventMetadata);
  }
}
