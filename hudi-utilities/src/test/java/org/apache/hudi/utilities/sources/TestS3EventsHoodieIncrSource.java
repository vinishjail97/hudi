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
          PATH_ONE + "subpath2/Q4_2021/forecast.gz");

  @Mock
  private SchemaProvider mockSchemaProvider;

  @ParameterizedTest
  @MethodSource("generateFiltersForS3EventsHoodieIncrSource")
  public void testS3EventsHoodieIncrSourceFiltering(List<String> filePaths,
                                                    String keyPrefix,
                                                    String fileExtension,
                                                    String pathRegex,
                                                    Long objectSize,
                                                    String fileFormat,
                                                    List<String> expectedFilePaths) {
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
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

  public static Stream<Arguments> generateFiltersForS3EventsHoodieIncrSource() {
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
         // conflicting extension and regex to not return any files
         Arguments.of(PATH_ONE_FILES, PATH_ONE, ".csv", "subpath1/.*\\.gz", 100L, null,
                Collections.emptyList()));
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
