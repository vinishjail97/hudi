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

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.CloudDataFetcher;
import org.apache.hudi.utilities.sources.helpers.CloudObjectIncrCheckpoint;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;
import org.apache.hudi.utilities.sources.helpers.QueryInfo;
import org.apache.hudi.utilities.sources.helpers.QueryRunner;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;
import java.util.List;

import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.utilities.sources.HoodieIncrSource.Config.DEFAULT_NUM_INSTANTS_PER_FETCH;
import static org.apache.hudi.utilities.sources.HoodieIncrSource.Config.DEFAULT_SOURCE_FILE_FORMAT;
import static org.apache.hudi.utilities.sources.HoodieIncrSource.Config.HOODIE_SRC_BASE_PATH;
import static org.apache.hudi.utilities.sources.HoodieIncrSource.Config.NUM_INSTANTS_PER_FETCH;
import static org.apache.hudi.utilities.sources.HoodieIncrSource.Config.SOURCE_FILE_FORMAT;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelectorCommon.getCloudObjectMetadataPerPartition;
import static org.apache.hudi.utilities.sources.helpers.CloudStoreIngestionConfig.DATAFILE_FORMAT;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.getMissingCheckpointStrategy;

/**
 * This source will use the S3 events meta information from hoodie table generate by {@link S3EventsSource}.
 */
public class S3EventsHoodieIncrSource extends HoodieIncrSource {

  private static final Logger LOG = LogManager.getLogger(S3EventsHoodieIncrSource.class);
  private final String srcPath;
  private final int numInstantsPerFetch;
  private final boolean checkIfFileExists;
  private final String fileFormat;
  private final IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy;
  private final QueryRunner queryRunner;
  private final CloudDataFetcher cloudDataFetcher;

  private final Option<SchemaProvider> schemaProvider;

  private final Option<SnapshotLoadQuerySplitter> snapshotLoadQuerySplitter;

  public static class Config {
    // control whether we do existence check for files before consuming them
    static final String ENABLE_EXISTS_CHECK = "hoodie.deltastreamer.source.s3incr.check.file.exists";
    static final Boolean DEFAULT_ENABLE_EXISTS_CHECK = false;

    // control whether to filter the s3 objects starting with this prefix
    public static final String S3_KEY_PREFIX = "hoodie.deltastreamer.source.s3incr.key.prefix";
    static final String S3_FS_PREFIX = "hoodie.deltastreamer.source.s3incr.fs.prefix";

    // control whether to ignore the s3 objects starting with this prefix
    static final String S3_IGNORE_KEY_PREFIX = "hoodie.deltastreamer.source.s3incr.ignore.key.prefix";
    // control whether to ignore the s3 objects with this substring
    static final String S3_IGNORE_KEY_SUBSTRING = "hoodie.deltastreamer.source.s3incr.ignore.key.substring";
    /**
     * {@value #SPARK_DATASOURCE_OPTIONS} is json string, passed to the reader while loading dataset.
     * Example delta streamer conf
     * - --hoodie-conf hoodie.deltastreamer.source.s3incr.spark.datasource.options={"header":"true","encoding":"UTF-8"}
     */
    public static final String SPARK_DATASOURCE_OPTIONS = "hoodie.deltastreamer.source.s3incr.spark.datasource.options";

    // ToDo make it a list of extensions
    static final String S3_ACTUAL_FILE_EXTENSIONS = "hoodie.deltastreamer.source.s3incr.file.extensions";
    static final String S3_PATH_REGEX = "hoodie.deltastreamer.source.cloud.data.select.path.regex";
  }

  public static final String S3_OBJECT_KEY = "s3.object.key";
  public static final String S3_OBJECT_SIZE = "s3.object.size";
  public static final String S3_BUCKET_NAME = "s3.bucket.name";

  public S3EventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    this(props, sparkContext, sparkSession, schemaProvider, new QueryRunner(sparkSession, props),
        new CloudDataFetcher(props, props.getString(DATAFILE_FORMAT, props.getString(SOURCE_FILE_FORMAT, DEFAULT_SOURCE_FILE_FORMAT))));
  }

  public S3EventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider,
      QueryRunner queryRunner,
      CloudDataFetcher cloudDataFetcher) {
    super(props, sparkContext, sparkSession, schemaProvider);
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(HOODIE_SRC_BASE_PATH));
    this.srcPath = props.getString(HOODIE_SRC_BASE_PATH);
    this.numInstantsPerFetch = props.getInteger(NUM_INSTANTS_PER_FETCH, DEFAULT_NUM_INSTANTS_PER_FETCH);
    this.checkIfFileExists = props.getBoolean(Config.ENABLE_EXISTS_CHECK, Config.DEFAULT_ENABLE_EXISTS_CHECK);
    this.fileFormat = props.getString(DATAFILE_FORMAT, props.getString(SOURCE_FILE_FORMAT, DEFAULT_SOURCE_FILE_FORMAT));
    this.missingCheckpointStrategy = getMissingCheckpointStrategy(props);
    this.queryRunner = queryRunner;
    this.cloudDataFetcher = cloudDataFetcher;
    this.schemaProvider = Option.ofNullable(schemaProvider);
    this.snapshotLoadQuerySplitter =  SnapshotLoadQuerySplitter.getInstance(props);

  }

  @Override
  public Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCheckpoint, long sourceLimit) {
    CloudObjectIncrCheckpoint cloudObjectIncrCheckpoint = CloudObjectIncrCheckpoint.fromString(lastCheckpoint);
    QueryInfo queryInfo =
        IncrSourceHelper.generateQueryInfo(
            sparkContext, srcPath, numInstantsPerFetch,
            Option.of(cloudObjectIncrCheckpoint.getCommit()),
            missingCheckpointStrategy,
            HoodieRecord.COMMIT_TIME_METADATA_FIELD,
            S3_OBJECT_KEY, S3_OBJECT_SIZE, true,
            Option.ofNullable(cloudObjectIncrCheckpoint.getKey()));
    LOG.info("Querying S3 with:" + cloudObjectIncrCheckpoint + ", queryInfo:" + queryInfo);

    if (isNullOrEmpty(cloudObjectIncrCheckpoint.getKey()) && queryInfo.areStartAndEndInstantsEqual()) {
      LOG.warn("Already caught up. No new data to process");
      return Pair.of(Option.empty(), queryInfo.getEndInstant());
    }
    Pair<QueryInfo, Dataset<Row>> queryInfoDatasetPair = queryRunner.run(queryInfo, snapshotLoadQuerySplitter);
    queryInfo = queryInfoDatasetPair.getLeft();
    Dataset<Row> filteredSourceData = applyFilter(queryInfoDatasetPair.getRight(), fileFormat);

    LOG.info("Adjusting end checkpoint:" + queryInfo.getEndInstant() + " based on sourceLimit :" + sourceLimit);
    Pair<CloudObjectIncrCheckpoint, Option<Dataset<Row>>> checkPointAndDataset =
        IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
            filteredSourceData, sourceLimit, queryInfo, cloudObjectIncrCheckpoint);
    if (!checkPointAndDataset.getRight().isPresent()) {
      LOG.info("Empty source, returning endpoint:" + queryInfo.getEndInstant());
      return Pair.of(Option.empty(), queryInfo.getEndInstant());
    }
    LOG.info("Adjusted end checkpoint :" + checkPointAndDataset.getLeft());


    String s3FS = props.getString(Config.S3_FS_PREFIX, "s3").toLowerCase();
    String s3Prefix = s3FS + "://";

    // Create S3 paths
    SerializableConfiguration serializableHadoopConf = new SerializableConfiguration(sparkContext.hadoopConfiguration());
    List<CloudObjectMetadata> cloudObjectMetadata = checkPointAndDataset.getRight().get()
        .select(S3_BUCKET_NAME, S3_OBJECT_KEY, S3_OBJECT_SIZE)
        .distinct()
        .mapPartitions(getCloudObjectMetadataPerPartition(s3Prefix, serializableHadoopConf, checkIfFileExists), Encoders.kryo(CloudObjectMetadata.class))
        .collectAsList();
    LOG.info("Total number of files to process :" + cloudObjectMetadata.size());

    Option<Dataset<Row>> datasetOption = cloudDataFetcher.getCloudObjectDataDF(sparkSession, cloudObjectMetadata, props, schemaProvider);
    return Pair.of(datasetOption, checkPointAndDataset.getLeft().toString());
  }

  Dataset<Row> applyFilter(Dataset<Row> source, String fileFormat) {
    String filter = S3_OBJECT_SIZE + " > 0";
    String s3KeyPrefix = props.getString(Config.S3_KEY_PREFIX, null);
    if (!StringUtils.isNullOrEmpty(s3KeyPrefix)) {
      filter = filter + " and " + S3_OBJECT_KEY + " like '" + s3KeyPrefix + "%'";
    }
    if (!StringUtils.isNullOrEmpty(props.getString(Config.S3_IGNORE_KEY_PREFIX, null))) {
      filter = filter + " and " + S3_OBJECT_KEY + " not like '" + props.getString(Config.S3_IGNORE_KEY_PREFIX) + "%'";
    }
    if (!StringUtils.isNullOrEmpty(props.getString(Config.S3_IGNORE_KEY_SUBSTRING, null))) {
      filter = filter + " and " + S3_OBJECT_KEY + " not like '%" + props.getString(Config.S3_IGNORE_KEY_SUBSTRING) + "%'";
    }
    // add file format filtering by default
    // ToDo this was an urgent fix for Zendesk, we need to make this config more formal with a
    // list of extensions
    String fileExtensionFilter = props.getString(Config.S3_ACTUAL_FILE_EXTENSIONS, fileFormat);
    filter = filter + " and " + S3_OBJECT_KEY + " like '%" + fileExtensionFilter + "'";

    if (!StringUtils.isNullOrEmpty(props.getString(Config.S3_PATH_REGEX, null))) {
      filter = filter + generateS3KeyRegexFilter(s3KeyPrefix, props.getString(Config.S3_PATH_REGEX));
    }
    return source.filter(filter);
  }

  String generateS3KeyRegexFilter(String s3KeyPrefix, String s3PathRegex) {
    if (StringUtils.isNullOrEmpty(s3KeyPrefix)) {
      return " and " + S3_OBJECT_KEY + " rlike '^" + s3PathRegex + "'";
    } else {
      String regexFormat = " and regexp_replace(" + S3_OBJECT_KEY + ", '^%s', '') rlike '^%s'";
      String prefixRegexWithSlash = s3KeyPrefix.endsWith("/") ? s3KeyPrefix : s3KeyPrefix + "/";
      return String.format(regexFormat, prefixRegexWithSlash, s3PathRegex);
    }
  }
}
