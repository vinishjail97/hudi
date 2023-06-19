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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import scala.collection.JavaConverters;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.hudi.common.model.HoodieRecord.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.table.HoodieTableConfig.ARCHIVELOG_FOLDER;
import static org.apache.hudi.config.HoodieQuarantineTableConfig.QUARANTINE_TABLE_BASE_PATH;
import static org.apache.hudi.config.HoodieQuarantineTableConfig.QUARANTINE_TABLE_INSERT_PARALLELISM_VALUE;
import static org.apache.hudi.config.HoodieQuarantineTableConfig.QUARANTINE_TABLE_UPSERT_PARALLELISM_VALUE;
import static org.apache.hudi.config.HoodieQuarantineTableConfig.QUARANTINE_TARGET_TABLE;
import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE;
import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME;

public class JsonQuarantineTableWriter<T extends QuarantineEvent> implements QuarantineTableWriterInterface<T>, Serializable {

  private static final Logger LOG = LogManager.getLogger(JsonQuarantineTableWriter.class);
  /**
   * error record/event of base table for which quarantine table is enabled
   */
  private static final String DATA_RECORD_FIELD = "data_record";

  /**
   * last completed timestamp of base table
   * this column in further used for partitioning table
   */
  private static final String BASE_TABLE_COMMITED_INSTANT_FIELD = "base_table_commited_instant_time";

  /**
   * type of failure
   */
  private static final String FAILURE_TYPE = "failure_type";

  private static final String BASE_TABLE_EMPTY_COMMIT = "00000000";
  private static final String QUARANTINE_TABLE_AVSC = "/delta-streamer-config/quarantine-table.avsc";

  /**
   * Delta Sync Config.
   */
  private final HoodieDeltaStreamer.Config cfg;
  /**
   * Bag of properties with source, hoodie client, key generator etc.
   * <p>
   * NOTE: These properties are already consolidated w/ CLI provided config-overrides
   */
  private final TypedProperties props;
  private final HoodieWriteConfig quarantineTableCfg;
  private final String basePath;
  private final List<JavaRDD<ErrorEvent>> errorEventsRdd = new ArrayList<>();

  /**
   * Filesystem used.
   */
  private transient FileSystem fs;

  /**
   * Spark context Wrapper.
   */
  private final transient HoodieSparkEngineContext sparkEngineContext;

  /**
   * Spark context.
   */
  private transient JavaSparkContext jssc;

  /**
   * Spark Session.
   */
  private transient SparkSession sparkSession;

  private transient Configuration conf;
  private transient SparkRDDWriteClient quarantineTableWriteClient;
  private transient Schema schema;

  public JsonQuarantineTableWriter(HoodieDeltaStreamer.Config cfg, SparkSession sparkSession,
                                   TypedProperties props, HoodieSparkEngineContext sparkEngineContext, FileSystem fs) throws IOException {

    this.cfg = cfg;
    this.sparkEngineContext = sparkEngineContext;
    this.jssc = sparkEngineContext.getJavaSparkContext();
    this.sparkSession = sparkSession;
    this.fs = fs;
    this.props = props;
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withProperties(props).build();
    this.basePath = writeConfig.getBasePath();
    this.conf = conf;
    try (InputStream inputStream = this.getClass().getResourceAsStream(QUARANTINE_TABLE_AVSC)) {
      this.schema = new Schema.Parser().parse(inputStream);
    }
    this.quarantineTableCfg = getQuarantineTableWriteConfig();
    this.quarantineTableWriteClient = new SparkRDDWriteClient<>(sparkEngineContext, quarantineTableCfg);
  }

  @Override
  public void addErrorEvents(JavaRDD<T> errorEvent) {
    errorEventsRdd.add(errorEvent.map(
        x -> new JsonQuarantineTableWriter.ErrorEvent(
            x.getPayload(),
            basePath,
            x.getReason().name(),
            new HashMap<>())));
  }

  @Override
  public Option<JavaRDD<HoodieAvroRecord>> getErrorEvents(String baseTableInstantTime, Option<String> commitedInstantTime) {
    String commitedInstantTimeStr = commitedInstantTime.isPresent() ? commitedInstantTime.get() : BASE_TABLE_EMPTY_COMMIT;
    final String commitedInstantTimeStrFormatted;
    if (commitedInstantTimeStr.startsWith("[") && commitedInstantTimeStr.endsWith("]")) {
      commitedInstantTimeStrFormatted = commitedInstantTimeStr.substring(1,commitedInstantTimeStr.length() - 1);
    } else {
      commitedInstantTimeStrFormatted = commitedInstantTimeStr;
    }
    return createErrorEventsRdd(errorEventsRdd.stream().map(r -> r.map(ev -> RowFactory.create(ev.dataRecord,
          ev.sourceTableBasePath,
          ev.failureType,
          baseTableInstantTime,
          commitedInstantTimeStrFormatted,
          JavaConverters.mapAsScalaMapConverter(ev.metadata).asScala()
      ))).reduce((x, y) -> x.union(y)));
  }

  @Override
  public void cleanErrorEvents() {
    LOG.info("Clearing out old quarantine events for table " + this.quarantineTableCfg.getBasePath());
    errorEventsRdd.clear();
  }

  @Override
  public String startCommit() {
    try {
      if (!fs.exists(new Path(quarantineTableCfg.getBasePath()))) {
        initialiseTable();
      }
    } catch (IOException e) {
      throw new HoodieException("Failed to check file exists in quarantineEvents processing", e);
    }
    final int maxRetries = 2;
    int retryNum = 1;
    RuntimeException lastException = null;
    while (retryNum <= maxRetries) {
      try {
        String instantTime = HoodieActiveTimeline.createNewInstantTime();
        String commitActionType = CommitUtils.getCommitActionType(WriteOperationType.UPSERT, HoodieTableType.COPY_ON_WRITE);
        quarantineTableWriteClient.startCommitWithTime(instantTime, commitActionType);
        return instantTime;
      } catch (IllegalArgumentException ie) {
        lastException = ie;
        retryNum++;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // No-Op
        }
      }
    }
    throw lastException;
  }

  public HoodieWriteConfig getQuarantineTableWriteConfig() {

    return HoodieWriteConfig.newBuilder().withPath(props.getString(QUARANTINE_TABLE_BASE_PATH.key()))
        .withAutoCommit(false)
        .withSchema(schema.toString()).withParallelism(props.getInteger(QUARANTINE_TABLE_INSERT_PARALLELISM_VALUE.key(),QUARANTINE_TABLE_INSERT_PARALLELISM_VALUE.defaultValue()),
            props.getInteger(QUARANTINE_TABLE_UPSERT_PARALLELISM_VALUE.key(),QUARANTINE_TABLE_UPSERT_PARALLELISM_VALUE.defaultValue()))
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(20, 30).build())
        .forTable(props.getString(QUARANTINE_TARGET_TABLE.key()))
        .build();
  }

  @Override
  public HoodieDeltaStreamer.Config getSourceDeltaStreamerConfig() {
    return this.cfg;
  }

  @Override
  public boolean upsertAndCommit(String baseTableInstantTime, Option<String> commitedInstantTime) {
    boolean result =  getErrorEvents(baseTableInstantTime, commitedInstantTime)
        .map(rdd -> {
          String instantTime = startCommit();
          JavaRDD<WriteStatus> writeStatusJavaRDD = quarantineTableWriteClient.insert(rdd, instantTime);
          boolean success = quarantineTableWriteClient.commit(instantTime, writeStatusJavaRDD, Option.empty(),
              HoodieActiveTimeline.COMMIT_ACTION, Collections.emptyMap());
          LOG.info("Error events ingestion Commit " + instantTime + " " + success);
          return success;
        }).orElse(true);
    cleanErrorEvents();
    return result;
  }

  private void initialiseTable() {
    try {
      HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(HoodieTableType.COPY_ON_WRITE)
          .setTableName(quarantineTableCfg.getTableName())
          .setArchiveLogFolder(ARCHIVELOG_FOLDER.defaultValue())
          .setPayloadClassName(quarantineTableCfg.getPayloadClass())
          .setBaseFileFormat("parquet")
          .setPartitionFields(PARTITION_PATH_METADATA_FIELD)
          .initTable(new Configuration(jssc.hadoopConfiguration()), props.getString(QUARANTINE_TABLE_BASE_PATH.key()));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public Option<JavaRDD<HoodieAvroRecord>> createErrorEventsRdd(Optional<JavaRDD<Row>> errorEventsRdd) {
    LOG.info("processing createErrorEventsRdd");
    if (!errorEventsRdd.isPresent() || errorEventsRdd.get().isEmpty()) {
      LOG.info("Returning empty ErrorEventsRdd");
      return Option.empty();
    }
    JavaRDD<Row> rowJavaRDD = errorEventsRdd.get();
    Option<JavaRDD<HoodieAvroRecord>> rddOption =  Option.of(HoodieSparkUtils.createRdd(
        sparkSession.createDataFrame(rowJavaRDD, AvroConversionUtils.convertAvroSchemaToStructType(schema)), HOODIE_RECORD_STRUCT_NAME,
        HOODIE_RECORD_NAMESPACE, false, Option.empty()
    ).toJavaRDD().map(x -> {
      LOG.info("Creating ErrorEventsRdd");
      HoodieRecordPayload recordPayload = DataSourceUtils.createPayload(QUARANTINE_PAYLOAD_CLASS, x);
      String partitionPath = String.valueOf(x.get(BASE_TABLE_COMMITED_INSTANT_FIELD)).substring(0, 8);
      // For improving indexing performance, records keys are created with common prefix as FAILURE_TYPE.
      String recordKey = String.format("%s_%s",x.get(FAILURE_TYPE),
          org.apache.commons.codec.digest.DigestUtils.sha256Hex(x.get(DATA_RECORD_FIELD).toString()));
      HoodieKey key = new HoodieKey(recordKey, partitionPath);
      return new HoodieAvroRecord<>(key, recordPayload);
    }));
    LOG.info("processing createErrorEventsRdd done");
    return rddOption;
  }

  private static class ErrorEvent implements Serializable {
    String dataRecord;
    String sourceTableBasePath;
    String failureType;
    Map<String, String> metadata;

    public ErrorEvent(String dataRecord, String sourceTableBasePath, String failureType, Map<String, String> metadata) {
      this.dataRecord = dataRecord;
      this.sourceTableBasePath = sourceTableBasePath;
      this.failureType = failureType;
      this.metadata = metadata;
    }
  }

}
