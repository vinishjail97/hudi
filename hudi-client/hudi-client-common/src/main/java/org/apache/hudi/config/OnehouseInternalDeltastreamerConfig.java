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

package org.apache.hudi.config;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE;

/**
 * All internal configs used by the OnehouseDeltastreamer class.
 */
public class OnehouseInternalDeltastreamerConfig extends HoodieConfig {

  public static final ConfigProperty<String> DELTASTREAMER_SOURCE_CLASS_NAME = ConfigProperty
      .key("hoodie.deltastreamer.source.class.name")
      .defaultValue("org.apache.hudi.utilities.sources.JsonDFSSource")
      .withDocumentation("Subclass of org.apache.hudi.utilities.sources to read data. "
          + "Built-in options: org.apache.hudi.utilities.sources.{JsonDFSSource (default), AvroDFSSource, "
          + "JsonKafkaSource, AvroKafkaSource, HiveIncrPullSource}");

  public static final ConfigProperty<String> DELTASTREAMER_SOURCE_ID = ConfigProperty
      .key("hoodie.deltastreamer.source.id")
      .noDefaultValue()
      .withDocumentation("A unique identifier for the deltastreamer source class provided above"
      + "For Onehouse users, this would be sourceUUID present in the org");

  public static final ConfigProperty<String> DELTASTREAMER_SOURCE_ESTIMATOR_TYPE = ConfigProperty
      .key("hoodie.deltastreamer.source.estimator.class")
      .defaultValue("org.apache.hudi.utilities.deltastreamer.internal.DefaultSourceDataAvailabilityEstimator")
      .withDocumentation("Subclass of org.apache.hudi.utilities.deltastreamer.internal.SourceDataAvailabilityEstimator "
          + "which compute the data available in hoodie.deltastreamer.source.class.name property and used by DeltaSync "
          + "to decide whether to go-ahead with the write operation");

  public static final ConfigProperty<Long> READ_SOURCE_LIMIT = ConfigProperty
      .key("hoodie.deltastreamer.read.source.limit")
      .defaultValue(Long.MAX_VALUE)
      .withDocumentation("Maximum amount of data to read from source. "
          + "Default: No limit, e.g: DFS-Source => max bytes to read, Kafka-Source => max events to read");

  public static final ConfigProperty<String> SCHEMAPROVIDER_CLASS_NAME = ConfigProperty
      .key("hoodie.deltastreamer.schema.provider.class.name")
      .noDefaultValue()
      .withDocumentation("subclass of org.apache.hudi.utilities.schema"
          + ".SchemaProvider to attach schemas to input & target table data, built in options: "
          + "org.apache.hudi.utilities.schema.FilebasedSchemaProvider."
          + "Source (See org.apache.hudi.utilities.sources.Source) implementation can implement their own SchemaProvider."
          + " For Sources that return Dataset<Row>, the schema is obtained implicitly."
          + "However, this CLI option allows overriding the schemaprovider returned by Source.");

  public static final ConfigProperty<String> TRANSFORMER_CLASS_NAME = ConfigProperty
      .key("hoodie.deltastreamer.transformer.class.names")
      .noDefaultValue()
      .withDocumentation("A subclass or a list of comma-separated subclasses of org.apache.hudi.utilities.transform.Transformer"
          + ". Allows transforming raw source Dataset to a target Dataset (conforming to target schema) before "
          + "writing. Default : Not set. E:g - org.apache.hudi.utilities.transform.SqlQueryBasedTransformer (which "
          + "allows a SQL query templated to be passed as a transformation function). "
          + "Pass a comma-separated list of subclass names to chain the transformations.");

  public static final ConfigProperty<Integer> MIN_SYNC_INTERVAL_SECS = ConfigProperty
      .key("hoodie.deltastreamer.min.sync.interval.secs")
      .defaultValue(300)
      .withDocumentation("the min sync interval of each sync in continuous mode");

  public static final ConfigProperty<String> DELTASTREAMER_CHECKPOINT = ConfigProperty
      .key("hoodie.deltastreamer.checkpoint")
      .noDefaultValue()
      .withDocumentation("Resume Delta Streamer from this checkpoint.");

  public static final ConfigProperty<String> DELTASTREAMER_INITIAL_CHECKPOINT_PROVIDER = ConfigProperty
      .key("hoodie.deltastreamer.initial.checkpoint.provider")
      .noDefaultValue()
      .withDocumentation("subclass of "
          + "org.apache.hudi.utilities.checkpointing.InitialCheckpointProvider. Generate check point for delta streamer "
          + "for the first run. This field will override the checkpoint of last commit using the checkpoint field. "
          + "Use this field only when switching source, for example, from DFS source to Kafka Source.");

  public static final ConfigProperty<String> DELTASTREAMER_META_SYNC_CLASSES = ConfigProperty
      .key("hoodie.deltastreamer.meta.sync.classes")
      .defaultValue("")
      .withDocumentation("Meta sync client class, using comma to separate multiple classes.");

  public static final ConfigProperty<String> DELTASTREAMER_ENABLE_FILTER_DUPES = ConfigProperty
      .key("hoodie.deltastreamer.filter.dupes.enable")
      .defaultValue("false")
      .withDocumentation("Should duplicate records from source be dropped/filtered out before insert/bulk-insert.");

  public static final ConfigProperty<Long> MIN_BYTES_INGESTION_SOURCE_PROP = ConfigProperty
      .key("hoodie.deltastreamer.min.bytes.ingestion.source")
      .defaultValue(1000000L) // 1MB
      .withDocumentation("Minimum amount of bytes to schedule an ingestion from a source without delay.");
  public static final ConfigProperty<String> MUTLI_WRITER_SOURCE_CHECKPOINT_ID = ConfigProperty
      .key("hoodie.deltastreamer.multiwriter.source.checkpoint.id")
      .noDefaultValue()
      .withDocumentation("Define Unique Id for source to be used in commit checkpoint");

  public static final ConfigProperty<Boolean> DISABLE_COMPACTION = ConfigProperty
      .key("hoodie.deltastreamer.disable.compaction")
      .defaultValue(false) // 1MB
      .withDocumentation("Disable Compaction");

  public static final ConfigProperty<Boolean> COMMIT_ON_NO_CHECKPOINT_CHANGE = ConfigProperty
      .key("hoodie.deltastreamer.allow.commit.on.no.checkpoint.change")
      .defaultValue(false)
      .withDocumentation("allow commits even if checkpoint has not changed before and after fetch data from "
          + " source. This might be useful in sources like SqlSource where there is not checkpoint. And is "
          + "not recommended to enable in continuous mode.");

  public static final ConfigProperty<Boolean> COMMIT_ON_NO_DATA = ConfigProperty
      .key("hoodie.deltastreamer.allow.commit.on.no.data")
      .defaultValue(true)
      .withDocumentation("allow commit if there is no new data. This is useful for sources where checkpointing "
          + " is not done as they are irrelevant, eg: gcs metadata table.");

  public static final ConfigProperty<Boolean> COMMIT_ON_ERRORS = ConfigProperty
      .key("hoodie.deltastreamer.allow.commit.on.errors")
      .defaultValue(false) // 1MB
      .withDocumentation("allow commits on errors in delta sync,"
          + " should be used along with quarantine enabled");

  public static final ConfigProperty<Boolean> SAMPLE_WRITES_ENABLED = ConfigProperty
      .key("hoodie.deltastreamer.sample.writes.enabled")
      .defaultValue(false)
      .withDocumentation("Set this to true to sample from the first batch of records and write to the auxiliary path, before writing to the table."
          + "The sampled records are used to calculate the average record size. The relevant write client will have `" + COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key()
          + "` being overwritten by the calculated result.");
  public static final ConfigProperty<Integer> SAMPLE_WRITES_SIZE = ConfigProperty
      .key("hoodie.deltastreamer.sample.writes.size")
      .defaultValue(5000)
      .withDocumentation("Number of records to sample from the first write. To improve the estimation's accuracy, "
          + "for smaller or more compressable record size, set the sample size bigger. For bigger or less compressable record size, set smaller.");

  public static final ConfigProperty<Boolean> DISABLE_OLD_PARQUET_LIST_STRUCTURE = ConfigProperty
      .key("hoodie.parquet.avro.disable.old.parquet.list.structure")
      .defaultValue(false)
      .withDocumentation("Disables the old parquet list structure and instead writes with 3-level lists");

  private OnehouseInternalDeltastreamerConfig() {
    super();
  }

  private OnehouseInternalDeltastreamerConfig(Properties props) {
    super(props);
  }

  public HoodieFileFormat getTableFileFormat() {
    return HoodieFileFormat.valueOf(getStringOrDefault(
        HoodieTableConfig.BASE_FILE_FORMAT, HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().name()).toUpperCase());
  }

  public String getDatabaseName() {
    return getString(HoodieTableConfig.DATABASE_NAME);
  }

  /**
   * Meta Sync Configs
   */
  public boolean isMetaSyncEnabled() {
    return getBooleanOrDefault(HoodieSyncConfig.META_SYNC_ENABLED);
  }

  /**
   * This configs are specificied using command line for hoodiedeltastreamer,
   * but with multi-table, we want these to be provided using props file too.
   */
  public String getSourceClassName() {
    return getString(DELTASTREAMER_SOURCE_CLASS_NAME);
  }

  public Long getReadSourceLimit() {
    return getLongOrDefault(READ_SOURCE_LIMIT);
  }

  public String getSchemaProviderClassName() {
    return getString(SCHEMAPROVIDER_CLASS_NAME);
  }

  public List<String> getTransformerClassName() {
    return Arrays.stream(getStringOrDefault(TRANSFORMER_CLASS_NAME, ",").split("\\s*,\\s*")).collect(Collectors.toList());
  }

  public Integer getMinSyncIntervalSecs() {
    return getIntOrDefault(MIN_SYNC_INTERVAL_SECS);
  }

  public Long getMinSourceBytesIngestion() {
    return getLongOrDefault(MIN_BYTES_INGESTION_SOURCE_PROP);
  }

  public String getCheckpoint() {
    return getString(DELTASTREAMER_CHECKPOINT);
  }

  public String getInitialCheckpointProvider() {
    return getString(DELTASTREAMER_INITIAL_CHECKPOINT_PROVIDER);
  }

  public String getMetaSyncClasses() {
    return getStringOrDefault(DELTASTREAMER_META_SYNC_CLASSES);
  }

  public boolean isFilterDupesEnabled() {
    return getBooleanOrDefault(DELTASTREAMER_ENABLE_FILTER_DUPES);
  }

  public boolean isAllowCommitOnErrors() {
    return getBooleanOrDefault(COMMIT_ON_ERRORS);
  }

  public boolean isCompactionDisabled() {
    return getBooleanOrDefault(DISABLE_COMPACTION);
  }

  public Boolean isAllowCommitOnNoCheckpointChange() {
    return getBooleanOrDefault(COMMIT_ON_NO_CHECKPOINT_CHANGE);
  }

  public Boolean isAllowCommitOnNoData() {
    return getBooleanOrDefault(COMMIT_ON_NO_DATA);
  }

  public static OnehouseInternalDeltastreamerConfig.Builder newBuilder() {
    return new OnehouseInternalDeltastreamerConfig.Builder();
  }

  public static class Builder {

    protected final OnehouseInternalDeltastreamerConfig config = new OnehouseInternalDeltastreamerConfig();

    public Builder() {
    }

    public OnehouseInternalDeltastreamerConfig.Builder withProps(Map kvprops) {
      config.getProps().putAll(kvprops);
      return this;
    }

    public OnehouseInternalDeltastreamerConfig build() {
      return new OnehouseInternalDeltastreamerConfig(config.getProps());
    }
  }
}
