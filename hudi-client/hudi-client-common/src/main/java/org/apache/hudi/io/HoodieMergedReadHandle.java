/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.io;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.common.util.StringUtils.nonEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

public class HoodieMergedReadHandle<T extends HoodieRecordPayload, I, K, O> extends HoodieReadHandle<T, I, K, O> {

  protected final Schema readerSchema;

  public HoodieMergedReadHandle(HoodieWriteConfig config,
                                Option<String> instantTime,
                                HoodieTable<T, I, K, O> hoodieTable,
                                Pair<String, String> partitionPathFileIDPair) {
    super(config, instantTime, hoodieTable, partitionPathFileIDPair);
    readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()), config.allowOperationMetadataField());
  }

  public List<HoodieRecord<T>> getMergedRecords() {
    Option<FileSlice> fileSliceOpt = getLatestFileSlice();
    if (!fileSliceOpt.isPresent()) {
      return Collections.emptyList();
    }
    checkState(nonEmpty(instantTime), String.format("Expected a valid instant time but got `%s`", instantTime));
    final FileSlice fileSlice = fileSliceOpt.get();
    final HoodieRecordLocation currentLocation = new HoodieRecordLocation(instantTime, fileSlice.getFileId());
    Option<HoodieFileReader> baseFileReader = Option.empty();
    HoodieMergedLogRecordScanner logRecordScanner = null;
    try {
      baseFileReader = getBaseFileReader(fileSlice);
      logRecordScanner = getLogRecordScanner(fileSlice);
      List<HoodieRecord<T>> mergedRecords = new ArrayList<>();
      doMergedRead(baseFileReader, logRecordScanner).forEach(r -> {
        r.unseal();
        r.setCurrentLocation(currentLocation);
        r.seal();
        mergedRecords.add(r);
      });
      return mergedRecords;
    } catch (IOException e) {
      throw new HoodieIndexException("Error in reading " + fileSlice, e);
    } finally {
      if (baseFileReader.isPresent()) {
        baseFileReader.get().close();
      }
      if (logRecordScanner != null) {
        logRecordScanner.close();
      }
    }
  }

  private Option<FileSlice> getLatestFileSlice() {
    if (nonEmpty(instantTime)
        && hoodieTable.getMetaClient().getCommitsTimeline().filterCompletedInstants().lastInstant().isPresent()) {
      return Option.fromJavaOptional(hoodieTable
          .getHoodieView()
          .getLatestMergedFileSlicesBeforeOrOn(partitionPathFileIDPair.getLeft(), instantTime)
          .filter(fileSlice -> fileSlice.getFileId().equals(partitionPathFileIDPair.getRight()))
          .findFirst());
    }
    return Option.empty();
  }

  private Option<HoodieFileReader> getBaseFileReader(FileSlice fileSlice) throws IOException {
    if (fileSlice.getBaseFile().isPresent()) {
      return Option.of(createNewFileReader());
    }
    return Option.empty();
  }

  private HoodieMergedLogRecordScanner getLogRecordScanner(FileSlice fileSlice) {
    List<String> logFilePaths = fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator())
        .map(l -> l.getPath().toString()).collect(toList());
    return HoodieMergedLogRecordScanner.newBuilder()
        .withFileSystem(hoodieTable.getMetaClient().getFs())
        .withBasePath(hoodieTable.getMetaClient().getBasePathV2().toString())
        .withLogFilePaths(logFilePaths)
        .withReaderSchema(readerSchema)
        .withLatestInstantTime(instantTime)
        .withMaxMemorySizeInBytes(IOUtils.getMaxMemoryPerCompaction(hoodieTable.getTaskContextSupplier(), config))
        .withReadBlocksLazily(config.getCompactionLazyBlockReadEnabled())
        .withReverseReader(config.getCompactionReverseLogReadEnabled())
        .withBufferSize(config.getMaxDFSStreamBufferSize())
        .withSpillableMapBasePath(config.getSpillableMapBasePath())
        .withPartition(fileSlice.getPartitionPath())
        .withDiskMapType(config.getCommonConfig().getSpillableDiskMapType())
        .withBitCaskDiskMapCompressionEnabled(config.getCommonConfig().isBitCaskDiskMapCompressionEnabled())
        .build();
  }

  private List<HoodieRecord<T>> doMergedRead(Option<HoodieFileReader> baseFileReaderOpt, HoodieMergedLogRecordScanner logRecordScanner) throws IOException {
    List<HoodieRecord<T>> mergedRecords = new ArrayList<>();
    Map<String, HoodieRecord<? extends HoodieRecordPayload>> deltaRecordMap = logRecordScanner.getRecords();
    Set<String> deltaRecordKeys = new HashSet<>(deltaRecordMap.keySet());

    if (baseFileReaderOpt.isPresent()) {
      HoodieFileReader baseFileReader = baseFileReaderOpt.get();
      ClosableIterator<GenericRecord> baseFileItr = baseFileReader.getRecordIterator(readerSchema);
      HoodieTableConfig tableConfig = hoodieTable.getMetaClient().getTableConfig();
      Option<Pair<String, String>> simpleKeyGenFieldsOpt =
          tableConfig.populateMetaFields() ? Option.empty() : Option.of(Pair.of(tableConfig.getRecordKeyFieldProp(), tableConfig.getPartitionFieldProp()));
      while (baseFileItr.hasNext()) {
        HoodieRecord<T> record = wrapIntoHoodieRecordPayloadWithParams(baseFileItr.next(),
            config.getProps(), simpleKeyGenFieldsOpt, logRecordScanner.isWithOperationField(), logRecordScanner.getPartitionName(), false, Option.empty());
        String key = record.getRecordKey();
        if (deltaRecordMap.containsKey(key)) {
          deltaRecordKeys.remove(key);
          Option<IndexedRecord> mergedPayload = ((HoodieRecordPayload) deltaRecordMap.get(key).getData()).combineAndGetUpdateValue(
              (IndexedRecord) (((HoodieRecordPayload) record.getData()).getInsertValue(readerSchema, config.getProps()).get()), readerSchema, config.getPayloadConfig().getProps());
          Option<HoodieAvroRecord> mergedRecord = mergedPayload.map(payload -> (HoodieAvroRecord) wrapIntoHoodieRecordPayloadWithParams(payload,
              config.getProps(), simpleKeyGenFieldsOpt, logRecordScanner.isWithOperationField(), logRecordScanner.getPartitionName(), false, Option.empty()));
          if (!mergedRecord.isPresent()) {
            continue;
          }
          mergedRecords.add(mergedRecord.get());
        } else {
          mergedRecords.add(record);
        }
      }
    }

    for (String key : deltaRecordKeys) {
      mergedRecords.add((HoodieRecord<T>) deltaRecordMap.get(key));
    }

    return mergedRecords;
  }

  private static Comparable getOrderingVal(GenericRecord record, Properties properties) {
    String orderField = properties.getProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY);
    if (orderField == null) {
      return true;
    }
    boolean consistentLogicalTimestampEnabled = Boolean.parseBoolean(properties.getProperty(
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));
    return (Comparable) HoodieAvroUtils.getNestedFieldVal((GenericRecord) record,
        orderField,
        true, consistentLogicalTimestampEnabled);
  }

  /**
   * Create a payload class via reflection, passing in an ordering/precombine value.
   */
  private static HoodieRecordPayload createPayload(String payloadClass, GenericRecord record, Comparable orderingVal)
      throws IOException {
    try {
      return (HoodieRecordPayload) ReflectionUtils.loadClass(payloadClass,
          new Class<?>[] {GenericRecord.class, Comparable.class}, record, orderingVal);
    } catch (Throwable e) {
      throw new IOException("Could not create payload for class: " + payloadClass, e);
    }
  }

  /**
   * Get ordering field.
   */
  private String getOrderingField(Properties properties) {
    String orderField = null;
    if (properties.containsKey(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY)) {
      orderField = properties.getProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY);
    } else if (properties.containsKey("hoodie.datasource.write.precombine.field")) {
      orderField = properties.getProperty("hoodie.datasource.write.precombine.field");
    } else if (properties.containsKey(HoodieTableConfig.PRECOMBINE_FIELD.key())) {
      orderField = properties.getProperty(HoodieTableConfig.PRECOMBINE_FIELD.key());
    }
    return orderField;
  }

  /**
   * Get payload class.
   */
  private String getPayloadClass(Properties properties) {
    String payloadClass = null;
    if (properties.containsKey(HoodieTableConfig.PAYLOAD_CLASS_NAME.key())) {
      payloadClass = properties.getProperty(HoodieTableConfig.PAYLOAD_CLASS_NAME.key());
    } else if (properties.containsKey("hoodie.datasource.write.payload.class")) {
      payloadClass = properties.getProperty("hoodie.datasource.write.payload.class");
    }
    return payloadClass;
  }

  private HoodieRecord wrapIntoHoodieRecordPayloadWithParams(
      IndexedRecord indexedRecord,
      Properties props,
      Option<Pair<String, String>> simpleKeyGenFieldsOpt,
      Boolean withOperation,
      Option<String> partitionNameOp,
      Boolean populateMetaFields,
      Option<Schema> schemaWithoutMetaFields) {
    String payloadClass = getPayloadClass(props);
    String preCombineField = getOrderingField(props);
    return HoodieAvroUtils.createHoodieRecordFromAvro(indexedRecord, payloadClass, preCombineField, simpleKeyGenFieldsOpt, withOperation, partitionNameOp, populateMetaFields, schemaWithoutMetaFields);
  }
}

