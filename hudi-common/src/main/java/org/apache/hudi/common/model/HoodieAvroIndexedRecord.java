/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.table.HoodieTableConfig.POPULATE_META_FIELDS;
import static org.fusesource.hawtjni.runtime.ArgFlag.SENTINEL;

/**
 * This only use by reader returning.
 */
public class HoodieAvroIndexedRecord extends HoodieRecord<IndexedRecord> {

  public HoodieAvroIndexedRecord(IndexedRecord data) {
    super(null, data);
  }

  public HoodieAvroIndexedRecord(HoodieKey key, IndexedRecord data) {
    super(key, data);
  }

  public HoodieAvroIndexedRecord(
      HoodieKey key,
      IndexedRecord data,
      HoodieOperation operation) {
    super(key, data, operation);
  }

  public HoodieAvroIndexedRecord(HoodieRecord<IndexedRecord> record) {
    super(record);
  }

  public HoodieAvroIndexedRecord() {
  }

  @Override
  public HoodieRecord newInstance() {
    return new HoodieAvroIndexedRecord(this);
  }

  public HoodieRecord<IndexedRecord> newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieAvroIndexedRecord(key, data, op);
  }

  @Override
  public HoodieRecord<IndexedRecord> newInstance(HoodieKey key) {
    return new HoodieAvroIndexedRecord(key, data, operation);
  }

  public String getRecordKey(Schema recordSchema, Option<BaseKeyGenerator> keyGeneratorOpt) {
    return keyGeneratorOpt.isPresent() ? keyGeneratorOpt.get().getRecordKey((GenericRecord) data) : ((GenericRecord) data).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
  }

  public String getRecordKey(Schema recordSchema, String keyFieldName) {
    return Option.ofNullable(data.getSchema().getField(keyFieldName))
        .map(keyField -> data.get(keyField.pos()))
        .map(Object::toString).orElse(null);
  }

  public Object[] getColumnValues(Schema recordSchema, String[] columns, boolean consistentLogicalTimestampEnabled) {
    throw new UnsupportedOperationException();
  }

  public HoodieRecord joinWith(HoodieRecord other, Schema targetSchema) {
    GenericRecord record = HoodieAvroUtils.stitchRecords((GenericRecord) data, (GenericRecord) other.getData(), targetSchema);
    return new HoodieAvroIndexedRecord(key, record, operation);
  }

  public HoodieAvroIndexedRecord prependMetaFields(Schema recordSchema, Schema targetSchema, MetadataValues metadataValues, Properties props) {
    GenericRecord newAvroRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(data, targetSchema, Collections.emptyMap());
    updateMetadataValuesInternal(newAvroRecord, metadataValues);
    return new HoodieAvroIndexedRecord(key, newAvroRecord, operation);
  }

  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties props, Schema newSchema, Map<String, String> renameCols) {
    GenericRecord record = HoodieAvroUtils.rewriteRecordWithNewSchema(data, newSchema, renameCols);
    return new HoodieAvroIndexedRecord(key, record, operation);
  }

  public HoodieRecord truncateRecordKey(Schema recordSchema, Properties props, String keyFieldName) {
    ((GenericRecord) data).put(keyFieldName, StringUtils.EMPTY_STRING);
    return this;
  }

  public boolean isDelete(Schema recordSchema, Properties props) {
    return false;
  }

  public boolean shouldIgnore(Schema recordSchema, Properties props) throws IOException {
    return getData().equals(SENTINEL);
  }

  public HoodieRecord<IndexedRecord> copy() {
    return this;
  }

  public HoodieRecord wrapIntoHoodieRecordPayloadWithKeyGen(Schema recordSchema,
                                                            Properties props, Option<BaseKeyGenerator> keyGen) {
    GenericRecord record = (GenericRecord) data;
    String key;
    String partition;
    if (keyGen.isPresent() && !Boolean.parseBoolean(props.getOrDefault(POPULATE_META_FIELDS.key(), POPULATE_META_FIELDS.defaultValue().toString()).toString())) {
      BaseKeyGenerator keyGeneratorOpt = keyGen.get();
      key = keyGeneratorOpt.getRecordKey(record);
      partition = keyGeneratorOpt.getPartitionPath(record);
    } else {
      key = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      partition = record.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
    }
    HoodieKey hoodieKey = new HoodieKey(key, partition);

    HoodieRecordPayload avroPayload = new RewriteAvroPayload(record);
    HoodieRecord hoodieRecord = new HoodieAvroRecord(hoodieKey, avroPayload);
    return hoodieRecord;
  }

  public Option<Map<String, String>> getMetadata() {
    return Option.empty();
  }

  public Comparable<?> getOrderingValue(Schema recordSchema, Properties props) {
    boolean consistentLogicalTimestampEnabled = Boolean.parseBoolean(props.getProperty(
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));
    return (Comparable<?>) HoodieAvroUtils.getNestedFieldVal((GenericRecord) data,
        getOrderingField(props),
        true, consistentLogicalTimestampEnabled);
  }

  public Option<HoodieAvroIndexedRecord> toIndexedRecord(Schema recordSchema, Properties props) {
    return Option.of(this);
  }

  /**
   * NOTE: This method is declared final to make sure there's no polymorphism and therefore
   *       JIT compiler could perform more aggressive optimizations
   */
  @SuppressWarnings("unchecked")
  protected final void writeRecordPayload(IndexedRecord payload, Kryo kryo, Output output) {
    // NOTE: We're leveraging Spark's default [[GenericAvroSerializer]] to serialize Avro
    Serializer<GenericRecord> avroSerializer = kryo.getSerializer(GenericRecord.class);

    kryo.writeObjectOrNull(output, payload, avroSerializer);
  }

  /**
   * NOTE: This method is declared final to make sure there's no polymorphism and therefore
   *       JIT compiler could perform more aggressive optimizations
   */
  @SuppressWarnings("unchecked")
  protected final IndexedRecord readRecordPayload(Kryo kryo, Input input) {
    // NOTE: We're leveraging Spark's default [[GenericAvroSerializer]] to serialize Avro
    Serializer<GenericRecord> avroSerializer = kryo.getSerializer(GenericRecord.class);

    return kryo.readObjectOrNull(input, GenericRecord.class, avroSerializer);
  }

  static void updateMetadataValuesInternal(GenericRecord avroRecord, MetadataValues metadataValues) {
    if (metadataValues.isEmpty()) {
      return; // no-op
    }

    String[] values = metadataValues.getValues();
    for (int pos = 0; pos < values.length; ++pos) {
      String value = values[pos];
      if (value != null) {
        avroRecord.put(HoodieMetadataField.values()[pos].getFieldName(), value);
      }
    }
  }

  /**
   * Get ordering field.
   */
  private static String getOrderingField(Properties properties) {
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
  private static String getPayloadClass(Properties properties) {
    String payloadClass = null;
    if (properties.containsKey(HoodieTableConfig.PAYLOAD_CLASS_NAME.key())) {
      payloadClass = properties.getProperty(HoodieTableConfig.PAYLOAD_CLASS_NAME.key());
    } else if (properties.containsKey("hoodie.datasource.write.payload.class")) {
      payloadClass = properties.getProperty("hoodie.datasource.write.payload.class");
    }
    return payloadClass;
  }
}
