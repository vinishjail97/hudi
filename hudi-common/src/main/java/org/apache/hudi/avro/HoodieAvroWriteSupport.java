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

package org.apache.hudi.avro;

import org.apache.avro.Schema;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;

import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.MessageType;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Wrap AvroWriterSupport for plugging in the bloom filter.
 */
public class HoodieAvroWriteSupport extends AvroWriteSupport {

  private final Option<HoodieBloomFilterWriteSupport<String>> bloomFilterWriteSupportOpt;
  private final Map<String, String> footerMetadata = new HashMap<>();
  private final Schema schema;

  public static final String OLD_HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY = "com.uber.hoodie.bloomfilter";
  public static final String HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY = "org.apache.hudi.bloomfilter";

  public HoodieAvroWriteSupport(MessageType schema, Schema avroSchema, Option<BloomFilter> bloomFilterOpt) {
    super(addFieldIdsToParquetSchema(schema, avroSchema), avroSchema, ConvertingGenericData.INSTANCE);
    this.bloomFilterWriteSupportOpt = bloomFilterOpt.map(HoodieBloomFilterAvroWriteSupport::new);
    this.schema = avroSchema;
  }

  @Override
  public WriteSupport.FinalizedWriteContext finalizeWrite() {
    Map<String, String> extraMetadata =
        CollectionUtils.combine(footerMetadata,
            bloomFilterWriteSupportOpt.map(HoodieBloomFilterWriteSupport::finalizeMetadata)
                .orElse(Collections.emptyMap())
        );

    return new WriteSupport.FinalizedWriteContext(extraMetadata);
  }

  public void add(String recordKey) {
    this.bloomFilterWriteSupportOpt.ifPresent(bloomFilterWriteSupport ->
        bloomFilterWriteSupport.addKey(recordKey));
  }

  public void addFooterMetadata(String key, String value) {
    footerMetadata.put(key, value);
  }

  private static class HoodieBloomFilterAvroWriteSupport extends HoodieBloomFilterWriteSupport<String> {
    public HoodieBloomFilterAvroWriteSupport(BloomFilter bloomFilter) {
      super(bloomFilter);
    }

    @Override
    protected byte[] getUTF8Bytes(String key) {
      return key.getBytes(StandardCharsets.UTF_8);
    }
  }

  private static MessageType addFieldIdsToParquetSchema(MessageType messageType, Schema schema) {
    // apply field IDs if present
    return HoodieAvroUtils.getIdTracking(schema).map(idTracking -> {
      List<IdMapping> idMappings = idTracking.getIdMappings();
      NameMapping nameMapping = NameMapping.of(idMappings.stream().map(HoodieAvroWriteSupport::toMappedField).collect(Collectors.toList()));
      return ParquetSchemaUtil.applyNameMapping(messageType, nameMapping);
    }).orElse(messageType);
  }

  private static MappedField toMappedField(IdMapping idMapping) {
    return MappedField.of(idMapping.getId(), idMapping.getName(),
        idMapping.getFields() == null ? null : MappedFields.of(idMapping.getFields().stream().map(HoodieAvroWriteSupport::toMappedField).collect(Collectors.toList())));
  }
}
