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

package org.apache.hudi.utilities.schema;

import org.apache.hudi.common.config.TypedProperties;

import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.stream.Collectors;

public class KafkaOffsetPostProcessor  extends SchemaPostProcessor {

  private static final Logger LOG = LogManager.getLogger(KafkaOffsetPostProcessor.class);

  public static final String KAFKA_SOURCE_OFFSET_COLUMN = "_hoodie_kafka_source_offset";
  public static final String KAFKA_SOURCE_PARTITION_COLUMN = "_hoodie_kafka_source_partition";
  public static final String KAFKA_SOURCE_TIMESTAMP_COLUMN = "_hoodie_kafka_source_timestamp";

  public KafkaOffsetPostProcessor(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
  }

  @Override
  public Schema processSchema(Schema schema) {
    List<Schema.Field> fieldList = schema.getFields();
    List<Schema.Field> newFieldList = fieldList.stream()
        .map(f -> new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal())).collect(Collectors.toList());
    newFieldList.add(new Schema.Field(KAFKA_SOURCE_OFFSET_COLUMN, Schema.create(Schema.Type.LONG)));
    newFieldList.add(new Schema.Field(KAFKA_SOURCE_PARTITION_COLUMN, Schema.create(Schema.Type.INT)));
    newFieldList.add(new Schema.Field(KAFKA_SOURCE_TIMESTAMP_COLUMN, Schema.create(Schema.Type.LONG)));
    Schema newSchema = Schema.createRecord(schema.getName() + "_processed", schema.getDoc(), schema.getNamespace(), false, newFieldList);
    return newSchema;
  }
}
