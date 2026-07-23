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

package org.apache.hudi.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.avro.AvroRecordContext.getFieldValueFromIndexedRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TestAvroRecordContext {

  private static final Schema MAP_AND_ARRAY_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"complex\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"int\"},"
          + "{\"name\":\"str_map\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"string\"}],\"default\":null},"
          + "{\"name\":\"int_array\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"int\"}],\"default\":null},"
          + "{\"name\":\"rec_map\",\"type\":[\"null\",{\"type\":\"map\",\"values\":{\"type\":\"record\","
          + "\"name\":\"inner\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}}],\"default\":null}]}");

  private static Stream<Arguments> testConvertValueToEngineType() {
    return Stream.of(
        Arguments.of(1L, 1L),
        Arguments.of("test", new Utf8("test")),
        Arguments.of(new Utf8("utf8_string"), new Utf8("utf8_string")),
        Arguments.of(1.23, 1.23));
  }

  @ParameterizedTest
  @MethodSource
  void testConvertValueToEngineType(Comparable input, Comparable expected) {
    Comparable actual = AvroRecordContext.getFieldAccessorInstance().convertValueToEngineType(input);
    assertEquals(expected, actual);
  }

  @Test
  void testGetFieldValueMapAndArrayLeavesReturnNull() {
    GenericRecord record = new GenericData.Record(MAP_AND_ARRAY_SCHEMA);
    record.put("id", 7);
    Map<Utf8, Utf8> strMap = new HashMap<>();
    strMap.put(new Utf8("a"), new Utf8("v1"));
    record.put("str_map", strMap);
    record.put("int_array", Arrays.asList(3, 1, 2));
    // rec_map left null

    // top-level scalar still resolves normally
    assertEquals(7, getFieldValueFromIndexedRecord(record, "id"));

    // Parquet-style synthetic accessors that traverse a MAP (".key_value.key/value") or an
    // ARRAY (".list.element") cannot be resolved to a single value and must return null instead
    // of throwing. Regression: these previously threw
    // IllegalStateException "Cannot get field from schema type: MAP" during MOR log-append
    // column-stats collection. Such nested leaves still get statistics from the base-file path.
    assertNull(getFieldValueFromIndexedRecord(record, "str_map.key_value.key"));
    assertNull(getFieldValueFromIndexedRecord(record, "str_map.key_value.value"));
    assertNull(getFieldValueFromIndexedRecord(record, "int_array.list.element"));
    // a deep path descending through a MAP into a record field also degrades to null
    assertNull(getFieldValueFromIndexedRecord(record, "rec_map.key_value.value.x"));

    // and null when the complex field itself is absent/null
    GenericRecord empty = new GenericData.Record(MAP_AND_ARRAY_SCHEMA);
    empty.put("id", 0);
    assertNull(getFieldValueFromIndexedRecord(empty, "str_map.key_value.value"));
    assertNull(getFieldValueFromIndexedRecord(empty, "int_array.list.element"));
  }
}
