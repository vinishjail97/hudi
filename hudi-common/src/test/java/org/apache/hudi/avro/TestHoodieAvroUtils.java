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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.SchemaCompatibilityException;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.avro.HoodieAvroUtils.getNestedFieldSchemaFromWriteSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests hoodie avro utilities.
 */
public class TestHoodieAvroUtils {

  private static String EVOLVED_SCHEMA = "{\"type\": \"record\",\"name\": \"testrec1\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"},"
      + "{\"name\": \"new_col_not_nullable_default_dummy_val\", \"type\": \"string\", \"default\": \"dummy_val\"},"
      + "{\"name\": \"new_col_nullable_wo_default\", \"type\": [\"int\", \"null\"]},"
      + "{\"name\": \"new_col_nullable_default_null\", \"type\": [\"null\" ,\"string\"],\"default\": null},"
      + "{\"name\": \"new_col_nullable_default_dummy_val\", \"type\": [\"string\" ,\"null\"],\"default\": \"dummy_val\"}]}";

  private static String EXAMPLE_SCHEMA = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"}]}";

  private static int NUM_FIELDS_IN_EXAMPLE_SCHEMA = 4;

  private static String SCHEMA_WITH_METADATA_FIELD = "{\"type\": \"record\",\"name\": \"testrec2\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"},"
      + "{\"name\": \"_hoodie_commit_time\", \"type\": [\"null\", \"string\"]},"
      + "{\"name\": \"nullable_field\",\"type\": [\"null\" ,\"string\"],\"default\": null},"
      + "{\"name\": \"nullable_field_wo_default\",\"type\": [\"null\" ,\"string\"]}]}";

  private static String SCHEMA_WITH_NON_NULLABLE_FIELD = "{\"type\": \"record\",\"name\": \"testrec3\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"},"
      + "{\"name\": \"nullable_field\",\"type\": [\"null\" ,\"string\"],\"default\": null},"
      + "{\"name\": \"non_nullable_field_wo_default\",\"type\": \"string\"},"
      + "{\"name\": \"non_nullable_field_with_default\",\"type\": \"string\", \"default\": \"dummy\"}]}";

  private static String SCHEMA_WITH_NON_NULLABLE_FIELD_WITH_DEFAULT = "{\"type\": \"record\",\"name\": \"testrec4\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"},"
      + "{\"name\": \"nullable_field\",\"type\": [\"null\" ,\"string\"],\"default\": null},"
      + "{\"name\": \"non_nullable_field_with_default\",\"type\": \"string\", \"default\": \"dummy\"}]}";

  private static String SCHEMA_WITH_DECIMAL_FIELD = "{\"type\":\"record\",\"name\":\"record\",\"fields\":["
      + "{\"name\":\"key_col\",\"type\":[\"null\",\"int\"],\"default\":null},"
      + "{\"name\":\"decimal_col\",\"type\":[\"null\","
      + "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":8,\"scale\":4}],\"default\":null}]}";

  private static String SCHEMA_WITH_NESTED_FIELD = "{\"name\":\"MyClass\",\"type\":\"record\",\"namespace\":\"com.acme.avro\",\"fields\":["
      + "{\"name\":\"firstname\",\"type\":\"string\"},"
      + "{\"name\":\"lastname\",\"type\":\"string\"},"
      + "{\"name\":\"student\",\"type\":{\"name\":\"student\",\"type\":\"record\",\"fields\":["
      + "{\"name\":\"firstname\",\"type\":[\"null\" ,\"string\"],\"default\": null},{\"name\":\"lastname\",\"type\":[\"null\" ,\"string\"],\"default\": null}]}}]}";

  private static String SCHEMA_WITH_NESTED_FIELD_RENAMED = "{\"name\":\"MyClass\",\"type\":\"record\",\"namespace\":\"com.acme.avro\",\"fields\":["
      + "{\"name\":\"fn\",\"type\":\"string\"},"
      + "{\"name\":\"ln\",\"type\":\"string\"},"
      + "{\"name\":\"ss\",\"type\":{\"name\":\"ss\",\"type\":\"record\",\"fields\":["
      + "{\"name\":\"fn\",\"type\":[\"null\" ,\"string\"],\"default\": null},{\"name\":\"ln\",\"type\":[\"null\" ,\"string\"],\"default\": null}]}}]}";

  private static final String COMPLEX_SCHEMA_PRE_EVOLUTION = "{\"type\":\"record\",\"name\":\"Sample\",\"namespace\":\"test\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"},"
      + "{\"name\":\"ts\",\"type\":\"long\"},{\"name\":\"level\",\"type\":\"string\"},{\"name\":\"nested_record\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Nested\","
      + "\"fields\":[{\"name\":\"nested_int\",\"type\":\"int\",\"default\":0},{\"name\":\"double_nested\",\"type\":{\"type\":\"record\",\"name\":\"DoubleNested\","
      + "\"fields\":[{\"name\":\"double_nested_int\",\"type\":\"int\",\"default\":0}]}}]}],\"default\":null},{\"name\":\"nullable_map_field\","
      + "\"type\":[\"null\",{\"type\":\"map\",\"values\":\"Nested\"}],\"default\":null},{\"name\":\"primitive_map_field\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}]}";
  private static final String COMPLEX_SCHEMA = "{\"type\":\"record\",\"name\":\"Sample\",\"namespace\":\"test\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"},{\"name\":\"ts\",\"type\":\"long\"},"
      + "{\"name\":\"level\",\"type\":[\"string\", \"null\"]},{\"name\":\"nested_record\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Nested\","
      + "\"fields\":[{\"name\":\"nested_int\",\"type\":\"int\",\"default\":0},{\"name\":\"double_nested\",\"type\":{\"type\":\"record\",\"name\":\"DoubleNested\","
      + "\"fields\":[{\"name\":\"double_nested_int\",\"type\":\"int\",\"default\":0}]}},{\"name\":\"level\",\"type\":\"string\"}]}],\"default\":null},"
      + "{\"name\":\"nullable_map_field\",\"type\":[\"null\",{\"type\":\"map\",\"values\":[\"Nested\", \"null\"]}],\"default\":null},"
      + "{\"name\":\"primitive_map_field\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"array_field\",\"type\":{\"type\":\"array\",\"items\":[\"null\", \"Nested\"]},\"default\":[]},"
      + "{\"name\":\"primitive_array_field\",\"type\":[{\"type\":\"array\",\"items\":\"string\"}, \"null\"]}]}";

  @Test
  public void testIdTrackingBootstrapWithSchema() {
    Schema input = new Schema.Parser().parse(COMPLEX_SCHEMA);
    // initially schema does not have id tracking set so must bootstrap even though there is a previous schema
    Schema schemaWithIdTracking = HoodieAvroUtils.addIdTracking(input, Option.of(input), true);
    IdTracking actual = HoodieAvroUtils.getIdTracking(schemaWithIdTracking).get();
    assertEquals(getExpectTrackingForComplexSchema(), actual);
    // validate that meta fields are not added to the schema itself
    assertEquals(input.getFields().size(), schemaWithIdTracking.getFields().size());
  }

  @Test
  public void testIdTrackingBootstrapWithoutSchema() {
    Schema input = new Schema.Parser().parse(COMPLEX_SCHEMA);
    Schema schemaWithIdTracking = HoodieAvroUtils.addIdTracking(input, Option.empty(), true);
    IdTracking actual = HoodieAvroUtils.getIdTracking(schemaWithIdTracking).get();
    assertEquals(getExpectTrackingForComplexSchema(), actual);
  }

  @Test
  public void testIdTrackingWithIdenticalSchemas() {
    Schema input = new Schema.Parser().parse(COMPLEX_SCHEMA);
    Schema inputWithIdTracking = HoodieAvroUtils.addIdTracking(input, Option.empty(), true);
    // Should be a no-op
    Schema schemaWithIdTracking = HoodieAvroUtils.addIdTracking(input, Option.of(inputWithIdTracking), true);
    IdTracking actual = HoodieAvroUtils.getIdTracking(schemaWithIdTracking).get();
    assertEquals(getExpectTrackingForComplexSchema(), actual);
  }

  @Test
  public void testIdTrackingWithPreviousSchema() {
    Schema initial = HoodieAvroUtils.addIdTracking(new Schema.Parser().parse(COMPLEX_SCHEMA_PRE_EVOLUTION), Option.empty(), true);
    Schema evolved = new Schema.Parser().parse(COMPLEX_SCHEMA);
    Schema schemaWithIdTracking = HoodieAvroUtils.addIdTracking(evolved, Option.of(initial), true);
    IdTracking actual = HoodieAvroUtils.getIdTracking(schemaWithIdTracking).get();
    assertEquals(getExpectTrackingForComplexSchemaEvolved(), actual);
  }

  @Test
  public void testIdTrackingWithPreviousSchemaWithoutMetaFields() {
    Schema initial = HoodieAvroUtils.addIdTracking(new Schema.Parser().parse(COMPLEX_SCHEMA_PRE_EVOLUTION), Option.empty(), false);
    Schema evolved = new Schema.Parser().parse(COMPLEX_SCHEMA);
    Schema schemaWithIdTracking = HoodieAvroUtils.addIdTracking(evolved, Option.of(initial), false);
    IdTracking actual = HoodieAvroUtils.getIdTracking(schemaWithIdTracking).get();
    assertEquals(getExpectTrackingForComplexSchemaEvolvedNoMetaFields(), actual);
  }

  @Test
  public void testIdTrackingWithFieldRemoval() {
    // create initial schema with 2 fields and assign IDs
    Schema initial = Schema.createRecord("test1", null, "hudi", false,
        Arrays.asList(new Schema.Field("field1", Schema.create(Schema.Type.STRING)), new Schema.Field("field2", Schema.create(Schema.Type.STRING))));
    Schema initialWithIdTracking = HoodieAvroUtils.addIdTracking(initial, Option.empty(), false);
    // remove the second field
    Schema withFieldRemoved = Schema.createRecord("test2", null, "hudi", false,
        Arrays.asList(new Schema.Field("field1", Schema.create(Schema.Type.STRING))));
    Schema withFieldRemovedAndIdTracking = HoodieAvroUtils.addIdTracking(withFieldRemoved, Option.of(initialWithIdTracking), false);
    IdTracking actualWithFieldRemoved = HoodieAvroUtils.getIdTracking(withFieldRemovedAndIdTracking).get();
    IdTracking expectedWithFieldRemoved = new IdTracking(Arrays.asList(new IdMapping("field1", 1)), 2);
    assertEquals(expectedWithFieldRemoved, actualWithFieldRemoved);
    // Adding a new field should be tracked with ID 3
    Schema withFieldAdded = Schema.createRecord("test2", null, "hudi", false,
        Arrays.asList(new Schema.Field("field1", Schema.create(Schema.Type.STRING)), new Schema.Field("field3", Schema.create(Schema.Type.STRING))));
    Schema withFieldAddedAndIdTracking = HoodieAvroUtils.addIdTracking(withFieldAdded, Option.of(withFieldRemovedAndIdTracking), false);
    IdTracking actualWithFieldAdded = HoodieAvroUtils.getIdTracking(withFieldAddedAndIdTracking).get();
    IdTracking expectedWithFieldAdded = new IdTracking(Arrays.asList(new IdMapping("field1", 1), new IdMapping("field3", 3)), 3);
    assertEquals(expectedWithFieldAdded, actualWithFieldAdded);
  }

  @Test
  public void testIdTrackingAddMetaFields() {
    // create initial schema with a meta field manually specified
    Schema initial = Schema.createRecord("test1", null, "hudi", false,
        Arrays.asList(new Schema.Field("_hoodie_commit_time", Schema.create(Schema.Type.STRING)), new Schema.Field("field1", Schema.create(Schema.Type.STRING))));
    Schema initialWithIdTracking = HoodieAvroUtils.addIdTracking(initial, Option.empty(), false);
    // add all meta fields and ensure IDs are properly assigned
    Schema withMetaFields = Schema.createRecord("test2", null, "hudi", false,
        Arrays.asList(new Schema.Field("_hoodie_commit_time", Schema.create(Schema.Type.STRING)), new Schema.Field("field1", Schema.create(Schema.Type.STRING))));
    Schema withMetaFieldsAndIdTracking = HoodieAvroUtils.addIdTracking(withMetaFields, Option.of(initialWithIdTracking), true);
    IdTracking actual = HoodieAvroUtils.getIdTracking(withMetaFieldsAndIdTracking).get();
    IdTracking expected = new IdTracking(Arrays.asList(
        new IdMapping("_hoodie_commit_time", 1),
        new IdMapping("field1", 2),
        new IdMapping("_hoodie_commit_seqno", 3),
        new IdMapping("_hoodie_record_key", 4),
        new IdMapping("_hoodie_partition_path", 5),
        new IdMapping("_hoodie_file_name", 6)), 6);
    assertEquals(expected, actual);
  }

  @Test
  public void testPropsPresent() {
    Schema schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    boolean piiPresent = false;
    for (Schema.Field field : schema.getFields()) {
      if (HoodieAvroUtils.isMetadataField(field.name())) {
        continue;
      }

      assertNotNull(field.name(), "field name is null");
      Map<String, Object> props = field.getObjectProps();
      assertNotNull(props, "The property is null");

      if (field.name().equals("pii_col")) {
        piiPresent = true;
        assertTrue(props.containsKey("column_category"), "sensitivity_level is removed in field 'pii_col'");
      } else {
        assertEquals(0, props.size(), "The property shows up but not set");
      }
    }
    assertTrue(piiPresent, "column pii_col doesn't show up");
  }

  @Test
  public void testDefaultValue() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EVOLVED_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    Schema schemaWithMetadata = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(EVOLVED_SCHEMA));
    GenericRecord rec1 = HoodieAvroUtils.rewriteRecord(rec, schemaWithMetadata);
    assertEquals("dummy_val", rec1.get("new_col_not_nullable_default_dummy_val"));
    assertNull(rec1.get("new_col_nullable_wo_default"));
    assertNull(rec1.get("new_col_nullable_default_null"));
    assertEquals("dummy_val", rec1.get("new_col_nullable_default_dummy_val"));
    assertNull(rec1.get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
  }

  @Test
  public void testDefaultValueWithSchemaEvolution() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    GenericRecord rec1 = HoodieAvroUtils.rewriteRecord(rec, new Schema.Parser().parse(EVOLVED_SCHEMA));
    assertEquals("dummy_val", rec1.get("new_col_not_nullable_default_dummy_val"));
    assertNull(rec1.get("new_col_nullable_wo_default"));
  }

  @Test
  public void testMetadataField() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    GenericRecord rec1 = HoodieAvroUtils.rewriteRecord(rec, new Schema.Parser().parse(SCHEMA_WITH_METADATA_FIELD));
    assertNull(rec1.get("_hoodie_commit_time"));
    assertNull(rec1.get("nullable_field"));
    assertNull(rec1.get("nullable_field_wo_default"));
  }

  @Test
  public void testNonNullableFieldWithoutDefault() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    assertThrows(SchemaCompatibilityException.class, () -> HoodieAvroUtils.rewriteRecord(rec, new Schema.Parser().parse(SCHEMA_WITH_NON_NULLABLE_FIELD)));
  }

  @Test
  public void testNonNullableFieldWithDefault() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    GenericRecord rec1 = HoodieAvroUtils.rewriteRecord(rec, new Schema.Parser().parse(SCHEMA_WITH_NON_NULLABLE_FIELD_WITH_DEFAULT));
    assertEquals("dummy", rec1.get("non_nullable_field_with_default"));
  }

  @Test
  public void testJsonNodeNullWithDefaultValues() {
    List<Schema.Field> fields = new ArrayList<>();
    Schema initialSchema = Schema.createRecord("test_record", "test record", "org.test.namespace", false);
    Schema.Field field1 = new Schema.Field("key", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field field2 = new Schema.Field("key1", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field field3 = new Schema.Field("key2", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    fields.add(field1);
    fields.add(field2);
    fields.add(field3);
    initialSchema.setFields(fields);
    GenericRecord rec = new GenericData.Record(initialSchema);
    rec.put("key", "val");
    rec.put("key1", "val1");
    rec.put("key2", "val2");

    List<Schema.Field> evolvedFields = new ArrayList<>();
    Schema evolvedSchema = Schema.createRecord("evolved_record", "evolved record", "org.evolved.namespace", false);
    Schema.Field evolvedField1 = new Schema.Field("key", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field evolvedField2 = new Schema.Field("key1", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field evolvedField3 = new Schema.Field("key2", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field evolvedField4 = new Schema.Field("evolved_field", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field evolvedField5 = new Schema.Field("evolved_field1", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    evolvedFields.add(evolvedField1);
    evolvedFields.add(evolvedField2);
    evolvedFields.add(evolvedField3);
    evolvedFields.add(evolvedField4);
    evolvedFields.add(evolvedField5);
    evolvedSchema.setFields(evolvedFields);

    GenericRecord rec1 = HoodieAvroUtils.rewriteRecord(rec, evolvedSchema);
    //evolvedField4.defaultVal() returns a JsonProperties.Null instance.
    assertNull(rec1.get("evolved_field"));
    //evolvedField5.defaultVal() returns null.
    assertNull(rec1.get("evolved_field1"));
  }

  @Test
  public void testAddingAndRemovingMetadataFields() {
    Schema schemaWithMetaCols = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    assertEquals(NUM_FIELDS_IN_EXAMPLE_SCHEMA + HoodieRecord.HOODIE_META_COLUMNS.size(), schemaWithMetaCols.getFields().size());
    Schema schemaWithoutMetaCols = HoodieAvroUtils.removeMetadataFields(schemaWithMetaCols);
    assertEquals(NUM_FIELDS_IN_EXAMPLE_SCHEMA, schemaWithoutMetaCols.getFields().size());
  }

  @Test
  public void testRemoveFields() {
    // partitioned table test.
    String schemaStr = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
        + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
        + "{\"name\": \"non_pii_col\", \"type\": \"string\"}]},";
    Schema expectedSchema = new Schema.Parser().parse(schemaStr);
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    GenericRecord rec1 = HoodieAvroUtils.removeFields(rec, Collections.singleton("pii_col"));
    assertEquals("key1", rec1.get("_row_key"));
    assertEquals("val1", rec1.get("non_pii_col"));
    assertEquals(3.5, rec1.get("timestamp"));
    if (HoodieAvroUtils.gteqAvro1_10()) {
      GenericRecord finalRec1 = rec1;
      assertThrows(AvroRuntimeException.class, () -> finalRec1.get("pii_col"));
    } else {
      assertNull(rec1.get("pii_col"));
    }
    assertEquals(expectedSchema, rec1.getSchema());

    // non-partitioned table test with empty list of fields.
    schemaStr = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
        + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
        + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
        + "{\"name\": \"pii_col\", \"type\": \"string\"}]},";
    expectedSchema = new Schema.Parser().parse(schemaStr);
    rec1 = HoodieAvroUtils.removeFields(rec, Collections.singleton(""));
    assertEquals(expectedSchema, rec1.getSchema());
  }

  @Test
  public void testGetRootLevelFieldName() {
    assertEquals("a", HoodieAvroUtils.getRootLevelFieldName("a.b.c"));
    assertEquals("a", HoodieAvroUtils.getRootLevelFieldName("a"));
    assertEquals("", HoodieAvroUtils.getRootLevelFieldName(""));
  }

  @Test
  public void testGetNestedFieldVal() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");

    Object rowKey = HoodieAvroUtils.getNestedFieldVal(rec, "_row_key", true, false);
    assertEquals("key1", rowKey);

    Object rowKeyNotExist = HoodieAvroUtils.getNestedFieldVal(rec, "fake_key", true, false);
    assertNull(rowKeyNotExist);

    // Field does not exist
    assertEquals("fake_key(Part -fake_key) field not found in record. Acceptable fields were :[timestamp, _row_key, non_pii_col, pii_col]",
        assertThrows(HoodieException.class, () ->
            HoodieAvroUtils.getNestedFieldVal(rec, "fake_key", false, false)).getMessage());

    // Field exists while value not
    assertNull(HoodieAvroUtils.getNestedFieldVal(rec, "timestamp", false, false));
  }

  @Test
  public void testGetNestedFieldValWithNestedField() {
    Schema nestedSchema = new Schema.Parser().parse(SCHEMA_WITH_NESTED_FIELD);
    GenericRecord rec = new GenericData.Record(nestedSchema);

    // test get .
    assertEquals(". field not found in record. Acceptable fields were :[firstname, lastname, student]",
        assertThrows(HoodieException.class, () ->
            HoodieAvroUtils.getNestedFieldVal(rec, ".", false, false)).getMessage());

    // test get fake_key
    assertEquals("fake_key(Part -fake_key) field not found in record. Acceptable fields were :[firstname, lastname, student]",
        assertThrows(HoodieException.class, () ->
            HoodieAvroUtils.getNestedFieldVal(rec, "fake_key", false, false)).getMessage());

    // test get student(null)
    assertNull(HoodieAvroUtils.getNestedFieldVal(rec, "student", false, false));

    // test get student
    GenericRecord studentRecord = new GenericData.Record(rec.getSchema().getField("student").schema());
    studentRecord.put("firstname", "person");
    rec.put("student", studentRecord);
    assertEquals(studentRecord, HoodieAvroUtils.getNestedFieldVal(rec, "student", false, false));

    // test get student.fake_key
    assertEquals("student.fake_key(Part -fake_key) field not found in record. Acceptable fields were :[firstname, lastname]",
        assertThrows(HoodieException.class, () ->
            HoodieAvroUtils.getNestedFieldVal(rec, "student.fake_key", false, false)).getMessage());

    // test get student.firstname
    assertEquals("person", HoodieAvroUtils.getNestedFieldVal(rec, "student.firstname", false, false));

    // test get student.lastname(null)
    assertNull(HoodieAvroUtils.getNestedFieldVal(rec, "student.lastname", false, false));

    // test get student.firstname.fake_key
    assertEquals("Cannot find a record at part value :firstname",
        assertThrows(HoodieException.class, () ->
            HoodieAvroUtils.getNestedFieldVal(rec, "student.firstname.fake_key", false, false)).getMessage());

    // test get student.lastname(null).fake_key
    assertEquals("Cannot find a record at part value :lastname",
        assertThrows(HoodieException.class, () ->
            HoodieAvroUtils.getNestedFieldVal(rec, "student.lastname.fake_key", false, false)).getMessage());
  }

  @Test
  public void testGetNestedFieldValWithDecimalField() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(SCHEMA_WITH_DECIMAL_FIELD));
    rec.put("key_col", "key");
    BigDecimal bigDecimal = new BigDecimal("1234.5678");
    ByteBuffer byteBuffer = ByteBuffer.wrap(bigDecimal.unscaledValue().toByteArray());
    rec.put("decimal_col", byteBuffer);

    Object decimalCol = HoodieAvroUtils.getNestedFieldVal(rec, "decimal_col", true, false);
    assertEquals(bigDecimal, decimalCol);

    Object obj = rec.get(1);
    assertTrue(obj instanceof ByteBuffer);
    ByteBuffer buffer = (ByteBuffer) obj;
    assertEquals(0, buffer.position());
  }

  @Test
  public void testGetNestedFieldSchema() throws IOException {
    Schema schema = SchemaTestUtil.getEvolvedSchema();
    GenericRecord rec = new GenericData.Record(schema);
    rec.put("field1", "key1");
    rec.put("field2", "val1");
    rec.put("name", "val2");
    rec.put("favorite_number", 2);
    // test simple field schema
    assertEquals(Schema.create(Schema.Type.STRING), getNestedFieldSchemaFromWriteSchema(rec.getSchema(), "field1"));

    GenericRecord rec2 = new GenericData.Record(schema);
    rec2.put("field1", "key1");
    rec2.put("field2", "val1");
    rec2.put("name", "val2");
    rec2.put("favorite_number", 12);
    // test comparison of non-string type
    assertEquals(-1, GenericData.get().compare(rec.get("favorite_number"), rec2.get("favorite_number"), getNestedFieldSchemaFromWriteSchema(rec.getSchema(), "favorite_number")));

    // test nested field schema
    Schema nestedSchema = new Schema.Parser().parse(SCHEMA_WITH_NESTED_FIELD);
    GenericRecord rec3 = new GenericData.Record(nestedSchema);
    rec3.put("firstname", "person1");
    rec3.put("lastname", "person2");
    GenericRecord studentRecord = new GenericData.Record(rec3.getSchema().getField("student").schema());
    studentRecord.put("firstname", "person1");
    studentRecord.put("lastname", "person2");
    rec3.put("student", studentRecord);

    assertEquals(Schema.create(Schema.Type.STRING), getNestedFieldSchemaFromWriteSchema(rec3.getSchema(), "student.firstname"));
    assertEquals(Schema.create(Schema.Type.STRING), getNestedFieldSchemaFromWriteSchema(nestedSchema, "student.firstname"));
  }

  @Test
  public void testReWriteAvroRecordWithNewSchema() {
    Schema nestedSchema = new Schema.Parser().parse(SCHEMA_WITH_NESTED_FIELD);
    GenericRecord rec3 = new GenericData.Record(nestedSchema);
    rec3.put("firstname", "person1");
    rec3.put("lastname", "person2");
    GenericRecord studentRecord = new GenericData.Record(rec3.getSchema().getField("student").schema());
    studentRecord.put("firstname", "person1");
    studentRecord.put("lastname", "person2");
    rec3.put("student", studentRecord);

    Schema nestedSchemaRename = new Schema.Parser().parse(SCHEMA_WITH_NESTED_FIELD_RENAMED);
    Map<String, String> colRenames = new HashMap<>();
    colRenames.put("fn", "firstname");
    colRenames.put("ln", "lastname");
    colRenames.put("ss", "student");
    colRenames.put("ss.fn", "firstname");
    colRenames.put("ss.ln", "lastname");
    GenericRecord studentRecordRename = HoodieAvroUtils.rewriteRecordWithNewSchema(rec3, nestedSchemaRename, colRenames);
    Assertions.assertEquals(GenericData.get().validate(nestedSchemaRename, studentRecordRename), true);
  }

  @Test
  public void testConvertDaysToDate() {
    Date now = new Date(System.currentTimeMillis());
    int days = HoodieAvroUtils.fromJavaDate(now);
    assertEquals(now.toLocalDate(), HoodieAvroUtils.toJavaDate(days).toLocalDate());
  }

  private static IdTracking getExpectTrackingForComplexSchema() {
    List<IdMapping> idMappings = Arrays.asList(
        new IdMapping("_hoodie_commit_time", 1),
        new IdMapping("_hoodie_commit_seqno", 2),
        new IdMapping("_hoodie_record_key", 3),
        new IdMapping("_hoodie_partition_path", 4),
        new IdMapping("_hoodie_file_name", 5),
        new IdMapping("key", 6),
        new IdMapping("ts", 7),
        new IdMapping("level", 8),
        new IdMapping("nested_record", 9, Arrays.asList(
            new IdMapping("nested_int", 14),
            new IdMapping("double_nested", 15, Arrays.asList(
                new IdMapping("double_nested_int", 17)
            )),
            new IdMapping("level", 16)
        )),
        new IdMapping("nullable_map_field", 10, Arrays.asList(
            new IdMapping("key", 18),
            new IdMapping("value", 19, Arrays.asList(
                new IdMapping("nested_int", 20),
                new IdMapping("double_nested", 21, Arrays.asList(
                    new IdMapping("double_nested_int", 23)
                )),
                new IdMapping("level", 22)
            ))
        )),
        new IdMapping("primitive_map_field", 11, Arrays.asList(new IdMapping("key", 24), new IdMapping("value", 25))),
        new IdMapping("array_field", 12, Arrays.asList(
            new IdMapping("element", 26, Arrays.asList(
                new IdMapping("nested_int", 27),
                new IdMapping("double_nested", 28, Arrays.asList(
                    new IdMapping("double_nested_int", 30)
                )),
                new IdMapping("level", 29)
            ))
        )),
        new IdMapping("primitive_array_field", 13, Arrays.asList(new IdMapping("element", 31))));
    return new IdTracking(idMappings, 31);
  }

  private static IdTracking getExpectTrackingForComplexSchemaEvolved() {
    List<IdMapping> idMappings = Arrays.asList(
        new IdMapping("_hoodie_commit_time", 1),
        new IdMapping("_hoodie_commit_seqno", 2),
        new IdMapping("_hoodie_record_key", 3),
        new IdMapping("_hoodie_partition_path", 4),
        new IdMapping("_hoodie_file_name", 5),
        new IdMapping("key", 6),
        new IdMapping("ts", 7),
        new IdMapping("level", 8),
        new IdMapping("nested_record", 9, Arrays.asList(
            new IdMapping("nested_int", 12),
            new IdMapping("double_nested", 13, Arrays.asList(
                new IdMapping("double_nested_int", 14)
            )),
            new IdMapping("level", 24)
        )),
        new IdMapping("nullable_map_field", 10, Arrays.asList(
            new IdMapping("key", 15),
            new IdMapping("value", 16, Arrays.asList(
                new IdMapping("nested_int", 17),
                new IdMapping("double_nested", 18, Arrays.asList(
                    new IdMapping("double_nested_int", 19)
                )),
                new IdMapping("level", 25)
            ))
        )),
        new IdMapping("primitive_map_field", 11, Arrays.asList(new IdMapping("key", 20), new IdMapping("value", 21))),
        new IdMapping("array_field", 22, Arrays.asList(
            new IdMapping("element", 26, Arrays.asList(
                new IdMapping("nested_int", 27),
                new IdMapping("double_nested", 28, Arrays.asList(
                    new IdMapping("double_nested_int", 30)
                )),
                new IdMapping("level", 29)
            ))
        )),
        new IdMapping("primitive_array_field", 23, Arrays.asList(new IdMapping("element", 31))));
    return new IdTracking(idMappings, 31);
  }

  private static IdTracking getExpectTrackingForComplexSchemaEvolvedNoMetaFields() {
    List<IdMapping> idMappings = Arrays.asList(
        new IdMapping("key", 1),
        new IdMapping("ts", 2),
        new IdMapping("level", 3),
        new IdMapping("nested_record", 4, Arrays.asList(
            new IdMapping("nested_int", 7),
            new IdMapping("double_nested", 8, Arrays.asList(
                new IdMapping("double_nested_int", 9)
            )),
            new IdMapping("level", 19)
        )),
        new IdMapping("nullable_map_field", 5, Arrays.asList(
            new IdMapping("key", 10),
            new IdMapping("value", 11, Arrays.asList(
                new IdMapping("nested_int", 12),
                new IdMapping("double_nested", 13, Arrays.asList(
                    new IdMapping("double_nested_int", 14)
                )),
                new IdMapping("level", 20)
            ))
        )),
        new IdMapping("primitive_map_field", 6, Arrays.asList(new IdMapping("key", 15), new IdMapping("value", 16))),
        new IdMapping("array_field", 17, Arrays.asList(
            new IdMapping("element", 21, Arrays.asList(
                new IdMapping("nested_int", 22),
                new IdMapping("double_nested", 23, Arrays.asList(
                    new IdMapping("double_nested_int", 25)
                )),
                new IdMapping("level", 24)
            ))
        )),
        new IdMapping("primitive_array_field", 18, Arrays.asList(new IdMapping("element", 26))));
    return new IdTracking(idMappings, 26);
  }
}
