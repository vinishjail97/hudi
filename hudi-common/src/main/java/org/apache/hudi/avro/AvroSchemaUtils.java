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

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ValidationUtils.checkState;

public class AvroSchemaUtils {

  private AvroSchemaUtils() {}

  /**
   * Appends provided new fields at the end of the given schema
   *
   * NOTE: No deduplication is made, this method simply appends fields at the end of the list
   *       of the source schema as is
   */
  public static Schema appendFieldsToSchema(Schema schema, List<Schema.Field> newFields) {
    List<Schema.Field> fields = schema.getFields().stream()
        .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()))
        .collect(Collectors.toList());
    fields.addAll(newFields);

    Schema newSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());
    newSchema.setFields(fields);
    return newSchema;
  }

  /**
   * Passed in {@code Union} schema and will try to resolve the field with the {@code fieldSchemaFullName}
   * w/in the union returning its corresponding schema
   *
   * @param schema target schema to be inspected
   * @param fieldSchemaFullName target field-name to be looked up w/in the union
   * @return schema of the field w/in the union identified by the {@code fieldSchemaFullName}
   */
  public static Schema resolveUnionSchema(Schema schema, String fieldSchemaFullName) {
    if (schema.getType() != Schema.Type.UNION) {
      return schema;
    }

    List<Schema> innerTypes = schema.getTypes();
    if (innerTypes.size() == 2 && schema.isNullable()) {
      // this is a basic nullable field so handle it more efficiently
      return resolveNullableSchema(schema);
    }

    Schema nonNullType =
        innerTypes.stream()
            .filter(it -> it.getType() != Schema.Type.NULL && Objects.equals(it.getFullName(), fieldSchemaFullName))
            .findFirst()
            .orElse(null);

    if (nonNullType == null) {
      throw new AvroRuntimeException(
          String.format("Unsupported Avro UNION type %s: Only UNION of a null type and a non-null type is supported", schema));
    }

    return nonNullType;
  }

  /**
   * Resolves typical Avro's nullable schema definition: {@code Union(Schema.Type.NULL, <NonNullType>)},
   * decomposing union and returning the target non-null type
   */
  public static Schema resolveNullableSchema(Schema schema) {
    if (schema.getType() != Schema.Type.UNION) {
      return schema;
    }

    List<Schema> innerTypes = schema.getTypes();

    if (innerTypes.size() != 2) {
      throw new AvroRuntimeException(
          String.format("Unsupported Avro UNION type %s: Only UNION of a null type and a non-null type is supported", schema));
    }
    Schema firstInnerType = innerTypes.get(0);
    Schema secondInnerType = innerTypes.get(1);
    if ((firstInnerType.getType() != Schema.Type.NULL && secondInnerType.getType() != Schema.Type.NULL)
        || (firstInnerType.getType() == Schema.Type.NULL && secondInnerType.getType() == Schema.Type.NULL)) {
      throw new AvroRuntimeException(
          String.format("Unsupported Avro UNION type %s: Only UNION of a null type and a non-null type is supported", schema));
    }
    return firstInnerType.getType() == Schema.Type.NULL ? secondInnerType : firstInnerType;
  }

  /**
   * Creates schema following Avro's typical nullable schema definition: {@code Union(Schema.Type.NULL, <NonNullType>)},
   * wrapping around provided target non-null type
   */
  public static Schema createNullableSchema(Schema.Type avroType) {
    return createNullableSchema(Schema.create(avroType));
  }

  public static Schema createNullableSchema(Schema schema) {
    checkState(schema.getType() != Schema.Type.NULL);
    return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
  }

  /**
   * Returns true in case when schema contains the field w/ provided name
   */
  public static boolean containsFieldInSchema(Schema schema, String fieldName) {
    try {
      Schema.Field field = schema.getField(fieldName);
      return field != null;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Returns true in case provided {@link Schema} is nullable (ie accepting null values),
   * returns false otherwise
   */
  public static boolean isNullable(Schema schema) {
    if (schema.getType() != Schema.Type.UNION) {
      return false;
    }

    List<Schema> innerTypes = schema.getTypes();
    return innerTypes.size() > 1 && innerTypes.stream().anyMatch(it -> it.getType() == Schema.Type.NULL);
  }
}
