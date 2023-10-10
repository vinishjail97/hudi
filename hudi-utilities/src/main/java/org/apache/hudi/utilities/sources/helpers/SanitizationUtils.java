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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.MapType;

public class SanitizationUtils {

  public static class Config {
    // sanitizes invalid columns both in the data read from source and also in the schema.
    // invalid definition here goes by avro naming convention (https://avro.apache.org/docs/current/spec.html#names).
    public static final ConfigProperty<Boolean> SANITIZE_AVRO_FIELD_NAMES = ConfigProperty
        .key("hoodie.deltastreamer.source.sanitize.invalid.column.names")
        .defaultValue(false)
        .withDocumentation("Sanitizes invalid column names both in the data and also in the schema");

    public static final ConfigProperty<String> AVRO_FIELD_NAME_INVALID_CHAR_MASK =  ConfigProperty
        .key("hoodie.deltastreamer.source.sanitize.invalid.char.mask")
        .defaultValue("__")
        .withDocumentation("Character mask to be used as replacement for invalid field names");
  }

  private static boolean isNameSanitizingEnabled(TypedProperties properties) {
    return properties.getBoolean(Config.SANITIZE_AVRO_FIELD_NAMES.key(), Config.SANITIZE_AVRO_FIELD_NAMES.defaultValue());
  }

  /**
   * Replacement mask for invalid characters encountered in avro names.
   * @return sanitized value.
   */
  private static String getInvalidCharMask(TypedProperties properties) {
    return properties.getString(Config.AVRO_FIELD_NAME_INVALID_CHAR_MASK.key(),
        Config.AVRO_FIELD_NAME_INVALID_CHAR_MASK.defaultValue());
  }

  private static DataType sanitizeDataTypeForAvro(DataType dataType, String invalidCharMask) {
    if (dataType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) dataType;
      DataType sanitizedDataType = sanitizeDataTypeForAvro(arrayType.elementType(), invalidCharMask);
      return new ArrayType(sanitizedDataType, arrayType.containsNull());
    } else if (dataType instanceof MapType) {
      MapType mapType = (MapType) dataType;
      DataType sanitizedKeyDataType = sanitizeDataTypeForAvro(mapType.keyType(), invalidCharMask);
      DataType sanitizedValueDataType = sanitizeDataTypeForAvro(mapType.valueType(), invalidCharMask);
      return new MapType(sanitizedKeyDataType, sanitizedValueDataType, mapType.valueContainsNull());
    } else if (dataType instanceof StructType) {
      return sanitizeStructTypeForAvro((StructType) dataType, invalidCharMask);
    }
    return dataType;
  }

  // TODO: Rebase this to use InternalSchema when it is ready.
  private static StructType sanitizeStructTypeForAvro(StructType structType, String invalidCharMask) {
    StructType sanitizedStructType = new StructType();
    StructField[] structFields = structType.fields();
    for (StructField s : structFields) {
      DataType currFieldDataTypeSanitized = sanitizeDataTypeForAvro(s.dataType(), invalidCharMask);
      StructField structFieldCopy = new StructField(HoodieAvroUtils.sanitizeName(s.name(), invalidCharMask),
          currFieldDataTypeSanitized, s.nullable(), s.metadata());
      sanitizedStructType = sanitizedStructType.add(structFieldCopy);
    }
    return sanitizedStructType;
  }

  private static Dataset<Row> sanitizeColumnNamesForAvro(Dataset<Row> inputDataset, String invalidCharMask) {
    StructField[] inputFields = inputDataset.schema().fields();
    Dataset<Row> targetDataset = inputDataset;
    for (StructField sf : inputFields) {
      DataType sanitizedFieldDataType = sanitizeDataTypeForAvro(sf.dataType(), invalidCharMask);
      if (!sanitizedFieldDataType.equals(sf.dataType())) {
        // Sanitizing column names for nested types can be thought of as going from one schema to another
        // which are structurally similar except for actual column names itself. So casting is safe and sufficient.
        targetDataset = targetDataset.withColumn(sf.name(), targetDataset.col(sf.name()).cast(sanitizedFieldDataType));
      }
      String possibleRename = HoodieAvroUtils.sanitizeName(sf.name(), invalidCharMask);
      if (!sf.name().equals(possibleRename)) {
        targetDataset = targetDataset.withColumnRenamed(sf.name(), possibleRename);
      }
    }
    return targetDataset;
  }

  public static Dataset<Row> sanitizeColumnNamesForAvro(Dataset<Row> inputDataset, TypedProperties props) {
    return isNameSanitizingEnabled(props) ? sanitizeColumnNamesForAvro(inputDataset, getInvalidCharMask(props))
        : inputDataset;
  }

}
