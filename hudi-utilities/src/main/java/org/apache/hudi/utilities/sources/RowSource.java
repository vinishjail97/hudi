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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class RowSource extends Source<Dataset<Row>> {

  public static class Config {
    // sanitizes invalid columns both in the data read from source and also in the schema provider.
    // invalid definition here goes by avro naming convention (https://avro.apache.org/docs/current/spec.html#names).
    public static final String SANITIZE_AVRO_FIELD_NAMES = "hoodie.deltastreamer.source.sanitize.invalid.column.names";
    static final Boolean DEFAULT_RENAME_INVALID_COLUMNS = false;

    // Replacement/Mask for invalid characters in avro names.
    public static final String AVRO_FIELD_NAME_INVALID_CHAR_MASK = "hoodie.deltastreamer.source.mask.for.invalid.char";
  }

  public RowSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider, SourceType.ROW);
  }

  protected abstract Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit);

  @Override
  protected final InputBatch<Dataset<Row>> fetchNewData(Option<String> lastCkptStr, long sourceLimit) {
    Pair<Option<Dataset<Row>>, String> res = fetchNextBatch(lastCkptStr, sourceLimit);
    return res.getKey().map(dsr -> {
      SchemaProvider rowSchemaProvider =
          UtilHelpers.createRowBasedSchemaProvider(dsr.schema(), props, sparkContext);
      return new InputBatch<>(res.getKey(), res.getValue(), rowSchemaProvider);
    }).orElseGet(() -> new InputBatch<>(res.getKey(), res.getValue()));
  }
}
