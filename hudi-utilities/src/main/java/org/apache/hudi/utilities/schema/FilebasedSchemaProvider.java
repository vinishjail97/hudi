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

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.exception.HoodieIOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A simple schema provider, that reads off files on DFS.
 */
public class FilebasedSchemaProvider extends SchemaProvider {

  private static final String AVRO_NAME = "name";

  /**
   * Configs supported.
   */
  public static class Config {
    private static final String SOURCE_SCHEMA_FILE_PROP = "hoodie.deltastreamer.schemaprovider.source.schema.file";
    private static final String TARGET_SCHEMA_FILE_PROP = "hoodie.deltastreamer.schemaprovider.target.schema.file";
  }

  private final FileSystem fs;

  protected Schema sourceSchema;

  protected Schema targetSchema;

  private List<Object> transformList(List<Object> src) {
    List<Object> target = new ArrayList<>();
    src.forEach(obj -> {
      if (obj instanceof List) {
        target.add(transformList((List<Object>) obj));
      } else if (obj instanceof Map) {
        target.add(transformMap((Map<String, Object>) obj));
      } else {
        target.add(obj);
      }
    });
    return target;
  }

  private Map<String, Object> transformMap(Map<String, Object> src) {
    Map<String, Object> target = new HashMap<>();
    src.forEach((currKey, currValue) -> {
      Object modifiedValue;
      if (currValue instanceof List) {
        modifiedValue = transformList((List<Object>) currValue);
      } else if (currValue instanceof Map) {
        modifiedValue = transformMap((Map<String, Object>) currValue);
      } else if (currValue instanceof String) {
        String currentStrValue = (String) currValue;
        modifiedValue = currentStrValue;
        if (currKey.equals(AVRO_NAME)) {
          modifiedValue = HoodieAvroUtils.sanitizeName(currentStrValue);
        }
      } else {
        modifiedValue = currValue;
      }
      target.put(currKey, modifiedValue);
    });
    return target;
  }

  private Schema parseAvroSchemaWithRenaming(String schemaStr) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Object> objMap = objectMapper.readValue(schemaStr, Map.class);
    Map<String, Object> modifiedMap = transformMap(objMap);
    return new Schema.Parser().parse(objectMapper.writeValueAsString(modifiedMap));
  }

  private ParseResult parseAvroSchemaWrapper(String schemaStr) {
    try {
      Schema avroSchema = parseAvroSchemaWithRenaming(schemaStr);
      return new ParseResult(avroSchema, false);
    } catch (Exception ex) {
      // for any exception, set parsing to failed and return.
      return new ParseResult(null, true);
    }
  }

  /*
   * We first rely on Avro to parse and then try to rename only for those failed.
   * This way we can improve our parsing capabilities without breaking existing functionality.
   * For example we don't yet support multiple named schemas defined in a file.
   */
  private Schema parseAvroSchema(String schemaStr) {
    try {
      return new Schema.Parser().parse(schemaStr);
    } catch (SchemaParseException spe) {
      // Rename avro fields and try parsing once again.
      ParseResult parseResult = parseAvroSchemaWrapper(schemaStr);
      if (parseResult.isParsingFailed()) {
        // throw original exception.
        throw spe;
      }
      return parseResult.getParsedSchema();
    }
  }

  private Schema readAvroSchemaFromFile(String schemaPath) {
    String schemaStr;
    FSDataInputStream in = null;
    try {
      in = this.fs.open(new Path(schemaPath));
      schemaStr = FileIOUtils.readAsUTFString(in);
    } catch (IOException ioe) {
      throw new HoodieIOException(String.format("Error reading schema from file %s", schemaPath), ioe);
    } finally {
      IOUtils.closeStream(in);
    }
    return parseAvroSchema(schemaStr);
  }

  public FilebasedSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(Config.SOURCE_SCHEMA_FILE_PROP));
    String sourceFile = props.getString(Config.SOURCE_SCHEMA_FILE_PROP);
    this.fs = FSUtils.getFs(sourceFile, jssc.hadoopConfiguration(), true);
    this.sourceSchema = readAvroSchemaFromFile(sourceFile);
    if (props.containsKey(Config.TARGET_SCHEMA_FILE_PROP)) {
      this.targetSchema = readAvroSchemaFromFile(props.getString(Config.TARGET_SCHEMA_FILE_PROP));
    }
  }

  @Override
  public Schema getSourceSchema() {
    return sourceSchema;
  }

  @Override
  public Schema getTargetSchema() {
    if (targetSchema != null) {
      return targetSchema;
    } else {
      return super.getTargetSchema();
    }
  }

  private static class ParseResult {
    private Schema parsedSchema;
    private boolean parsingFailed;

    public ParseResult(Schema parsedSchema, boolean parsingFailed) {
      this.parsedSchema = parsedSchema;
      this.parsingFailed = parsingFailed;
    }

    public Schema getParsedSchema() {
      return this.parsedSchema;
    }

    public boolean isParsingFailed() {
      return this.parsingFailed;
    }
  }
}
