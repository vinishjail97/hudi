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

  /*
   * Replace all invalid characters with "_";
   * Avro names should start with [A-Za-z_] and subsequently contain only [A-Za-z0-9_].
   * Please refer https://avro.apache.org/docs/1.11.1/specification/ for additional details.
   */
  private String renameForAvroNamingRules(String s) {
    if (s == null || s.isEmpty()) {
      return s;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      Character c = s.charAt(i);
      if (Character.isLetter(c) || c == '_') {
        sb.append(c);
      } else if (Character.isDigit(c)) {
        sb.append(i == 0 ? '_' : c);
      } else {
        // invalid character here. replace with '_';
        sb.append('_');
      }
    }
    return sb.toString();
  }

  private List<Object> transformList(List<Object> src) {
    List<Object> target = new ArrayList<>();
    for (Object obj : src) {
      if (obj instanceof List) {
        target.add(transformList((List<Object>) obj));
      } else if (obj instanceof Map) {
        target.add(transformMap((Map<String, Object>) obj));
      } else {
        target.add(obj);
      }
    }
    return target;
  }

  private Map<String, Object> transformMap(Map<String, Object> src) {
    Map<String, Object> target = new HashMap<>();
    for (Map.Entry<String, Object> e : src.entrySet()) {
      Object currValue = e.getValue();
      Object modifiedValue;
      if (currValue instanceof List) {
        modifiedValue = transformList((List<Object>) currValue);
      } else if (currValue instanceof Map) {
        modifiedValue = transformMap((Map<String, Object>) currValue);
      } else if (currValue instanceof String) {
        String currentStrValue = (String) currValue;
        modifiedValue = currentStrValue;
        if (e.getKey().equals(AVRO_NAME)) {
          modifiedValue = renameForAvroNamingRules(currentStrValue);
        }
      } else {
        modifiedValue = currValue;
      }
      target.put(e.getKey(), modifiedValue);
    }
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
   * We first rely on Avro to parse and we try to repair names only for those failed.
   * This way we can improve our repair process without breaking existing functionality.
   * For example we don't support repairing where multiple named schemas are defined in a file.
   */
  private Schema parseAvroSchema(String schemaStr) {
    try {
      return new Schema.Parser().parse(schemaStr);
    } catch (SchemaParseException spe) {
      // Here modify avro names and try parsing once again.
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
