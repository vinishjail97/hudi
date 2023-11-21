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

package org.apache.hudi.sync.common.util;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConfigUtils {
  /**
   * Config stored in hive serde properties to tell query engine (spark/flink) to
   * read the table as a read-optimized table when this config is true.
   */
  public static final String IS_QUERY_AS_RO_TABLE = "hoodie.query.as.ro.table";

  /**
   * Config stored in hive serde properties to tell query engine (spark) the
   * location to read.
   */
  public static final String TABLE_SERDE_PATH = "path";

  /**
   * Convert the key-value config to a map.The format of the config
   * is a key-value pair just like "k1=v1\nk2=v2\nk3=v3".
   *
   * @param keyValueConfig
   * @return
   */
  public static Map<String, String> toMap(String keyValueConfig) {
    if (StringUtils.isNullOrEmpty(keyValueConfig)) {
      return new HashMap<>();
    }
    String[] keyvalues = keyValueConfig.split("\n");
    Map<String, String> tableProperties = new HashMap<>();
    for (String keyValue : keyvalues) {
      // Handle multiple new lines and lines that contain only spaces after splitting
      if (keyValue.trim().isEmpty()) {
        continue;
      }
      String[] keyValueArray = keyValue.split("=");
      if (keyValueArray.length == 1 || keyValueArray.length == 2) {
        String key = keyValueArray[0].trim();
        String value = keyValueArray.length == 2 ? keyValueArray[1].trim() : "";
        tableProperties.put(key, value);
      } else {
        throw new IllegalArgumentException("Bad key-value config: " + keyValue + ", must be the"
            + " format 'key = value'");
      }
    }
    return tableProperties;
  }

  /**
   * Convert map config to key-value string.The format of the config
   * is a key-value pair just like "k1=v1\nk2=v2\nk3=v3".
   *
   * @param config
   * @return
   */
  public static String configToString(Map<String, String> config) {
    if (config == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : config.entrySet()) {
      if (sb.length() > 0) {
        sb.append("\n");
      }
      sb.append(entry.getKey()).append("=").append(entry.getValue());
    }
    return sb.toString();
  }

  public static Configuration createHadoopConf(Properties props) {
    Configuration hadoopConf = new Configuration();
    props.stringPropertyNames().forEach(k -> hadoopConf.set(k, props.getProperty(k)));
    return hadoopConf;
  }

  /**
   * Gets the raw value for a {@link ConfigProperty} config from properties. The key and
   * alternative keys are used to fetch the config.
   *
   * @param props          Configs in {@link Properties}.
   * @param configProperty {@link ConfigProperty} config to fetch.
   * @return {@link Option} of value if the config exists; empty {@link Option} otherwise.
   */
  public static Option<Object> getRawValueWithAltKeys(Properties props,
                                                      ConfigProperty<?> configProperty) {
    if (props.containsKey(configProperty.key())) {
      return Option.ofNullable(props.get(configProperty.key()));
    }
    for (String alternative : configProperty.getAlternatives()) {
      if (props.containsKey(alternative)) {
        return Option.ofNullable(props.get(alternative));
      }
    }
    return Option.empty();
  }

  /**
   * Gets the boolean value for a {@link ConfigProperty} config from properties. The key and
   * alternative keys are used to fetch the config. The default value of {@link ConfigProperty}
   * config, if exists, is returned if the config is not found in the properties.
   *
   * @param props          Configs in {@link Properties}.
   * @param configProperty {@link ConfigProperty} config to fetch.
   * @return boolean value if the config exists; default boolean value if the config does not exist
   * and there is default value defined in the {@link ConfigProperty} config; {@code false} otherwise.
   */
  public static boolean getBooleanWithAltKeys(Properties props,
                                              ConfigProperty<?> configProperty) {
    Option<Object> rawValue = getRawValueWithAltKeys(props, configProperty);
    boolean defaultValue = configProperty.hasDefaultValue()
        ? Boolean.parseBoolean(configProperty.defaultValue().toString()) : false;
    return rawValue.map(v -> Boolean.parseBoolean(v.toString())).orElse(defaultValue);
  }
}
