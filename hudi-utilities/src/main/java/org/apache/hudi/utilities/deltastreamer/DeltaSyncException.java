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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.exception.HoodieException;

public class DeltaSyncException extends HoodieException {
  private final Type type;

  public DeltaSyncException(Type type, String message, Throwable ex) {
    super(createFullMessage(type, message), ex);
    this.type = type;
  }

  public DeltaSyncException(Type type, String message) {
    super(createFullMessage(type, message));
    this.type = type;
  }

  private static String createFullMessage(Type type, String message) {
    return String.format("%s %s", type.name(), message);
  }

  public Type getType() {
    return type;
  }

  public enum Type {
    UNKNOWN,
    READ_FROM_SOURCE,
    SCHEMA_COMPATIBILITY,
    TRANSFORM_PLAN,
    USER_TRANSFORM_EXECUTION,
    PLATFORM_TRANSFORM_EXECUTION,
    WRITE,
    META_SYNC
  }
}
