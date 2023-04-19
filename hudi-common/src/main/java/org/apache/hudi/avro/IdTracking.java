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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Stores the field ID information for a given schema. This allows Hudi to track consistent IDs for fields across commits.
 * The lastIdUsed is the highest ID value previously used. Field IDs should be monotomically increasing.
 */
public class IdTracking {

  public static IdTracking EMPTY = new IdTracking(Collections.emptyList(), 0);
  private final List<IdMapping> idMappings;
  private final int lastIdUsed;

  @JsonCreator
  public IdTracking(@JsonProperty("idMappings") List<IdMapping> idMappings, @JsonProperty("lastIdUsed") int lastIdUsed) {
    this.idMappings = idMappings;
    this.lastIdUsed = lastIdUsed;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IdTracking that = (IdTracking) o;
    return lastIdUsed == that.lastIdUsed && idMappings.equals(that.idMappings);
  }

  @Override
  public int hashCode() {
    return Objects.hash(idMappings, lastIdUsed);
  }

  public List<IdMapping> getIdMappings() {
    return idMappings;
  }

  public int getLastIdUsed() {
    return lastIdUsed;
  }

  @Override
  public String toString() {
    return "IdTracking{"
        + "idMappings=" + idMappings
        + ", lastIdUsed=" + lastIdUsed
        + '}';
  }
}
