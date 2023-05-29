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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.exception.HoodieValidationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.hudi.utilities.deltastreamer.QuarantineTableWriterInterface.QUARANTINE_TABLE_CURRUPT_RECORD_COL_NAME;
import static org.apache.spark.sql.functions.lit;

import java.util.Arrays;

public final class ErrorTableUtils {

  /**
   * validates for constraints on ErrorRecordColumn when ErrorTable enabled configs are set.
   * @param dataset
   */
  public static void validate(Dataset<Row> dataset) {
    if (!isErrorTableCorruptRecordColumnPresent(dataset)) {
      throw new HoodieValidationException(String.format("Invalid condition, columnName=%s "
              + "is not present in transformer " + "output schema", QUARANTINE_TABLE_CURRUPT_RECORD_COL_NAME));
    }
  }

  public static Dataset<Row> addNullValueErrorTableCorruptRecordColumn(Dataset<Row> dataset) {
    if (!isErrorTableCorruptRecordColumnPresent(dataset)) {
      dataset = dataset.withColumn(QUARANTINE_TABLE_CURRUPT_RECORD_COL_NAME, lit(null));
    }
    return dataset;
  }

  private static boolean isErrorTableCorruptRecordColumnPresent(Dataset<Row> dataset) {
    return Arrays.stream(dataset.columns()).anyMatch(col -> col.equals(QUARANTINE_TABLE_CURRUPT_RECORD_COL_NAME));
  }
}
