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

import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.deltastreamer.internal.QuarantineEvent;
import org.apache.spark.api.java.JavaRDD;

public interface QuarantineTableWriterInterface<T extends QuarantineEvent> {

  String QUARANTINE_PAYLOAD_CLASS = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload";

  String QUARANTINE_TABLE_CURRUPT_RECORD_COL_NAME = "_corrupt_record";

  HoodieWriteConfig getQuarantineTableWriteConfig();

  HoodieDeltaStreamer.Config getSourceDeltaStreamerConfig();

  /***
   *
   * @param errorEvent
   */
  void addErrorEvents(JavaRDD<T> errorEvent);

  /***
   *
   * @param basetableInstantTime
   * @param commitedInstantTime
   * @return
   */
  Option<JavaRDD<HoodieAvroRecord>> getErrorEvents(String basetableInstantTime, Option<String> commitedInstantTime);

  /***
   * implement this to clean error events state to be executed after error events committed .
   */
  void cleanErrorEvents();

  String startCommit();

  /***
   *
   * @param instantTime
   * @param baseTableInstantTime
   * @param commitedInstantTime
   * @return
   */
  boolean upsertAndCommit(String instantTime, String baseTableInstantTime, Option<String> commitedInstantTime);

}
