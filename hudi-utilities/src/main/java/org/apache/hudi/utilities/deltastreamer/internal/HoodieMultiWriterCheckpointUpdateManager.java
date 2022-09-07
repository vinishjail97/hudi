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

package org.apache.hudi.utilities.deltastreamer.internal;

import org.apache.hudi.callback.HoodieWriteCommitCallback;
import org.apache.hudi.callback.common.HoodieWriteCommitCallbackMessage;
import org.apache.hudi.callback.common.HoodieWriteCommitCallbackMultiWriter;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.config.OnehouseInternalDeltastreamerConfig.MUTLI_WRITER_SOURCE_CHECKPOINT_ID;

public class HoodieMultiWriterCheckpointUpdateManager implements HoodieWriteCommitCallback {
  private static final Logger LOG = LogManager.getLogger(HoodieMultiWriterCheckpointUpdateManager.class);

  private static final ObjectMapper OM = new ObjectMapper();

  private void updateCheckPointConfigs(Map<String, String> extraMetadata, String checkpointStr) {
    extraMetadata.put(HoodieWriteConfig.DELTASTREAMER_CHECKPOINT_KEY, checkpointStr);
  }

  public void updateCheckPointForMultiWriter(HoodieCommitMetadata metadata, TypedProperties props, Map<String,String> extraMetadata, Option<Pair<HoodieInstant,
      Map<String, String>>> lastCompletedTxnAndMetadata) {

    String sourceCheckpointId = props.getString(MUTLI_WRITER_SOURCE_CHECKPOINT_ID.key());
    LOG.info("multi_writer source checkpoint id=%s" + sourceCheckpointId);
    String tableCheckPoint = extraMetadata.get(HoodieWriteConfig.DELTASTREAMER_CHECKPOINT_KEY);
    try {
      Map<String, String> checkpointMap;
      if (lastCompletedTxnAndMetadata.isPresent()) {
        String lastCheckpointVal = lastCompletedTxnAndMetadata.get().getRight()
            .get(HoodieWriteConfig.DELTASTREAMER_CHECKPOINT_KEY);
        checkpointMap = OM.readValue(lastCheckpointVal, Map.class);
        checkpointMap.put(sourceCheckpointId, tableCheckPoint);
      } else {
        checkpointMap = new HashMap<>();
        checkpointMap.put(sourceCheckpointId, tableCheckPoint);
      }
      updateCheckPointConfigs(extraMetadata, OM.writeValueAsString(checkpointMap));
      extraMetadata.forEach(metadata::addMetadata);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to parse checkpoint as map", e);
    }
  }

  @Override
  public void call(HoodieWriteCommitCallbackMessage callbackMessage) {
    HoodieWriteCommitCallbackMultiWriter multiWriter = (HoodieWriteCommitCallbackMultiWriter) callbackMessage;
    if (multiWriter.getProps().containsKey(MUTLI_WRITER_SOURCE_CHECKPOINT_ID.key()) && multiWriter.getExtraMetadata().isPresent()) {
      updateCheckPointForMultiWriter(multiWriter.getMetadata(), multiWriter.getProps(),
          multiWriter.getExtraMetadata().get(), multiWriter.getLastCompletedTxnAndMetadata());
    }
  }
}
