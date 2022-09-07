/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.callback.common;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import java.util.List;
import java.util.Map;

public class HoodieWriteCommitCallbackMultiWriter extends HoodieWriteCommitCallbackMessage {

  private Option<Pair<HoodieInstant, Map<String, String>>> lastCompletedTxnAndMetadata;

  public HoodieCommitMetadata getMetadata() {
    return metadata;
  }

  private HoodieCommitMetadata metadata;

  public TypedProperties getProps() {
    return props;
  }

  private TypedProperties props;

  public Option<Pair<HoodieInstant, Map<String, String>>> getLastCompletedTxnAndMetadata() {
    return lastCompletedTxnAndMetadata;
  }

  public HoodieWriteCommitCallbackMultiWriter(String commitTime, String tableName, String basePath, List<HoodieWriteStat> hoodieWriteStat,
                                              Option<Pair<HoodieInstant, Map<String, String>>> lastCompletedTxnAndMetadata,
                                              HoodieCommitMetadata metadata, TypedProperties props) {
    super(commitTime, tableName, basePath, hoodieWriteStat);
    this.lastCompletedTxnAndMetadata = lastCompletedTxnAndMetadata;
    this.metadata = metadata;
    this.props = props;
  }

  public HoodieWriteCommitCallbackMultiWriter(String commitTime, String tableName, String basePath, List<HoodieWriteStat> hoodieWriteStat,
                                              Option<String> commitActionType,
                                              Option<Map<String, String>> extraMetadata,
                                              Option<Pair<HoodieInstant, Map<String, String>>> lastCompletedTxnAndMetadata,
                                              HoodieCommitMetadata metadata, TypedProperties props) {
    super(commitTime, tableName, basePath, hoodieWriteStat, commitActionType, extraMetadata);
    this.lastCompletedTxnAndMetadata = lastCompletedTxnAndMetadata;
    this.metadata = metadata;
    this.props = props;
  }
}
