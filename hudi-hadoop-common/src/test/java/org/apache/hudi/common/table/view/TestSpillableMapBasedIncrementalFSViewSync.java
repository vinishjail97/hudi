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

package org.apache.hudi.common.table.view;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;

/**
 * Tests spillable map based incremental fs view sync {@link SpillableMapBasedFileSystemView}.
 */
public class TestSpillableMapBasedIncrementalFSViewSync extends TestIncrementalFSViewSync {

  @Override
  protected SyncableFileSystemView getFileSystemView(HoodieTableMetaClient metaClient, HoodieTimeline timeline) {
    HoodieTableMetadata tableMetadata = new FileSystemBackedTableMetadata(getEngineContext(), metaClient.getTableConfig(), metaClient.getStorage(),
        metaClient.getBasePath().toString());
    return new SpillableMapBasedFileSystemView(tableMetadata, metaClient, timeline,
        FileSystemViewStorageConfig.newBuilder().withMaxMemoryForView(0L).withIncrementalTimelineSync(true).build(),
        HoodieCommonConfig.newBuilder().build());
  }
}
