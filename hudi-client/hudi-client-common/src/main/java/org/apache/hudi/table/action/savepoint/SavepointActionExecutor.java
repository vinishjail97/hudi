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

package org.apache.hudi.table.action.savepoint;

import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieSavepointException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.deserializeCleanerPlan;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.deserializeHoodieCleanMetadata;

public class SavepointActionExecutor<T extends HoodieRecordPayload, I, K, O> extends BaseActionExecutor<T, I, K, O, HoodieSavepointMetadata> {

  private static final Logger LOG = LogManager.getLogger(SavepointActionExecutor.class);

  private final String user;
  private final String comment;

  public SavepointActionExecutor(HoodieEngineContext context,
                                 HoodieWriteConfig config,
                                 HoodieTable<T, I, K, O> table,
                                 String instantTime,
                                 String user,
                                 String comment) {
    super(context, config, table, instantTime);
    this.user = user;
    this.comment = comment;
  }

  @Override
  public HoodieSavepointMetadata execute() {
    if (!table.getCompletedCommitsTimeline().containsInstant(instantTime)) {
      throw new HoodieSavepointException("Could not savepoint non-existing commit " + instantTime);
    }

    try {
      // Check the last commit that was not cleaned and check if savepoint time is > that commit
      Option<HoodieInstant> cleanInstant = table.getCleanTimeline().lastInstant();
      String lastCommitRetained = cleanInstant.map(instant -> {
        try {
          if (instant.isCompleted()) {
            return deserializeHoodieCleanMetadata(
                table.getActiveTimeline().getInstantDetails(instant).get())
                .getEarliestCommitToRetain();
          } else {
            // clean is pending or inflight
            return deserializeCleanerPlan(
                table.getActiveTimeline().getInstantDetails(new HoodieInstant(REQUESTED, instant.getAction(), instant.getTimestamp())).get())
                .getEarliestInstantToRetain().getTimestamp();
          }
        } catch (IOException e) {
          throw new HoodieSavepointException("Failed to savepoint " + instantTime, e);
        }
      }).orElse(table.getCompletedCommitsTimeline().firstInstant().get().getTimestamp());

      // Cannot allow savepoint time on a commit that could have been cleaned
      ValidationUtils.checkArgument(HoodieTimeline.compareTimestamps(instantTime, HoodieTimeline.GREATER_THAN_OR_EQUALS, lastCommitRetained),
          "Could not savepoint commit " + instantTime + " as this is beyond the lookup window " + lastCommitRetained);

      context.setJobStatus(this.getClass().getSimpleName(), "Collecting latest files for savepoint " + instantTime + " " + table.getConfig().getTableName());
      List<String> partitions = FSUtils.getAllPartitionPaths(context, config.getMetadataConfig(), table.getMetaClient().getBasePath());
      Map<String, List<String>> latestFilesMap = context.mapToPair(partitions, partitionPath -> {
        // Scan all partitions files with this commit time
        LOG.info("Collecting latest files in partition path " + partitionPath);
        TableFileSystemView.BaseFileOnlyView view = table.getBaseFileOnlyView();
        List<String> latestFiles = view.getLatestBaseFilesBeforeOrOn(partitionPath, instantTime)
            .map(HoodieBaseFile::getFileName).collect(Collectors.toList());
        return new ImmutablePair<>(partitionPath, latestFiles);
      }, null);
      HoodieSavepointMetadata metadata = TimelineMetadataUtils.convertSavepointMetadata(user, comment, latestFilesMap);
      // Nothing to save in the savepoint
      table.getActiveTimeline().createNewInstant(
          new HoodieInstant(true, HoodieTimeline.SAVEPOINT_ACTION, instantTime));
      table.getActiveTimeline()
          .saveAsComplete(new HoodieInstant(true, HoodieTimeline.SAVEPOINT_ACTION, instantTime),
              TimelineMetadataUtils.serializeSavepointMetadata(metadata));
      LOG.info("Savepoint " + instantTime + " created");
      return metadata;
    } catch (IOException e) {
      throw new HoodieSavepointException("Failed to savepoint " + instantTime, e);
    }
  }
}
