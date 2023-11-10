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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.model.HoodieArchivedLogFile;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieArchivedTimeline}.
 */
public class TestHoodieArchivedTimeline extends HoodieCommonTestHarness {

  private HoodieArchivedTimeline timeline;

  @BeforeEach
  public void setUp() throws IOException {
    initMetaClient();

  }

  @AfterEach
  public void clean() {
    cleanMetaClient();
  }

  @Test
  public void testArchivedInstantsNotLoadedToMemory() throws Exception {
    List<HoodieInstant> instants = createInstants();
    // Creating a timeline without time range filter does not load instants to memory
    timeline = new HoodieArchivedTimeline(metaClient);

    // Test that hoodie archived timeline does not load instants to memory by default
    validateInstantsLoaded(timeline, Arrays.asList("01", "03", "05", "08", "09", "11"), false);
    assertEquals(instants, timeline.getInstants().collect(Collectors.toList()));
  }

  @Test
  public void testLoadArchivedInstantsInStartTsRangeToMemory() throws Exception {
    List<HoodieInstant> instants = createInstants();

    timeline = new HoodieArchivedTimeline(metaClient, "08");

    // Note that instant 11 should not be loaded as it is not completed
    validateInstantsLoaded(timeline, Arrays.asList("01", "03", "05", "11"), false);
    validateInstantsLoaded(timeline, Arrays.asList("08", "09"), true);

    // Timeline should only keep completed instants
    List<HoodieInstant> completedInstants = getCompletedInstantForTs(instants, Arrays.asList("08", "09"));
    assertEquals(completedInstants, timeline.getInstants().collect(Collectors.toList()));
  }

  @Test
  public void testLoadArchivedInstantsInInclusiveTsRangeToMemory() throws Exception {
    List<HoodieInstant> instants = createInstants();

    timeline = new HoodieArchivedTimeline(metaClient, "05", "09");

    validateInstantsLoaded(timeline, Arrays.asList("01", "03", "11"), false);
    validateInstantsLoaded(timeline, Arrays.asList("05", "08", "09"), true);

    List<HoodieInstant> completedInstants = getCompletedInstantForTs(instants, Arrays.asList("05", "08", "09"));
    assertEquals(completedInstants, timeline.getInstants().collect(Collectors.toList()));
  }

  @Test
  public void testLoadArchivedCompletedInstantsToMemory() throws Exception {
    List<HoodieInstant> instants = createInstants();

    timeline = new HoodieArchivedTimeline(metaClient, "01", "11");

    // Instants 01 and 11 should not be loaded to memory since they are not completed
    validateInstantsLoaded(timeline, Arrays.asList("01", "11"), false);
    validateInstantsLoaded(timeline, Arrays.asList("03", "05", "08", "09"), true);

    // All the completed instants should be returned
    assertEquals(instants.stream().filter(HoodieInstant::isCompleted).collect(Collectors.toList()), timeline.getInstants().collect(Collectors.toList()));
  }

  @Test
  public void testLoadArchivedCompactionInstantsToMemory() throws Exception {
    List<HoodieInstant> instants = createInstants();

    timeline = new HoodieArchivedTimeline(metaClient);
    timeline.loadCompactionDetailsInMemory("08");

    // Only compaction commit (timestamp 08) should be loaded to memory
    validateInstantsLoaded(timeline, Arrays.asList("01", "03", "05", "09", "11"), false);
    validateInstantsLoaded(timeline, Arrays.asList("08"), true);

    // All the instants should be returned although only compaction instants should be loaded to memory
    assertEquals(instants, timeline.getInstants().collect(Collectors.toList()));
  }

  @Test
  public void testLoadAllArchivedCompletedInstantsByLogFilePaths() throws Exception {
    List<HoodieInstant> instants = createInstants();

    List<String> archivedLogFilePaths = getArchiveLogFilePaths();
    timeline = new HoodieArchivedTimeline(metaClient, new HashSet<>(archivedLogFilePaths));

    // Instants 01 and 11 should not be loaded to memory since they are not completed
    validateInstantsLoaded(timeline, Arrays.asList("01", "11"), false);
    validateInstantsLoaded(timeline, Arrays.asList("03", "05", "08", "09"), true);

    // All the completed instants should be returned
    assertEquals(instants.stream().filter(HoodieInstant::isCompleted).collect(Collectors.toList()), timeline.getInstants().collect(Collectors.toList()));
  }

  @Test
  public void testLoadFilteredArchivedCompletedInstantsBySingleLogFilePath() throws Exception {
    List<HoodieInstant> instants = createInstants();

    List<String> archivedLogFilePaths = getArchiveLogFilePaths();
    timeline = new HoodieArchivedTimeline(metaClient, Collections.singleton(archivedLogFilePaths.get(0)));

    // Instants 03 should be loaded to memory (since they are in log file 0)
    validateInstantsLoaded(timeline, Arrays.asList("03"), true);
    validateInstantsLoaded(timeline, Arrays.asList("01", "05", "08", "09", "11"), false);

    assertEquals(instants.stream().filter(HoodieInstant::isCompleted)
        .filter(instant -> Collections.singletonList("03").contains(instant.getTimestamp())).collect(Collectors.toList()),
        timeline.getInstants().collect(Collectors.toList()));
  }

  @Test
  public void testLoadFilteredArchivedCompletedInstantsByMultipleLogFilePath() throws Exception {
    List<HoodieInstant> instants = createInstants();

    List<String> archivedLogFilePaths = getArchiveLogFilePaths();
    timeline = new HoodieArchivedTimeline(metaClient, new HashSet<>(archivedLogFilePaths.subList(0, 2)));

    // Instants 03, 05, 08, 09 should be loaded to memory (since they are in log file 0 and 1)
    validateInstantsLoaded(timeline, Arrays.asList("03", "05", "08", "09"), true);
    validateInstantsLoaded(timeline, Arrays.asList("01", "11"), false);

    assertEquals(instants.stream().filter(HoodieInstant::isCompleted)
            .filter(instant -> Arrays.asList("03", "05", "08", "09").contains(instant.getTimestamp())).collect(Collectors.toList()),
        timeline.getInstants().collect(Collectors.toList()));
  }

  /**
   * Validate whether the instants of given timestamps of the hudi archived timeline are loaded to memory or not.
   * @param hoodieArchivedTimeline archived timeline to test against
   * @param instantTsList list of instant timestamps to validate
   * @param isInstantLoaded flag to check whether the instants are loaded to memory or not
   */
  private void validateInstantsLoaded(HoodieArchivedTimeline hoodieArchivedTimeline, List<String> instantTsList, boolean isInstantLoaded) {
    Set<String> instantTsSet = new HashSet<>(instantTsList);
    timeline.getInstants().filter(instant -> instantTsSet.contains(instant.getTimestamp())).forEach(instant -> {
      if (isInstantLoaded) {
        assertTrue(hoodieArchivedTimeline.getInstantDetails(instant).isPresent());
      } else {
        assertFalse(hoodieArchivedTimeline.getInstantDetails(instant).isPresent());
      }
    });
  }

  /**
   * Get list of completed hoodie instants for given timestamps.
   */
  private List<HoodieInstant> getCompletedInstantForTs(List<HoodieInstant> instants, List<String> instantTsList) {
    return instants.stream().filter(HoodieInstant::isCompleted)
        .filter(instant -> (new HashSet<>(instantTsList).contains(instant.getTimestamp()))).collect(Collectors.toList());
  }

  /**
   * Create instants for testing. If archiveInstants is true, create archived commits.
   * archived commit 1 - instant1, instant2
   * archived commit 2 - instant3, instant4, instant5
   * archived commit 3 - instant6
   *
   * log file 1 - 01 (inflight), 03 (complete), and 05 (inflight)
   * log file 2 - instant 05 (complete), 08 (complete), and 09 (complete)
   * log file 3 - instant 11 (inflight)
   */
  private List<HoodieInstant> createInstants() throws Exception {
    HoodieInstant instant1Requested = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "01");
    HoodieInstant instant1Inflight = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "01");

    HoodieInstant instant2Requested = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "03");
    HoodieInstant instant2Inflight = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "03");
    HoodieInstant instant2Complete = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "03");

    HoodieInstant instant3Requested = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "05");
    HoodieInstant instant3Inflight = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "05");
    HoodieInstant instant3Complete = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "05");

    HoodieInstant instant4Requested = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, "08");
    HoodieInstant instant4Inflight = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "08");
    HoodieInstant instant4Complete = new HoodieInstant(false, HoodieTimeline.COMPACTION_ACTION, "08");

    HoodieInstant instant5Requested = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "09");
    HoodieInstant instant5Inflight = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "09");
    HoodieInstant instant5Complete = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "09");

    HoodieInstant instant6Requested = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "11");
    HoodieInstant instant6Inflight = new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "11");

    List<HoodieInstant> instants = Arrays.asList(instant1Requested, instant1Inflight, instant2Requested, instant2Inflight,
        instant2Complete, instant3Requested, instant3Inflight, instant3Complete, instant4Requested, instant4Inflight,
        instant4Complete, instant5Requested, instant5Inflight, instant5Complete, instant6Requested, instant6Inflight);

    List<IndexedRecord> records = new ArrayList<>();
    Path archiveFilePath = HoodieArchivedTimeline.getArchiveLogPath(metaClient.getArchivePath());
    HoodieLogFormat.Writer writer;

    // Write archive commit 1
    writer = buildWriter(archiveFilePath);
    records.add(createArchivedMetaWrapper(instant1Requested));
    records.add(createArchivedMetaWrapper(instant1Inflight));
    records.add(createArchivedMetaWrapper(instant2Requested));
    writeArchiveLog(writer, records);

    records.add(createArchivedMetaWrapper(instant2Inflight));
    records.add(createArchivedMetaWrapper(instant2Complete));
    records.add(createArchivedMetaWrapper(instant3Requested));
    writeArchiveLog(writer, records);
    writer.close();

    // Write archive commit 2
    writer = buildWriter(archiveFilePath);
    records.add(createArchivedMetaWrapper(instant3Inflight));
    writeArchiveLog(writer, records);

    records.add(createArchivedMetaWrapper(instant3Complete));
    records.add(createArchivedMetaWrapper(instant4Requested));
    records.add(createArchivedMetaWrapper(instant4Inflight));
    records.add(createArchivedMetaWrapper(instant4Complete));
    records.add(createArchivedMetaWrapper(instant5Requested));
    records.add(createArchivedMetaWrapper(instant5Inflight));
    records.add(createArchivedMetaWrapper(instant5Complete));
    writeArchiveLog(writer, records);
    writer.close();

    // Write archive commit 3
    writer = buildWriter(archiveFilePath);
    records.add(createArchivedMetaWrapper(instant6Requested));
    records.add(createArchivedMetaWrapper(instant6Inflight));
    writeArchiveLog(writer, records);
    writer.close();

    return instants;
  }

  /**
   * Get list of archived log file paths.
   */
  private List<String> getArchiveLogFilePaths() throws IOException {
    return Arrays.stream(metaClient.getFs().globStatus(new Path(metaClient.getArchivePath() + "/.commits_.archive*")))
        .map(x -> x.getPath().toString()).collect(Collectors.toList());
  }

  private HoodieArchivedMetaEntry createArchivedMetaWrapper(HoodieInstant hoodieInstant) throws IOException {
    HoodieArchivedMetaEntry archivedMetaWrapper = new HoodieArchivedMetaEntry();
    archivedMetaWrapper.setCommitTime(hoodieInstant.getTimestamp());
    archivedMetaWrapper.setActionState(hoodieInstant.getState().name());
    switch (hoodieInstant.getAction()) {
      case HoodieTimeline.COMMIT_ACTION:
        archivedMetaWrapper.setActionType(ActionType.commit.name());
        // Hoodie commit metadata is required for archived timeline to load instants to memory
        archivedMetaWrapper.setHoodieCommitMetadata(org.apache.hudi.avro.model.HoodieCommitMetadata.newBuilder().build());
        break;
      case HoodieTimeline.COMPACTION_ACTION:
        archivedMetaWrapper.setActionType(ActionType.compaction.name());
        archivedMetaWrapper.setHoodieCompactionPlan(HoodieCompactionPlan.newBuilder().build());
        break;
      default:
        break;
    }
    return archivedMetaWrapper;
  }

  private Writer buildWriter(Path archiveFilePath) throws IOException {
    return HoodieLogFormat.newWriterBuilder().onParentPath(archiveFilePath.getParent())
        .withFileId(archiveFilePath.getName()).withFileExtension(HoodieArchivedLogFile.ARCHIVE_EXTENSION)
        .withFs(metaClient.getFs()).overBaseCommit("").build();
  }

  private void writeArchiveLog(Writer writer, List<IndexedRecord> records) throws Exception {
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, HoodieArchivedMetaEntry.getClassSchema().toString());
    final String keyField = metaClient.getTableConfig().getRecordKeyFieldProp();
    HoodieAvroDataBlock block = new HoodieAvroDataBlock(records, header, keyField);
    writer.appendBlock(block);
    records.clear();
  }
}
