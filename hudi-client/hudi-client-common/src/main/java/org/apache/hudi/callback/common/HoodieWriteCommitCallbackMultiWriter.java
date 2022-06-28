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
