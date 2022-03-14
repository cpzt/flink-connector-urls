package com.github.cpzt.connector.url.enumerator.state;

import static org.apache.flink.util.Preconditions.checkNotNull;

import com.github.cpzt.connector.url.split.URLSourceSplit;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import javax.annotation.Nullable;
import org.apache.flink.connector.file.src.PendingSplitsCheckpointSerializer;

/**
 * A checkpoint of the current state of the containing the currently pending splits that are not yet
 * assigned.
 */
public class PendingSplitsState {
  /** The splits in the checkpoint. */
  private final Collection<URLSourceSplit> splits;

  /**
   * The paths that are no longer in the enumerator checkpoint, but have been processed before and
   * should this be ignored. Relevant only for sources in continuous monitoring mode.
   */
  private final Collection<URL> alreadyProcessedURLs;

  /**
   * The cached byte representation from the last serialization step. This helps to avoid paying
   * repeated serialization cost for the same checkpoint object. This field is used by {@link
   * PendingSplitsCheckpointSerializer}.
   */
  @Nullable byte[] serializedFormCache;

  protected PendingSplitsState(
      Collection<URLSourceSplit> splits, Collection<URL> alreadyProcessedURLs) {
    this.splits = Collections.unmodifiableCollection(splits);
    this.alreadyProcessedURLs = Collections.unmodifiableCollection(alreadyProcessedURLs);
  }

  // ------------------------------------------------------------------------
  public Collection<URLSourceSplit> getSplits() {
    return splits;
  }

  public Collection<URL> getAlreadyProcessedURLs() {
    return alreadyProcessedURLs;
  }

  // ------------------------------------------------------------------------

  @Override
  public String toString() {
    return "PendingSplitsState{"
        + "splits="
        + splits
        + ", alreadyProcessedURLs="
        + alreadyProcessedURLs
        + '}';
  }

  // ------------------------------------------------------------------------
  //  factories
  // ------------------------------------------------------------------------

  public static PendingSplitsState fromCollectionSnapshot(
      final Collection<URLSourceSplit> splits) {
    checkNotNull(splits);

    // create a copy of the collection to make sure this checkpoint is immutable
    final Collection<URLSourceSplit> copy = new ArrayList<>(splits);
    return new PendingSplitsState(copy, Collections.emptySet());
  }

  public static PendingSplitsState fromCollectionSnapshot(
      final Collection<URLSourceSplit> splits,
      final Collection<URL> alreadyProcessedURLs) {
    checkNotNull(splits);

    // create a copy of the collection to make sure this checkpoint is immutable
    final Collection<URLSourceSplit> splitsCopy = new ArrayList<>(splits);
    final Collection<URL> pathsCopy = new ArrayList<>(alreadyProcessedURLs);

    return new PendingSplitsState(splitsCopy, pathsCopy);
  }

  static PendingSplitsState reusingCollection(
      final Collection<URLSourceSplit> splits,
      final Collection<URL> alreadyProcessedURLs) {
    return new PendingSplitsState(splits, alreadyProcessedURLs);
  }
}
