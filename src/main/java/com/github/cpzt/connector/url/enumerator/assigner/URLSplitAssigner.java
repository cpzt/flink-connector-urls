package com.github.cpzt.connector.url.enumerator.assigner;

import com.github.cpzt.connector.url.split.URLSourceSplit;
import java.util.Collection;
import java.util.Optional;

public interface URLSplitAssigner {
  /**
   * Gets the next split.
   *
   * <p>When this method returns an empty {@code Optional}, then the set of splits is assumed to
   * be done and the source will finish once the readers finished their current splits.
   */
  Optional<URLSourceSplit> getNext();

  /**
   * Adds a set of splits to this assigner. This happens for example when some split processing
   * failed and the splits need to be re-added, or when new splits got discovered.
   */
  void addSplits(Collection<URLSourceSplit> splits);

  /** Gets the remaining splits that this assigner has pending. */
  Collection<URLSourceSplit> remainingSplits();
}
