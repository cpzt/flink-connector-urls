package com.github.cpzt.connector.url.reader;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

public class URLRecords implements RecordsWithSplitIds<URLRecord> {

  @Nullable
  private String splitId;

  @Nullable private Iterator<URLRecord> recordsForSplitCurrent;

  private final Iterator<URLRecord> recordsForSplit;

  private final Set<String> finishedSplits;

  // ------------------------------------------------------------------------

  private URLRecords(
      @Nullable String splitId,
      Collection<URLRecord> recordsForSplit,
      Set<String> finishedSplits) {
    this.splitId = splitId;
    this.recordsForSplit = checkNotNull(recordsForSplit).iterator();
    this.finishedSplits = checkNotNull(finishedSplits);
  }

  @Nullable
  @Override
  public String nextSplit() {
    // move the split one (from current value to null)
    final String nextSplit = this.splitId;
    this.splitId = null;

    // move the iterator, from null to value (if first move) or to null (if second move)
    this.recordsForSplitCurrent = nextSplit != null ? this.recordsForSplit : null;

    return nextSplit;
  }

  @Nullable
  @Override
  public URLRecord nextRecordFromSplit() {
    final Iterator<URLRecord> recordsForSplit = this.recordsForSplitCurrent;
    if (recordsForSplit != null) {
      return recordsForSplit.hasNext() ? recordsForSplit.next() : null;
    } else {
      throw new IllegalStateException();
    }
  }

  @Override
  public Set<String> finishedSplits() {
    return finishedSplits;
  }

  // ------------------------------------------------------------------------

  public static URLRecords forRecords(
      final String splitId, Collection<URLRecord> recordsForSplit) {
    return new URLRecords(splitId, recordsForSplit, Collections.emptySet());
  }

  public static URLRecords finishedSplit(String splitId) {
    return new URLRecords(null, Collections.emptySet(), Collections.singleton(splitId));
  }
}
