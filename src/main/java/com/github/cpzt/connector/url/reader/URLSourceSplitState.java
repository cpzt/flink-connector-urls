package com.github.cpzt.connector.url.reader;

import static org.apache.flink.util.Preconditions.checkNotNull;

import com.github.cpzt.connector.url.split.URLSourceSplit;
import javax.annotation.Nullable;
import org.apache.flink.util.FlinkRuntimeException;

public class URLSourceSplitState {
  private final URLSourceSplit split;
  private @Nullable
  Long offset;

  // ------------------------------------------------------------------------

  public URLSourceSplitState(URLSourceSplit split) {
    this.split = checkNotNull(split);
    this.offset = split.getReaderPosition().orElse(null);
  }

  public @Nullable Long getOffset() {
    return offset;
  }

  public void setOffset(@Nullable Long offset) {
    // we skip sanity / boundary checks here for efficiency.
    // illegal boundaries will eventually be caught when constructing the split on checkpoint.
    this.offset = offset;
  }

  public URLSourceSplit toURLSourceSplit() {
    final URLSourceSplit updatedSplit = split.updateWithCheckpointedPosition(offset);

    // some sanity checks to avoid surprises and not accidentally lose split information
    if (updatedSplit == null) {
      throw new FlinkRuntimeException(
          "Split returned 'null' in updateWithCheckpointedPosition(): " + split);
    }
    if (updatedSplit.getClass() != split.getClass()) {
      throw new FlinkRuntimeException(
          String.format(
              "Split returned different type in updateWithCheckpointedPosition(). "
                  + "Split type is %s, returned type is %s",
              split.getClass().getName(), updatedSplit.getClass().getName()));
    }

    return updatedSplit;
  }
}
