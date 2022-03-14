package com.github.cpzt.connector.url.split;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.Serializable;
import java.net.URL;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SourceSplit;

public class URLSourceSplit implements SourceSplit, Serializable {

  private static final long serialVersionUID = 1L;

  /** The unique ID of the split. Unique within the scope of this source. */
  private final String id;

  /** The url referenced by this split. */
  private final URL url;

  /** The position of the first byte in the file to process. */
  private final long offset;

  /** The number of bytes in the file to process. */
  private final long length;

  /** The precise reader position in the split, to resume from. */
  @Nullable private final Long readerPosition;

  /**
   * The splits are frequently serialized into checkpoints. Caching the byte representation makes
   * repeated serialization cheap. This field is used by {@link URLSourceSplitSerializer}.
   */
  @Nullable
  transient byte[] serializedFormCache;

  public URLSourceSplit(String id, URL url, long offset, long length) {
    this(id, url, offset, length, null);
  }

  public URLSourceSplit(
      String id, URL url, long offset, long length, @Nullable Long readerPosition) {
    this(id, url, offset, length, readerPosition, null);
  }

  public URLSourceSplit(
      String id,
      URL url,
      long offset,
      long length,
      @Nullable Long readerPosition,
      @Nullable byte[] serializedForm) {
    checkArgument(offset >= 0, "offset must be >= 0");
    checkArgument(length >= 0, "length must be >= 0");

    this.id = checkNotNull(id);
    this.url = checkNotNull(url);
    this.offset = offset;
    this.length = length;
    this.readerPosition = readerPosition;
    this.serializedFormCache = serializedForm;
  }

  @Override
  public String splitId() {
    return id;
  }

  public URL url() {
    return url;
  }

  public long offset() {
    return offset;
  }

  public long length() {
    return length;
  }

  public Optional<Long> getReaderPosition() {
    return Optional.ofNullable(readerPosition);
  }

  public URLSourceSplit updateWithCheckpointedPosition(@Nullable Long position) {
    return new URLSourceSplit(id, url, offset, length, position);
  }

  // ------------------------------------------------------------------------
  @Override
  public String toString() {
    return "URLSourceSplit{"
        + "id='"
        + id
        + '\''
        + ", url="
        + url
        + ", offset="
        + offset
        + ", length="
        + length
        + ", readerPosition="
        + readerPosition
        + '}';
  }
}
