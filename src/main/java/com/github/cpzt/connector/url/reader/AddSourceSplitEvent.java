package com.github.cpzt.connector.url.reader;

import com.github.cpzt.connector.url.split.URLSourceSplit;
import org.apache.flink.api.connector.source.SourceEvent;

public class AddSourceSplitEvent implements SourceEvent {

  private URLSourceSplit[] splits;

  public AddSourceSplitEvent(URLSourceSplit[] splits) {
    this.splits = splits;
  }

  public URLSourceSplit[] getSplits() {
    return splits;
  }



}
