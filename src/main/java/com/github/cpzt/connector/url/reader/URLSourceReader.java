package com.github.cpzt.connector.url.reader;

import com.github.cpzt.connector.url.split.URLSourceSplit;
import java.util.Map;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.table.data.RowData;

public class URLSourceReader extends SingleThreadMultiplexSourceReaderBase<
    URLRecord, RowData, URLSourceSplit, URLSourceSplitState> {

  public URLSourceReader(SourceReaderContext readerContext) {
    super(
        URLSourceSplitReader::new,
        new URLSourceRecordEmitter(),
        readerContext.getConfiguration(),
        readerContext);
  }

  @Override
  public void start() {
    // we request a split only if we did not get splits during the checkpoint restore
    if (getNumberOfCurrentlyAssignedSplits() == 0) {
      context.sendSplitRequest();
    }
  }

  @Override
  protected void onSplitFinished(Map<String, URLSourceSplitState> finishedSplitIds) {
    context.sendSplitRequest();
  }

  @Override
  protected URLSourceSplitState initializedState(URLSourceSplit split) {
    return new URLSourceSplitState(split);
  }

  @Override
  protected URLSourceSplit toSplitType(String splitId, URLSourceSplitState splitState) {
    return splitState.toURLSourceSplit();
  }
}
