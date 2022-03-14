package com.github.cpzt.connector.url.reader;


import com.github.cpzt.connector.url.split.URLSourceSplit;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URLSourceReader extends URLSingleThreadMultiplexSourceReaderBase<
    URLRecord, RowData, URLSourceSplit, URLSourceSplitState> {

  private static final Logger LOG = LoggerFactory.getLogger(URLSourceReader.class);

  /** The latest fetched batch of records-by-split from the split reader. */
  @Nullable private RecordsWithSplitIds<URLRecord> currentFetch;

  private Function<RowData, URLSourceSplit[]> addSplitFunction;


  public URLSourceReader(SourceReaderContext readerContext, Function<RowData, URLSourceSplit[]> addSplitFunction) {
    super(
        URLSourceSplitReader::new,
        addSplitFunction,
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

  @Override
  public void sendSourceEventToEnumerator(URLSourceSplit[] splits) {

    SourceEvent sourceEvent = new AddSourceSplitEvent(splits);
    context.sendSourceEventToCoordinator(sourceEvent);
    LOG.info("add split");
  }


}
