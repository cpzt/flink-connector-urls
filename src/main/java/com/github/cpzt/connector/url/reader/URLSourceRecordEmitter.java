package com.github.cpzt.connector.url.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.table.data.RowData;

public class URLSourceRecordEmitter
    implements RecordEmitter<URLRecord, RowData, URLSourceSplitState> {

  @Override
  public void emitRecord(URLRecord element, SourceOutput<RowData> output,
      URLSourceSplitState splitState) throws Exception {

    output.collect(element.getRecord());
    splitState.setOffset(element.getOffset());
  }
}
