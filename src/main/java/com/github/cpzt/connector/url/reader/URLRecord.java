package com.github.cpzt.connector.url.reader;

import org.apache.flink.table.data.RowData;

public class URLRecord implements IRecord<RowData> {
  private final RowData record;
  private final long offset;

  public URLRecord(RowData record, long offset) {
    this.record = record;
    this.offset = offset;
  }

  public RowData getRecord() {
    return record;
  }

  public long getOffset() {
    return offset;
  }

  @Override
  public String toString() {
    return "UrlRecord{" + "record=" + record + ", offset=" + offset + '}';
  }
}
