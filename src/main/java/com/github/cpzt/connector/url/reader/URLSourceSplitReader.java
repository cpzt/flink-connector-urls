package com.github.cpzt.connector.url.reader;

import com.github.cpzt.connector.url.split.URLSourceSplit;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Queue;
import javax.annotation.Nullable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URLSourceSplitReader implements SplitReader<URLRecord, URLSourceSplit> {

  private static final Logger LOG = LoggerFactory.getLogger(URLSourceSplitReader.class);

  private final Queue<URLSourceSplit> splits;

  @Nullable private BufferedReader currentReader;
  @Nullable private String currentSplitId;

  public URLSourceSplitReader() {
    this.splits = new ArrayDeque<>();
  }

  @Override
  public RecordsWithSplitIds<URLRecord> fetch() throws IOException {
    checkSplitOrStartNext();

    assert currentReader != null;

    final String line = currentReader.readLine();

    if (line == null) {
      return finishSplit();
    }

    final GenericRowData rowData = new GenericRowData(1);
    rowData.setField(0, StringData.fromString(line));
    final URLRecord record = new URLRecord(rowData, 0);

    return URLRecords.forRecords(currentSplitId, Collections.singleton(record));

  }

  @Override
  public void handleSplitsChanges(SplitsChange<URLSourceSplit> splitsChanges) {
    if (!(splitsChanges instanceof SplitsAddition)) {
      throw new UnsupportedOperationException(
          String.format(
              "The SplitChange type of %s is not supported.",
              splitsChanges.getClass()));
    }

    LOG.debug("Handling split change {}", splitsChanges);
    splits.addAll(splitsChanges.splits());
  }

  @Override
  public void wakeUp() {}

  @Override
  public void close() throws Exception {
    if (currentReader != null) {
      currentReader.close();
    }
  }


  // ------------------------------------------------------------------------
  private void checkSplitOrStartNext() throws IOException {
    if (currentReader != null) {
      return;
    }

    final URLSourceSplit nextSplit = splits.poll();
    if (nextSplit == null) {
      throw new IOException("Cannot fetch from another split - no split remaining");
    }

    final URL url = nextSplit.url();

    HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    conn.setRequestMethod("GET");

    InputStreamReader currentInStream = new InputStreamReader(conn.getInputStream());

    currentReader =
        new BufferedReader(currentInStream);
    currentSplitId = nextSplit.splitId();
  }

  private URLRecords finishSplit() throws IOException {
    if (currentReader != null) {
      currentReader.close();
      try (BufferedReader bufferedReader = currentReader = null) {
      }
    }

    final URLRecords finishRecords = URLRecords.finishedSplit(currentSplitId);
    currentSplitId = null;
    return finishRecords;
  }
}
