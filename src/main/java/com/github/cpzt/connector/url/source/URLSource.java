package com.github.cpzt.connector.url.source;

import static org.apache.flink.util.Preconditions.checkNotNull;

import com.github.cpzt.connector.url.enumerator.URLSourceEnumerator;
import com.github.cpzt.connector.url.enumerator.state.PendingSplitsState;
import com.github.cpzt.connector.url.enumerator.state.PendingSplitsStateSerializer;
import com.github.cpzt.connector.url.reader.IFunction;
import com.github.cpzt.connector.url.reader.URLSourceReader;
import com.github.cpzt.connector.url.split.URLSourceSplit;
import com.github.cpzt.connector.url.split.URLSourceSplitSerializer;
import java.net.URL;
import java.util.Collections;
import java.util.function.Function;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

public class URLSource implements Source<RowData, URLSourceSplit, PendingSplitsState>,
    ResultTypeQueryable<RowData> {

  private final TypeInformation<RowData> producedTypeInfo;
  private final URL[] urls;

  public URLSourceReader sourceReader;

  private Function<RowData, URLSourceSplit[]> addSplitFunction = null;


  public URLSource(TypeInformation<RowData> producedTypeInfo, URL[] urls) {
    this.producedTypeInfo = checkNotNull(producedTypeInfo);
    this.urls = checkNotNull(urls);
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SourceReader<RowData, URLSourceSplit> createReader(SourceReaderContext readerContext)
      throws Exception {
    sourceReader = new URLSourceReader(readerContext, addSplitFunction);
    return sourceReader;
  }

  @Override
  public SplitEnumerator<URLSourceSplit, PendingSplitsState> createEnumerator(
      SplitEnumeratorContext<URLSourceSplit> context) throws Exception {
    return new URLSourceEnumerator(
        context, urls, Collections.emptyList(), Collections.emptyList());
  }

  @Override
  public SplitEnumerator<URLSourceSplit, PendingSplitsState> restoreEnumerator(
      SplitEnumeratorContext<URLSourceSplit> context, PendingSplitsState state)
      throws Exception {
    return new URLSourceEnumerator(context, urls, state.getSplits(), state.getAlreadyProcessedURLs());
  }

  @Override
  public SimpleVersionedSerializer<URLSourceSplit> getSplitSerializer() {
    return URLSourceSplitSerializer.INSTANCE;
  }

  @Override
  public SimpleVersionedSerializer<PendingSplitsState> getEnumeratorCheckpointSerializer() {
    return new PendingSplitsStateSerializer(getSplitSerializer());
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return producedTypeInfo;
  }

  public URLSource withAddSplitFunction(IFunction<RowData, URLSourceSplit[]> function) {
    if (null != function) {
      this.addSplitFunction = function;
    }
    return this;
  }
}
