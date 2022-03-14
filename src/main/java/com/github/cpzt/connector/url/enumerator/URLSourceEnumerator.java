package com.github.cpzt.connector.url.enumerator;

import static org.apache.flink.util.Preconditions.checkNotNull;

import com.github.cpzt.connector.url.enumerator.assigner.SimpleSplitAssigner;
import com.github.cpzt.connector.url.enumerator.assigner.URLSplitAssigner;
import com.github.cpzt.connector.url.enumerator.state.PendingSplitsState;
import com.github.cpzt.connector.url.reader.AddSourceSplitEvent;
import com.github.cpzt.connector.url.split.URLSourceSplit;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URLSourceEnumerator implements
    SplitEnumerator<URLSourceSplit, PendingSplitsState> {

  private static final Logger LOG = LoggerFactory.getLogger(URLSourceEnumerator.class);

  private final char[] currentId = "0000000000000".toCharArray();

  private final SplitEnumeratorContext<URLSourceSplit> context;
  private final URLSplitAssigner splitAssigner;

  private final URL[] urls;
  private final LinkedHashMap<Integer, String> readersAwaitingSplit;

  private final HashSet<URL> alreadyDiscoveredURLs;

  public URLSourceEnumerator(
      SplitEnumeratorContext<URLSourceSplit> context,
      URL[] urls,
      Collection<URLSourceSplit> splits,
      Collection<URL> alreadyDiscoveredURLs) {

    this.context = context;
    this.urls = checkNotNull(urls);
    this.splitAssigner = new SimpleSplitAssigner(checkNotNull(splits));;
    this.alreadyDiscoveredURLs = new HashSet<>(checkNotNull(alreadyDiscoveredURLs));
    this.readersAwaitingSplit = new LinkedHashMap<>();
  }

  @Override
  public void start() {
    context.callAsync(
        () -> this.toURLSourceSplit(urls),
        this::processDiscoveredSplits,
        2000,
        1000);
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    readersAwaitingSplit.put(subtaskId, requesterHostname);
    assignSplits();
  }

  @Override
  public void addSplitsBack(List<URLSourceSplit> splits, int subtaskId) {
    LOG.info("URL Source Enumerator adds splits back: {}", splits);
    splitAssigner.addSplits(splits);
  }

  @Override
  public void addReader(int subtaskId) {
    // this source is purely lazy-pull-based, nothing to do upon registration
  }

  @Override
  public PendingSplitsState snapshotState(long checkpointId) throws Exception {
    final PendingSplitsState state =
        PendingSplitsState.fromCollectionSnapshot(
            splitAssigner.remainingSplits(), alreadyDiscoveredURLs);

    LOG.debug("Source state is {}", state);
    return state;
  }

  @Override
  public void close() throws IOException {
    // no resources to close
  }

  @Override
  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    if (sourceEvent instanceof AddSourceSplitEvent) {
      URLSourceSplit[] splits = ((AddSourceSplitEvent) sourceEvent).getSplits();
      splitAssigner.addSplits(new ArrayList<>(Arrays.asList(splits)));
      LOG.info("add split");
    }
  }

  // ------------------------------------------------------------------------

  private void processDiscoveredSplits(Collection<URLSourceSplit> splits, Throwable error) {
    if (error != null) {
      LOG.error("Failed to enumerate urls", error);
      return;
    }

    final Collection<URLSourceSplit> newSplits =
        splits.stream()
            .filter((split) -> alreadyDiscoveredURLs.add(split.url()))
            .collect(Collectors.toList());
    splitAssigner.addSplits(newSplits);

    assignSplits();
  }

  private Collection<URLSourceSplit> toURLSourceSplit(URL[] urls) {
    ArrayList<URLSourceSplit> splits = new ArrayList<>();

    Stream.of(urls).forEach(url -> {
      URLSourceSplit split = new URLSourceSplit(getNextId(), url, 0, 0);
      splits.add(split);
    });

    return splits;
  }

  protected final String getNextId() {
    // because we just increment numbers, we increment the char representation directly,
    // rather than incrementing an integer and converting it to a string representation
    // every time again (requires quite some expensive conversion logic).
    incrementCharArrayByOne(currentId, currentId.length - 1);
    return new String(currentId);
  }

  private static void incrementCharArrayByOne(char[] array, int pos) {
    char c = array[pos];
    c++;

    if (c > '9') {
      c = '0';
      incrementCharArrayByOne(array, pos - 1);
    }
    array[pos] = c;
  }

  private void assignSplits() {
    final Iterator<Entry<Integer, String>> awaitingReader =
        readersAwaitingSplit.entrySet().iterator();

    while (awaitingReader.hasNext()) {
      final Map.Entry<Integer, String> nextAwaiting = awaitingReader.next();

      // if the reader that requested another split has failed in the meantime, remove
      // it from the list of waiting readers
      if (!context.registeredReaders().containsKey(nextAwaiting.getKey())) {
        awaitingReader.remove();
        continue;
      }

      final String hostname = nextAwaiting.getValue();
      final int awaitingSubtask = nextAwaiting.getKey();
      final Optional<URLSourceSplit> nextSplit = splitAssigner.getNext();
      if (nextSplit.isPresent()) {
        context.assignSplit(nextSplit.get(), awaitingSubtask);
        awaitingReader.remove();
      } else {
        break;
      }
    }
  }
}
