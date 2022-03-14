package com.github.cpzt.connector.url.enumerator.assigner;

import com.github.cpzt.connector.url.split.URLSourceSplit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

public class SimpleSplitAssigner implements URLSplitAssigner{

  private final ArrayList<URLSourceSplit> splits;

  public SimpleSplitAssigner(Collection<URLSourceSplit> splits) {
    this.splits = new ArrayList<>(splits);
  }


  @Override
  public Optional<URLSourceSplit> getNext() {
    final int size = splits.size();
    return size == 0 ? Optional.empty() : Optional.of(splits.remove(size - 1));
  }

  @Override
  public void addSplits(Collection<URLSourceSplit> newSplits) {
    splits.addAll(newSplits);
  }

  @Override
  public Collection<URLSourceSplit> remainingSplits() {
    return splits;
  }

  @Override
  public String toString() {
    return "SimpleSplitAssigner{" + "splits=" + splits + '}';
  }
}
