package org.apache.solr.spelling.suggest;

import java.util.Collections;

import org.apache.solr.util.SortedIterator;
import org.apache.solr.util.TermFreqIterator;

/**
 * This wrapper buffers incoming elements and makes sure they are sorted in
 * ascending lexicographic order.
 */
public class SortedTermFreqIteratorWrapper extends BufferingTermFreqIteratorWrapper implements SortedIterator {

  public SortedTermFreqIteratorWrapper(TermFreqIterator source) {
    super(source);
    Collections.sort(entries);
  }
}
