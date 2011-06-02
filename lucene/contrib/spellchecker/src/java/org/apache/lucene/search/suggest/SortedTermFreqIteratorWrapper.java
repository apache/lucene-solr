package org.apache.lucene.search.suggest;

import java.util.Collections;

import org.apache.lucene.search.spell.SortedIterator;
import org.apache.lucene.search.spell.TermFreqIterator;

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
