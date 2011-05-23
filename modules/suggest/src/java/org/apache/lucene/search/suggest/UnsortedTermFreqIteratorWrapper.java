package org.apache.lucene.search.suggest;

import java.util.Collections;

import org.apache.lucene.search.spell.TermFreqIterator;

/**
 * This wrapper buffers the incoming elements and makes sure they are in
 * random order.
 */
public class UnsortedTermFreqIteratorWrapper extends BufferingTermFreqIteratorWrapper {

  public UnsortedTermFreqIteratorWrapper(TermFreqIterator source) {
    super(source);
    Collections.shuffle(entries);
  }
}
