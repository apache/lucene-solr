package org.apache.solr.spelling.suggest;

import java.util.Collections;

import org.apache.solr.util.TermFreqIterator;

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
