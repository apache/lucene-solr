package org.apache.solr.spelling.suggest;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.solr.util.TermFreqIterator;

/**
 * A {@link TermFreqIterator} over a sequence of {@link TermFreq}s.
 */
public final class TermFreqArrayIterator implements TermFreqIterator {
  private final Iterator<TermFreq> i;
  private TermFreq current;

  public TermFreqArrayIterator(Iterator<TermFreq> i) {
    this.i = i;
  }

  public TermFreqArrayIterator(TermFreq [] i) {
    this(Arrays.asList(i));
  }

  public TermFreqArrayIterator(Iterable<TermFreq> i) {
    this(i.iterator());
  }
  
  public float freq() {
    return current.v;
  }
  
  public boolean hasNext() {
    return i.hasNext();
  }
  
  public String next() {
    return (current = i.next()).term;
  }

  public void remove() { throw new UnsupportedOperationException(); }
}