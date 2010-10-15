package org.apache.solr.util;

import java.util.Iterator;

public interface TermFreqIterator extends Iterator<String> {

  public float freq();
  
  public static class TermFreqIteratorWrapper implements TermFreqIterator {
    private Iterator wrapped;
    
    public TermFreqIteratorWrapper(Iterator wrapped) {
      this.wrapped = wrapped;
    }

    public float freq() {
      return 1.0f;
    }

    public boolean hasNext() {
      return wrapped.hasNext();
    }

    public String next() {
      return wrapped.next().toString();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
    
  }
}
