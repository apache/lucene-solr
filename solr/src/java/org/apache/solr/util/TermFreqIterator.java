package org.apache.solr.util;

import java.util.Iterator;

public interface TermFreqIterator extends Iterator<String> {

  public float freq();
  
  public static class TermFreqIteratorWrapper implements TermFreqIterator {
    private Iterator wrapped;
    
    public TermFreqIteratorWrapper(Iterator wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public float freq() {
      return 1.0f;
    }

    @Override
    public boolean hasNext() {
      return wrapped.hasNext();
    }

    @Override
    public String next() {
      return wrapped.next().toString();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
    
  }
}
