package org.apache.solr.spelling.suggest;


import java.util.ArrayList;
import java.util.List;

import org.apache.solr.util.TermFreqIterator;

/**
 * This wrapper buffers incoming elements.
 */
public class BufferingTermFreqIteratorWrapper implements TermFreqIterator {

  /** Entry in the buffer. */
  public static final class Entry implements Comparable<Entry> {
    String word;
    float freq;
    
    public Entry(String word, float freq) {
      this.word = word;
      this.freq = freq;
    }
    
    @Override
    public int compareTo(Entry o) {
      return word.compareTo(o.word);
    }    
  }

  protected ArrayList<Entry> entries = new ArrayList<Entry>();
  
  protected int curPos;
  protected Entry curEntry;
  
  public BufferingTermFreqIteratorWrapper(TermFreqIterator source) {
    // read all source data into buffer
    while (source.hasNext()) {
      String w = source.next();
      Entry e = new Entry(w, source.freq());
      entries.add(e);
    }
    curPos = 0;
  }

  @Override
  public float freq() {
    return curEntry.freq;
  }

  @Override
  public boolean hasNext() {
    return curPos < entries.size();
  }

  @Override
  public String next() {
    curEntry = entries.get(curPos);
    curPos++;
    return curEntry.word;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove is not supported");
  }
  
  public List<Entry> entries() {
    return entries;
  }
}
