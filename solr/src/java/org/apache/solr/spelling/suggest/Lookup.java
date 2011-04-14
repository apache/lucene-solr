package org.apache.solr.spelling.suggest;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.util.PriorityQueue;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.TermFreqIterator;

public abstract class Lookup {
  /**
   * Result of a lookup.
   */
  public static final class LookupResult implements Comparable<LookupResult> {
    public final String key;
    public final float value;
    
    public LookupResult(String key, float value) {
      this.key = key;
      this.value = value;
    }
    
    @Override
    public String toString() {
      return key + "/" + value;
    }

    /** Compare alphabetically. */
    public int compareTo(LookupResult o) {
      return this.key.compareTo(o.key);
    }
  }
  
  public static final class LookupPriorityQueue extends PriorityQueue<LookupResult> {
    
    public LookupPriorityQueue(int size) {
      super(size);
    }

    @Override
    protected boolean lessThan(LookupResult a, LookupResult b) {
      return a.value < b.value;
    }
    
    public LookupResult[] getResults() {
      int size = size();
      LookupResult[] res = new LookupResult[size];
      for (int i = size - 1; i >= 0; i--) {
        res[i] = pop();
      }
      return res;
    }
  }
  
  /** Initialize the lookup. */
  public abstract void init(NamedList config, SolrCore core);
  
  /** Build lookup from a dictionary. Some implementations may require sorted
   * or unsorted keys from the dictionary's iterator - use
   * {@link SortedTermFreqIteratorWrapper} or
   * {@link UnsortedTermFreqIteratorWrapper} in such case.
   */
  public void build(Dictionary dict) throws IOException {
    Iterator<String> it = dict.getWordsIterator();
    TermFreqIterator tfit;
    if (it instanceof TermFreqIterator) {
      tfit = (TermFreqIterator)it;
    } else {
      tfit = new TermFreqIterator.TermFreqIteratorWrapper(it);
    }
    build(tfit);
  }
  
  protected abstract void build(TermFreqIterator tfit) throws IOException;
  
  /**
   * Persist the constructed lookup data to a directory. Optional operation.
   * @param storeDir directory where data can be stored.
   * @return true if successful, false if unsuccessful or not supported.
   * @throws IOException when fatal IO error occurs.
   */
  public abstract boolean store(File storeDir) throws IOException;

  /**
   * Discard current lookup data and load it from a previously saved copy.
   * Optional operation.
   * @param storeDir directory where lookup data was stored.
   * @return true if completed successfully, false if unsuccessful or not supported.
   * @throws IOException when fatal IO error occurs.
   */
  public abstract boolean load(File storeDir) throws IOException;
  
  /**
   * Look up a key and return possible completion for this key.
   * @param key lookup key. Depending on the implementation this may be
   * a prefix, misspelling, or even infix.
   * @param onlyMorePopular return only more popular results
   * @param num maximum number of results to return
   * @return a list of possible completions, with their relative weight (e.g. popularity)
   */
  public abstract List<LookupResult> lookup(String key, boolean onlyMorePopular, int num);

  /**
   * Modify the lookup data by recording additional data. Optional operation.
   * @param key new lookup key
   * @param value value to associate with this key
   * @return true if new key is added, false if it already exists or operation
   * is not supported.
   */
  public abstract boolean add(String key, Object value);
  
  /**
   * Get value associated with a specific key.
   * @param key lookup key
   * @return associated value
   */
  public abstract Object get(String key);  
}
