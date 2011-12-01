package org.apache.lucene.search.suggest.fst;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.util.BytesRef;

/**
 * Collects {@link BytesRef} and then allows one to iterate over their sorted order. Implementations
 * of this interface will be called in a single-threaded scenario.  
 */
public interface BytesRefSorter {
  /**
   * Adds a single suggestion entry (possibly compound with its bucket).
   * 
   * @throws IOException If an I/O exception occurs.
   * @throws IllegalStateException If an addition attempt is performed after
   * a call to {@link #iterator()} has been made.
   */
  void add(BytesRef utf8) throws IOException, IllegalStateException;

  /**
   * Sorts the entries added in {@link #add(BytesRef)} and returns 
   * an iterator over all sorted entries.
   * 
   * @throws IOException If an I/O exception occurs.
   */
  Iterator<BytesRef> iterator() throws IOException;
}
