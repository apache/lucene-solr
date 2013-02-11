package org.apache.lucene.index;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.util.BytesRef;

/**
 * A per-document set of presorted byte[] values.
 * <p>
 * Per-Document values in a SortedDocValues are deduplicated, dereferenced,
 * and sorted into a dictionary of unique values. A pointer to the
 * dictionary value (ordinal) can be retrieved for each document. Ordinals
 * are dense and in increasing sorted order.
 */
public abstract class SortedSetDocValues {
  
  /** Sole constructor. (For invocation by subclass 
   * constructors, typically implicit.) */
  protected SortedSetDocValues() {}

  /**
   * Returns an iterator over the ordinals for the specified docID.
   * @param  docID document ID to lookup
   * @return iterator over ordinals for the document: these are dense, 
   *         start at 0, then increment by 1 for the next value in sorted order. 
   */
  public abstract OrdIterator getOrds(int docID, OrdIterator reuse);

  /** Retrieves the value for the specified ordinal.
   * @param ord ordinal to lookup
   * @param result will be populated with the ordinal's value
   * @see #getOrds
   */
  public abstract void lookupOrd(long ord, BytesRef result);

  /**
   * Returns the number of unique values.
   * @return number of unique values in this SortedDocValues. This is
   *         also equivalent to one plus the maximum ordinal.
   */
  public abstract long getValueCount();


  /** An empty SortedDocValues which returns {@link OrdIterator#EMPTY} for every document */
  public static final SortedSetDocValues EMPTY = new SortedSetDocValues() {
    @Override
    public OrdIterator getOrds(int docID, OrdIterator reuse) {
      return OrdIterator.EMPTY;
    }

    @Override
    public void lookupOrd(long ord, BytesRef result) {
      throw new IndexOutOfBoundsException();
    }

    @Override
    public long getValueCount() {
      return 0;
    }
  };

  /** If {@code key} exists, returns its ordinal, else
   *  returns {@code -insertionPoint-1}, like {@code
   *  Arrays.binarySearch}.
   *
   *  @param key Key to look up
   **/
  public long lookupTerm(BytesRef key) {
    BytesRef spare = new BytesRef();
    long low = 0;
    long high = getValueCount()-1;

    while (low <= high) {
      long mid = (low + high) >>> 1;
      lookupOrd(mid, spare);
      int cmp = spare.compareTo(key);

      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }

    return -(low + 1);  // key not found.
  }
  
  /** An iterator over the ordinals in a document (in increasing order) */
  public static abstract class OrdIterator {
    /** Indicates enumeration has ended: no more ordinals for this document */
    public static final long NO_MORE_ORDS = Long.MAX_VALUE;
    /** An iterator that always returns {@link #NO_MORE_ORDS} */
    public static final OrdIterator EMPTY = new OrdIterator() {
      @Override
      public long nextOrd() {
        return NO_MORE_ORDS;
      }
    };
    
    /** Returns next ordinal, or {@link #NO_MORE_ORDS} */
    public abstract long nextOrd();
  }
}
