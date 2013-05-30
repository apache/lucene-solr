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
 * A per-document byte[] with presorted values.
 * <p>
 * Per-Document values in a SortedDocValues are deduplicated, dereferenced,
 * and sorted into a dictionary of unique values. A pointer to the
 * dictionary value (ordinal) can be retrieved for each document. Ordinals
 * are dense and in increasing sorted order.
 */
public abstract class SortedDocValues extends BinaryDocValues {
  
  /** Sole constructor. (For invocation by subclass 
   * constructors, typically implicit.) */
  protected SortedDocValues() {}

  /**
   * Returns the ordinal for the specified docID.
   * @param  docID document ID to lookup
   * @return ordinal for the document: this is dense, starts at 0, then
   *         increments by 1 for the next value in sorted order. 
   */
  public abstract int getOrd(int docID);

  /** Retrieves the value for the specified ordinal.
   * @param ord ordinal to lookup
   * @param result will be populated with the ordinal's value
   * @see #getOrd(int) 
   */
  public abstract void lookupOrd(int ord, BytesRef result);

  /**
   * Returns the number of unique values.
   * @return number of unique values in this SortedDocValues. This is
   *         also equivalent to one plus the maximum ordinal.
   */
  public abstract int getValueCount();

  @Override
  public void get(int docID, BytesRef result) {
    int ord = getOrd(docID);
    if (ord == -1) {
      result.bytes = MISSING;
      result.length = 0;
      result.offset = 0;
    } else {
      lookupOrd(ord, result);
    }
  }

  /** An empty SortedDocValues which returns {@link #MISSING} for every document */
  public static final SortedDocValues EMPTY = new SortedDocValues() {
    @Override
    public int getOrd(int docID) {
      return 0;
    }

    @Override
    public void lookupOrd(int ord, BytesRef result) {
      result.bytes = MISSING;
      result.offset = 0;
      result.length = 0;
    }

    @Override
    public int getValueCount() {
      return 1;
    }
  };

  /** If {@code key} exists, returns its ordinal, else
   *  returns {@code -insertionPoint-1}, like {@code
   *  Arrays.binarySearch}.
   *
   *  @param key Key to look up
   **/
  public int lookupTerm(BytesRef key) {
    BytesRef spare = new BytesRef();
    int low = 0;
    int high = getValueCount()-1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
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
  
  /** 
   * Returns a {@link TermsEnum} over the values.
   * The enum supports {@link TermsEnum#ord()} and {@link TermsEnum#seekExact(long)}.
   */
  public TermsEnum termsEnum() {
    return new SortedDocValuesTermsEnum(this);
  }
}
