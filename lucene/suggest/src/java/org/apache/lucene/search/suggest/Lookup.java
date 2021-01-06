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
package org.apache.lucene.search.suggest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.PriorityQueue;

/**
 * Simple Lookup interface for {@link CharSequence} suggestions.
 *
 * @lucene.experimental
 */
public abstract class Lookup implements Accountable {

  /**
   * Result of a lookup.
   *
   * @lucene.experimental
   */
  public static final class LookupResult implements Comparable<LookupResult> {
    /** the key's text */
    public final CharSequence key;

    /** Expert: custom Object to hold the result of a highlighted suggestion. */
    public final Object highlightKey;

    /** the key's weight */
    public final long value;

    /** the key's payload (null if not present) */
    public final BytesRef payload;

    /** the key's contexts (null if not present) */
    public final Set<BytesRef> contexts;

    /** Create a new result from a key+weight pair. */
    public LookupResult(CharSequence key, long value) {
      this(key, null, value, null, null);
    }

    /** Create a new result from a key+weight+payload triple. */
    public LookupResult(CharSequence key, long value, BytesRef payload) {
      this(key, null, value, payload, null);
    }

    /** Create a new result from a key+highlightKey+weight+payload triple. */
    public LookupResult(CharSequence key, Object highlightKey, long value, BytesRef payload) {
      this(key, highlightKey, value, payload, null);
    }

    /** Create a new result from a key+weight+payload+contexts triple. */
    public LookupResult(CharSequence key, long value, BytesRef payload, Set<BytesRef> contexts) {
      this(key, null, value, payload, contexts);
    }

    /** Create a new result from a key+weight+contexts triple. */
    public LookupResult(CharSequence key, long value, Set<BytesRef> contexts) {
      this(key, null, value, null, contexts);
    }

    /** Create a new result from a key+highlightKey+weight+payload+contexts triple. */
    public LookupResult(
        CharSequence key,
        Object highlightKey,
        long value,
        BytesRef payload,
        Set<BytesRef> contexts) {
      this.key = key;
      this.highlightKey = highlightKey;
      this.value = value;
      this.payload = payload;
      this.contexts = contexts;
    }

    @Override
    public String toString() {
      return key + "/" + value;
    }

    /** Compare alphabetically. */
    @Override
    public int compareTo(LookupResult o) {
      return CHARSEQUENCE_COMPARATOR.compare(key, o.key);
    }
  }

  /** A simple char-by-char comparator for {@link CharSequence} */
  public static final Comparator<CharSequence> CHARSEQUENCE_COMPARATOR =
      new CharSequenceComparator();

  private static class CharSequenceComparator implements Comparator<CharSequence> {

    @Override
    public int compare(CharSequence o1, CharSequence o2) {
      final int l1 = o1.length();
      final int l2 = o2.length();

      final int aStop = Math.min(l1, l2);
      for (int i = 0; i < aStop; i++) {
        int diff = o1.charAt(i) - o2.charAt(i);
        if (diff != 0) {
          return diff;
        }
      }
      // One is a prefix of the other, or, they are equal:
      return l1 - l2;
    }
  }

  /** A {@link PriorityQueue} collecting a fixed size of high priority {@link LookupResult} */
  public static final class LookupPriorityQueue extends PriorityQueue<LookupResult> {
    // TODO: should we move this out of the interface into a utility class?
    /** Creates a new priority queue of the specified size. */
    public LookupPriorityQueue(int size) {
      super(size);
    }

    @Override
    protected boolean lessThan(LookupResult a, LookupResult b) {
      return a.value < b.value;
    }

    /**
     * Returns the top N results in descending order.
     *
     * @return the top N results in descending order.
     */
    public LookupResult[] getResults() {
      int size = size();
      LookupResult[] res = new LookupResult[size];
      for (int i = size - 1; i >= 0; i--) {
        res[i] = pop();
      }
      return res;
    }
  }

  /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
  public Lookup() {}

  /**
   * Build lookup from a dictionary. Some implementations may require sorted or unsorted keys from
   * the dictionary's iterator - use {@link SortedInputIterator} or {@link UnsortedInputIterator} in
   * such case.
   */
  public void build(Dictionary dict) throws IOException {
    build(dict.getEntryIterator());
  }

  /** Calls {@link #load(DataInput)} after converting {@link InputStream} to {@link DataInput} */
  public boolean load(InputStream input) throws IOException {
    DataInput dataIn = new InputStreamDataInput(input);
    try {
      return load(dataIn);
    } finally {
      IOUtils.close(input);
    }
  }

  /**
   * Calls {@link #store(DataOutput)} after converting {@link OutputStream} to {@link DataOutput}
   */
  public boolean store(OutputStream output) throws IOException {
    DataOutput dataOut = new OutputStreamDataOutput(output);
    try {
      return store(dataOut);
    } finally {
      IOUtils.close(output);
    }
  }

  /**
   * Get the number of entries the lookup was built with
   *
   * @return total number of suggester entries
   */
  public abstract long getCount() throws IOException;

  /**
   * Builds up a new internal {@link Lookup} representation based on the given {@link
   * InputIterator}. The implementation might re-sort the data internally.
   */
  public abstract void build(InputIterator inputIterator) throws IOException;

  /**
   * Look up a key and return possible completion for this key.
   *
   * @param key lookup key. Depending on the implementation this may be a prefix, misspelling, or
   *     even infix.
   * @param onlyMorePopular return only more popular results
   * @param num maximum number of results to return
   * @return a list of possible completions, with their relative weight (e.g. popularity)
   */
  public List<LookupResult> lookup(CharSequence key, boolean onlyMorePopular, int num)
      throws IOException {
    return lookup(key, null, onlyMorePopular, num);
  }

  /**
   * Look up a key and return possible completion for this key.
   *
   * @param key lookup key. Depending on the implementation this may be a prefix, misspelling, or
   *     even infix.
   * @param contexts contexts to filter the lookup by, or null if all contexts are allowed; if the
   *     suggestion contains any of the contexts, it's a match
   * @param onlyMorePopular return only more popular results
   * @param num maximum number of results to return
   * @return a list of possible completions, with their relative weight (e.g. popularity)
   */
  public abstract List<LookupResult> lookup(
      CharSequence key, Set<BytesRef> contexts, boolean onlyMorePopular, int num)
      throws IOException;

  /**
   * Look up a key and return possible completion for this key. This needs to be overridden by all
   * implementing classes as the default implementation just returns null
   *
   * @param key the lookup key
   * @param contextFilerQuery A query for further filtering the result of the key lookup
   * @param num maximum number of results to return
   * @param allTermsRequired true is all terms are required
   * @param doHighlight set to true if key should be highlighted
   * @return a list of suggestions/completions. The default implementation returns null, meaning
   *     each @Lookup implementation should override this and provide their own implementation
   * @throws IOException when IO exception occurs
   */
  public List<LookupResult> lookup(
      CharSequence key,
      BooleanQuery contextFilerQuery,
      int num,
      boolean allTermsRequired,
      boolean doHighlight)
      throws IOException {
    return null;
  }

  /**
   * Persist the constructed lookup data to a directory. Optional operation.
   *
   * @param output {@link DataOutput} to write the data to.
   * @return true if successful, false if unsuccessful or not supported.
   * @throws IOException when fatal IO error occurs.
   */
  public abstract boolean store(DataOutput output) throws IOException;

  /**
   * Discard current lookup data and load it from a previously saved copy. Optional operation.
   *
   * @param input the {@link DataInput} to load the lookup data.
   * @return true if completed successfully, false if unsuccessful or not supported.
   * @throws IOException when fatal IO error occurs.
   */
  public abstract boolean load(DataInput input) throws IOException;
}
