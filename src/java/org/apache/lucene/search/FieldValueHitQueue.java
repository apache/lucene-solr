package org.apache.lucene.search;

/**
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

import java.io.IOException;

import org.apache.lucene.util.PriorityQueue;

/**
 * Expert: A hit queue for sorting by hits by terms in more than one field.
 * Uses <code>FieldCache.DEFAULT</code> for maintaining
 * internal term lookup tables.
 * 
 * This class will not resolve SortField.AUTO types, and expects the type
 * of all SortFields used for construction to already have been resolved. 
 * {@link SortField#detectFieldType(IndexReader, String)} is a utility method which
 * may be used for field type detection.
 *
 * <b>NOTE:</b> This API is experimental and might change in
 * incompatible ways in the next release.
 *
 * @since 2.9
 * @version $Id:
 * @see Searcher#search(Query,Filter,int,Sort)
 * @see FieldCache
 */
public abstract class FieldValueHitQueue extends PriorityQueue {

  final static class Entry {
    int slot;
    int docID;
    float score;

    Entry(int slot, int docID, float score) {
      this.slot = slot;
      this.docID = docID;
      this.score = score;
    }
    
    public String toString() {
      return "slot:" + slot + " docID:" + docID + " score=" + score;
    }
  }

  /**
   * An implementation of {@link FieldValueHitQueue} which is optimized in case
   * there is just one comparator.
   */
  private static final class OneComparatorFieldValueHitQueue extends FieldValueHitQueue {

    private final FieldComparator comparator;
    private final int oneReverseMul;
    
    public OneComparatorFieldValueHitQueue(SortField[] fields, int size)
        throws IOException {
      super(fields);
      if (fields.length == 0) {
        throw new IllegalArgumentException("Sort must contain at least one field");
      }

      SortField field = fields[0];
      // AUTO is resolved before we are called
      assert field.getType() != SortField.AUTO;
      comparator = field.getComparator(size, 0);
      oneReverseMul = field.reverse ? -1 : 1;

      comparators[0] = comparator;
      reverseMul[0] = oneReverseMul;
      
      initialize(size);
    }

    /**
     * Returns whether <code>a</code> is less relevant than <code>b</code>.
     * @param a ScoreDoc
     * @param b ScoreDoc
     * @return <code>true</code> if document <code>a</code> should be sorted after document <code>b</code>.
     */
    protected boolean lessThan(final Object a, final Object b) {
      final Entry hitA = (Entry) a;
      final Entry hitB = (Entry) b;

      assert hitA != hitB;
      assert hitA.slot != hitB.slot;

      final int c = oneReverseMul * comparator.compare(hitA.slot, hitB.slot);
      if (c != 0) {
        return c > 0;
      }

      // avoid random sort order that could lead to duplicates (bug #31241):
      return hitA.docID > hitB.docID;
    }

  }
  
  /**
   * An implementation of {@link FieldValueHitQueue} which is optimized in case
   * there is more than one comparator.
   */
  private static final class MultiComparatorsFieldValueHitQueue extends FieldValueHitQueue {

    public MultiComparatorsFieldValueHitQueue(SortField[] fields, int size)
        throws IOException {
      super(fields);

      int numComparators = comparators.length;
      for (int i = 0; i < numComparators; ++i) {
        SortField field = fields[i];

        // AUTO is resolved before we are called
        assert field.getType() != SortField.AUTO;

        reverseMul[i] = field.reverse ? -1 : 1;
        comparators[i] = field.getComparator(size, i);
      }

      initialize(size);
    }
  
    protected boolean lessThan(final Object a, final Object b) {
      final Entry hitA = (Entry) a;
      final Entry hitB = (Entry) b;

      assert hitA != hitB;
      assert hitA.slot != hitB.slot;

      int numComparators = comparators.length;
      for (int i = 0; i < numComparators; ++i) {
        final int c = reverseMul[i] * comparators[i].compare(hitA.slot, hitB.slot);
        if (c != 0) {
          // Short circuit
          return c > 0;
        }
      }

      // avoid random sort order that could lead to duplicates (bug #31241):
      return hitA.docID > hitB.docID;
    }
    
  }
  
  // prevent instantiation and extension.
  private FieldValueHitQueue(SortField[] fields) {
    // When we get here, fields.length is guaranteed to be > 0, therefore no
    // need to check it again.
    
    // All these are required by this class's API - need to return arrays.
    // Therefore even in the case of a single comparator, create an array
    // anyway.
    this.fields = fields;
    int numComparators = fields.length;
    comparators = new FieldComparator[numComparators];
    reverseMul = new int[numComparators];
  }

  /**
   * Creates a hit queue sorted by the given list of fields.
   * 
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length <code>numHits</code>.
   * 
   * @param fields
   *          SortField array we are sorting by in priority order (highest
   *          priority first); cannot be <code>null</code> or empty
   * @param size
   *          The number of hits to retain. Must be greater than zero.
   * @throws IOException
   */
  public static FieldValueHitQueue create(SortField[] fields, int size) throws IOException {

    if (fields.length == 0) {
      throw new IllegalArgumentException("Sort must contain at least one field");
    }

    if (fields.length == 1) {
      return new OneComparatorFieldValueHitQueue(fields, size);
    } else {
      return new MultiComparatorsFieldValueHitQueue(fields, size);
    }
  }
  
  FieldComparator[] getComparators() { return comparators; }

  int[] getReverseMul() { return reverseMul; }

  /** Stores the sort criteria being used. */
  protected final SortField[] fields;
  protected final FieldComparator[] comparators;
  protected final int[] reverseMul;

  protected abstract boolean lessThan (final Object a, final Object b);

  /**
   * Given a queue Entry, creates a corresponding FieldDoc
   * that contains the values used to sort the given document.
   * These values are not the raw values out of the index, but the internal
   * representation of them. This is so the given search hit can be collated by
   * a MultiSearcher with other search hits.
   * 
   * @param entry The Entry used to create a FieldDoc
   * @return The newly created FieldDoc
   * @see Searchable#search(Weight,Filter,int,Sort)
   */
  FieldDoc fillFields(final Entry entry) {
    final int n = comparators.length;
    final Comparable[] fields = new Comparable[n];
    for (int i = 0; i < n; ++i) {
      fields[i] = comparators[i].value(entry.slot);
    }
    //if (maxscore > 1.0f) doc.score /= maxscore;   // normalize scores
    return new FieldDoc(entry.docID, entry.score, fields);
  }

  /** Returns the SortFields being used by this hit queue. */
  SortField[] getFields() {
    return fields;
  }
}
