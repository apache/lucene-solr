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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;;

/**
 * Expert: A hit queue for sorting by hits by terms in more than one field.
 * Uses <code>FieldCache.DEFAULT</code> for maintaining
 * internal term lookup tables.
 *
 * <b>NOTE:</b> This API is experimental and might change in
 * incompatible ways in the next release.
 *
 * @since   lucene 2.9
 * @version $Id:
 * @see Searcher#search(Query,Filter,int,Sort)
 * @see FieldCache
 */
public class FieldValueHitQueue extends PriorityQueue {

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
      return "slot:" + slot + " docID:" + docID;
    }
  }

  /**
   * Creates a hit queue sorted by the given list of fields.
   * @param fields SortField array we are sorting by in
   *   priority order (highest priority first); cannot be <code>null</code> or empty
   * @param size  The number of hits to retain.  Must be
   *   greater than zero.
   * @param subReaders Array of IndexReaders we will search,
   *   in order that they will be searched
   * @throws IOException
   */
  public FieldValueHitQueue(SortField[] fields, int size, IndexReader[] subReaders) throws IOException {
    numComparators = fields.length;
    comparators = new FieldComparator[numComparators];
    reverseMul = new int[numComparators];

    if (fields.length == 0) {
      throw new IllegalArgumentException("Sort must contain at least one field");
    }

    this.fields = fields;
    for (int i=0; i<numComparators; ++i) {
      SortField field = fields[i];

      // AUTO is resolved before we are called
      assert field.getType() != SortField.AUTO;

      reverseMul[i] = field.reverse ? -1 : 1;
      comparators[i] = field.getComparator(subReaders, size, i, field.reverse);
    }

    if (numComparators == 1) {
      comparator1 = comparators[0];
      reverseMul1 = reverseMul[0];
    } else {
      comparator1 = null;
      reverseMul1 = 0;
    }

    initialize(size);
  }
  
  /** Stores a comparator corresponding to each field being sorted by */
  private final FieldComparator[] comparators;
  private final FieldComparator comparator1;
  private final int numComparators;
  private final int[] reverseMul;
  private final int reverseMul1;

  FieldComparator[] getComparators() {
    return comparators;
  }

  int[] getReverseMul() {
    return reverseMul;
  }

  /** Stores the sort criteria being used. */
  private final SortField[] fields;

  /**
   * Returns whether <code>a</code> is less relevant than <code>b</code>.
   * @param a ScoreDoc
   * @param b ScoreDoc
   * @return <code>true</code> if document <code>a</code> should be sorted after document <code>b</code>.
   */
  protected boolean lessThan (final Object a, final Object b) {
    final Entry hitA = (Entry) a;
    final Entry hitB = (Entry) b;

    assert hitA != hitB;
    assert hitA.slot != hitB.slot;

    if (numComparators == 1) {
      // Common case
      final int c = reverseMul1 * comparator1.compare(hitA.slot, hitB.slot);
      if (c != 0) {
        return c > 0;
      }
    } else {
      // run comparators
      for (int i=0; i<numComparators; ++i) {
        final int c = reverseMul[i] * comparators[i].compare(hitA.slot, hitB.slot);
        if (c != 0) {
          // Short circuit
          return c > 0;
        }
      }
    }

    // avoid random sort order that could lead to duplicates (bug #31241):
    return hitA.docID > hitB.docID;
  }


  /**
   * Given a FieldDoc object, stores the values used
   * to sort the given document.  These values are not the raw
   * values out of the index, but the internal representation
   * of them.  This is so the given search hit can be collated
   * by a MultiSearcher with other search hits.
   * @param  doc  The FieldDoc to store sort values into.
   * @return  The same FieldDoc passed in.
   * @see Searchable#search(Weight,Filter,int,Sort)
   */
  FieldDoc fillFields (final Entry entry) {
    final int n = comparators.length;
    final Comparable[] fields = new Comparable[n];
    for (int i=0; i<n; ++i)
      fields[i] = comparators[i].value(entry.slot);
    //if (maxscore > 1.0f) doc.score /= maxscore;   // normalize scores
    return new FieldDoc(entry.docID,
                        entry.score,
                        fields);
  }


  /** Returns the SortFields being used by this hit queue. */
  SortField[] getFields() {
    return fields;
  }
  
  /**
   * Attempts to detect the given field type for an IndexReader.
   */
  static int detectFieldType(IndexReader reader, String fieldKey) throws IOException {
    String field = ((String)fieldKey).intern();
    TermEnum enumerator = reader.terms (new Term (field));
    try {
      Term term = enumerator.term();
      if (term == null) {
        throw new RuntimeException ("no terms in field " + field + " - cannot determine sort type");
      }
      int ret = 0;
      if (term.field() == field) {
        String termtext = term.text().trim();

        /**
         * Java 1.4 level code:

         if (pIntegers.matcher(termtext).matches())
         return IntegerSortedHitQueue.comparator (reader, enumerator, field);

         else if (pFloats.matcher(termtext).matches())
         return FloatSortedHitQueue.comparator (reader, enumerator, field);
         */

        // Java 1.3 level code:
        try {
          Integer.parseInt (termtext);
          ret = SortField.INT;
        } catch (NumberFormatException nfe1) {
          try {
            Long.parseLong(termtext);
            ret = SortField.LONG;
          } catch (NumberFormatException nfe2) {
            try {
              Float.parseFloat (termtext);
              ret = SortField.FLOAT;
            } catch (NumberFormatException nfe3) {
              ret = SortField.STRING;
            }
          }
        }         
      } else {
        throw new RuntimeException ("field \"" + field + "\" does not appear to be indexed");
      }
      return ret;
    } finally {
      enumerator.close();
    }
  }
}
