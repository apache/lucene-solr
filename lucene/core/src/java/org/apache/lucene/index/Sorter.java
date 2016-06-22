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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.Comparator;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.TimSorter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * Sorts documents of a given index by returning a permutation on the document
 * IDs.
 * @lucene.experimental
 */
final class Sorter {
  final Sort sort;
  
  /** Creates a new Sorter to sort the index with {@code sort} */
  Sorter(Sort sort) {
    if (sort.needsScores()) {
      throw new IllegalArgumentException("Cannot sort an index with a Sort that refers to the relevance score");
    }
    this.sort = sort;
  }

  /**
   * A permutation of doc IDs. For every document ID between <tt>0</tt> and
   * {@link IndexReader#maxDoc()}, <code>oldToNew(newToOld(docID))</code> must
   * return <code>docID</code>.
   */
  static abstract class DocMap {

    /** Given a doc ID from the original index, return its ordinal in the
     *  sorted index. */
    abstract int oldToNew(int docID);

    /** Given the ordinal of a doc ID, return its doc ID in the original index. */
    abstract int newToOld(int docID);

    /** Return the number of documents in this map. This must be equal to the
     *  {@link org.apache.lucene.index.LeafReader#maxDoc() number of documents} of the
     *  {@link org.apache.lucene.index.LeafReader} which is sorted. */
    abstract int size();
  }

  /** Check consistency of a {@link DocMap}, useful for assertions. */
  static boolean isConsistent(DocMap docMap) {
    final int maxDoc = docMap.size();
    for (int i = 0; i < maxDoc; ++i) {
      final int newID = docMap.oldToNew(i);
      final int oldID = docMap.newToOld(newID);
      assert newID >= 0 && newID < maxDoc : "doc IDs must be in [0-" + maxDoc + "[, got " + newID;
      assert i == oldID : "mapping is inconsistent: " + i + " --oldToNew--> " + newID + " --newToOld--> " + oldID;
      if (i != oldID || newID < 0 || newID >= maxDoc) {
        return false;
      }
    }
    return true;
  }

  /** A comparator of doc IDs. */
  static abstract class DocComparator {

    /** Compare docID1 against docID2. The contract for the return value is the
     *  same as {@link Comparator#compare(Object, Object)}. */
    public abstract int compare(int docID1, int docID2);

  }

  private static final class DocValueSorter extends TimSorter {
    
    private final int[] docs;
    private final Sorter.DocComparator comparator;
    private final int[] tmp;
    
    DocValueSorter(int[] docs, Sorter.DocComparator comparator) {
      super(docs.length / 64);
      this.docs = docs;
      this.comparator = comparator;
      tmp = new int[docs.length / 64];
    }
    
    @Override
    protected int compare(int i, int j) {
      return comparator.compare(docs[i], docs[j]);
    }
    
    @Override
    protected void swap(int i, int j) {
      int tmpDoc = docs[i];
      docs[i] = docs[j];
      docs[j] = tmpDoc;
    }

    @Override
    protected void copy(int src, int dest) {
      docs[dest] = docs[src];
    }

    @Override
    protected void save(int i, int len) {
      System.arraycopy(docs, i, tmp, 0, len);
    }

    @Override
    protected void restore(int i, int j) {
      docs[j] = tmp[i];
    }

    @Override
    protected int compareSaved(int i, int j) {
      return comparator.compare(tmp[i], docs[j]);
    }
  }

  /** Computes the old-to-new permutation over the given comparator. */
  private static Sorter.DocMap sort(final int maxDoc, DocComparator comparator) {
    // check if the index is sorted
    boolean sorted = true;
    for (int i = 1; i < maxDoc; ++i) {
      if (comparator.compare(i-1, i) > 0) {
        sorted = false;
        break;
      }
    }
    if (sorted) {
      return null;
    }

    // sort doc IDs
    final int[] docs = new int[maxDoc];
    for (int i = 0; i < maxDoc; i++) {
      docs[i] = i;
    }
    
    DocValueSorter sorter = new DocValueSorter(docs, comparator);
    // It can be common to sort a reader, add docs, sort it again, ... and in
    // that case timSort can save a lot of time
    sorter.sort(0, docs.length); // docs is now the newToOld mapping

    // The reason why we use MonotonicAppendingLongBuffer here is that it
    // wastes very little memory if the index is in random order but can save
    // a lot of memory if the index is already "almost" sorted
    final PackedLongValues.Builder newToOldBuilder = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
    for (int i = 0; i < maxDoc; ++i) {
      newToOldBuilder.add(docs[i]);
    }
    final PackedLongValues newToOld = newToOldBuilder.build();

    // invert the docs mapping:
    for (int i = 0; i < maxDoc; ++i) {
      docs[(int) newToOld.get(i)] = i;
    } // docs is now the oldToNew mapping

    final PackedLongValues.Builder oldToNewBuilder = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
    for (int i = 0; i < maxDoc; ++i) {
      oldToNewBuilder.add(docs[i]);
    }
    final PackedLongValues oldToNew = oldToNewBuilder.build();
    
    return new Sorter.DocMap() {

      @Override
      public int oldToNew(int docID) {
        return (int) oldToNew.get(docID);
      }

      @Override
      public int newToOld(int docID) {
        return (int) newToOld.get(docID);
      }

      @Override
      public int size() {
        return maxDoc;
      }
    };
  }

  /**
   * Returns a mapping from the old document ID to its new location in the
   * sorted index. Implementations can use the auxiliary
   * {@link #sort(int, DocComparator)} to compute the old-to-new permutation
   * given a list of documents and their corresponding values.
   * <p>
   * A return value of <tt>null</tt> is allowed and means that
   * <code>reader</code> is already sorted.
   * <p>
   * <b>NOTE:</b> deleted documents are expected to appear in the mapping as
   * well, they will however be marked as deleted in the sorted view.
   */
  DocMap sort(LeafReader reader) throws IOException {
    SortField fields[] = sort.getSort();
    final int reverseMul[] = new int[fields.length];
    final LeafFieldComparator comparators[] = new LeafFieldComparator[fields.length];
    
    for (int i = 0; i < fields.length; i++) {
      reverseMul[i] = fields[i].getReverse() ? -1 : 1;
      comparators[i] = fields[i].getComparator(1, i).getLeafComparator(reader.getContext());
      comparators[i].setScorer(FAKESCORER);
    }
    final DocComparator comparator = new DocComparator() {
      @Override
      public int compare(int docID1, int docID2) {
        try {
          for (int i = 0; i < comparators.length; i++) {
            // TODO: would be better if copy() didnt cause a term lookup in TermOrdVal & co,
            // the segments are always the same here...
            comparators[i].copy(0, docID1);
            comparators[i].setBottom(0);
            int comp = reverseMul[i] * comparators[i].compareBottom(docID2);
            if (comp != 0) {
              return comp;
            }
          }
          return Integer.compare(docID1, docID2); // docid order tiebreak
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
    return sort(reader.maxDoc(), comparator);
  }

  /**
   * Returns the identifier of this {@link Sorter}.
   * <p>This identifier is similar to {@link Object#hashCode()} and should be
   * chosen so that two instances of this class that sort documents likewise
   * will have the same identifier. On the contrary, this identifier should be
   * different on different {@link Sort sorts}.
   */
  public String getID() {
    return sort.toString();
  }

  @Override
  public String toString() {
    return getID();
  }
  
  static final Scorer FAKESCORER = new Scorer(null) {

    float score;
    int doc = -1;
    int freq = 1;

    @Override
    public int docID() {
      return doc;
    }

    public DocIdSetIterator iterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int freq() throws IOException {
      return freq;
    }

    @Override
    public float score() throws IOException {
      return score;
    }
  };
  
}
