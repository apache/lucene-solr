package org.apache.lucene.index.sorter;

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

import java.io.IOException;
import java.util.Comparator;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.SorterTemplate;
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;

/**
 * Sorts documents of a given index by returning a permutation on the document
 * IDs.
 * <p><b>NOTE</b>: A {@link Sorter} implementation can be easily written from
 * a {@link DocComparator document comparator} by using the
 * {@link #sort(int, DocComparator)} helper method. This is especially useful
 * when documents are directly comparable by their field values.
 * @lucene.experimental
 */
public abstract class Sorter {

  /**
   * A permutation of doc IDs. For every document ID between <tt>0</tt> and
   * {@link IndexReader#maxDoc()}, <code>oldToNew(newToOld(docID))</code> must
   * return <code>docID</code>.
   */
  public static abstract class DocMap {

    /** Given a doc ID from the original index, return its ordinal in the
     *  sorted index. */
    public abstract int oldToNew(int docID);

    /** Given the ordinal of a doc ID, return its doc ID in the original index. */
    public abstract int newToOld(int docID);

    /** Return the number of documents in this map. This must be equal to the
     *  {@link AtomicReader#maxDoc() number of documents} of the
     *  {@link AtomicReader} which is sorted. */
    public abstract int size();

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
  public static abstract class DocComparator {

    /** Compare docID1 against docID2. The contract for the return value is the
     *  same as {@link Comparator#compare(Object, Object)}. */
    public abstract int compare(int docID1, int docID2);

  }

  /**
   * Sorts documents in reverse order. <b>NOTE</b>: This {@link Sorter} is not
   * idempotent. Sorting an {@link AtomicReader} once or twice will return two
   * different {@link AtomicReader} views. This {@link Sorter} should not be
   * used with {@link SortingMergePolicy}.
   */
  public static final Sorter REVERSE_DOCS = new Sorter() {
    @Override
    public DocMap sort(final AtomicReader reader) throws IOException {
      final int maxDoc = reader.maxDoc();
      return new DocMap() {
        @Override
        public int oldToNew(int docID) {
          return maxDoc - docID - 1;
        }
        @Override
        public int newToOld(int docID) {
          return maxDoc - docID - 1;
        }
        @Override
        public int size() {
          return maxDoc;
        }
      };
    }
    
    @Override
    public String getID() {
      return "ReverseDocs";
    }
  };
  
  private static final class DocValueSorterTemplate extends SorterTemplate {
    
    private final int[] docs;
    private final Sorter.DocComparator comparator;
    
    private int pivot;
    
    public DocValueSorterTemplate(int[] docs, Sorter.DocComparator comparator) {
      this.docs = docs;
      this.comparator = comparator;
    }
    
    @Override
    protected int compare(int i, int j) {
      return comparator.compare(docs[i], docs[j]);
    }
    
    @Override
    protected int comparePivot(int j) {
      return comparator.compare(pivot, docs[j]);
    }
    
    @Override
    protected void setPivot(int i) {
      pivot = docs[i];
    }
    
    @Override
    protected void swap(int i, int j) {
      int tmpDoc = docs[i];
      docs[i] = docs[j];
      docs[j] = tmpDoc;
    }
  }

  /** Computes the old-to-new permutation over the given comparator. */
  protected static Sorter.DocMap sort(final int maxDoc, DocComparator comparator) {
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
    
    SorterTemplate sorter = new DocValueSorterTemplate(docs, comparator);
    // It can be common to sort a reader, add docs, sort it again, ... and in
    // that case timSort can save a lot of time
    sorter.timSort(0, docs.length - 1); // docs is now the newToOld mapping

    // The reason why we use MonotonicAppendingLongBuffer here is that it
    // wastes very little memory if the index is in random order but can save
    // a lot of memory if the index is already "almost" sorted
    final MonotonicAppendingLongBuffer newToOld = new MonotonicAppendingLongBuffer();
    for (int i = 0; i < maxDoc; ++i) {
      newToOld.add(docs[i]);
    }

    for (int i = 0; i < maxDoc; ++i) {
      docs[(int) newToOld.get(i)] = i;
    } // docs is now the oldToNew mapping

    final MonotonicAppendingLongBuffer oldToNew = new MonotonicAppendingLongBuffer();
    for (int i = 0; i < maxDoc; ++i) {
      oldToNew.add(docs[i]);
    }
    
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
  public abstract DocMap sort(AtomicReader reader) throws IOException;

  /**
   * Returns the identifier of this {@link Sorter}.
   * <p>This identifier is similar to {@link Object#hashCode()} and should be
   * chosen so that two instances of this class that sort documents likewise
   * will have the same identifier. On the contrary, this identifier should be
   * different on different {@link Sorter sorters}.
   */
  public abstract String getID();

  @Override
  public String toString() {
    return getID();
  }
  
}
