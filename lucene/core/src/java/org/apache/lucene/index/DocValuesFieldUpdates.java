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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PagedMutable;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Holds updates of a single DocValues field, for a set of documents within one segment.
 * 
 * @lucene.experimental
 */
abstract class DocValuesFieldUpdates implements Accountable {
  
  protected static final int PAGE_SIZE = 1024;
  private static final long HAS_VALUE_MASK = 1;
  private static final long HAS_NO_VALUE_MASK = 0;
  private static final int SHIFT = 1; // we use the first bit of each value to mark if the doc has a value or not

  /**
   * An iterator over documents and their updated values. Only documents with
   * updates are returned by this iterator, and the documents are returned in
   * increasing order.
   */
  static abstract class Iterator extends DocValuesIterator {

    @Override
    public final boolean advanceExact(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public final int advance(int target) {
      throw new UnsupportedOperationException();
    }
    @Override
    public final long cost() {
      throw new UnsupportedOperationException();
    }

    @Override
    public abstract int nextDoc(); // no IOException

    /**
     * Returns a long value for the current document if this iterator is a long iterator.
     */
    abstract long longValue();

    /**
     * Returns a binary value for the current document if this iterator is a binary value iterator.
     */
    abstract BytesRef binaryValue();

    /** Returns delGen for this packet. */
    abstract long delGen();

    /**
     * Returns true if this doc has a value
     */
    abstract boolean hasValue();

    /**
     * Wraps the given iterator as a BinaryDocValues instance.
     */
    static BinaryDocValues asBinaryDocValues(Iterator iterator) {
      return new BinaryDocValues() {
        @Override
        public int docID() {
          return iterator.docID();
        }
        @Override
        public BytesRef binaryValue() {
          return iterator.binaryValue();
        }
        @Override
        public boolean advanceExact(int target) {
          return iterator.advanceExact(target);
        }
        @Override
        public int nextDoc() {
          return iterator.nextDoc();
        }
        @Override
        public int advance(int target) {
          return iterator.advance(target);
        }
        @Override
        public long cost() {
          return iterator.cost();
        }
      };
    }
    /**
     * Wraps the given iterator as a NumericDocValues instance.
     */
    static NumericDocValues asNumericDocValues(Iterator iterator) {
      return new NumericDocValues() {
        @Override
        public long longValue() {
          return iterator.longValue();
        }
        @Override
        public boolean advanceExact(int target) {
          throw new UnsupportedOperationException();
        }
        @Override
        public int docID() {
          return iterator.docID();
        }
        @Override
        public int nextDoc() {
          return iterator.nextDoc();
        }
        @Override
        public int advance(int target) {
          return iterator.advance(target);
        }
        @Override
        public long cost() {
          return iterator.cost();
        }
      };
    }
  }



  /** Merge-sorts multiple iterators, one per delGen, favoring the largest delGen that has updates for a given docID. */
  public static Iterator mergedIterator(Iterator[] subs) {

    if (subs.length == 1) {
      return subs[0];
    }

    PriorityQueue<Iterator> queue = new PriorityQueue<Iterator>(subs.length) {
        @Override
        protected boolean lessThan(Iterator a, Iterator b) {
          // sort by smaller docID
          int cmp = Integer.compare(a.docID(), b.docID());
          if (cmp == 0) {
            // then by larger delGen
            cmp = Long.compare(b.delGen(), a.delGen());

            // delGens are unique across our subs:
            assert cmp != 0;
          }

          return cmp < 0;
        }
      };

    for (Iterator sub : subs) {
      if (sub.nextDoc() != NO_MORE_DOCS) {
        queue.add(sub);
      }
    }

    if (queue.size() == 0) {
      return null;
    }

    return new Iterator() {
      private int doc = -1;
      @Override
      public int nextDoc() {
          // Advance all sub iterators past current doc
          while (true) {
            if (queue.size() == 0) {
              doc = NO_MORE_DOCS;
              break;
            }
            int newDoc = queue.top().docID();
            if (newDoc != doc) {
              assert newDoc > doc: "doc=" + doc + " newDoc=" + newDoc;
              doc = newDoc;
              break;
            }
            if (queue.top().nextDoc() == NO_MORE_DOCS) {
              queue.pop();
            } else {
              queue.updateTop();
            }
          }
        return doc;
      }

      @Override
      public int docID() {
        return doc;
      }

      @Override
      long longValue() {
        return queue.top().longValue();
      }

      @Override
      BytesRef binaryValue() {
        return queue.top().binaryValue();
      }

      @Override
      public long delGen() {
        throw new UnsupportedOperationException();
      }

      @Override
      boolean hasValue() {
        return queue.top().hasValue();
      }
    };
  }

  final String field;
  final DocValuesType type;
  final long delGen;
  private final int bitsPerValue;
  private boolean finished;
  protected final int maxDoc;
  protected PagedMutable docs;
  protected int size;

  protected DocValuesFieldUpdates(int maxDoc, long delGen, String field, DocValuesType type) {
    this.maxDoc = maxDoc;
    this.delGen = delGen;
    this.field = field;
    if (type == null) {
      throw new NullPointerException("DocValuesType must not be null");
    }
    this.type = type;
    bitsPerValue = PackedInts.bitsRequired(maxDoc - 1) + SHIFT;
    docs = new PagedMutable(1, PAGE_SIZE, bitsPerValue, PackedInts.DEFAULT);
  }

  final boolean getFinished() {
    return finished;
  }
  
  abstract void add(int doc, long value);

  abstract void add(int doc, BytesRef value);

  /**
   * Adds the value for the given docID.
   * This method prevents conditional calls to {@link Iterator#longValue()} or {@link Iterator#binaryValue()}
   * since the implementation knows if it's a long value iterator or binary value
   */
  abstract void add(int docId, Iterator iterator);

  /**
   * Returns an {@link Iterator} over the updated documents and their
   * values.
   */
  // TODO: also use this for merging, instead of having to write through to disk first
  abstract Iterator iterator();

  /** Freezes internal data structures and sorts updates by docID for efficient iteration. */
  final synchronized void finish() {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;
    // shrink wrap
    if (size < docs.size()) {
      resize(size);
    }
    if (size > 0) {
      // We need a stable sort but InPlaceMergeSorter performs lots of swaps
      // which hurts performance due to all the packed ints we are using.
      // Another option would be TimSorter, but it needs additional API (copy to
      // temp storage, compare with item in temp storage, etc.) so we instead
      // use quicksort and record ords of each update to guarantee stability.
      final PackedInts.Mutable ords = PackedInts.getMutable(size, PackedInts.bitsRequired(size - 1), PackedInts.DEFAULT);
      for (int i = 0; i < size; ++i) {
        ords.set(i, i);
      }
      new IntroSorter() {
        @Override
        protected void swap(int i, int j) {
          final long tmpOrd = ords.get(i);
          ords.set(i, ords.get(j));
          ords.set(j, tmpOrd);

          DocValuesFieldUpdates.this.swap(i, j);
        }

        @Override
        protected int compare(int i, int j) {
          // increasing docID order:
          // NOTE: we can have ties here, when the same docID was updated in the same segment, in which case we rely on sort being
          // stable and preserving original order so the last update to that docID wins
          int cmp = Long.compare(docs.get(i)>>>1, docs.get(j)>>>1);
          if (cmp == 0) {
            cmp = (int) (ords.get(i) - ords.get(j));
          }
          return cmp;
        }

        long pivotDoc;
        int pivotOrd;

        @Override
        protected void setPivot(int i) {
          pivotDoc = docs.get(i) >>> 1;
          pivotOrd = (int) ords.get(i);
        }

        @Override
        protected int comparePivot(int j) {
          int cmp = Long.compare(pivotDoc, docs.get(j) >>> 1);
          if (cmp == 0) {
            cmp = pivotOrd - (int) ords.get(j);
          }
          return cmp;
        }
      }.sort(0, size);
    }
  }

  /** Returns true if this instance contains any updates. */
  synchronized boolean any() {
    return size > 0;
  }

  synchronized final int size() {
    return size;
  }

  /**
   * Adds an update that resets the documents value.
   * @param doc the doc to update
   */
  synchronized void reset(int doc) {
    addInternal(doc, HAS_NO_VALUE_MASK);
  }
  final synchronized int add(int doc) {
    return addInternal(doc, HAS_VALUE_MASK);
  }

  private synchronized int addInternal(int doc, long hasValueMask) {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    assert doc < maxDoc;

    // TODO: if the Sorter interface changes to take long indexes, we can remove that limitation
    if (size == Integer.MAX_VALUE) {
      throw new IllegalStateException("cannot support more than Integer.MAX_VALUE doc/value entries");
    }
    // grow the structures to have room for more elements
    if (docs.size() == size) {
      grow(size+1);
    }
    docs.set(size, (((long)doc) << SHIFT) | hasValueMask);
    ++size;
    return size-1;
  }

  protected void swap(int i, int j) {
    long tmpDoc = docs.get(j);
    docs.set(j, docs.get(i));
    docs.set(i, tmpDoc);
  }

  protected void grow(int size) {
    docs = docs.grow(size);
  }

  protected void resize(int size) {
    docs = docs.resize(size);
  }

  protected final void ensureFinished() {
    if (finished == false) {
      throw new IllegalStateException("call finish first");
    }
  }
  @Override
  public long ramBytesUsed() {
    return docs.ramBytesUsed()
        + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
        + 2 * Integer.BYTES
        + 2 + Long.BYTES
        + RamUsageEstimator.NUM_BYTES_OBJECT_REF;
  }

  // TODO: can't this just be NumericDocValues now?  avoid boxing the long value...
  protected abstract static class AbstractIterator extends DocValuesFieldUpdates.Iterator {
    private final int size;
    private final PagedMutable docs;
    private long idx = 0; // long so we don't overflow if size == Integer.MAX_VALUE
    private int doc = -1;
    private final long delGen;
    private boolean hasValue;

    AbstractIterator(int size, PagedMutable docs, long delGen) {
      this.size = size;
      this.docs = docs;
      this.delGen = delGen;
    }

    @Override
    public final int nextDoc() {
      if (idx >= size) {
        return doc = DocIdSetIterator.NO_MORE_DOCS;
      }
      long longDoc = docs.get(idx);
      ++idx;
      for (; idx < size; idx++) {
        // scan forward to last update to this doc
        final long nextLongDoc = docs.get(idx);
        if ((longDoc >>> 1) != (nextLongDoc >>> 1)) {
          break;
        }
        longDoc = nextLongDoc;
      }
      hasValue = (longDoc & HAS_VALUE_MASK) >  0;
      if (hasValue) {
        set(idx - 1);
      }
      doc = (int)(longDoc >> SHIFT);
      return doc;
    }

    /**
     * Called when the iterator moved to the next document
     * @param idx the internal index to set the value to
     */
    protected abstract void set(long idx);

    @Override
    public final int docID() {
      return doc;
    }

    @Override
    final long delGen() {
      return delGen;
    }

    @Override
    final boolean hasValue() {
      return hasValue;
    }
  }

  static abstract class SingleValueDocValuesFieldUpdates extends DocValuesFieldUpdates {
    private final BitSet bitSet;
    private BitSet hasNoValue;
    private boolean hasAtLeastOneValue;
    protected SingleValueDocValuesFieldUpdates(int maxDoc, long delGen, String field, DocValuesType type) {
      super(maxDoc, delGen, field, type);
      this.bitSet = new SparseFixedBitSet(maxDoc);
    }

    @Override
    void add(int doc, long value) {
      assert longValue() == value;
      bitSet.set(doc);
      this.hasAtLeastOneValue = true;
      if (hasNoValue != null) {
        hasNoValue.clear(doc);
      }
    }

    @Override
    void add(int doc, BytesRef value) {
      assert binaryValue().equals(value);
      bitSet.set(doc);
      this.hasAtLeastOneValue = true;
      if (hasNoValue != null) {
        hasNoValue.clear(doc);
      }
    }

    @Override
    synchronized void reset(int doc) {
      bitSet.set(doc);
      this.hasAtLeastOneValue = true;
      if (hasNoValue == null) {
        hasNoValue = new SparseFixedBitSet(maxDoc);
      }
      hasNoValue.set(doc);
    }

    @Override
    void add(int docId, Iterator iterator) {
      throw new UnsupportedOperationException();
    }

    protected abstract BytesRef binaryValue();
    
    protected abstract long longValue();

    @Override
    synchronized boolean any() {
      return super.any() || hasAtLeastOneValue;
    }

    @Override
    public long ramBytesUsed() {
      return super.ramBytesUsed() + bitSet.ramBytesUsed() + (hasNoValue == null ? 0 : hasNoValue.ramBytesUsed());
    }

    @Override
    Iterator iterator() {
      BitSetIterator iterator = new BitSetIterator(bitSet, maxDoc);
      return new DocValuesFieldUpdates.Iterator() {

        @Override
        public int docID() {
          return iterator.docID();
        }

        @Override
        public int nextDoc() {
          return iterator.nextDoc();
        }

        @Override
        long longValue() {
          return SingleValueDocValuesFieldUpdates.this.longValue();
        }

        @Override
        BytesRef binaryValue() {
          return SingleValueDocValuesFieldUpdates.this.binaryValue();
        }

        @Override
        long delGen() {
          return delGen;
        }

        @Override
        boolean hasValue() {
          if (hasNoValue != null) {
            return hasNoValue.get(docID()) == false;
          }
          return true;
        }
      };
    }
  }
}
