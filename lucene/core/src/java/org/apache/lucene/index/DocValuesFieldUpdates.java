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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;
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
    bitsPerValue = PackedInts.bitsRequired(maxDoc - 1);
    docs = new PagedMutable(1, PAGE_SIZE, bitsPerValue, PackedInts.COMPACT);
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
    new InPlaceMergeSorter() {
      @Override
      protected void swap(int i, int j) {
        DocValuesFieldUpdates.this.swap(i, j);
      }

      @Override
      protected int compare(int i, int j) {
        // increasing docID order:
        // NOTE: we can have ties here, when the same docID was updated in the same segment, in which case we rely on sort being
        // stable and preserving original order so the last update to that docID wins
        return Long.compare(docs.get(i), docs.get(j));
      }
    }.sort(0, size);
  }
  
  /** Returns true if this instance contains any updates. */
  synchronized final boolean any() {
    return size > 0;
  }

  synchronized final int size() {
    return size;
  }

  final synchronized int add(int doc) {
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

    docs.set(size, doc);
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
      doc = (int) docs.get(idx);
      ++idx;
      while (idx < size && docs.get(idx) == doc) {
        // scan forward to last update to this doc
        ++idx;
      }
      set(idx-1);
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
  }
}
