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

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;

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
  protected boolean finished;
  protected final int maxDoc;
    
  protected DocValuesFieldUpdates(int maxDoc, long delGen, String field, DocValuesType type) {
    this.maxDoc = maxDoc;
    this.delGen = delGen;
    this.field = field;
    if (type == null) {
      throw new NullPointerException("DocValuesType must not be null");
    }
    this.type = type;
  }

  boolean getFinished() {
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
  abstract void finish();
  
  /** Returns true if this instance contains any updates. */
  abstract boolean any();
  
  abstract int size();

}
