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
import org.apache.lucene.util.PriorityQueue;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Holds updates of a single DocValues field, for a set of documents within one segment.
 * 
 * @lucene.experimental
 */
abstract class DocValuesFieldUpdates {
  
  protected static final int PAGE_SIZE = 1024;

  /**
   * An iterator over documents and their updated values. Only documents with
   * updates are returned by this iterator, and the documents are returned in
   * increasing order.
   */
  static abstract class Iterator {
    
    /**
     * Returns the next document which has an update, or
     * {@link DocIdSetIterator#NO_MORE_DOCS} if there are no more documents to
     * return.
     */
    abstract int nextDoc();
    
    /** Returns the current document this iterator is on. */
    abstract int doc();
    
    /**
     * Returns the value of the document returned from {@link #nextDoc()}. A
     * {@code null} value means that it was unset for this document.
     */
    abstract Object value();

    /** Returns delGen for this packet. */
    abstract long delGen();
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
          int cmp = Integer.compare(a.doc(), b.doc());
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
      private int doc;

      private boolean first = true;
      
      @Override
      public int nextDoc() {
        // TODO: can we do away with this first boolean?
        if (first == false) {
          // Advance all sub iterators past current doc
          while (true) {
            if (queue.size() == 0) {
              doc = NO_MORE_DOCS;
              break;
            }
            int newDoc = queue.top().doc();
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
        } else {
          doc = queue.top().doc();
          first = false;
        }
        return doc;
      }
        
      @Override
      public int doc() {
        return doc;
      }

      @Override
      public Object value() {
        return queue.top().value();
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

  public boolean getFinished() {
    return finished;
  }
  
  /**
   * Add an update to a document. For unsetting a value you should pass
   * {@code null}.
   */
  public abstract void add(int doc, Object value);
  
  /**
   * Returns an {@link Iterator} over the updated documents and their
   * values.
   */
  // TODO: also use this for merging, instead of having to write through to disk first
  public abstract Iterator iterator();

  /** Freezes internal data structures and sorts updates by docID for efficient iteration. */
  public abstract void finish();
  
  /** Returns true if this instance contains any updates. */
  public abstract boolean any();
  
  /** Returns approximate RAM bytes used. */
  public abstract long ramBytesUsed();

  public abstract int size();
}
