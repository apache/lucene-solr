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
import java.util.List;

import org.apache.lucene.search.DocIdSetIterator; // javadocs
import org.apache.lucene.util.PriorityQueue;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** Utility class to help merging documents from sub-readers according to either simple
 *  concatenated (unsorted) order, or by a specified index-time sort, skipping
 *  deleted documents and remapping non-deleted documents. */

public abstract class DocIDMerger<T extends DocIDMerger.Sub> {

  /** Represents one sub-reader being merged */
  public static abstract class Sub {
    /** Mapped doc ID */
    public int mappedDocID;

    final MergeState.DocMap docMap;

    /** Sole constructor */
    public Sub(MergeState.DocMap docMap) {
      this.docMap = docMap;
    }

    /** Returns the next document ID from this sub reader, and {@link DocIdSetIterator#NO_MORE_DOCS} when done */
    public abstract int nextDoc() throws IOException;

    /**
     * Like {@link #nextDoc()} but skips over unmapped docs and returns the next mapped doc ID, or
     * {@link DocIdSetIterator#NO_MORE_DOCS} when exhausted. This method sets {@link #mappedDocID}
     * as a side effect.
     */
    public final int nextMappedDoc() throws IOException {
      while (true) {
        int doc = nextDoc();
        if (doc == NO_MORE_DOCS) {
          return this.mappedDocID = NO_MORE_DOCS;
        }
        int mappedDoc = docMap.get(doc);
        if (mappedDoc != -1) {
          return this.mappedDocID = mappedDoc;
        }
      }
    }
  }

  /** Construct this from the provided subs, specifying the maximum sub count */
  public static <T extends DocIDMerger.Sub> DocIDMerger<T> of(List<T> subs, int maxCount, boolean indexIsSorted) throws IOException {
    if (indexIsSorted && maxCount > 1) {
      return new SortedDocIDMerger<>(subs, maxCount);
    } else {
      return new SequentialDocIDMerger<>(subs);
    }
  }

  /** Construct this from the provided subs */
  public static <T extends DocIDMerger.Sub> DocIDMerger<T> of(List<T> subs, boolean indexIsSorted) throws IOException {
    return of(subs, subs.size(), indexIsSorted);
  }

  /** Reuse API, currently only used by postings during merge */
  public abstract void reset() throws IOException;

  /** Returns null when done.
   *  <b>NOTE:</b> after the iterator has exhausted you should not call this
   *  method, as it may result in unpredicted behavior. */
  public abstract T next() throws IOException;

  private DocIDMerger() {}

  private static class SequentialDocIDMerger<T extends DocIDMerger.Sub> extends DocIDMerger<T> {

    private final List<T> subs;
    private T current;
    private int nextIndex;

    private SequentialDocIDMerger(List<T> subs) throws IOException {
      this.subs = subs;
      reset();
    }

    @Override
    public void reset() throws IOException {
      if (subs.size() > 0) {
        current = subs.get(0);
        nextIndex = 1;
      } else {
        current = null;
        nextIndex = 0;
      }
    }

    @Override
    public T next() throws IOException {
      while (current.nextMappedDoc() == NO_MORE_DOCS) {
        if (nextIndex == subs.size()) {
          current = null;
          return null;
        }
        current = subs.get(nextIndex);
        nextIndex++;
      }
      return current;
    }

  }

  private static class SortedDocIDMerger<T extends DocIDMerger.Sub> extends DocIDMerger<T> {

    private final List<T> subs;
    private T current;
    private final PriorityQueue<T> queue;

    private SortedDocIDMerger(List<T> subs, int maxCount) throws IOException {
      if (maxCount <= 1) {
        throw new IllegalArgumentException();
      }
      this.subs = subs;
      queue = new PriorityQueue<T>(maxCount - 1) {
        @Override
        protected boolean lessThan(Sub a, Sub b) {
          assert a.mappedDocID != b.mappedDocID;
          return a.mappedDocID < b.mappedDocID;
        }
      };
      reset();
    }

    @Override
    public void reset() throws IOException {
      // caller may not have fully consumed the queue:
      queue.clear();
      current = null;
      boolean first = true;
      for (T sub : subs) {
        if (first) {
          // by setting mappedDocID = -1, this entry is guaranteed to be the top of the queue
          // so the first call to next() will advance it
          sub.mappedDocID = -1;
          current = sub;
          first = false;
        } else if (sub.nextMappedDoc() != NO_MORE_DOCS) {
          queue.add(sub);
        } // else all docs in this sub were deleted; do not add it to the queue!
      }
    }

    @Override
    public T next() throws IOException {
      int nextDoc = current.nextMappedDoc();
      if (nextDoc == NO_MORE_DOCS) {
        if (queue.size() == 0) {
          current = null;
        } else {
          current = queue.pop();
        }
      } else if (queue.size() > 0 && nextDoc > queue.top().mappedDocID) {
        T newCurrent = queue.top();
        queue.updateTop(current);
        current = newCurrent;
      }

      return current;
    }
  }

}
