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
    public abstract int nextDoc();
  }

  /** Construct this from the provided subs, specifying the maximum sub count */
  public static <T extends DocIDMerger.Sub> DocIDMerger<T> of(List<T> subs, int maxCount, boolean indexIsSorted) {
    if (indexIsSorted && maxCount > 1) {
      return new SortedDocIDMerger<>(subs, maxCount);
    } else {
      return new SequentialDocIDMerger<>(subs);
    }
  }

  /** Construct this from the provided subs */
  public static <T extends DocIDMerger.Sub> DocIDMerger<T> of(List<T> subs, boolean indexIsSorted) {
    return of(subs, subs.size(), indexIsSorted);
  }

  /** Reuse API, currently only used by postings during merge */
  public abstract void reset();

  /** Returns null when done */
  public abstract T next();

  private DocIDMerger() {}

  private static class SequentialDocIDMerger<T extends DocIDMerger.Sub> extends DocIDMerger<T> {

    private final List<T> subs;
    private T current;
    private int nextIndex;

    private SequentialDocIDMerger(List<T> subs) {
      this.subs = subs;
      reset();
    }

    @Override
    public void reset() {
      if (subs.size() > 0) {
        current = subs.get(0);
        nextIndex = 1;
      } else {
        current = null;
        nextIndex = 0;
      }
    }

    @Override
    public T next() {
      if (current == null) {
        // NOTE: it's annoying that caller is allowed to call us again even after we returned null before
        return null;
      }
      while (true) {
        int docID = current.nextDoc();
        if (docID == NO_MORE_DOCS) {
          if (nextIndex == subs.size()) {
            current = null;
            return null;
          }
          current = subs.get(nextIndex);
          nextIndex++;
          continue;
        }

        int mappedDocID = current.docMap.get(docID);
        if (mappedDocID != -1) {
          current.mappedDocID = mappedDocID;
          return current;
        }
      }
    }

  }

  private static class SortedDocIDMerger<T extends DocIDMerger.Sub> extends DocIDMerger<T> {

    private final List<T> subs;
    private final PriorityQueue<T> queue;

    private SortedDocIDMerger(List<T> subs, int maxCount) {
      this.subs = subs;
      queue = new PriorityQueue<T>(maxCount) {
        @Override
        protected boolean lessThan(Sub a, Sub b) {
          assert a.mappedDocID != b.mappedDocID;
          return a.mappedDocID < b.mappedDocID;
        }
      };
      reset();
    }

    @Override
    public void reset() {
      // caller may not have fully consumed the queue:
      queue.clear();
      boolean first = true;
      for(T sub : subs) {
        if (first) {
          // by setting mappedDocID = -1, this entry is guaranteed to be the top of the queue
          // so the first call to next() will advance it
          sub.mappedDocID = -1;
          first = false;
        } else {
          int mappedDocID;
          while (true) {
            int docID = sub.nextDoc();
            if (docID == NO_MORE_DOCS) {
              mappedDocID = NO_MORE_DOCS;
              break;
            }
            mappedDocID = sub.docMap.get(docID);
            if (mappedDocID != -1) {
              break;
            }
          }
          if (mappedDocID == NO_MORE_DOCS) {
            // all docs in this sub were deleted; do not add it to the queue!
            continue;
          }
          sub.mappedDocID = mappedDocID;
        }
        queue.add(sub);
      }
    }

    @Override
    public T next() {
      T top = queue.top();
      if (top == null) {
        // NOTE: it's annoying that caller is allowed to call us again even after we returned null before
        return null;
      }

      while (true) {
        int docID = top.nextDoc();
        if (docID == NO_MORE_DOCS) {
          queue.pop();
          top = queue.top();
          break;
        }
        int mappedDocID = top.docMap.get(docID);
        if (mappedDocID == -1) {
          // doc was deleted
          continue;
        } else {
          top.mappedDocID = mappedDocID;
          top = queue.updateTop();
          break;
        }
      }

      return top;
    }
  }

}
