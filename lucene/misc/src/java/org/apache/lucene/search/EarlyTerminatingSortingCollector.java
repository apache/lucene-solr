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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SortingMergePolicy;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TotalHitCountCollector;

/**
 * A {@link Collector} that early terminates collection of documents on a
 * per-segment basis, if the segment was sorted according to the given
 * {@link Sort}.
 *
 * <p>
 * <b>NOTE:</b> the {@code Collector} detects segments sorted according to a
 * {@link SortingMergePolicy}'s {@link Sort} and so it's best used in conjunction
 * with a {@link SortingMergePolicy}. Also,it collects up to a specified
 * {@code numDocsToCollect} from each segment, and therefore is mostly suitable
 * for use in conjunction with collectors such as {@link TopDocsCollector}, and
 * not e.g. {@link TotalHitCountCollector}.
 * <p>
 * <b>NOTE</b>: If you wrap a {@code TopDocsCollector} that sorts in the same
 * order as the index order, the returned {@link TopDocsCollector#topDocs() TopDocs}
 * will be correct. However the total of {@link TopDocsCollector#getTotalHits()
 * hit count} will be underestimated since not all matching documents will have
 * been collected.
 * <p>
 * <b>NOTE</b>: This {@code Collector} uses {@link Sort#toString()} to detect
 * whether a segment was sorted with the same {@code Sort}. This has
 * two implications:
 * <ul>
 * <li>if a custom comparator is not implemented correctly and returns
 * different identifiers for equivalent instances, this collector will not
 * detect sorted segments,</li>
 * <li>if you suddenly change the {@link IndexWriter}'s
 * {@code SortingMergePolicy} to sort according to another criterion and if both
 * the old and the new {@code Sort}s have the same identifier, this
 * {@code Collector} will incorrectly detect sorted segments.</li>
 * </ul>
 *
 * @lucene.experimental
 */
public class EarlyTerminatingSortingCollector extends FilterCollector {

  /** Returns whether collection can be early-terminated if it sorts with the
   *  provided {@link Sort} and if segments are merged with the provided
   *  {@link Sort}. */
  public static boolean canEarlyTerminate(Sort searchSort, Sort mergePolicySort) {
    final SortField[] fields1 = searchSort.getSort();
    final SortField[] fields2 = mergePolicySort.getSort();
    // early termination is possible if fields1 is a prefix of fields2
    if (fields1.length > fields2.length) {
      return false;
    }
    return Arrays.asList(fields1).equals(Arrays.asList(fields2).subList(0, fields1.length));
  }

  /** Sort used to sort the search results */
  protected final Sort sort;
  /** Number of documents to collect in each segment */
  protected final int numDocsToCollect;
  private final Sort mergePolicySort;
  private final AtomicBoolean terminatedEarly = new AtomicBoolean(false);

  /**
   * Create a new {@link EarlyTerminatingSortingCollector} instance.
   *
   * @param in
   *          the collector to wrap
   * @param sort
   *          the sort you are sorting the search results on
   * @param numDocsToCollect
   *          the number of documents to collect on each segment. When wrapping
   *          a {@link TopDocsCollector}, this number should be the number of
   *          hits.
   * @param mergePolicySort
   *          the sort your {@link SortingMergePolicy} uses
   * @throws IllegalArgumentException if the sort order doesn't allow for early
   *          termination with the given merge policy.
   */
  public EarlyTerminatingSortingCollector(Collector in, Sort sort, int numDocsToCollect, Sort mergePolicySort) {
    super(in);
    if (numDocsToCollect <= 0) {
      throw new IllegalArgumentException("numDocsToCollect must always be > 0, got " + numDocsToCollect);
    }
    if (canEarlyTerminate(sort, mergePolicySort) == false) {
      throw new IllegalStateException("Cannot early terminate with sort order " + sort + " if segments are sorted with " + mergePolicySort);
    }
    this.sort = sort;
    this.numDocsToCollect = numDocsToCollect;
    this.mergePolicySort = mergePolicySort;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    if (SortingMergePolicy.isSorted(context.reader(), mergePolicySort)) {
      // segment is sorted, can early-terminate
      return new FilterLeafCollector(super.getLeafCollector(context)) {
        private int numCollected;

        @Override
        public void collect(int doc) throws IOException {
          super.collect(doc);
          if (++numCollected >= numDocsToCollect) {
            terminatedEarly.set(true);
            throw new CollectionTerminatedException();
          }
        }

      };
    } else {
      return super.getLeafCollector(context);
    }
  }

  public boolean terminatedEarly() {
    return terminatedEarly.get();
  }

}
