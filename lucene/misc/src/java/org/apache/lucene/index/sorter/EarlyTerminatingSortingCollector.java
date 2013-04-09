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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TotalHitCountCollector;

/**
 * A {@link Collector} that early terminates collection of documents on a
 * per-segment basis, if the segment was sorted according to the given
 * {@link Sorter}.
 * 
 * <p>
 * <b>NOTE:</b> the {@link Collector} detects sorted segments according to
 * {@link SortingMergePolicy}, so it's best used in conjunction with it. Also,
 * it collects up to a specified num docs from each segment, and therefore is
 * mostly suitable for use in conjunction with collectors such as
 * {@link TopDocsCollector}, and not e.g. {@link TotalHitCountCollector}.
 * <p>
 * <b>NOTE</b>: If you wrap a {@link TopDocsCollector} that sorts in the same
 * order as the index order, the returned {@link TopDocsCollector#topDocs()}
 * will be correct. However the total of {@link TopDocsCollector#getTotalHits()
 * hit count} will be underestimated since not all matching documents will have
 * been collected.
 * <p>
 * <b>NOTE</b>: This {@link Collector} uses {@link Sorter#getID()} to detect
 * whether a segment was sorted with the same {@link Sorter} as the one given in
 * {@link #EarlyTerminatingSortingCollector(Collector, Sorter, int)}. This has
 * two implications:
 * <ul>
 * <li>if {@link Sorter#getID()} is not implemented correctly and returns
 * different identifiers for equivalent {@link Sorter}s, this collector will not
 * detect sorted segments,</li>
 * <li>if you suddenly change the {@link IndexWriter}'s
 * {@link SortingMergePolicy} to sort according to another criterion and if both
 * the old and the new {@link Sorter}s have the same identifier, this
 * {@link Collector} will incorrectly detect sorted segments.</li>
 * </ul>
 * 
 * @lucene.experimental
 */
public class EarlyTerminatingSortingCollector extends Collector {

  protected final Collector in;
  protected final Sorter sorter;
  protected final int numDocsToCollect;
  
  protected int segmentTotalCollect;
  protected boolean segmentSorted;

  private int numCollected;

  /**
   * Create a new {@link EarlyTerminatingSortingCollector} instance.
   * 
   * @param in
   *          the collector to wrap
   * @param sorter
   *          the same sorter as the one which is used by {@link IndexWriter}'s
   *          {@link SortingMergePolicy}
   * @param numDocsToCollect
   *          the number of documents to collect on each segment. When wrapping
   *          a {@link TopDocsCollector}, this number should be the number of
   *          hits.
   */
  public EarlyTerminatingSortingCollector(Collector in, Sorter sorter, int numDocsToCollect) {
    if (numDocsToCollect <= 0) {
      throw new IllegalStateException("numDocsToCollect must always be > 0, got " + segmentTotalCollect);
    }
    this.in = in;
    this.sorter = sorter;
    this.numDocsToCollect = numDocsToCollect;
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    in.setScorer(scorer);
  }

  @Override
  public void collect(int doc) throws IOException {
    in.collect(doc);
    if (++numCollected >= segmentTotalCollect) {
      throw new CollectionTerminatedException();
    }
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    in.setNextReader(context);
    segmentSorted = SortingMergePolicy.isSorted(context.reader(), sorter);
    segmentTotalCollect = segmentSorted ? numDocsToCollect : Integer.MAX_VALUE;
    numCollected = 0;
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return !segmentSorted && in.acceptsDocsOutOfOrder();
  }

}
