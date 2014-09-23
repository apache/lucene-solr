package org.apache.solr.search;

/**
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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.FilterCollector;
/**
 * <p>
 *  A wrapper {@link Collector} that throws {@link EarlyTerminatingCollectorException})
 *  once a specified maximum number of documents are collected.
 * </p>
 */
public class EarlyTerminatingCollector extends FilterCollector {

  private final int maxDocsToCollect;

  private int numCollected = 0;
  private int prevReaderCumulativeSize = 0;
  private int currentReaderSize = 0;

  /**
   * <p>
   *  Wraps a {@link Collector}, throwing {@link EarlyTerminatingCollectorException}
   *  once the specified maximum is reached.
   * </p>
   * @param delegate - the Collector to wrap.
   * @param maxDocsToCollect - the maximum number of documents to Collect
   *
   */
  public EarlyTerminatingCollector(Collector delegate, int maxDocsToCollect) {
    super(delegate);
    assert 0 < maxDocsToCollect;
    assert null != delegate;

    this.maxDocsToCollect = maxDocsToCollect;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context)
      throws IOException {
    prevReaderCumulativeSize += currentReaderSize; // not current any more
    currentReaderSize = context.reader().maxDoc() - 1;

    return new FilterLeafCollector(super.getLeafCollector(context)) {

      /**
       * This collector requires that docs be collected in order, otherwise
       * the computed number of scanned docs in the resulting
       * {@link EarlyTerminatingCollectorException} will be meaningless.
       */
      @Override
      public boolean acceptsDocsOutOfOrder() {
        return false;
      }

      @Override
      public void collect(int doc) throws IOException {
        super.collect(doc);
        numCollected++;
        if (maxDocsToCollect <= numCollected) {
          throw new EarlyTerminatingCollectorException
            (numCollected, prevReaderCumulativeSize + (doc + 1));
        }
      }

    };
  }

}
