package org.apache.lucene.search.join;

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
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

/**
 * An {@link IndexSearcher} to use in conjunction with
 * {@link ToParentBlockJoinCollector}.
 */
public class ToParentBlockJoinIndexSearcher extends IndexSearcher {

  /** Creates a searcher searching the provided index. Search on individual
   *  segments will be run in the provided {@link ExecutorService}.
   * @see IndexSearcher#IndexSearcher(IndexReader, ExecutorService) */
  public ToParentBlockJoinIndexSearcher(IndexReader r, ExecutorService executor) {
    super(r, executor);
  }

  /** Creates a searcher searching the provided index.
   * @see IndexSearcher#IndexSearcher(IndexReader) */
  public ToParentBlockJoinIndexSearcher(IndexReader r) {
    super(r);
  }

  @Override
  protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
    for (LeafReaderContext ctx : leaves) { // search each subreader
      // we force the use of Scorer (not BulkScorer) to make sure
      // that the scorer passed to LeafCollector.setScorer supports
      // Scorer.getChildren
      Scorer scorer = weight.scorer(ctx);
      if (scorer != null) {
        final LeafCollector leafCollector = collector.getLeafCollector(ctx);
        leafCollector.setScorer(scorer);
        final Bits liveDocs = ctx.reader().getLiveDocs();
        final DocIdSetIterator it = scorer.iterator();
        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
          if (liveDocs == null || liveDocs.get(doc)) {
            leafCollector.collect(doc);
          }
        }
      }
    }
  }

}
