package org.apache.lucene.search;

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

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.util.Bits;

/** 
 * Helper class that adds some extra checks to ensure correct
 * usage of {@code IndexSearcher} and {@code Weight}.
 * TODO: Extend this by more checks, that's just a start.
 */
public class AssertingIndexSearcher extends IndexSearcher {
  final Random random;
  public  AssertingIndexSearcher(Random random, IndexReader r) {
    super(r);
    this.random = new Random(random.nextLong());
  }
  
  public  AssertingIndexSearcher(Random random, IndexReaderContext context) {
    super(context);
    this.random = new Random(random.nextLong());
  }
  
  public  AssertingIndexSearcher(Random random, IndexReader r, ExecutorService ex) {
    super(r, ex);
    this.random = new Random(random.nextLong());
  }
  
  public  AssertingIndexSearcher(Random random, IndexReaderContext context, ExecutorService ex) {
    super(context, ex);
    this.random = new Random(random.nextLong());
  }
  
  /** Ensures, that the returned {@code Weight} is not normalized again, which may produce wrong scores. */
  @Override
  public Weight createNormalizedWeight(Query query) throws IOException {
    final Weight w = super.createNormalizedWeight(query);
    return new Weight() {
      @Override
      public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
        return w.explain(context, doc);
      }

      @Override
      public Query getQuery() {
        return w.getQuery();
      }

      @Override
      public void normalize(float norm, float topLevelBoost) {
        throw new IllegalStateException("Weight already normalized.");
      }

      @Override
      public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder,
          boolean topScorer, Bits acceptDocs) throws IOException {
        Scorer scorer = w.scorer(context, scoreDocsInOrder, topScorer, acceptDocs);
        if (scorer != null) {
          // check that scorer obeys disi contract for docID() before next()/advance
          try {
            int docid = scorer.docID();
            assert docid == -1 || docid == DocIdSetIterator.NO_MORE_DOCS;
          } catch (UnsupportedOperationException ignored) {
            // from a top-level BS1
            assert topScorer;
          }
        }
        return scorer;
      }

      @Override
      public float getValueForNormalization() throws IOException {
        throw new IllegalStateException("Weight already normalized.");
      }

      @Override
      public boolean scoresDocsOutOfOrder() {
        return w.scoresDocsOutOfOrder();
      }
    };
  }

  @Override
  protected Query wrapFilter(Query query, Filter filter) {
    if (random.nextBoolean())
      return super.wrapFilter(query, filter);
    return (filter == null) ? query : new FilteredQuery(query, filter) {
      @Override
      protected boolean useRandomAccess(Bits bits, int firstFilterDoc) {
        return random.nextBoolean();
      }
    };
  }
}
