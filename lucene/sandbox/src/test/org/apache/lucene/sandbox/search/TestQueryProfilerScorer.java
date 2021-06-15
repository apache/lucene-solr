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

package org.apache.lucene.sandbox.search;

import java.io.IOException;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.LuceneTestCase;

public class TestQueryProfilerScorer extends LuceneTestCase {

  private static class FakeScorer extends Scorer {

    public float maxScore, minCompetitiveScore;

    protected FakeScorer(Weight weight) {
      super(weight);
    }

    @Override
    public DocIdSetIterator iterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return maxScore;
    }

    @Override
    public float score() throws IOException {
      return 1f;
    }

    @Override
    public int docID() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setMinCompetitiveScore(float minScore) {
      this.minCompetitiveScore = minScore;
    }
  }

  public void testPropagateMinCompetitiveScore() throws IOException {
    Query query = new MatchAllDocsQuery();
    Weight weight =
        query.createWeight(new IndexSearcher(new MultiReader()), ScoreMode.TOP_SCORES, 1f);
    FakeScorer fakeScorer = new FakeScorer(weight);
    QueryProfilerBreakdown profile = new QueryProfilerBreakdown();
    QueryProfilerWeight queryProfilerWeight = new QueryProfilerWeight(query, weight, profile);
    QueryProfilerScorer queryProfilerScorer =
        new QueryProfilerScorer(queryProfilerWeight, fakeScorer, profile);
    queryProfilerScorer.setMinCompetitiveScore(0.42f);
    assertEquals(0.42f, fakeScorer.minCompetitiveScore, 0f);
  }

  public void testPropagateMaxScore() throws IOException {
    Query query = new MatchAllDocsQuery();
    Weight weight =
        query.createWeight(new IndexSearcher(new MultiReader()), ScoreMode.TOP_SCORES, 1f);
    FakeScorer fakeScorer = new FakeScorer(weight);
    QueryProfilerBreakdown profile = new QueryProfilerBreakdown();
    QueryProfilerWeight queryProfilerWeight = new QueryProfilerWeight(query, weight, profile);
    QueryProfilerScorer queryProfilerScorer =
        new QueryProfilerScorer(queryProfilerWeight, fakeScorer, profile);
    queryProfilerScorer.setMinCompetitiveScore(0.42f);
    fakeScorer.maxScore = 42f;
    assertEquals(42f, queryProfilerScorer.getMaxScore(DocIdSetIterator.NO_MORE_DOCS), 0f);
  }
}
