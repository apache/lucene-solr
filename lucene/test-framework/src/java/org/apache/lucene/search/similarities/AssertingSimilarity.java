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
package org.apache.lucene.search.similarities;

import java.io.IOException;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;

/** wraps a similarity with checks for testing */
public class AssertingSimilarity extends Similarity {
  private final Similarity delegate;

  public AssertingSimilarity(Similarity delegate) {
    this.delegate = delegate;
  }

  @Override
  public long computeNorm(FieldInvertState state) {
    assert state != null;
    assert state.getLength() > 0;
    assert state.getPosition() >= 0;
    assert state.getOffset() >= 0;
    assert state.getMaxTermFrequency() >= 0; // TODO: seems to be 0 for omitTFAP? 
    assert state.getMaxTermFrequency() <= state.getLength();
    assert state.getNumOverlap() >= 0;
    assert state.getNumOverlap() < state.getLength();
    assert state.getUniqueTermCount() > 0;
    assert state.getUniqueTermCount() <= state.getLength();
    return delegate.computeNorm(state);
  }

  @Override
  public SimWeight computeWeight(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
    assert boost >= 0;
    assert collectionStats != null;
    assert termStats.length > 0;
    for (TermStatistics term : termStats) {
      assert term != null;
    }
    // TODO: check that TermStats is in bounds with respect to collection? e.g. docFreq <= maxDoc
    SimWeight weight = delegate.computeWeight(boost, collectionStats, termStats);
    assert weight != null;
    return new AssertingWeight(weight, boost);
  }
  
  static class AssertingWeight extends SimWeight {
    final SimWeight delegate;
    final float boost;
    
    AssertingWeight(SimWeight delegate, float boost) {
      this.delegate = delegate;
      this.boost = boost;
    }
  }

  @Override
  public SimScorer simScorer(SimWeight weight, LeafReaderContext context) throws IOException {
    assert weight != null;
    assert context != null;
    AssertingWeight assertingWeight = (AssertingWeight)weight;
    SimScorer delegateScorer = delegate.simScorer(assertingWeight.delegate, context);
    assert delegateScorer != null;

    return new SimScorer() {
      @Override
      public float score(int doc, float freq) throws IOException {
        // doc in bounds
        assert doc >= 0;
        assert doc < context.reader().maxDoc();
        // freq in bounds
        assert Float.isFinite(freq);
        assert freq > 0;
        // result in bounds
        float score = delegateScorer.score(doc, freq);
        assert Float.isFinite(score);
        assert score <= maxScore(freq);
        assert score >= 0;
        return score;
      }

      @Override
      public float maxScore(float maxFreq) {
        float maxScore = delegateScorer.maxScore(maxFreq);
        assert Float.isNaN(maxScore) == false;
        return maxScore;
      }

      @Override
      public Explanation explain(int doc, Explanation freq) throws IOException {
        // doc in bounds
        assert doc >= 0;
        assert doc < context.reader().maxDoc();
        // freq in bounds 
        assert freq != null;
        assert Float.isFinite(freq.getValue());
        // result in bounds
        Explanation explanation = delegateScorer.explain(doc, freq);
        assert explanation != null;
        assert Float.isFinite(explanation.getValue());
        // result matches score exactly
        assert explanation.getValue() == delegateScorer.score(doc, freq.getValue());
        return explanation;
      }
    };
  }

  @Override
  public String toString() {
    return "Asserting(" + super.toString() + ")";
  }

}
