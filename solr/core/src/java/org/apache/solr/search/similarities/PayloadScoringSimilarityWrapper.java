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

package org.apache.solr.search.similarities;

import java.io.IOException;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.util.PayloadDecoder;

/**
 * The computation Lucene's PayloadScoreQuery uses is SimScorer#computePayloadFactor.
 * This wrapper delegates to a main similarity except for this one method.
 */
public class PayloadScoringSimilarityWrapper extends Similarity {
  private Similarity delegate;
  private PayloadDecoder decoder;

  public PayloadScoringSimilarityWrapper(Similarity delegate, PayloadDecoder decoder) {
    this.delegate = delegate;
    this.decoder = decoder;
  }

  @Override
  public String toString() {
    return "PayloadScoring(" + delegate.toString() + ", decoder=" + decoder.toString() + ")";
  }

  @Override
  public SimWeight computeWeight(CollectionStatistics collectionStats, TermStatistics... termStats) {
    return delegate.computeWeight(collectionStats, termStats);
  }

  @Override
  public long computeNorm(FieldInvertState state) {
    return delegate.computeNorm(state);
  }

  @Override
  public SimScorer simScorer(SimWeight weight, LeafReaderContext context) throws IOException {
    final SimScorer simScorer = delegate.simScorer(weight,context);
    SimScorer payloadSimScorer = new SimScorer() {
      @Override
      public float score(int doc, float freq) {
        return simScorer.score(doc,freq);
      }

      @Override
      public float computeSlopFactor(int distance) {
        return simScorer.computeSlopFactor(distance);
      }

      @Override
      public float computePayloadFactor(int doc, int start, int end, BytesRef payload) {
        return decoder.decode(doc, start, end, payload);
      }
    };

    return payloadSimScorer;
  }
}
