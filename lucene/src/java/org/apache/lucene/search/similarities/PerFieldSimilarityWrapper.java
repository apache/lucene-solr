package org.apache.lucene.search.similarities;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.Norm;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;

/**
 * Provides the ability to use a different {@link Similarity} for different fields.
 * <p>
 * Subclasses should implement {@link #get(String)} to return an appropriate
 * Similarity (for example, using field-specific parameter values) for the field.
 * 
 * @lucene.experimental
 */
public abstract class PerFieldSimilarityWrapper extends Similarity {

  @Override
  public final void computeNorm(FieldInvertState state, Norm norm) {
    get(state.getName()).computeNorm(state, norm);
  }

  @Override
  public final SimWeight computeWeight(float queryBoost, CollectionStatistics collectionStats, TermStatistics... termStats) {
    PerFieldSimWeight weight = new PerFieldSimWeight();
    weight.delegate = get(collectionStats.field());
    weight.delegateWeight = weight.delegate.computeWeight(queryBoost, collectionStats, termStats);
    return weight;
  }

  @Override
  public final ExactSimScorer exactSimScorer(SimWeight weight, AtomicReaderContext context) throws IOException {
    PerFieldSimWeight perFieldWeight = (PerFieldSimWeight) weight;
    return perFieldWeight.delegate.exactSimScorer(perFieldWeight.delegateWeight, context);
  }

  @Override
  public final SloppySimScorer sloppySimScorer(SimWeight weight, AtomicReaderContext context) throws IOException {
    PerFieldSimWeight perFieldWeight = (PerFieldSimWeight) weight;
    return perFieldWeight.delegate.sloppySimScorer(perFieldWeight.delegateWeight, context);
  }
  
  /** 
   * Returns a {@link Similarity} for scoring a field.
   */
  public abstract Similarity get(String name);
  
  static class PerFieldSimWeight extends SimWeight {
    Similarity delegate;
    SimWeight delegateWeight;
    
    @Override
    public float getValueForNormalization() {
      return delegateWeight.getValueForNormalization();
    }
    
    @Override
    public void normalize(float queryNorm, float topLevelBoost) {
      delegateWeight.normalize(queryNorm, topLevelBoost);
    }
  }
}
