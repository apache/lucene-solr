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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;

/**
 * Provides the ability to use a different {@link Similarity} for different fields.
 * <p>
 * Subclasses should implement {@link #get(String)} to return an appropriate
 * Similarity (for example, using field-specific parameter values) for the field.
 * <p>
 * For Lucene 6, you should pass a default similarity that is used for all non
 * field-specific methods. From Lucene 7 on, this is no longer required.
 * 
 * @lucene.experimental
 */
public abstract class PerFieldSimilarityWrapper extends Similarity {
  
  /** Default similarity used for query norm and coordination factors. */
  protected final Similarity defaultSim;
  
  /**
   * Constructor taking a default similarity for all non-field specific calculations.
   * @param defaultSim is used for all non field-specific calculations, like
   * {@link #queryNorm(float)} and {@link #coord(int, int)}.
   */
  public PerFieldSimilarityWrapper(Similarity defaultSim) {
    this.defaultSim = defaultSim;
  }
  
  /**
   * Backwards compatibility constructor for 6.x series that creates a per-field
   * similarity where all non field-specific methods return a constant (1).
   * <p>
   * From Lucene 7 on, this will get the default again, because coordination
   * factors and query normalization will be removed.
   * @deprecated specify a default similarity for non field-specific calculations.
   */
  @Deprecated
  public PerFieldSimilarityWrapper() {
    // a fake similarity that is only used to return the default of 1 for queryNorm and coord.
    this(new Similarity() {
      @Override
      public long computeNorm(FieldInvertState state) {
        throw new AssertionError();
      }

      @Override
      public SimWeight computeWeight(CollectionStatistics collectionStats, TermStatistics... termStats) {
        throw new AssertionError();
      }

      @Override
      public SimScorer simScorer(SimWeight weight, LeafReaderContext context) throws IOException {
        throw new AssertionError();
      }
    });
  }

  @Override
  public final long computeNorm(FieldInvertState state) {
    return get(state.getName()).computeNorm(state);
  }

  @Override
  public final SimWeight computeWeight(CollectionStatistics collectionStats, TermStatistics... termStats) {
    PerFieldSimWeight weight = new PerFieldSimWeight();
    weight.delegate = get(collectionStats.field());
    weight.delegateWeight = weight.delegate.computeWeight(collectionStats, termStats);
    return weight;
  }

  @Override
  public final SimScorer simScorer(SimWeight weight, LeafReaderContext context) throws IOException {
    PerFieldSimWeight perFieldWeight = (PerFieldSimWeight) weight;
    return perFieldWeight.delegate.simScorer(perFieldWeight.delegateWeight, context);
  }
  
  @Override
  public final float coord(int overlap, int maxOverlap) {
    return defaultSim.coord(overlap, maxOverlap);
  }

  @Override
  public final float queryNorm(float valueForNormalization) {
    return defaultSim.queryNorm(valueForNormalization);
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
    public void normalize(float queryNorm, float boost) {
      delegateWeight.normalize(queryNorm, boost);
    }
  }
}
