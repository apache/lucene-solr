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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Similarity implementation that randomizes Similarity implementations
 * per-field.
 * <p>
 * The choices are 'sticky', so the selected algorithm is always used
 * for the same field.
 */
public class RandomSimilarity extends PerFieldSimilarityWrapper {
  final List<Similarity> knownSims;
  final Map<String,Similarity> previousMappings = new HashMap<>();
  final int perFieldSeed;
  
  public RandomSimilarity(Random random) {
    super(new ClassicSimilarity() {
      final int coordType = random.nextInt(3); // 0 = no coord, 1 = coord, 2 = crazy coord
      final boolean shouldQueryNorm = random.nextBoolean();

      @Override
      public float coord(int overlap, int maxOverlap) {
        if (coordType == 0) {
          return 1.0f;
        } else if (coordType == 1) {
          return super.coord(overlap, maxOverlap);
        } else {
          return overlap / ((float)maxOverlap + 1);
        }
      }
      
      @Override
      public float queryNorm(float sumOfSquaredWeights) {
        if (shouldQueryNorm) {
          return super.queryNorm(sumOfSquaredWeights);
        } else {
          return 1.0f;
        }
      }
      
      @Override
      public synchronized String toString() {
        final String coordMethod;
        if (coordType == 0) {
          coordMethod = "no";
        } else if (coordType == 1) {
          coordMethod = "yes";
        } else {
          coordMethod = "crazy";
        }
        return "queryNorm=" + shouldQueryNorm + ",coord=" + coordMethod;
      }

    });
    perFieldSeed = random.nextInt();
    knownSims = new ArrayList<>(allSims);
    Collections.shuffle(knownSims, random);
  }
  
  @Override
  public synchronized Similarity get(String field) {
    assert field != null;
    Similarity sim = previousMappings.get(field);
    if (sim == null) {
      sim = knownSims.get(Math.max(0, Math.abs(perFieldSeed ^ field.hashCode())) % knownSims.size());
      previousMappings.put(field, sim);
    }
    return sim;
  }
  
  // all the similarities that we rotate through
  /** The DFR basic models to test. */
  static BasicModel[] BASIC_MODELS = {
    /* TODO: enable new BasicModelBE(), */ /* TODO: enable new BasicModelD(), */ new BasicModelG(),
    new BasicModelIF(), new BasicModelIn(), new BasicModelIne(),
    /* TODO: enable new BasicModelP() */
  };
  /** The DFR aftereffects to test. */
  static AfterEffect[] AFTER_EFFECTS = {
    new AfterEffectB(), new AfterEffectL(), new AfterEffect.NoAfterEffect()
  };
  /** The DFR normalizations to test. */
  static Normalization[] NORMALIZATIONS = {
    new NormalizationH1(), new NormalizationH2(),
    new NormalizationH3(), new NormalizationZ()
    // TODO: if we enable NoNormalization, we have to deal with
    // a couple tests (e.g. TestDocBoost, TestSort) that expect length normalization
    // new Normalization.NoNormalization()
  };
  /** The distributions for IB. */
  static Distribution[] DISTRIBUTIONS = {
    new DistributionLL(), new DistributionSPL()
  };
  /** Lambdas for IB. */
  static Lambda[] LAMBDAS = {
    new LambdaDF(), new LambdaTTF()
  };
  /** Independence measures for DFI */
  static Independence[] INDEPENDENCE_MEASURES = {
    new IndependenceStandardized(), new IndependenceSaturated(), new IndependenceChiSquared() 
  };
  static List<Similarity> allSims;
  static {
    allSims = new ArrayList<>();
    allSims.add(new ClassicSimilarity());
    allSims.add(new BM25Similarity());
    // We cannot do this, because this similarity behaves in "non-traditional" ways:
    // allSims.add(new BooleanSimilarity());
    for (BasicModel basicModel : BASIC_MODELS) {
      for (AfterEffect afterEffect : AFTER_EFFECTS) {
        for (Normalization normalization : NORMALIZATIONS) {
          allSims.add(new DFRSimilarity(basicModel, afterEffect, normalization));
        }
      }
    }
    for (Distribution distribution : DISTRIBUTIONS) {
      for (Lambda lambda : LAMBDAS) {
        for (Normalization normalization : NORMALIZATIONS) {
          allSims.add(new IBSimilarity(distribution, lambda, normalization));
        }
      }
    }
    /* TODO: enable Dirichlet 
    allSims.add(new LMDirichletSimilarity()); */
    allSims.add(new LMJelinekMercerSimilarity(0.1f));
    allSims.add(new LMJelinekMercerSimilarity(0.7f));
    for (Independence independence : INDEPENDENCE_MEASURES) {
      allSims.add(new DFISimilarity(independence));
    }
  }
  
  @Override
  public synchronized String toString() {
    return "RandomSimilarity(" + defaultSim + "): " + previousMappings.toString();
  }
}
