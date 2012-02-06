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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.search.similarities.AfterEffect;
import org.apache.lucene.search.similarities.AfterEffectB;
import org.apache.lucene.search.similarities.AfterEffectL;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BasicModel;
import org.apache.lucene.search.similarities.BasicModelBE;
import org.apache.lucene.search.similarities.BasicModelD;
import org.apache.lucene.search.similarities.BasicModelG;
import org.apache.lucene.search.similarities.BasicModelIF;
import org.apache.lucene.search.similarities.BasicModelIn;
import org.apache.lucene.search.similarities.BasicModelIne;
import org.apache.lucene.search.similarities.BasicModelP;
import org.apache.lucene.search.similarities.DFRSimilarity;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Distribution;
import org.apache.lucene.search.similarities.DistributionLL;
import org.apache.lucene.search.similarities.DistributionSPL;
import org.apache.lucene.search.similarities.IBSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Lambda;
import org.apache.lucene.search.similarities.LambdaDF;
import org.apache.lucene.search.similarities.LambdaTTF;
import org.apache.lucene.search.similarities.Normalization;
import org.apache.lucene.search.similarities.NormalizationH1;
import org.apache.lucene.search.similarities.NormalizationH2;
import org.apache.lucene.search.similarities.NormalizationH3;
import org.apache.lucene.search.similarities.NormalizationZ;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;

public class RandomSimilarityProvider extends PerFieldSimilarityWrapper {
  final DefaultSimilarity defaultSim = new DefaultSimilarity();
  final List<Similarity> knownSims;
  Map<String,Similarity> previousMappings = new HashMap<String,Similarity>();
  final int perFieldSeed;
  final boolean shouldCoord;
  final boolean shouldQueryNorm;
  
  public RandomSimilarityProvider(Random random) {
    perFieldSeed = random.nextInt();
    shouldCoord = random.nextBoolean();
    shouldQueryNorm = random.nextBoolean();
    knownSims = new ArrayList<Similarity>(allSims);
    Collections.shuffle(knownSims, random);
  }
  
  @Override
  public float coord(int overlap, int maxOverlap) {
    if (shouldCoord) {
      return defaultSim.coord(overlap, maxOverlap);
    } else {
      return 1.0f;
    }
  }
  
  @Override
  public float queryNorm(float sumOfSquaredWeights) {
    if (shouldQueryNorm) {
      return defaultSim.queryNorm(sumOfSquaredWeights);
    } else {
      return 1.0f;
    }
  }
  
  @Override
  public synchronized Similarity get(String field) {
    assert field != null;
    Similarity sim = previousMappings.get(field);
    if (sim == null) {
      sim = knownSims.get(Math.abs(perFieldSeed ^ field.hashCode()) % knownSims.size());
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
  static List<Similarity> allSims;
  static {
    allSims = new ArrayList<Similarity>();
    allSims.add(new DefaultSimilarity());
    allSims.add(new BM25Similarity());
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
  }
  
  @Override
  public synchronized String toString() {
    return "RandomSimilarityProvider(queryNorm=" + shouldQueryNorm + ",coord=" + shouldCoord + "): " + previousMappings.toString();
  }
}
