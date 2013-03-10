package org.apache.lucene.facet.sampling;

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.facet.search.ScoredDocIDs;
import org.apache.lucene.facet.search.ScoredDocIDsIterator;
import org.apache.lucene.facet.util.ScoredDocIdsUtils;

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

/**
 * Simple random sampler
 */
public class RandomSampler extends Sampler {
  
  private final Random random;

  public RandomSampler() {
    super();
    this.random = new Random();
  }

  public RandomSampler(SamplingParams params, Random random) throws IllegalArgumentException {
    super(params);
    this.random = random;
  }

  @Override
  protected SampleResult createSample(ScoredDocIDs docids, int actualSize, int sampleSetSize) throws IOException {
    final int[] sample = new int[sampleSetSize];
    final int maxStep = (actualSize * 2 ) / sampleSetSize; //floor
    int remaining = actualSize;
    ScoredDocIDsIterator it = docids.iterator();
    int i = 0;
    // select sample docs with random skipStep, make sure to leave sufficient #docs for selection after last skip
    while (i<sample.length && remaining>(sampleSetSize-maxStep-i)) {
      int skipStep = 1 + random.nextInt(maxStep);
      // Skip over 'skipStep' documents
      for (int j=0; j<skipStep; j++) {
        it.next();
        -- remaining;
      }
      sample[i++] = it.getDocID();
    }
    // Add leftover documents to the sample set
    while (i<sample.length) {
      it.next();
      sample[i++] = it.getDocID();
    }
    ScoredDocIDs sampleRes = ScoredDocIdsUtils.createScoredDocIDsSubset(docids, sample);
    SampleResult res = new SampleResult(sampleRes, sampleSetSize/(double)actualSize);
    return res;
  }
  
}
