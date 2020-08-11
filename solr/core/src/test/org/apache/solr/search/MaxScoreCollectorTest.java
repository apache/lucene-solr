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
package org.apache.solr.search;

import java.io.IOException;

import org.apache.lucene.search.Scorable;
import org.apache.solr.SolrTestCase;

public class MaxScoreCollectorTest extends SolrTestCase {
  
  public void test() throws IOException {
    MaxScoreCollector collector = new MaxScoreCollector();
    DummyScorer scorer = new DummyScorer();
    collector.setScorer(scorer);
    assertEquals(Float.NaN, collector.getMaxScore(), 0f);
    assertEquals(0f, scorer.minCompetitiveScore, 0f);
    
    collector.collect(0);
    assertEquals(Float.MIN_VALUE, collector.getMaxScore(), 0f);
    assertEquals(0f, scorer.minCompetitiveScore, 0f);
    
    scorer.nextScore = 1f;
    collector.collect(0);
    assertEquals(1f, collector.getMaxScore(), 0f);
    assertEquals(Math.nextUp(1f), scorer.minCompetitiveScore, 0f);

    scorer.nextScore = 0f;
    collector.collect(0);
    assertEquals(1f, collector.getMaxScore(), 0f);
    assertEquals(Math.nextUp(1f), scorer.minCompetitiveScore, 0f);
    
    scorer.nextScore = -1f;
    collector.collect(0);
    assertEquals(1f, collector.getMaxScore(), 0f);
    assertEquals(Math.nextUp(1f), scorer.minCompetitiveScore, 0f);
    
    scorer.nextScore = Float.MAX_VALUE;
    collector.collect(0);
    assertEquals(Float.MAX_VALUE, collector.getMaxScore(), 0f);
    assertEquals(Float.POSITIVE_INFINITY, scorer.minCompetitiveScore, 0f);
    
    scorer.nextScore = Float.POSITIVE_INFINITY;
    collector.collect(0);
    assertEquals(Float.POSITIVE_INFINITY, collector.getMaxScore(), 0f);
    assertEquals(Float.POSITIVE_INFINITY, scorer.minCompetitiveScore, 0f);
    
    
    scorer.nextScore = Float.NaN;
    collector.collect(0);
    assertEquals(Float.NaN, collector.getMaxScore(), 0f);
    assertEquals(Float.NaN, scorer.minCompetitiveScore, 0f);
  }
  
  private final static class DummyScorer extends Scorable {
    
    float nextScore = 0f;
    float minCompetitiveScore = 0f;
    
    @Override
    public float score() throws IOException {
      return nextScore;
    }

    @Override
    public int docID() {
      return 0;
    }
    
    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
      this.minCompetitiveScore = minScore;
    }
    
  }

}
