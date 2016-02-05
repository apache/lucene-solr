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
package org.apache.lucene.search;


import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestScoreCachingWrappingScorer extends LuceneTestCase {

  private static final class SimpleScorer extends Scorer {
    private int idx = 0;
    private int doc = -1;
    
    public SimpleScorer(Weight weight) {
      super(weight);
    }
    
    @Override public float score() {
      // advance idx on purpose, so that consecutive calls to score will get
      // different results. This is to emulate computation of a score. If
      // ScoreCachingWrappingScorer is used, this should not be called more than
      // once per document.
      return idx == scores.length ? Float.NaN : scores[idx++];
    }
    
    @Override public int freq() throws IOException {
      return 1;
    }

    @Override public int docID() { return doc; }

    @Override
    public DocIdSetIterator iterator() {
      return new DocIdSetIterator() {
        @Override public int docID() { return doc; }

        @Override public int nextDoc() {
          return ++doc < scores.length ? doc : NO_MORE_DOCS;
        }
        
        @Override public int advance(int target) {
          doc = target;
          return doc < scores.length ? doc : NO_MORE_DOCS;
        }

        @Override
        public long cost() {
          return scores.length;
        }
      };
    }
  }
  
  private static final class ScoreCachingCollector extends SimpleCollector {

    private int idx = 0;
    private Scorer scorer;
    float[] mscores;
    
    public ScoreCachingCollector(int numToCollect) {
      mscores = new float[numToCollect];
    }
    
    @Override public void collect(int doc) throws IOException {
      // just a sanity check to avoid IOOB.
      if (idx == mscores.length) {
        return; 
      }
      
      // just call score() a couple of times and record the score.
      mscores[idx] = scorer.score();
      mscores[idx] = scorer.score();
      mscores[idx] = scorer.score();
      ++idx;
    }

    @Override public void setScorer(Scorer scorer) {
      this.scorer = new ScoreCachingWrappingScorer(scorer);
    }
    
    @Override
    public boolean needsScores() {
      return true;
    }

  }

  private static final float[] scores = new float[] { 0.7767749f, 1.7839992f,
      8.9925785f, 7.9608946f, 0.07948637f, 2.6356435f, 7.4950366f, 7.1490803f,
      8.108544f, 4.961808f, 2.2423935f, 7.285586f, 4.6699767f };
  
  public void testGetScores() throws Exception {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    writer.commit();
    IndexReader ir = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(ir);
    Weight fake = new TermQuery(new Term("fake", "weight")).createWeight(searcher, true);
    Scorer s = new SimpleScorer(fake);
    ScoreCachingCollector scc = new ScoreCachingCollector(scores.length);
    scc.setScorer(s);
    
    // We need to iterate on the scorer so that its doc() advances.
    int doc;
    while ((doc = s.iterator().nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      scc.collect(doc);
    }
    
    for (int i = 0; i < scores.length; i++) {
      assertEquals(scores[i], scc.mscores[i], 0f);
    }
    ir.close();
    directory.close();
  }
  
}
