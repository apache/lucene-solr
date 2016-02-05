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


import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestPositiveScoresOnlyCollector extends LuceneTestCase {

  private static final class SimpleScorer extends Scorer {
    private int idx = -1;
    
    public SimpleScorer(Weight weight) {
      super(weight);
    }
    
    @Override public float score() {
      return idx == scores.length ? Float.NaN : scores[idx];
    }
    
    @Override public int freq() {
      return 1;
    }

    @Override public int docID() { return idx; }

    @Override
    public DocIdSetIterator iterator() {
      return new DocIdSetIterator() {
        @Override
        public int docID() {
          return idx;
        }

        @Override public int nextDoc() {
          return ++idx != scores.length ? idx : NO_MORE_DOCS;
        }
        
        @Override public int advance(int target) {
          idx = target;
          return idx < scores.length ? idx : NO_MORE_DOCS;
        }

        @Override
        public long cost() {
          return scores.length;
        } 
      };
    }
  }

  // The scores must have positive as well as negative values
  private static final float[] scores = new float[] { 0.7767749f, -1.7839992f,
      8.9925785f, 7.9608946f, -0.07948637f, 2.6356435f, 7.4950366f, 7.1490803f,
      -8.108544f, 4.961808f, 2.2423935f, -7.285586f, 4.6699767f };

  public void testNegativeScores() throws Exception {
  
    // The Top*Collectors previously filtered out documents with <= scores. This
    // behavior has changed. This test checks that if PositiveOnlyScoresFilter
    // wraps one of these collectors, documents with <= 0 scores are indeed
    // filtered.
    
    int numPositiveScores = 0;
    for (int i = 0; i < scores.length; i++) {
      if (scores[i] > 0) {
        ++numPositiveScores;
      }
    }
    
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    writer.addDocument(new Document());
    writer.commit();
    IndexReader ir = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(ir);
    Weight fake = new TermQuery(new Term("fake", "weight")).createWeight(searcher, true);
    Scorer s = new SimpleScorer(fake);
    TopDocsCollector<ScoreDoc> tdc = TopScoreDocCollector.create(scores.length);
    Collector c = new PositiveScoresOnlyCollector(tdc);
    LeafCollector ac = c.getLeafCollector(ir.leaves().get(0));
    ac.setScorer(s);
    while (s.iterator().nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      ac.collect(0);
    }
    TopDocs td = tdc.topDocs();
    ScoreDoc[] sd = td.scoreDocs;
    assertEquals(numPositiveScores, td.totalHits);
    for (int i = 0; i < sd.length; i++) {
      assertTrue("only positive scores should return: " + sd[i].score, sd[i].score > 0);
    }
    ir.close();
    directory.close();
  }
  
}
