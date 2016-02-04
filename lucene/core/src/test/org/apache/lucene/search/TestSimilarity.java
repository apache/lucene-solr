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


import org.apache.lucene.document.Field;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;

/** Similarity unit test.
 *
 *
 */
public class TestSimilarity extends LuceneTestCase {
  
  public static class SimpleSimilarity extends ClassicSimilarity {
    @Override
    public float queryNorm(float sumOfSquaredWeights) { return 1.0f; }
    @Override
    public float coord(int overlap, int maxOverlap) { return 1.0f; }
    @Override public float lengthNorm(FieldInvertState state) { return state.getBoost(); }
    @Override public float tf(float freq) { return freq; }
    @Override public float sloppyFreq(int distance) { return 2.0f; }
    @Override public float idf(long docFreq, long docCount) { return 1.0f; }
    @Override public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics[] stats) {
      return Explanation.match(1.0f, "Inexplicable"); 
    }
  }

  public void testSimilarity() throws Exception {
    Directory store = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), store, 
        newIndexWriterConfig(new MockAnalyzer(random()))
        .setSimilarity(new SimpleSimilarity()));
    
    Document d1 = new Document();
    d1.add(newTextField("field", "a c", Field.Store.YES));

    Document d2 = new Document();
    d2.add(newTextField("field", "a b c", Field.Store.YES));
    
    writer.addDocument(d1);
    writer.addDocument(d2);
    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(reader);
    searcher.setSimilarity(new SimpleSimilarity());

    Term a = new Term("field", "a");
    Term b = new Term("field", "b");
    Term c = new Term("field", "c");

    searcher.search(new TermQuery(b), new SimpleCollector() {
         private Scorer scorer;
         @Override
        public void setScorer(Scorer scorer) {
           this.scorer = scorer; 
         }
         @Override
        public final void collect(int doc) throws IOException {
           assertEquals(1.0f, scorer.score(), 0);
         }
         @Override
         public boolean needsScores() {
           return true;
         }
       });

    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(a), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(b), BooleanClause.Occur.SHOULD);
    //System.out.println(bq.toString("field"));
    searcher.search(bq.build(), new SimpleCollector() {
         private int base = 0;
         private Scorer scorer;
         @Override
        public void setScorer(Scorer scorer) {
           this.scorer = scorer; 
         }
         @Override
        public final void collect(int doc) throws IOException {
           //System.out.println("Doc=" + doc + " score=" + score);
           assertEquals((float)doc+base+1, scorer.score(), 0);
         }
         @Override
         protected void doSetNextReader(LeafReaderContext context) throws IOException {
           base = context.docBase;
         }
         @Override
         public boolean needsScores() {
           return true;
         }
       });

    PhraseQuery pq = new PhraseQuery(a.field(), a.bytes(), c.bytes());
    //System.out.println(pq.toString("field"));
    searcher.search(pq,
       new SimpleCollector() {
         private Scorer scorer;
         @Override
         public void setScorer(Scorer scorer) {
          this.scorer = scorer; 
         }
         @Override
         public final void collect(int doc) throws IOException {
           //System.out.println("Doc=" + doc + " score=" + score);
           assertEquals(1.0f, scorer.score(), 0);
         }
         @Override
         public boolean needsScores() {
           return true;
         }
       });

    pq = new PhraseQuery(2, a.field(), a.bytes(), c.bytes());
    //System.out.println(pq.toString("field"));
    searcher.search(pq, new SimpleCollector() {
      private Scorer scorer;
      @Override
      public void setScorer(Scorer scorer) {
        this.scorer = scorer; 
      }
      @Override
      public final void collect(int doc) throws IOException {
        //System.out.println("Doc=" + doc + " score=" + score);
        assertEquals(2.0f, scorer.score(), 0);
      }
      @Override
      public boolean needsScores() {
        return true;
      }
    });

    reader.close();
    store.close();
  }
}
