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

import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/** Similarity unit test.
 *
 *
 * @version $Revision$
 */
public class TestSimilarity extends LuceneTestCase {
  public TestSimilarity(String name) {
    super(name);
  }
  
  public static class SimpleSimilarity extends Similarity {
    public float lengthNorm(String field, int numTerms) { return 1.0f; }
    public float queryNorm(float sumOfSquaredWeights) { return 1.0f; }
    public float tf(float freq) { return freq; }
    public float sloppyFreq(int distance) { return 2.0f; }
    public float idf(Collection terms, Searcher searcher) { return 1.0f; }
    public float idf(int docFreq, int numDocs) { return 1.0f; }
    public float coord(int overlap, int maxOverlap) { return 1.0f; }
  }

  public void testSimilarity() throws Exception {
    RAMDirectory store = new RAMDirectory();
    IndexWriter writer = new IndexWriter(store, new SimpleAnalyzer(), true, 
                                         IndexWriter.MaxFieldLength.LIMITED);
    writer.setSimilarity(new SimpleSimilarity());
    
    Document d1 = new Document();
    d1.add(new Field("field", "a c", Field.Store.YES, Field.Index.ANALYZED));

    Document d2 = new Document();
    d2.add(new Field("field", "a b c", Field.Store.YES, Field.Index.ANALYZED));
    
    writer.addDocument(d1);
    writer.addDocument(d2);
    writer.optimize();
    writer.close();

    Searcher searcher = new IndexSearcher(store);
    searcher.setSimilarity(new SimpleSimilarity());

    Term a = new Term("field", "a");
    Term b = new Term("field", "b");
    Term c = new Term("field", "c");

    searcher.search(new TermQuery(b), new Collector() {
         private Scorer scorer;
         public void setScorer(Scorer scorer) throws IOException {
           this.scorer = scorer; 
         }
         public final void collect(int doc) throws IOException {
           assertTrue(scorer.score() == 1.0f);
         }
         public void setNextReader(IndexReader reader, int docBase) {}
         public boolean acceptsDocsOutOfOrder() {
           return true;
         }
       });

    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(a), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(b), BooleanClause.Occur.SHOULD);
    //System.out.println(bq.toString("field"));
    searcher.search(bq, new Collector() {
         private int base = 0;
         private Scorer scorer;
         public void setScorer(Scorer scorer) throws IOException {
           this.scorer = scorer; 
         }
         public final void collect(int doc) throws IOException {
           //System.out.println("Doc=" + doc + " score=" + score);
           assertTrue(scorer.score() == (float)doc+base+1);
         }
         public void setNextReader(IndexReader reader, int docBase) {
           base = docBase;
         }
         public boolean acceptsDocsOutOfOrder() {
           return true;
         }
       });

    PhraseQuery pq = new PhraseQuery();
    pq.add(a);
    pq.add(c);
    //System.out.println(pq.toString("field"));
    searcher.search
      (pq,
       new Collector() {
        private Scorer scorer;
        public void setScorer(Scorer scorer) throws IOException {
          this.scorer = scorer; 
        }
         public final void collect(int doc) throws IOException {
           //System.out.println("Doc=" + doc + " score=" + score);
           assertTrue(scorer.score() == 1.0f);
         }
         public void setNextReader(IndexReader reader, int docBase) {}
         public boolean acceptsDocsOutOfOrder() {
           return true;
         }
       });

    pq.setSlop(2);
    //System.out.println(pq.toString("field"));
    searcher.search(pq, new Collector() {
        private Scorer scorer;
        public void setScorer(Scorer scorer) throws IOException {
          this.scorer = scorer; 
        }
         public final void collect(int doc) throws IOException {
           //System.out.println("Doc=" + doc + " score=" + score);
           assertTrue(scorer.score() == 2.0f);
         }
         public void setNextReader(IndexReader reader, int docBase) {}
         public boolean acceptsDocsOutOfOrder() {
           return true;
         }
       });
  }
}
