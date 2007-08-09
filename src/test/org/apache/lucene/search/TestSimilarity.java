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

import junit.framework.TestCase;

import java.util.Collection;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/** Similarity unit test.
 *
 *
 * @version $Revision$
 */
public class TestSimilarity extends TestCase {
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
    IndexWriter writer = new IndexWriter(store, new SimpleAnalyzer(), true);
    writer.setSimilarity(new SimpleSimilarity());
    
    Document d1 = new Document();
    d1.add(new Field("field", "a c", Field.Store.YES, Field.Index.TOKENIZED));

    Document d2 = new Document();
    d2.add(new Field("field", "a b c", Field.Store.YES, Field.Index.TOKENIZED));
    
    writer.addDocument(d1);
    writer.addDocument(d2);
    writer.optimize();
    writer.close();

    Searcher searcher = new IndexSearcher(store);
    searcher.setSimilarity(new SimpleSimilarity());

    Term a = new Term("field", "a");
    Term b = new Term("field", "b");
    Term c = new Term("field", "c");

    searcher.search
      (new TermQuery(b),
       new HitCollector() {
         public final void collect(int doc, float score) {
           assertTrue(score == 1.0f);
         }
       });

    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(a), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(b), BooleanClause.Occur.SHOULD);
    //System.out.println(bq.toString("field"));
    searcher.search
      (bq,
       new HitCollector() {
         public final void collect(int doc, float score) {
           //System.out.println("Doc=" + doc + " score=" + score);
           assertTrue(score == (float)doc+1);
         }
       });

    PhraseQuery pq = new PhraseQuery();
    pq.add(a);
    pq.add(c);
    //System.out.println(pq.toString("field"));
    searcher.search
      (pq,
       new HitCollector() {
         public final void collect(int doc, float score) {
           //System.out.println("Doc=" + doc + " score=" + score);
           assertTrue(score == 1.0f);
         }
       });

    pq.setSlop(2);
    //System.out.println(pq.toString("field"));
    searcher.search
      (pq,
       new HitCollector() {
         public final void collect(int doc, float score) {
           //System.out.println("Doc=" + doc + " score=" + score);
           assertTrue(score == 2.0f);
         }
       });
  }
}
