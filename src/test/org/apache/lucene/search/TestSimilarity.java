package org.apache.lucene.search;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import junit.framework.TestCase;

import java.util.Vector;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/** Similarity unit test.
 *
 * @author Doug Cutting
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
    public float idf(Vector terms, Searcher searcher) { return 1.0f; }
    public float idf(int docFreq, int numDocs) { return 1.0f; }
    public float coord(int overlap, int maxOverlap) { return 1.0f; }
  }

  public void testSimilarity() throws Exception {
    RAMDirectory store = new RAMDirectory();
    IndexWriter writer = new IndexWriter(store, new SimpleAnalyzer(), true);
    writer.setSimilarity(new SimpleSimilarity());
    
    Document d1 = new Document();
    d1.add(Field.Text("field", "a c"));

    Document d2 = new Document();
    d2.add(Field.Text("field", "a b c"));
    
    writer.addDocument(d1);
    writer.addDocument(d2);
    writer.optimize();
    writer.close();

    final float[] scores = new float[4];

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
    bq.add(new TermQuery(a), false, false);
    bq.add(new TermQuery(b), false, false);
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
