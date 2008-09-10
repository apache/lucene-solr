package org.apache.lucene.index;

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

import java.util.Collection;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockRAMDirectory;


public class TestOmitTf extends LuceneTestCase {
    
  public static class SimpleSimilarity extends Similarity {
    public float lengthNorm(String field, int numTerms) { return 1.0f; }
    public float queryNorm(float sumOfSquaredWeights) { return 1.0f; }
    
    public float tf(float freq) { return freq; }
    
    public float sloppyFreq(int distance) { return 2.0f; }
    public float idf(Collection terms, Searcher searcher) { return 1.0f; }
    public float idf(int docFreq, int numDocs) { return 1.0f; }
    public float coord(int overlap, int maxOverlap) { return 1.0f; }
  }


  // Tests whether the DocumentWriter correctly enable the
  // omitTf bit in the FieldInfo
  public void testOmitTf() throws Exception {
    Directory ram = new MockRAMDirectory();
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriter writer = new IndexWriter(ram, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
    Document d = new Document();
        
    // this field will have Tf
    Field f1 = new Field("f1", "This field has term freqs", Field.Store.NO, Field.Index.ANALYZED);
    d.add(f1);
       
    // this field will NOT have Tf
    Field f2 = new Field("f2", "This field has NO Tf in all docs", Field.Store.NO, Field.Index.ANALYZED);
    f2.setOmitTf(true);
    d.add(f2);
        
    writer.addDocument(d);
    writer.optimize();
    // now we add another document which has term freq for field f2 and not for f1 and verify if the SegmentMerger
    // keep things constant
    d = new Document();
        
    // Reverese
    f1.setOmitTf(true);
    d.add(f1);
        
    f2.setOmitTf(false);        
    d.add(f2);
        
    writer.addDocument(d);
    // force merge
    writer.optimize();
    // flush
    writer.close();
    _TestUtil.checkIndex(ram);

    // only one segment in the index, so we can cast to SegmentReader
    SegmentReader reader = (SegmentReader) IndexReader.open(ram);
    FieldInfos fi = reader.fieldInfos();
    assertTrue("OmitTf field bit should be set.", fi.fieldInfo("f1").omitTf);
    assertTrue("OmitTf field bit should be set.", fi.fieldInfo("f2").omitTf);
        
    reader.close();
    ram.close();
  }
 
  // Tests whether merging of docs that have different
  // omitTf for the same field works
  public void testMixedMerge() throws Exception {
    Directory ram = new MockRAMDirectory();
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriter writer = new IndexWriter(ram, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
    writer.setMaxBufferedDocs(3);
    writer.setMergeFactor(2);
    Document d = new Document();
        
    // this field will have Tf
    Field f1 = new Field("f1", "This field has term freqs", Field.Store.NO, Field.Index.ANALYZED);
    d.add(f1);
       
    // this field will NOT have Tf
    Field f2 = new Field("f2", "This field has NO Tf in all docs", Field.Store.NO, Field.Index.ANALYZED);
    f2.setOmitTf(true);
    d.add(f2);

    for(int i=0;i<30;i++)
      writer.addDocument(d);
        
    // now we add another document which has term freq for field f2 and not for f1 and verify if the SegmentMerger
    // keep things constant
    d = new Document();
        
    // Reverese
    f1.setOmitTf(true);
    d.add(f1);
        
    f2.setOmitTf(false);        
    d.add(f2);
        
    for(int i=0;i<30;i++)
      writer.addDocument(d);
        
    // force merge
    writer.optimize();
    // flush
    writer.close();

    _TestUtil.checkIndex(ram);

    // only one segment in the index, so we can cast to SegmentReader
    SegmentReader reader = (SegmentReader) IndexReader.open(ram);
    FieldInfos fi = reader.fieldInfos();
    assertTrue("OmitTf field bit should be set.", fi.fieldInfo("f1").omitTf);
    assertTrue("OmitTf field bit should be set.", fi.fieldInfo("f2").omitTf);
        
    reader.close();
    ram.close();
  }

  // Make sure first adding docs that do not omitTf for
  // field X, then adding docs that do omitTf for that same
  // field, 
  public void testMixedRAM() throws Exception {
    Directory ram = new MockRAMDirectory();
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriter writer = new IndexWriter(ram, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
    writer.setMaxBufferedDocs(10);
    writer.setMergeFactor(2);
    Document d = new Document();
        
    // this field will have Tf
    Field f1 = new Field("f1", "This field has term freqs", Field.Store.NO, Field.Index.ANALYZED);
    d.add(f1);
       
    // this field will NOT have Tf
    Field f2 = new Field("f2", "This field has NO Tf in all docs", Field.Store.NO, Field.Index.ANALYZED);
    d.add(f2);

    for(int i=0;i<5;i++)
      writer.addDocument(d);

    f2.setOmitTf(true);
        
    for(int i=0;i<20;i++)
      writer.addDocument(d);

    // force merge
    writer.optimize();

    // flush
    writer.close();

    _TestUtil.checkIndex(ram);

    // only one segment in the index, so we can cast to SegmentReader
    SegmentReader reader = (SegmentReader) IndexReader.open(ram);
    FieldInfos fi = reader.fieldInfos();
    assertTrue("OmitTf field bit should not be set.", !fi.fieldInfo("f1").omitTf);
    assertTrue("OmitTf field bit should be set.", fi.fieldInfo("f2").omitTf);
        
    reader.close();
    ram.close();
  }

  private void assertNoPrx(Directory dir) throws Throwable {
    final String[] files = dir.list();
    for(int i=0;i<files.length;i++)
      assertFalse(files[i].endsWith(".prx"));
  }

  // Verifies no *.prx exists when all fields omit term freq:
  public void testNoPrxFile() throws Throwable {
    Directory ram = new MockRAMDirectory();
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriter writer = new IndexWriter(ram, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
    writer.setMaxBufferedDocs(3);
    writer.setMergeFactor(2);
    writer.setUseCompoundFile(false);
    Document d = new Document();
        
    Field f1 = new Field("f1", "This field has term freqs", Field.Store.NO, Field.Index.ANALYZED);
    f1.setOmitTf(true);
    d.add(f1);

    for(int i=0;i<30;i++)
      writer.addDocument(d);

    writer.commit();

    assertNoPrx(ram);
        
    // force merge
    writer.optimize();
    // flush
    writer.close();

    assertNoPrx(ram);
    _TestUtil.checkIndex(ram);
    ram.close();
  }
 
  // Test scores with one field with Term Freqs and one without, otherwise with equal content 
  public void testBasic() throws Exception {
    Directory dir = new MockRAMDirectory();  
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriter writer = new IndexWriter(dir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
    writer.setMergeFactor(2);
    writer.setMaxBufferedDocs(2);
    writer.setSimilarity(new SimpleSimilarity());
        
        
    StringBuffer sb = new StringBuffer(265);
    String term = "term";
    for(int i = 0; i<30; i++){
      Document d = new Document();
      sb.append(term).append(" ");
      String content  = sb.toString();
      Field noTf = new Field("noTf", content + (i%2==0 ? "" : " notf"), Field.Store.NO, Field.Index.ANALYZED);
      noTf.setOmitTf(true);
      d.add(noTf);
          
      Field tf = new Field("tf", content + (i%2==0 ? " tf" : ""), Field.Store.NO, Field.Index.ANALYZED);
      d.add(tf);
          
      writer.addDocument(d);
      //System.out.println(d);
    }
        
    writer.optimize();
    // flush
    writer.close();
    _TestUtil.checkIndex(dir);

    /*
     * Verify the index
     */         
    Searcher searcher = new IndexSearcher(dir);
    searcher.setSimilarity(new SimpleSimilarity());
        
    Term a = new Term("noTf", term);
    Term b = new Term("tf", term);
    Term c = new Term("noTf", "notf");
    Term d = new Term("tf", "tf");
    TermQuery q1 = new TermQuery(a);
    TermQuery q2 = new TermQuery(b);
    TermQuery q3 = new TermQuery(c);
    TermQuery q4 = new TermQuery(d);

        
    searcher.search(q1,
                    new CountingHitCollector() {
                      public final void collect(int doc, float score) {
                        //System.out.println("Q1: Doc=" + doc + " score=" + score);
                        assertTrue(score==1.0f);
                        super.collect(doc, score);
                      }
                    });
    //System.out.println(CountingHitCollector.getCount());
        
        
    searcher.search(q2,
                    new CountingHitCollector() {
                      public final void collect(int doc, float score) {
                        //System.out.println("Q2: Doc=" + doc + " score=" + score);  
                        assertTrue(score==1.0f+doc);
                        super.collect(doc, score);
                      }
                    });
    //System.out.println(CountingHitCollector.getCount());
         
        
        
        
        
    searcher.search(q3,
                    new CountingHitCollector() {
                      public final void collect(int doc, float score) {
                        //System.out.println("Q1: Doc=" + doc + " score=" + score);
                        assertTrue(score==1.0f);
                        assertFalse(doc%2==0);
                        super.collect(doc, score);
                      }
                    });
    //System.out.println(CountingHitCollector.getCount());
        
        
    searcher.search(q4,
                    new CountingHitCollector() {
                      public final void collect(int doc, float score) {
                        //System.out.println("Q1: Doc=" + doc + " score=" + score);
                        assertTrue(score==1.0f);
                        assertTrue(doc%2==0);
                        super.collect(doc, score);
                      }
                    });
    //System.out.println(CountingHitCollector.getCount());
        
        
        
    BooleanQuery bq = new BooleanQuery();
    bq.add(q1,Occur.MUST);
    bq.add(q4,Occur.MUST);
        
    searcher.search(bq,
                    new CountingHitCollector() {
                      public final void collect(int doc, float score) {
                        //System.out.println("BQ: Doc=" + doc + " score=" + score);
                        super.collect(doc, score);
                      }
                    });
    assertTrue(15 == CountingHitCollector.getCount());
        
    searcher.close();     
    dir.close();
  }
     
  public static class CountingHitCollector extends HitCollector {
    static int count=0;
    static int sum=0;
    CountingHitCollector(){count=0;sum=0;}
    public void collect(int doc, float score) {
      count++;
      sum += doc;  // use it to avoid any possibility of being optimized away
    }

    public static int getCount() { return count; }
    public static int getSum() { return sum; }
  }
}
