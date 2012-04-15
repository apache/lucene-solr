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

import java.io.IOException;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;


public class TestOmitTf extends LuceneTestCase {
  
  public static class SimpleSimilarity extends TFIDFSimilarity {
    public float queryNorm(float sumOfSquaredWeights) { return 1.0f; }
    public float coord(int overlap, int maxOverlap) { return 1.0f; }
    @Override public void computeNorm(FieldInvertState state, Norm norm) { norm.setByte(encodeNormValue(state.getBoost())); }
    @Override public float tf(float freq) { return freq; }
    @Override public float sloppyFreq(int distance) { return 2.0f; }
    @Override public float idf(long docFreq, long numDocs) { return 1.0f; }
    @Override public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics[] termStats) {
      return new Explanation(1.0f, "Inexplicable");
    }
    @Override public float scorePayload(int doc, int start, int end, BytesRef payload) { return 1.0f; }
  }

  private static final FieldType omitType = new FieldType(TextField.TYPE_UNSTORED);
  private static final FieldType normalType = new FieldType(TextField.TYPE_UNSTORED);
  
  static {
    omitType.setIndexOptions(IndexOptions.DOCS_ONLY);
  }

  // Tests whether the DocumentWriter correctly enable the
  // omitTermFreqAndPositions bit in the FieldInfo
  public void testOmitTermFreqAndPositions() throws Exception {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(ram, newIndexWriterConfig( TEST_VERSION_CURRENT, analyzer));
    Document d = new Document();
        
    // this field will have Tf
    Field f1 = newField("f1", "This field has term freqs", normalType);
    d.add(f1);
       
    // this field will NOT have Tf
    Field f2 = newField("f2", "This field has NO Tf in all docs", omitType);
    d.add(f2);
        
    writer.addDocument(d);
    writer.forceMerge(1);
    // now we add another document which has term freq for field f2 and not for f1 and verify if the SegmentMerger
    // keep things constant
    d = new Document();
        
    // Reverse
    f1 = newField("f1", "This field has term freqs", omitType);
    d.add(f1);
        
    f2 = newField("f2", "This field has NO Tf in all docs", normalType);     
    d.add(f2);
        
    writer.addDocument(d);

    // force merge
    writer.forceMerge(1);
    // flush
    writer.close();

    SegmentReader reader = getOnlySegmentReader(IndexReader.open(ram));
    FieldInfos fi = reader.getFieldInfos();
    assertEquals("OmitTermFreqAndPositions field bit should be set.", IndexOptions.DOCS_ONLY, fi.fieldInfo("f1").indexOptions);
    assertEquals("OmitTermFreqAndPositions field bit should be set.", IndexOptions.DOCS_ONLY, fi.fieldInfo("f2").indexOptions);
        
    reader.close();
    ram.close();
  }
 
  // Tests whether merging of docs that have different
  // omitTermFreqAndPositions for the same field works
  public void testMixedMerge() throws Exception {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(
        ram,
        newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer).
            setMaxBufferedDocs(3).
            setMergePolicy(newLogMergePolicy(2))
    );
    Document d = new Document();
        
    // this field will have Tf
    Field f1 = newField("f1", "This field has term freqs", normalType);
    d.add(f1);
       
    // this field will NOT have Tf
    Field f2 = newField("f2", "This field has NO Tf in all docs", omitType);
    d.add(f2);

    for(int i=0;i<30;i++)
      writer.addDocument(d);
        
    // now we add another document which has term freq for field f2 and not for f1 and verify if the SegmentMerger
    // keep things constant
    d = new Document();
        
    // Reverese
    f1 = newField("f1", "This field has term freqs", omitType);
    d.add(f1);
        
    f2 = newField("f2", "This field has NO Tf in all docs", normalType);     
    d.add(f2);
        
    for(int i=0;i<30;i++)
      writer.addDocument(d);
        
    // force merge
    writer.forceMerge(1);
    // flush
    writer.close();

    SegmentReader reader = getOnlySegmentReader(IndexReader.open(ram));
    FieldInfos fi = reader.getFieldInfos();
    assertEquals("OmitTermFreqAndPositions field bit should be set.", IndexOptions.DOCS_ONLY, fi.fieldInfo("f1").indexOptions);
    assertEquals("OmitTermFreqAndPositions field bit should be set.", IndexOptions.DOCS_ONLY, fi.fieldInfo("f2").indexOptions);
        
    reader.close();
    ram.close();
  }

  // Make sure first adding docs that do not omitTermFreqAndPositions for
  // field X, then adding docs that do omitTermFreqAndPositions for that same
  // field, 
  public void testMixedRAM() throws Exception {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(
        ram,
        newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer).
            setMaxBufferedDocs(10).
            setMergePolicy(newLogMergePolicy(2))
    );
    Document d = new Document();
        
    // this field will have Tf
    Field f1 = newField("f1", "This field has term freqs", normalType);
    d.add(f1);
       
    // this field will NOT have Tf
    Field f2 = newField("f2", "This field has NO Tf in all docs", omitType);
    d.add(f2);

    for(int i=0;i<5;i++)
      writer.addDocument(d);

    for(int i=0;i<20;i++)
      writer.addDocument(d);

    // force merge
    writer.forceMerge(1);

    // flush
    writer.close();

    SegmentReader reader = getOnlySegmentReader(IndexReader.open(ram));
    FieldInfos fi = reader.getFieldInfos();
    assertEquals("OmitTermFreqAndPositions field bit should not be set.", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, fi.fieldInfo("f1").indexOptions);
    assertEquals("OmitTermFreqAndPositions field bit should be set.", IndexOptions.DOCS_ONLY, fi.fieldInfo("f2").indexOptions);
        
    reader.close();
    ram.close();
  }

  private void assertNoPrx(Directory dir) throws Throwable {
    final String[] files = dir.listAll();
    for(int i=0;i<files.length;i++) {
      assertFalse(files[i].endsWith(".prx"));
      assertFalse(files[i].endsWith(".pos"));
    }
  }

  // Verifies no *.prx exists when all fields omit term freq:
  public void testNoPrxFile() throws Throwable {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(ram, newIndexWriterConfig(
                                                                   TEST_VERSION_CURRENT, analyzer).setMaxBufferedDocs(3).setMergePolicy(newLogMergePolicy()));
    LogMergePolicy lmp = (LogMergePolicy) writer.getConfig().getMergePolicy();
    lmp.setMergeFactor(2);
    lmp.setUseCompoundFile(false);
    Document d = new Document();
        
    Field f1 = newField("f1", "This field has term freqs", omitType);
    d.add(f1);

    for(int i=0;i<30;i++)
      writer.addDocument(d);

    writer.commit();

    assertNoPrx(ram);
    
    // now add some documents with positions, and check
    // there is no prox after full merge
    d = new Document();
    f1 = newField("f1", "This field has positions", TextField.TYPE_UNSTORED);
    d.add(f1);
    
    for(int i=0;i<30;i++)
      writer.addDocument(d);
 
    // force merge
    writer.forceMerge(1);
    // flush
    writer.close();

    assertNoPrx(ram);
    ram.close();
  }
 
  // Test scores with one field with Term Freqs and one without, otherwise with equal content 
  public void testBasic() throws Exception {
    Directory dir = newDirectory();  
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer).
            setMaxBufferedDocs(2).
            setSimilarity(new SimpleSimilarity()).
            setMergePolicy(newLogMergePolicy(2))
    );
        
    StringBuilder sb = new StringBuilder(265);
    String term = "term";
    for(int i = 0; i<30; i++){
      Document d = new Document();
      sb.append(term).append(" ");
      String content  = sb.toString();
      Field noTf = newField("noTf", content + (i%2==0 ? "" : " notf"), omitType);
      d.add(noTf);
          
      Field tf = newField("tf", content + (i%2==0 ? " tf" : ""), normalType);
      d.add(tf);
          
      writer.addDocument(d);
      //System.out.println(d);
    }
        
    writer.forceMerge(1);
    // flush
    writer.close();

    /*
     * Verify the index
     */         
    IndexReader reader = IndexReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setSimilarity(new SimpleSimilarity());
        
    Term a = new Term("noTf", term);
    Term b = new Term("tf", term);
    Term c = new Term("noTf", "notf");
    Term d = new Term("tf", "tf");
    TermQuery q1 = new TermQuery(a);
    TermQuery q2 = new TermQuery(b);
    TermQuery q3 = new TermQuery(c);
    TermQuery q4 = new TermQuery(d);

    PhraseQuery pq = new PhraseQuery();
    pq.add(a);
    pq.add(c);
    try {
      searcher.search(pq, 10);
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // expected
    }
        
    searcher.search(q1,
                    new CountingHitCollector() {
                      private Scorer scorer;
                      @Override
                      public final void setScorer(Scorer scorer) {
                        this.scorer = scorer;
                      }
                      @Override
                      public final void collect(int doc) throws IOException {
                        //System.out.println("Q1: Doc=" + doc + " score=" + score);
                        float score = scorer.score();
                        assertTrue(score==1.0f);
                        super.collect(doc);
                      }
                    });
    //System.out.println(CountingHitCollector.getCount());
        
        
    searcher.search(q2,
                    new CountingHitCollector() {
                      private Scorer scorer;
                      @Override
                      public final void setScorer(Scorer scorer) {
                        this.scorer = scorer;
                      }
                      @Override
                      public final void collect(int doc) throws IOException {
                        //System.out.println("Q2: Doc=" + doc + " score=" + score);
                        float score = scorer.score();
                        assertEquals(1.0f+doc, score, 0.00001f);
                        super.collect(doc);
                      }
                    });
    //System.out.println(CountingHitCollector.getCount());
         
        
        
        
        
    searcher.search(q3,
                    new CountingHitCollector() {
                      private Scorer scorer;
                      @Override
                      public final void setScorer(Scorer scorer) {
                        this.scorer = scorer;
                      }
                      @Override
                      public final void collect(int doc) throws IOException {
                        //System.out.println("Q1: Doc=" + doc + " score=" + score);
                        float score = scorer.score();
                        assertTrue(score==1.0f);
                        assertFalse(doc%2==0);
                        super.collect(doc);
                      }
                    });
    //System.out.println(CountingHitCollector.getCount());
        
        
    searcher.search(q4,
                    new CountingHitCollector() {
                      private Scorer scorer;
                      @Override
                      public final void setScorer(Scorer scorer) {
                        this.scorer = scorer;
                      }
                      @Override
                      public final void collect(int doc) throws IOException {
                        float score = scorer.score();
                        //System.out.println("Q1: Doc=" + doc + " score=" + score);
                        assertTrue(score==1.0f);
                        assertTrue(doc%2==0);
                        super.collect(doc);
                      }
                    });
    //System.out.println(CountingHitCollector.getCount());
        
        
        
    BooleanQuery bq = new BooleanQuery();
    bq.add(q1,Occur.MUST);
    bq.add(q4,Occur.MUST);
        
    searcher.search(bq,
                    new CountingHitCollector() {
                      @Override
                      public final void collect(int doc) throws IOException {
                        //System.out.println("BQ: Doc=" + doc + " score=" + score);
                        super.collect(doc);
                      }
                    });
    assertEquals(15, CountingHitCollector.getCount());
         
    reader.close();
    dir.close();
  }
     
  public static class CountingHitCollector extends Collector {
    static int count=0;
    static int sum=0;
    private int docBase = -1;
    CountingHitCollector(){count=0;sum=0;}
    @Override
    public void setScorer(Scorer scorer) throws IOException {}
    @Override
    public void collect(int doc) throws IOException {
      count++;
      sum += doc + docBase;  // use it to avoid any possibility of being merged away
    }

    public static int getCount() { return count; }
    public static int getSum() { return sum; }
    
    @Override
    public void setNextReader(AtomicReaderContext context) {
      docBase = context.docBase;
    }
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }
  }
  
  /** test that when freqs are omitted, that totalTermFreq and sumTotalTermFreq are -1 */
  public void testStats() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_UNSTORED);
    ft.setIndexOptions(IndexOptions.DOCS_ONLY);
    ft.freeze();
    Field f = newField("foo", "bar", ft);
    doc.add(f);
    iw.addDocument(doc);
    IndexReader ir = iw.getReader();
    iw.close();
    Terms terms = MultiFields.getTerms(ir, "foo");
    assertEquals(-1, MultiFields.totalTermFreq(ir, "foo", new BytesRef("bar")));
    assertEquals(-1, terms.getSumTotalTermFreq());
    ir.close();
    dir.close();
  }
}
