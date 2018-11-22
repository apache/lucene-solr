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
package org.apache.lucene.index;


import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;


public class TestOmitTf extends LuceneTestCase {
  
  public static class SimpleSimilarity extends TFIDFSimilarity {
    @Override public float lengthNorm(int length) { return 1; }
    @Override public float tf(float freq) { return freq; }
    @Override public float idf(long docFreq, long docCount) { return 1.0f; }
    @Override public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics[] termStats) {
      return Explanation.match(1.0f, "Inexplicable");
    }
  }

  private static final FieldType omitType = new FieldType(TextField.TYPE_NOT_STORED);
  private static final FieldType normalType = new FieldType(TextField.TYPE_NOT_STORED);
  
  static {
    omitType.setIndexOptions(IndexOptions.DOCS);
  }

  // Make sure first adding docs that do not omitTermFreqAndPositions for
  // field X, then adding docs that do omitTermFreqAndPositions for that same
  // field, 
  public void testMixedRAM() throws Exception {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(
        ram,
        newIndexWriterConfig(analyzer).
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

    LeafReader reader = getOnlyLeafReader(DirectoryReader.open(ram));
    FieldInfos fi = reader.getFieldInfos();
    assertEquals("OmitTermFreqAndPositions field bit should not be set.", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, fi.fieldInfo("f1").getIndexOptions());
    assertEquals("OmitTermFreqAndPositions field bit should be set.", IndexOptions.DOCS, fi.fieldInfo("f2").getIndexOptions());
        
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
    IndexWriter writer = new IndexWriter(ram, newIndexWriterConfig(analyzer)
                                                .setMaxBufferedDocs(3)
                                                .setMergePolicy(newLogMergePolicy()));
    LogMergePolicy lmp = (LogMergePolicy) writer.getConfig().getMergePolicy();
    lmp.setMergeFactor(2);
    lmp.setNoCFSRatio(0.0);
    Document d = new Document();
        
    Field f1 = newField("f1", "This field has term freqs", omitType);
    d.add(f1);

    for(int i=0;i<30;i++)
      writer.addDocument(d);

    writer.commit();

    assertNoPrx(ram);
    
    writer.close();
    ram.close();
  }
 
  // Test scores with one field with Term Freqs and one without, otherwise with equal content 
  public void testBasic() throws Exception {
    Directory dir = newDirectory();  
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(analyzer)
            .setMaxBufferedDocs(2)
            .setSimilarity(new SimpleSimilarity())
            .setMergePolicy(newLogMergePolicy(2))
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
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);
    searcher.setSimilarity(new SimpleSimilarity());
        
    Term a = new Term("noTf", term);
    Term b = new Term("tf", term);
    Term c = new Term("noTf", "notf");
    Term d = new Term("tf", "tf");
    TermQuery q1 = new TermQuery(a);
    TermQuery q2 = new TermQuery(b);
    TermQuery q3 = new TermQuery(c);
    TermQuery q4 = new TermQuery(d);

    PhraseQuery pq = new PhraseQuery(a.field(), a.bytes(), c.bytes());
    Exception expected = expectThrows(Exception.class, () -> {
      searcher.search(pq, 10);
    });
    Throwable cause = expected;
    // If the searcher uses an executor service, the IAE is wrapped into other exceptions
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    assertTrue("Expected an IAE, got " + cause, cause instanceof IllegalStateException);
        
    searcher.search(q1,
                    new CountingHitCollector() {
                      private Scorable scorer;
                      @Override
                      public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE;
                      }
                      @Override
                      public final void setScorer(Scorable scorer) {
                        this.scorer = scorer;
                      }
                      @Override
                      public final void collect(int doc) throws IOException {
                        //System.out.println("Q1: Doc=" + doc + " score=" + score);
                        float score = scorer.score();
                        assertTrue("got score=" + score, score==1.0f);
                        super.collect(doc);
                      }
                    });
    //System.out.println(CountingHitCollector.getCount());
        
        
    searcher.search(q2,
                    new CountingHitCollector() {
                      private Scorable scorer;
                      @Override
                      public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE;
                      }
                      @Override
                      public final void setScorer(Scorable scorer) {
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
                      private Scorable scorer;
                      @Override
                      public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE;
                      }
                      @Override
                      public final void setScorer(Scorable scorer) {
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
                      private Scorable scorer;
                      @Override
                      public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE;
                      }
                      @Override
                      public final void setScorer(Scorable scorer) {
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
        
        
        
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(q1,Occur.MUST);
    bq.add(q4,Occur.MUST);
        
    searcher.search(bq.build(),
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
     
  public static class CountingHitCollector extends SimpleCollector {
    static int count=0;
    static int sum=0;
    private int docBase = -1;
    CountingHitCollector(){count=0;sum=0;}
    @Override
    public void collect(int doc) throws IOException {
      count++;
      sum += doc + docBase;  // use it to avoid any possibility of being merged away
    }

    public static int getCount() { return count; }
    public static int getSum() { return sum; }
    
    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      docBase = context.docBase;
    }
    
    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }
  }
  
  /** test that when freqs are omitted, that totalTermFreq and sumTotalTermFreq are docFreq, and sumDocFreq */
  public void testStats() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS);
    ft.freeze();
    Field f = newField("foo", "bar", ft);
    doc.add(f);
    iw.addDocument(doc);
    IndexReader ir = iw.getReader();
    iw.close();
    assertEquals(ir.docFreq(new Term("foo", new BytesRef("bar"))), ir.totalTermFreq(new Term("foo", new BytesRef("bar"))));
    assertEquals(ir.getSumDocFreq("foo"), ir.getSumTotalTermFreq("foo"));
    ir.close();
    dir.close();
  }
}
