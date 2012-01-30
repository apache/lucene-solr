package org.apache.lucene.index;
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


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.PruningReader;
import org.apache.lucene.index.pruning.CarmelTopKTermPruningPolicy;
import org.apache.lucene.index.pruning.PruningPolicy;
import org.apache.lucene.index.pruning.StorePruningPolicy;
import org.apache.lucene.index.pruning.TFTermPruningPolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;


public class TestPruningReader extends LuceneTestCase {

  // parameters for the Carmel-TopK-Pruning 
  private static final int R = 1; //number of terms in the query
  private static final int K = 2; // top K results
  private static final float EPSILON = .001f; // error in score

  RAMDirectory sourceDir = new RAMDirectory();

  /** once computed base on how index is created, these are the full scores, i.e. before pruning */ 
  private static Map<Term,ScoreDoc[]> fullScores = initFullScores(); 
  private static Map<Term,ScoreDoc[]> prunedScores = initPrunedScores(); 
  
  private void assertTD(IndexReader ir, Term t, int[] ids) throws Exception {
    TermPositions td = ir.termPositions(t);
    assertNotNull(td);
    try {
      int i = 0;
      while(td.next()) {
        assertEquals(t + ", i=" + i, ids[i], td.doc());
        i++;
      }
      assertEquals(ids.length, i);
    } finally {
      td.close();
    }
  }
  
  /**
   * Scores of the full, unpruned index.
   */
  private static Map<Term, ScoreDoc[]> initFullScores() {
    HashMap<Term, ScoreDoc[]> res = new HashMap<Term, ScoreDoc[]>();
    Term t;
    ScoreDoc sd[]; 
    t = new Term("body","one");
    sd = new ScoreDoc[] {
        new ScoreDoc(4, 0.74011815f),
        new ScoreDoc(2, 0.54939526f),
        new ScoreDoc(3, 0.54939526f),
        new ScoreDoc(1, 0.44857934f),
        new ScoreDoc(0, 0.42292467f) 
        };
    res.put(t,sd);
    t = new Term("body","two");
    sd = new ScoreDoc[] {
        new ScoreDoc(2, 0.7679404f),
        new ScoreDoc(1, 0.62702066f),
        new ScoreDoc(0, 0.5911608f),
        new ScoreDoc(4, 0.5172657f)
    };
    res.put(t,sd);
    t = new Term("body","three");
    sd = new ScoreDoc[] {
        new ScoreDoc(3, 0.7679404f),
        new ScoreDoc(1, 0.62702066f),
        new ScoreDoc(0, 0.5911608f)
    };
    res.put(t,sd);
    t = new Term("test","one");
    sd = new ScoreDoc[] {
        new ScoreDoc(4, 2.9678855f)
    };
    res.put(t,sd);
    t = new Term("allthesame","allthesame"); 
    sd = new ScoreDoc[] {
        new ScoreDoc(0, 0.84584934f),
        new ScoreDoc(1, 0.84584934f),
        new ScoreDoc(2, 0.84584934f),
        new ScoreDoc(3, 0.84584934f),
        new ScoreDoc(4, 0.84584934f)
    };
    res.put(t,sd);
    return res;
  }

  /**
   * Expected scores of the pruned index - with EPSILON=0.001, K=2, R=1 
   */
  private static Map<Term, ScoreDoc[]> initPrunedScores() {
    HashMap<Term, ScoreDoc[]> res = new HashMap<Term, ScoreDoc[]>();
    Term t;
    ScoreDoc sd[]; 
    t = new Term("body","one");
    sd = new ScoreDoc[] {
        new ScoreDoc(4, 0.74011815f),
        new ScoreDoc(2, 0.54939526f),
        new ScoreDoc(3, 0.54939526f),
    };
    res.put(t,sd);
    t = new Term("body","two");
    sd = new ScoreDoc[] {
        new ScoreDoc(2, 0.7679404f),
        new ScoreDoc(1, 0.62702066f),
    };
    res.put(t,sd);
    t = new Term("body","three");
    sd = new ScoreDoc[] {
        new ScoreDoc(3, 0.7679404f),
        new ScoreDoc(1, 0.62702066f),
    };
    res.put(t,sd);
    t = new Term("test","one");
    sd = new ScoreDoc[] {
        new ScoreDoc(4, 2.9678855f)
    };
    res.put(t,sd);
    t = new Term("allthesame","allthesame"); // must keep all because all are the same! 
    sd = new ScoreDoc[] {
        new ScoreDoc(0, 0.84584934f),
        new ScoreDoc(1, 0.84584934f),
        new ScoreDoc(2, 0.84584934f),
        new ScoreDoc(3, 0.84584934f),
        new ScoreDoc(4, 0.84584934f)
    };
    res.put(t,sd);
    return res;
  }

  private void assertTDCount(IndexReader ir, Term t, int count) throws Exception {
    TermPositions td = ir.termPositions(t);
    assertNotNull(td);
    try {
      int i = 0;
      while (td.next()) i++;
      assertEquals(t.toString(), count, i);
    } finally {
      td.close();
    }
  }
  
  public void setUp() throws Exception {
    super.setUp();
    WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer(TEST_VERSION_CURRENT);
    IndexWriter iw = new IndexWriter(sourceDir, new IndexWriterConfig(TEST_VERSION_CURRENT, analyzer));
    Document doc = new Document();
    doc.add(new Field("body", "one two three four", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("id", "0", Field.Store.YES, Field.Index.NO));
    doc.add(new Field("allthesame", "allthesame", Field.Store.YES, Field.Index.ANALYZED));
    iw.addDocument(doc);
    doc = new Document();
    doc.add(new Field("body", "one two three one two three", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("id", "1", Field.Store.YES, Field.Index.NO));
    doc.add(new Field("allthesame", "allthesame", Field.Store.YES, Field.Index.ANALYZED));
    iw.addDocument(doc);
    doc = new Document();
    doc.add(new Field("body", "one two one two one two", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("id", "2", Field.Store.YES, Field.Index.NO));
    doc.add(new Field("allthesame", "allthesame", Field.Store.YES, Field.Index.ANALYZED));
    iw.addDocument(doc);
    doc = new Document();
    doc.add(new Field("body", "one three one three one three", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("id", "3", Field.Store.YES, Field.Index.NO));
    doc.add(new Field("allthesame", "allthesame", Field.Store.YES, Field.Index.ANALYZED));
    iw.addDocument(doc);
    doc = new Document();
    doc.add(new Field("body", "one one one one two", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("test", "one two one two three three three four", Field.Store.YES, Field.Index.ANALYZED_NO_NORMS, Field.TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(new Field("id", "4", Field.Store.YES, Field.Index.NO));
    doc.add(new Field("allthesame", "allthesame", Field.Store.YES, Field.Index.ANALYZED));
    iw.addDocument(doc);
    // to be deleted
    doc = new Document();
    doc.add(new Field("body", "one three one three one three five five five", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("id", "5", Field.Store.YES, Field.Index.NO));
    doc.add(new Field("allthesame", "allthesame", Field.Store.YES, Field.Index.ANALYZED));
    iw.addDocument(doc);
    iw.close();
    IndexReader ir = IndexReader.open(sourceDir, false);
    ir.deleteDocument(5);
    ir.close();
  }

  public void testTfPruning() throws Exception {
    RAMDirectory targetDir = new RAMDirectory();
    IndexReader in = IndexReader.open(sourceDir, true);
    TFTermPruningPolicy tfp = new TFTermPruningPolicy(in, null, null, 2);
    PruningReader tfr = new PruningReader(in, null, tfp);
    // verify
    assertTD(tfr, new Term("body", "one"), new int[]{1, 2, 3, 4});
    assertTD(tfr, new Term("body", "two"), new int[]{1, 2});
    assertTD(tfr, new Term("body", "three"), new int[]{1, 3});
    assertTD(tfr, new Term("test", "one"), new int[]{4});
    assertTDCount(tfr, new Term("body", "four"), 0);
    assertTDCount(tfr, new Term("test", "four"), 0);
    // verify new reader
    WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer(TEST_VERSION_CURRENT);
    IndexWriter iw = new IndexWriter(targetDir, new IndexWriterConfig(TEST_VERSION_CURRENT, analyzer));
    iw.addIndexes(new IndexReader[]{tfr});
    iw.close();
    IndexReader ir = IndexReader.open(targetDir, true);
    assertTD(ir, new Term("body", "one"), new int[]{1, 2, 3, 4});
    assertTD(ir, new Term("body", "two"), new int[]{1, 2});
    assertTD(ir, new Term("body", "three"), new int[]{1, 3});
    assertTD(ir, new Term("test", "one"), new int[]{4});
    tfr.close();
    ir.close();
  }
  
  public void testCarmelTopKPruning() throws Exception {
    IndexReader in = IndexReader.open(sourceDir, true);
    // validate full scores - without pruning, just to make sure we test the right thing
    validateDocScores(fullScores, in, false, false); // validate both docs and scores
    // prune reader
    CarmelTopKTermPruningPolicy tfp = new CarmelTopKTermPruningPolicy(in, null, K, EPSILON, R, null);
    PruningReader tfr = new PruningReader(in, null, tfp);
    
    // create the pruned index
    RAMDirectory targetDir = new RAMDirectory();
    WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer(TEST_VERSION_CURRENT);
    IndexWriter iw = new IndexWriter(targetDir, new IndexWriterConfig(TEST_VERSION_CURRENT, analyzer));
    iw.addIndexes(new IndexReader[]{tfr});
    iw.close();
    in.close();

    // validate scores of pruned index
    IndexReader ir = IndexReader.open(targetDir, true);
    validateDocScores(prunedScores, ir, false, true); // validated only docs (scores have changed after pruning)
    ir.close();
  }
  
  private void validateDocScores(Map<Term,ScoreDoc[]> baseScores, IndexReader in, boolean print, boolean onlyDocs) throws IOException {
    validateDocScores(baseScores, in, new Term("body", "one"), print, onlyDocs);
    validateDocScores(baseScores, in, new Term("body", "two"), print, onlyDocs);
    validateDocScores(baseScores, in, new Term("body", "three"), print, onlyDocs);
    validateDocScores(baseScores, in, new Term("test", "one"), print, onlyDocs);
    validateDocScores(baseScores, in, new Term("allthesame", "allthesame"), print, onlyDocs);
  }
  
  /** validate the doc-scores, optionally also print them */
  private void validateDocScores(Map<Term,ScoreDoc[]> baseScores, IndexReader in, Term term, boolean print, boolean onlyDocs) throws IOException {
    if (print) {
      printDocScores(baseScores, in, term);
    }
    float delta = .0001f;
    IndexSearcher is = new IndexSearcher(in);
    TermQuery q = new TermQuery(term);
    ScoreDoc[] sd = is.search(q, 100).scoreDocs;
    assertNotNull("unknown result for term: "+term, baseScores.get(term));
    assertEquals("wrong number of results!", baseScores.get(term).length, sd.length);
    for (int i = 0; i < sd.length; i++) {
      assertEquals("wrong doc!", baseScores.get(term)[i].doc, sd[i].doc);
      if (!onlyDocs) {
        assertEquals("wrong score!", baseScores.get(term)[i].score, sd[i].score, delta);
      }
    }
  }

  /** Print the doc scores (in a code format */
  private void printDocScores(Map<Term,ScoreDoc[]> baseScores, IndexReader in, Term term) throws IOException {
    IndexSearcher is = new IndexSearcher(in);
    TermQuery q = new TermQuery(term);
    ScoreDoc[] scoreDocs = is.search(q, 100).scoreDocs;
    System.out.println("t = new Term(\""+term.field+"\",\""+term.text+"\");");
    System.out.println("sd = new ScoreDoc[] {");
    for (ScoreDoc sd : scoreDocs) {
      System.out.println("    new ScoreDoc("+sd.doc+", "+sd.score+"f),");
    }
    System.out.println("res.put(t,sd);");
  }

  public void testThresholds() throws Exception {
    Map<String, Integer> thresholds = new HashMap<String, Integer>();
    thresholds.put("test", 3);
    IndexReader in = IndexReader.open(sourceDir, true);
    TFTermPruningPolicy tfp = new TFTermPruningPolicy(in, null, thresholds, 2);
    PruningReader tfr = new PruningReader(in, null, tfp);
    assertTDCount(tfr, new Term("test", "one"), 0);
    assertTDCount(tfr, new Term("test", "two"), 0);
    assertTD(tfr, new Term("test", "three"), new int[]{4});
    assertTDCount(tfr, new Term("test", "four"), 0);
  }
  
  public void testRemoveFields() throws Exception {
    RAMDirectory targetDir = new RAMDirectory();
    Map<String, Integer> removeFields = new HashMap<String, Integer>();
    removeFields.put("test", PruningPolicy.DEL_POSTINGS | PruningPolicy.DEL_STORED);
    IndexReader in = IndexReader.open(sourceDir, true);
    TFTermPruningPolicy tfp = new TFTermPruningPolicy(in, removeFields, null, 2);
    StorePruningPolicy stp = new StorePruningPolicy(in, removeFields);
    PruningReader tfr = new PruningReader(in, stp, tfp);
    Document doc = tfr.document(4);
    // removed stored values?
    assertNull(doc.get("test"));
    // removed postings ?
    TermEnum te = tfr.terms();
    while (te.next()) {
      assertFalse("test".equals(te.term().field()));
    }
    // but vectors should be present !
    TermFreqVector tv = tfr.getTermFreqVector(4, "test");
    assertNotNull(tv);
    assertEquals(4, tv.getTerms().length); // term "four" not deleted yet from TermEnum
    // verify new reader
    WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer(TEST_VERSION_CURRENT);
    IndexWriter iw = new IndexWriter(targetDir, new IndexWriterConfig(TEST_VERSION_CURRENT, analyzer));
    iw.addIndexes(new IndexReader[]{tfr});
    iw.close();
    IndexReader ir = IndexReader.open(targetDir, true);
    tv = ir.getTermFreqVector(4, "test");
    assertNotNull(tv);
    assertEquals(3, tv.getTerms().length); // term "four" was deleted from TermEnum
  }
  
}
