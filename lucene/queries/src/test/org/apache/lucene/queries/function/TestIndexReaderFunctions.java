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
package org.apache.lucene.queries.function;

import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestIndexReaderFunctions extends LuceneTestCase {

  static Directory dir;
  static Analyzer analyzer;
  static IndexReader reader;
  static IndexSearcher searcher;

  static final List<String[]> documents = Arrays.asList(
      /*             id,  double, float, int,  long,   string, text,                       double MV (x3),             int MV (x3)*/
      new String[] { "0", "3.63", "5.2", "35", "4343", "test", "this is a test test test", "2.13", "3.69",  "-0.11",   "1", "7", "5"},
      new String[] { "1", "5.65", "9.3", "54", "1954", "bar",  "second test",              "12.79", "123.456", "0.01", "12", "900", "-1" });

  @BeforeClass
  public static void beforeClass() throws Exception {
    dir = newDirectory();
    analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwConfig = newIndexWriterConfig(analyzer);
    iwConfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConfig);
    for (String [] doc : documents) {
      Document document = new Document();
      document.add(new StringField("id", doc[0], Field.Store.NO));
      document.add(new SortedDocValuesField("id", new BytesRef(doc[0])));
      document.add(new StringField("string", doc[5], Field.Store.NO));
      document.add(new SortedDocValuesField("string", new BytesRef(doc[5])));
      document.add(new TextField("text", doc[6], Field.Store.NO));
      iw.addDocument(document);
    }

    reader = iw.getReader();
    searcher = newSearcher(reader);
    iw.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    IOUtils.close(reader, dir, analyzer);
    searcher = null;
    reader = null;
    dir = null;
    analyzer = null;
  }

  public void testDocFreq() throws Exception {
    DoubleValuesSource vs = IndexReaderFunctions.docFreq(new Term("text", "test"));
    assertHits(vs, new float[] { 2f, 2f });
    assertEquals("docFreq(text:test)", vs.toString());
    assertCacheable(vs, false);
  }

  public void testMaxDoc() throws Exception {
    DoubleValuesSource vs = IndexReaderFunctions.maxDoc();
    assertHits(vs, new float[] { 2f, 2f });
    assertEquals("maxDoc()", vs.toString());
    assertCacheable(vs, false);
  }

  public void testNumDocs() throws Exception {
    DoubleValuesSource vs = IndexReaderFunctions.numDocs();
    assertHits(vs, new float[] { 2f, 2f });
    assertEquals("numDocs()", vs.toString());
    assertCacheable(vs, false);
  }

  public void testSumTotalTermFreq() throws Exception {
    LongValuesSource vs = IndexReaderFunctions.sumTotalTermFreq("text");
    assertHits(vs.toDoubleValuesSource(), new float[] { 8f, 8f });
    assertEquals("sumTotalTermFreq(text)", vs.toString());
    assertCacheable(vs, false);
  }

  public void testTermFreq() throws Exception {
    assertHits(IndexReaderFunctions.termFreq(new Term("string", "bar")), new float[] { 0f, 1f });
    assertHits(IndexReaderFunctions.termFreq(new Term("text", "test")), new float[] { 3f, 1f });
    assertHits(IndexReaderFunctions.termFreq(new Term("bogus", "bogus")), new float[] { 0F, 0F });
    assertEquals("termFreq(string:bar)", IndexReaderFunctions.termFreq(new Term("string", "bar")).toString());
    assertCacheable(IndexReaderFunctions.termFreq(new Term("text", "test")), true);
  }

  public void testTotalTermFreq() throws Exception {
    DoubleValuesSource vs = IndexReaderFunctions.totalTermFreq(new Term("text", "test"));
    assertHits(vs, new float[] { 4f, 4f });
    assertEquals("totalTermFreq(text:test)", vs.toString());
    assertCacheable(vs, false);
  }

  public void testNumDeletedDocs() throws Exception {
    DoubleValuesSource vs = IndexReaderFunctions.numDeletedDocs();
    assertHits(vs, new float[] { 0, 0 });
    assertEquals("numDeletedDocs()", vs.toString());
    assertCacheable(vs, false);
  }

  public void testSumDocFreq() throws Exception {
    DoubleValuesSource vs = IndexReaderFunctions.sumDocFreq("text");
    assertHits(vs, new float[] { 6, 6 });
    assertEquals("sumDocFreq(text)", vs.toString());
    assertCacheable(vs, false);
  }

  public void testDocCount() throws Exception {
    DoubleValuesSource vs = IndexReaderFunctions.docCount("text");
    assertHits(vs, new float[] { 2, 2 });
    assertEquals("docCount(text)", vs.toString());
    assertCacheable(vs, false);
  }

  void assertCacheable(DoubleValuesSource vs, boolean expected) throws Exception {
    Query q = new FunctionScoreQuery(new MatchAllDocsQuery(), vs);
    Weight w = searcher.createWeight(q, ScoreMode.COMPLETE, 1);
    LeafReaderContext ctx = reader.leaves().get(0);
    assertEquals(expected, w.isCacheable(ctx));
  }

  void assertCacheable(LongValuesSource vs, boolean expected) throws Exception {
    Query q = new FunctionScoreQuery(new MatchAllDocsQuery(), vs.toDoubleValuesSource());
    Weight w = searcher.createWeight(q, ScoreMode.COMPLETE, 1);
    LeafReaderContext ctx = reader.leaves().get(0);
    assertEquals(expected, w.isCacheable(ctx));
  }

  void assertHits(DoubleValuesSource vs, float scores[]) throws Exception {
    Query q = new FunctionScoreQuery(new MatchAllDocsQuery(), vs);
    ScoreDoc expected[] = new ScoreDoc[scores.length];
    int expectedDocs[] = new int[scores.length];
    for (int i = 0; i < expected.length; i++) {
      expectedDocs[i] = i;
      expected[i] = new ScoreDoc(i, scores[i]);
    }
    TopDocs docs = searcher.search(q, documents.size(),
        new Sort(new SortField("id", SortField.Type.STRING)), true);
    CheckHits.checkHits(random(), q, "", searcher, expectedDocs);
    CheckHits.checkHitsQuery(q, expected, docs.scoreDocs, expectedDocs);
    CheckHits.checkExplanations(q, "", searcher);
    assertSort(vs, expected);
  }

  void assertSort(DoubleValuesSource vs, ScoreDoc expected[]) throws Exception {
    boolean reversed = random().nextBoolean();
    Arrays.sort(expected, (a, b) -> reversed ? (int) (b.score - a.score) : (int) (a.score - b.score));
    int[] expectedDocs = new int[expected.length];
    for (int i = 0; i < expected.length; i++) {
      expectedDocs[i] = expected[i].doc;
    }
    TopDocs docs = searcher.search(new MatchAllDocsQuery(), expected.length,
        new Sort(vs.getSortField(reversed)));
    CheckHits.checkHitsQuery(new MatchAllDocsQuery(), expected, docs.scoreDocs, expectedDocs);
  }

}
