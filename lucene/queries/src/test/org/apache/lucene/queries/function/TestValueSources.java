package org.apache.lucene.queries.function;

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

import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.queries.function.valuesource.ByteFieldSource;
import org.apache.lucene.queries.function.valuesource.BytesRefFieldSource;
import org.apache.lucene.queries.function.valuesource.ConstValueSource;
import org.apache.lucene.queries.function.valuesource.DivFloatFunction;
import org.apache.lucene.queries.function.valuesource.DocFreqValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleConstValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource;
import org.apache.lucene.queries.function.valuesource.FloatFieldSource;
import org.apache.lucene.queries.function.valuesource.IDFValueSource;
import org.apache.lucene.queries.function.valuesource.IfFunction;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.queries.function.valuesource.JoinDocFreqValueSource;
import org.apache.lucene.queries.function.valuesource.LinearFloatFunction;
import org.apache.lucene.queries.function.valuesource.LiteralValueSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.queries.function.valuesource.MaxDocValueSource;
import org.apache.lucene.queries.function.valuesource.MaxFloatFunction;
import org.apache.lucene.queries.function.valuesource.MinFloatFunction;
import org.apache.lucene.queries.function.valuesource.NormValueSource;
import org.apache.lucene.queries.function.valuesource.NumDocsValueSource;
import org.apache.lucene.queries.function.valuesource.PowFloatFunction;
import org.apache.lucene.queries.function.valuesource.ProductFloatFunction;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.queries.function.valuesource.RangeMapFloatFunction;
import org.apache.lucene.queries.function.valuesource.ReciprocalFloatFunction;
import org.apache.lucene.queries.function.valuesource.ScaleFloatFunction;
import org.apache.lucene.queries.function.valuesource.ShortFieldSource;
import org.apache.lucene.queries.function.valuesource.SumFloatFunction;
import org.apache.lucene.queries.function.valuesource.SumTotalTermFreqValueSource;
import org.apache.lucene.queries.function.valuesource.TFValueSource;
import org.apache.lucene.queries.function.valuesource.TermFreqValueSource;
import org.apache.lucene.queries.function.valuesource.TotalTermFreqValueSource;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

// TODO: add separate docvalues test
/**
 * barebones tests for function queries.
 */
public class TestValueSources extends LuceneTestCase {
  static Directory dir;
  static IndexReader reader;
  static IndexSearcher searcher;
  
  static final List<String[]> documents = Arrays.asList(new String[][] {
      /*             id,  byte, double, float, int,    long, short, string, text */ 
      new String[] { "0",  "5", "3.63", "5.2", "35", "4343", "945", "test", "this is a test test test" },
      new String[] { "1", "12", "5.65", "9.3", "54", "1954", "123", "bar",  "second test" },
  });
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    dir = newDirectory();
    IndexWriterConfig iwConfig = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwConfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConfig);
    Document document = new Document();
    Field idField = new StringField("id", "", Field.Store.NO);
    document.add(idField);
    Field byteField = new StringField("byte", "", Field.Store.NO);
    document.add(byteField);
    Field doubleField = new StringField("double", "", Field.Store.NO);
    document.add(doubleField);
    Field floatField = new StringField("float", "", Field.Store.NO);
    document.add(floatField);
    Field intField = new StringField("int", "", Field.Store.NO);
    document.add(intField);
    Field longField = new StringField("long", "", Field.Store.NO);
    document.add(longField);
    Field shortField = new StringField("short", "", Field.Store.NO);
    document.add(shortField);
    Field stringField = new StringField("string", "", Field.Store.NO);
    document.add(stringField);
    Field textField = new TextField("text", "", Field.Store.NO);
    document.add(textField);
    
    for (String [] doc : documents) {
      idField.setStringValue(doc[0]);
      byteField.setStringValue(doc[1]);
      doubleField.setStringValue(doc[2]);
      floatField.setStringValue(doc[3]);
      intField.setStringValue(doc[4]);
      longField.setStringValue(doc[5]);
      shortField.setStringValue(doc[6]);
      stringField.setStringValue(doc[7]);
      textField.setStringValue(doc[8]);
      iw.addDocument(document);
    }
    
    reader = iw.getReader();
    searcher = newSearcher(reader);
    iw.close();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    searcher = null;
    reader.close();
    reader = null;
    dir.close();
    dir = null;
  }
  
  public void testByte() throws Exception {
    assertHits(new FunctionQuery(new ByteFieldSource("byte")),
        new float[] { 5f, 12f });
  }
  
  public void testConst() throws Exception {
    assertHits(new FunctionQuery(new ConstValueSource(0.3f)),
        new float[] { 0.3f, 0.3f });
  }
  
  public void testDiv() throws Exception {
    assertHits(new FunctionQuery(new DivFloatFunction(
        new ConstValueSource(10f), new ConstValueSource(5f))),
        new float[] { 2f, 2f });
  }
  
  public void testDocFreq() throws Exception {
    assertHits(new FunctionQuery(
        new DocFreqValueSource("bogus", "bogus", "text", new BytesRef("test"))),
        new float[] { 2f, 2f });
  }
  
  public void testDoubleConst() throws Exception {
    assertHits(new FunctionQuery(new DoubleConstValueSource(0.3d)),
        new float[] { 0.3f, 0.3f });
  }
  
  public void testDouble() throws Exception {
    assertHits(new FunctionQuery(new DoubleFieldSource("double")),
        new float[] { 3.63f, 5.65f });
  }
  
  public void testFloat() throws Exception {
    assertHits(new FunctionQuery(new FloatFieldSource("float")),
        new float[] { 5.2f, 9.3f });
  }
  
  public void testIDF() throws Exception {
    Similarity saved = searcher.getSimilarity();
    try {
      searcher.setSimilarity(new DefaultSimilarity());
      assertHits(new FunctionQuery(
          new IDFValueSource("bogus", "bogus", "text", new BytesRef("test"))),
          new float[] { 0.5945349f, 0.5945349f });
    } finally {
      searcher.setSimilarity(saved);
    }
  }
  
  public void testIf() throws Exception {
    assertHits(new FunctionQuery(new IfFunction(
        new BytesRefFieldSource("id"),
        new ConstValueSource(1.0f),
        new ConstValueSource(2.0f)
        )),
        new float[] { 1f, 1f });
    // true just if a value exists...
    assertHits(new FunctionQuery(new IfFunction(
        new LiteralValueSource("false"),
        new ConstValueSource(1.0f),
        new ConstValueSource(2.0f)
        )),
        new float[] { 1f, 1f });
  }
  
  public void testInt() throws Exception {
    assertHits(new FunctionQuery(new IntFieldSource("int")),
        new float[] { 35f, 54f });
  }
  
  public void testJoinDocFreq() throws Exception {
    assertHits(new FunctionQuery(new JoinDocFreqValueSource("string", "text")),
        new float[] { 2f, 0f });
  }
  
  public void testLinearFloat() throws Exception {
    assertHits(new FunctionQuery(new LinearFloatFunction(new ConstValueSource(2.0f), 3, 1)),
        new float[] { 7f, 7f });
  }
  
  public void testLong() throws Exception {
    assertHits(new FunctionQuery(new LongFieldSource("long")),
        new float[] { 4343f, 1954f });
  }
  
  public void testMaxDoc() throws Exception {
    assertHits(new FunctionQuery(new MaxDocValueSource()),
        new float[] { 2f, 2f });
  }
  
  public void testMaxFloat() throws Exception {
    assertHits(new FunctionQuery(new MaxFloatFunction(new ValueSource[] {
        new ConstValueSource(1f), new ConstValueSource(2f)})),
        new float[] { 2f, 2f });
  }
  
  public void testMinFloat() throws Exception {
    assertHits(new FunctionQuery(new MinFloatFunction(new ValueSource[] {
        new ConstValueSource(1f), new ConstValueSource(2f)})),
        new float[] { 1f, 1f });
  }
  
  public void testNorm() throws Exception {
    Similarity saved = searcher.getSimilarity();
    try {
      // no norm field (so agnostic to indexed similarity)
      searcher.setSimilarity(new DefaultSimilarity());
      assertHits(new FunctionQuery(
          new NormValueSource("byte")),
          new float[] { 0f, 0f });
    } finally {
      searcher.setSimilarity(saved);
    }
  }
  
  public void testNumDocs() throws Exception {
    assertHits(new FunctionQuery(new NumDocsValueSource()),
        new float[] { 2f, 2f });
  }
  
  public void testPow() throws Exception {
    assertHits(new FunctionQuery(new PowFloatFunction(
        new ConstValueSource(2f), new ConstValueSource(3f))),
        new float[] { 8f, 8f });
  }
  
  public void testProduct() throws Exception {
    assertHits(new FunctionQuery(new ProductFloatFunction(new ValueSource[] {
        new ConstValueSource(2f), new ConstValueSource(3f)})),
        new float[] { 6f, 6f });
  }
  
  public void testQuery() throws Exception {
    assertHits(new FunctionQuery(new QueryValueSource(
        new FunctionQuery(new ConstValueSource(2f)), 0f)),
        new float[] { 2f, 2f });
  }
  
  public void testRangeMap() throws Exception {
    assertHits(new FunctionQuery(new RangeMapFloatFunction(new FloatFieldSource("float"),
        5, 6, 1, 0f)),
        new float[] { 1f, 0f });
  }
  
  public void testReciprocal() throws Exception {
    assertHits(new FunctionQuery(new ReciprocalFloatFunction(new ConstValueSource(2f),
        3, 1, 4)),
        new float[] { 0.1f, 0.1f });
  }
  
  public void testScale() throws Exception {
    assertHits(new FunctionQuery(new ScaleFloatFunction(new IntFieldSource("int"), 0, 1)),
       new float[] { 0.0f, 1.0f });
  }
  
  public void testShort() throws Exception {
    assertHits(new FunctionQuery(new ShortFieldSource("short")),
        new float[] { 945f, 123f });
  }
  
  public void testSumFloat() throws Exception {
    assertHits(new FunctionQuery(new SumFloatFunction(new ValueSource[] {
        new ConstValueSource(1f), new ConstValueSource(2f)})),
        new float[] { 3f, 3f });
  }
  
  public void testSumTotalTermFreq() throws Exception {
    if (Codec.getDefault().getName().equals("Lucene3x")) {
      assertHits(new FunctionQuery(new SumTotalTermFreqValueSource("text")),
          new float[] { -1f, -1f });
    } else {
      assertHits(new FunctionQuery(new SumTotalTermFreqValueSource("text")),
          new float[] { 8f, 8f });
    }
  }
  
  public void testTermFreq() throws Exception {
    assertHits(new FunctionQuery(
        new TermFreqValueSource("bogus", "bogus", "text", new BytesRef("test"))),
        new float[] { 3f, 1f });
    assertHits(new FunctionQuery(
        new TermFreqValueSource("bogus", "bogus", "string", new BytesRef("bar"))),
        new float[] { 0f, 1f });
  }
  
  public void testTF() throws Exception {
    Similarity saved = searcher.getSimilarity();
    try {
      // no norm field (so agnostic to indexed similarity)
      searcher.setSimilarity(new DefaultSimilarity());
      assertHits(new FunctionQuery(
          new TFValueSource("bogus", "bogus", "text", new BytesRef("test"))),
          new float[] { (float)Math.sqrt(3d), (float)Math.sqrt(1d) });
      assertHits(new FunctionQuery(
          new TFValueSource("bogus", "bogus", "string", new BytesRef("bar"))),
          new float[] { 0f, 1f });
    } finally {
      searcher.setSimilarity(saved);
    }
  }
  
  public void testTotalTermFreq() throws Exception {
    if (Codec.getDefault().getName().equals("Lucene3x")) {
      assertHits(new FunctionQuery(
          new TotalTermFreqValueSource("bogus", "bogus", "text", new BytesRef("test"))),
          new float[] { -1f, -1f });
    } else {
      assertHits(new FunctionQuery(
          new TotalTermFreqValueSource("bogus", "bogus", "text", new BytesRef("test"))),
          new float[] { 4f, 4f });
    }
  }
  
  void assertHits(Query q, float scores[]) throws Exception {
    ScoreDoc expected[] = new ScoreDoc[scores.length];
    int expectedDocs[] = new int[scores.length];
    for (int i = 0; i < expected.length; i++) {
      expectedDocs[i] = i;
      expected[i] = new ScoreDoc(i, scores[i]);
    }
    TopDocs docs = searcher.search(q, documents.size(), 
        new Sort(new SortField("id", SortField.Type.STRING)));
    CheckHits.checkHits(random(), q, "", searcher, expectedDocs);
    CheckHits.checkHitsQuery(q, expected, docs.scoreDocs, expectedDocs);
    CheckHits.checkExplanations(q, "", searcher);
  }
}
