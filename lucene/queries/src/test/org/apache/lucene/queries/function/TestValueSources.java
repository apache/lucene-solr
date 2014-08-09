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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
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
      /*             id,  double, float, int,  long,   string, text */ 
      new String[] { "0", "3.63", "5.2", "35", "4343", "test", "this is a test test test" },
      new String[] { "1", "5.65", "9.3", "54", "1954", "bar",  "second test" },
  });
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    dir = newDirectory();
    IndexWriterConfig iwConfig = newIndexWriterConfig(new MockAnalyzer(random()));
    iwConfig.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConfig);
    Document document = new Document();
    Field idField = new StringField("id", "", Field.Store.NO);
    document.add(idField);
    Field idDVField = new SortedDocValuesField("id", new BytesRef());
    document.add(idDVField);
    Field doubleField = new DoubleField("double", 0d, Field.Store.NO);
    document.add(doubleField);
    Field doubleDVField = new NumericDocValuesField("double", 0);
    document.add(doubleDVField);
    Field floatField = new FloatField("float", 0f, Field.Store.NO);
    document.add(floatField);
    Field floatDVField = new NumericDocValuesField("float", 0);
    document.add(floatDVField);
    Field intField = new IntField("int", 0, Field.Store.NO);
    document.add(intField);
    Field intDVField = new NumericDocValuesField("int", 0);
    document.add(intDVField);
    Field longField = new LongField("long", 0L, Field.Store.NO);
    document.add(longField);
    Field longDVField = new NumericDocValuesField("long", 0);
    document.add(longDVField);
    Field stringField = new StringField("string", "", Field.Store.NO);
    document.add(stringField);
    Field stringDVField = new SortedDocValuesField("string", new BytesRef());
    document.add(stringDVField);
    Field textField = new TextField("text", "", Field.Store.NO);
    document.add(textField);
    
    for (String [] doc : documents) {
      idField.setStringValue(doc[0]);
      idDVField.setBytesValue(new BytesRef(doc[0]));
      doubleField.setDoubleValue(Double.valueOf(doc[1]));
      doubleDVField.setLongValue(Double.doubleToRawLongBits(Double.valueOf(doc[1])));
      floatField.setFloatValue(Float.valueOf(doc[2]));
      floatDVField.setLongValue(Float.floatToRawIntBits(Float.valueOf(doc[2])));
      intField.setIntValue(Integer.valueOf(doc[3]));
      intDVField.setLongValue(Integer.valueOf(doc[3]));
      longField.setLongValue(Long.valueOf(doc[4]));
      longDVField.setLongValue(Long.valueOf(doc[4]));
      stringField.setStringValue(doc[5]);
      stringDVField.setBytesValue(new BytesRef(doc[5]));
      textField.setStringValue(doc[6]);
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
    assertHits(new FunctionQuery(new RangeMapFloatFunction(new FloatFieldSource("float"),
        5, 6, new SumFloatFunction(new ValueSource[] {new ConstValueSource(1f), new ConstValueSource(2f)}),
        new ConstValueSource(11f))),
        new float[] { 3f, 11f });
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
  
  public void testSumFloat() throws Exception {
    assertHits(new FunctionQuery(new SumFloatFunction(new ValueSource[] {
        new ConstValueSource(1f), new ConstValueSource(2f)})),
        new float[] { 3f, 3f });
  }
  
  public void testSumTotalTermFreq() throws Exception {
    assertHits(new FunctionQuery(new SumTotalTermFreqValueSource("text")),
          new float[] { 8f, 8f });
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
    assertHits(new FunctionQuery(
        new TotalTermFreqValueSource("bogus", "bogus", "text", new BytesRef("test"))),
        new float[] { 4f, 4f });
  }
  
  void assertHits(Query q, float scores[]) throws Exception {
    ScoreDoc expected[] = new ScoreDoc[scores.length];
    int expectedDocs[] = new int[scores.length];
    for (int i = 0; i < expected.length; i++) {
      expectedDocs[i] = i;
      expected[i] = new ScoreDoc(i, scores[i]);
    }
    TopDocs docs = searcher.search(q, null, documents.size(),
        new Sort(new SortField("id", SortField.Type.STRING)), true, false);
    CheckHits.checkHits(random(), q, "", searcher, expectedDocs);
    CheckHits.checkHitsQuery(q, expected, docs.scoreDocs, expectedDocs);
    CheckHits.checkExplanations(q, "", searcher);
  }
}
