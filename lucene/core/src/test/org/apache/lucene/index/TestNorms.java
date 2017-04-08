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
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Test that norms info is preserved during index life - including
 * separate norms, addDocument, addIndexes, forceMerge.
 */
@SuppressCodecs({ "Memory", "Direct", "SimpleText" })
@Slow
public class TestNorms extends LuceneTestCase {
  static final String BYTE_TEST_FIELD = "normsTestByte";

  static class CustomNormEncodingSimilarity extends TFIDFSimilarity {

    @Override
    public long encodeNormValue(float f) {
      return (long) f;
    }
    
    @Override
    public float decodeNormValue(long norm) {
      return norm;
    }

    @Override
    public float lengthNorm(FieldInvertState state) {
      return state.getLength();
    }

    @Override public float tf(float freq) { return 0; }
    @Override public float idf(long docFreq, long docCount) { return 0; }
    @Override public float sloppyFreq(int distance) { return 0; }
    @Override public float scorePayload(int doc, int start, int end, BytesRef payload) { return 0; }
  }
  
  // LUCENE-1260
  public void testCustomEncoder() throws Exception {
    Directory dir = newDirectory();
    MockAnalyzer analyzer = new MockAnalyzer(random());

    IndexWriterConfig config = newIndexWriterConfig(analyzer);
    config.setSimilarity(new CustomNormEncodingSimilarity());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    Document doc = new Document();
    Field foo = newTextField("foo", "", Field.Store.NO);
    Field bar = newTextField("bar", "", Field.Store.NO);
    doc.add(foo);
    doc.add(bar);
    
    for (int i = 0; i < 100; i++) {
      bar.setStringValue("singleton");
      writer.addDocument(doc);
    }
    
    IndexReader reader = writer.getReader();
    writer.close();
    
    NumericDocValues fooNorms = MultiDocValues.getNormValues(reader, "foo");
    for (int i = 0; i < reader.maxDoc(); i++) {
      assertEquals(i, fooNorms.nextDoc());
      assertEquals(0, fooNorms.longValue());
    }
    
    NumericDocValues barNorms = MultiDocValues.getNormValues(reader, "bar");
    for (int i = 0; i < reader.maxDoc(); i++) {
      assertEquals(i, barNorms.nextDoc());
      assertEquals(1, barNorms.longValue());
    }
    
    reader.close();
    dir.close();
  }
  
  public void testMaxByteNorms() throws IOException {
    Directory dir = newFSDirectory(createTempDir("TestNorms.testMaxByteNorms"));
    buildIndex(dir);
    DirectoryReader open = DirectoryReader.open(dir);
    NumericDocValues normValues = MultiDocValues.getNormValues(open, BYTE_TEST_FIELD);
    assertNotNull(normValues);
    for (int i = 0; i < open.maxDoc(); i++) {
      Document document = open.document(i);
      int expected = Integer.parseInt(document.get(BYTE_TEST_FIELD).split(" ")[0]);
      assertEquals(i, normValues.nextDoc());
      assertEquals(expected, normValues.longValue());
    }
    open.close();
    dir.close();
  }
  
  // TODO: create a testNormsNotPresent ourselves by adding/deleting/merging docs

  public void buildIndex(Directory dir) throws IOException {
    Random random = random();
    MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setMaxTokenLength(TestUtil.nextInt(random(), 1, IndexWriter.MAX_TERM_LENGTH));
    IndexWriterConfig config = newIndexWriterConfig(analyzer);
    Similarity provider = new MySimProvider();
    config.setSimilarity(provider);
    RandomIndexWriter writer = new RandomIndexWriter(random, dir, config);
    final LineFileDocs docs = new LineFileDocs(random);
    int num = atLeast(100);
    for (int i = 0; i < num; i++) {
      Document doc = docs.nextDoc();
      int boost = TestUtil.nextInt(random, 1, 255);
      String value = IntStream.range(0, boost).mapToObj(k -> Integer.toString(boost)).collect(Collectors.joining(" "));
      Field f = new TextField(BYTE_TEST_FIELD, value, Field.Store.YES);
      doc.add(f);
      writer.addDocument(doc);
      doc.removeField(BYTE_TEST_FIELD);
      if (rarely()) {
        writer.commit();
      }
    }
    writer.commit();
    writer.close();
    docs.close();
  }


  public class MySimProvider extends PerFieldSimilarityWrapper {
    Similarity delegate = new ClassicSimilarity();

    @Override
    public Similarity get(String field) {
      if (BYTE_TEST_FIELD.equals(field)) {
        return new ByteEncodingBoostSimilarity();
      } else {
        return delegate;
      }
    }
  }

  
  public static class ByteEncodingBoostSimilarity extends Similarity {

    @Override
    public long computeNorm(FieldInvertState state) {
      return state.getLength();
    }

    @Override
    public SimWeight computeWeight(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SimScorer simScorer(SimWeight weight, LeafReaderContext context) throws IOException {
      throw new UnsupportedOperationException();
    }
  } 

  public void testEmptyValueVsNoValue() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig cfg = newIndexWriterConfig().setMergePolicy(newLogMergePolicy());
    IndexWriter w = new IndexWriter(dir, cfg);
    Document doc = new Document();
    w.addDocument(doc);
    doc.add(newTextField("foo", "", Store.NO));
    w.addDocument(doc);
    w.forceMerge(1);
    IndexReader reader = DirectoryReader.open(w);
    w.close();
    LeafReader leafReader = getOnlyLeafReader(reader);
    NumericDocValues normValues = leafReader.getNormValues("foo");
    assertNotNull(normValues);
    assertEquals(1, normValues.nextDoc()); // doc 0 does not have norms
    assertEquals(0, normValues.longValue());
    reader.close();
    dir.close();
  }
}
