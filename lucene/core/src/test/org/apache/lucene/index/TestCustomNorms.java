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
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.ExactSimScorer;
import org.apache.lucene.search.similarities.Similarity.SimWeight;
import org.apache.lucene.search.similarities.Similarity.SloppySimScorer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util._TestUtil;

/**
 * 
 */
@SuppressCodecs("Lucene3x")
public class TestCustomNorms extends LuceneTestCase {
  final String floatTestField = "normsTestFloat";
  final String exceptionTestField = "normsTestExcp";

  public void testFloatNorms() throws IOException {

    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(TEST_VERSION_CURRENT,
        new MockAnalyzer(random()));
    Similarity provider = new MySimProvider();
    config.setSimilarity(provider);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    final LineFileDocs docs = new LineFileDocs(random());
    int num = atLeast(100);
    for (int i = 0; i < num; i++) {
      Document doc = docs.nextDoc();
      float nextFloat = random().nextFloat();
      Field f = new TextField(floatTestField, "" + nextFloat, Field.Store.YES);
      f.setBoost(nextFloat);

      doc.add(f);
      writer.addDocument(doc);
      doc.removeField(floatTestField);
      if (rarely()) {
        writer.commit();
      }
    }
    writer.commit();
    writer.close();
    AtomicReader open = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir));
    DocValues normValues = open.normValues(floatTestField);
    assertNotNull(normValues);
    Source source = normValues.getSource();
    assertTrue(source.hasArray());
    assertEquals(Type.FLOAT_32, normValues.getType());
    float[] norms = (float[]) source.getArray();
    for (int i = 0; i < open.maxDoc(); i++) {
      Document document = open.document(i);
      float expected = Float.parseFloat(document.get(floatTestField));
      assertEquals(expected, norms[i], 0.0f);
    }
    open.close();
    dir.close();
    docs.close();
  }

  public void testExceptionOnRandomType() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(TEST_VERSION_CURRENT,
        new MockAnalyzer(random()));
    Similarity provider = new MySimProvider();
    config.setSimilarity(provider);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    final LineFileDocs docs = new LineFileDocs(random());
    int num = atLeast(100);
    try {
      for (int i = 0; i < num; i++) {
        Document doc = docs.nextDoc();
        float nextFloat = random().nextFloat();
        Field f = new TextField(exceptionTestField, "" + nextFloat, Field.Store.YES);
        f.setBoost(nextFloat);

        doc.add(f);
        writer.addDocument(doc);
        doc.removeField(exceptionTestField);
        if (rarely()) {
          writer.commit();
        }
      }
      fail("expected exception - incompatible types");
    } catch (IllegalArgumentException e) {
      // expected
    }
    writer.commit();
    writer.close();
    dir.close();
    docs.close();

  }
  
  public void testIllegalCustomEncoder() throws Exception {
    Directory dir = newDirectory();
    IllegalCustomEncodingSimilarity similarity = new IllegalCustomEncodingSimilarity();
    IndexWriterConfig config = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    config.setSimilarity(similarity);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    Document doc = new Document();
    Field foo = newTextField("foo", "", Field.Store.NO);
    Field bar = newTextField("bar", "", Field.Store.NO);
    doc.add(foo);
    doc.add(bar);
    
    int numAdded = 0;
    for (int i = 0; i < 100; i++) {
      try {
        bar.setStringValue("singleton");
        similarity.useByte = random().nextBoolean();
        writer.addDocument(doc);
        numAdded++;
      } catch (IllegalArgumentException e) {}
    }
    
    
    IndexReader reader = writer.getReader();
    writer.close();
    assertEquals(numAdded, reader.numDocs());
    IndexReaderContext topReaderContext = reader.getContext();
    for (final AtomicReaderContext ctx : topReaderContext.leaves()) {
      AtomicReader atomicReader = ctx.reader();
      Source source = random().nextBoolean() ? atomicReader.normValues("foo").getSource() : atomicReader.normValues("foo").getDirectSource();
      Bits liveDocs = atomicReader.getLiveDocs();
      Type t = source.getType();
      for (int i = 0; i < atomicReader.maxDoc(); i++) {
          assertEquals(0, source.getFloat(i), 0.000f);
      }
      
  
      source = random().nextBoolean() ? atomicReader.normValues("bar").getSource() : atomicReader.normValues("bar").getDirectSource();
      for (int i = 0; i < atomicReader.maxDoc(); i++) {
        if (liveDocs == null || liveDocs.get(i)) {
          assertEquals("type: " + t, 1, source.getFloat(i), 0.000f);
        } else {
          assertEquals("type: " + t, 0, source.getFloat(i), 0.000f);
        }
      }
    }
    reader.close();
    dir.close();
  }

  public class MySimProvider extends PerFieldSimilarityWrapper {
    Similarity delegate = new DefaultSimilarity();

    @Override
    public float queryNorm(float sumOfSquaredWeights) {
      return delegate.queryNorm(sumOfSquaredWeights);
    }

    @Override
    public Similarity get(String field) {
      if (floatTestField.equals(field)) {
        return new FloatEncodingBoostSimilarity();
      } else if (exceptionTestField.equals(field)) {
        return new RandomTypeSimilarity(random());
      } else {
        return delegate;
      }
    }

    @Override
    public float coord(int overlap, int maxOverlap) {
      return delegate.coord(overlap, maxOverlap);
    }
  }

  public static class FloatEncodingBoostSimilarity extends Similarity {

    @Override
    public void computeNorm(FieldInvertState state, Norm norm) {
      float boost = state.getBoost();
      norm.setFloat(boost);
    }
    
    @Override
    public SimWeight computeWeight(float queryBoost, CollectionStatistics collectionStats, TermStatistics... termStats) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ExactSimScorer exactSimScorer(SimWeight weight, AtomicReaderContext context) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public SloppySimScorer sloppySimScorer(SimWeight weight, AtomicReaderContext context) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  public static class RandomTypeSimilarity extends Similarity {

    private final Random random;
    
    public RandomTypeSimilarity(Random random) {
      this.random = random;
    }

    @Override
    public void computeNorm(FieldInvertState state, Norm norm) {
      float boost = state.getBoost();
      int nextInt = random.nextInt(10);
      switch (nextInt) {
      case 0:
        norm.setDouble((double) boost);
        break;
      case 1:
        norm.setFloat(boost);
        break;
      case 2:
        norm.setLong((long) boost);
        break;
      case 3:
        norm.setBytes(new BytesRef(new byte[6]));
        break;
      case 4:
        norm.setInt((int) boost);
        break;
      case 5:
        norm.setShort((short) boost);
        break;
      default:
        norm.setByte((byte) boost);
      }

    }

    @Override
    public SimWeight computeWeight(float queryBoost, CollectionStatistics collectionStats, TermStatistics... termStats) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ExactSimScorer exactSimScorer(SimWeight weight, AtomicReaderContext context) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public SloppySimScorer sloppySimScorer(SimWeight weight, AtomicReaderContext context) throws IOException {
      throw new UnsupportedOperationException();
    }
  }
  
  class IllegalCustomEncodingSimilarity extends Similarity {
    
    public boolean useByte = false;

    @Override
    public void computeNorm(FieldInvertState state, Norm norm) {
      if (useByte) {
        norm.setByte((byte)state.getLength());
      } else {
        norm.setFloat((float)state.getLength());
      }
    }

    @Override
    public SimWeight computeWeight(float queryBoost, CollectionStatistics collectionStats, TermStatistics... termStats) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ExactSimScorer exactSimScorer(SimWeight weight, AtomicReaderContext context) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public SloppySimScorer sloppySimScorer(SimWeight weight, AtomicReaderContext context) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

}
