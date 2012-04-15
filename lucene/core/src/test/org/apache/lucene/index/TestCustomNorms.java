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
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Before;

/**
 * 
 */
public class TestCustomNorms extends LuceneTestCase {
  final String floatTestField = "normsTestFloat";
  final String exceptionTestField = "normsTestExcp";

  @Before
  public void setUp() throws Exception {
    super.setUp();
    assumeFalse("cannot work with preflex codec", Codec.getDefault().getName()
        .equals("Lucene3x"));
    assumeFalse("cannot work with simple text codec", Codec.getDefault()
        .getName().equals("SimpleText"));

  }

  public void testFloatNorms() throws IOException {

    MockDirectoryWrapper dir = newDirectory();
    dir.setCheckIndexOnClose(false); // can't set sim to checkindex yet
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
      Field f = new Field(floatTestField, "" + nextFloat, TextField.TYPE_STORED);
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
  }

  public void testExceptionOnRandomType() throws IOException {
    MockDirectoryWrapper dir = newDirectory();
    dir.setCheckIndexOnClose(false); // can't set sim to checkindex yet
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
        Field f = new Field(exceptionTestField, "" + nextFloat,
            TextField.TYPE_STORED);
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

  public static class FloatEncodingBoostSimilarity extends DefaultSimilarity {

    @Override
    public void computeNorm(FieldInvertState state, Norm norm) {
      float boost = state.getBoost();
      norm.setFloat(boost);
    }
  }

  public static class RandomTypeSimilarity extends DefaultSimilarity {

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
  }

}
