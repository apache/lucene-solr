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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Test that norms info is preserved during index life - including
 * separate norms, addDocument, addIndexes, forceMerge.
 */
public class TestNorms extends LuceneTestCase {
  final String byteTestField = "normsTestByte";

  class CustomNormEncodingSimilarity extends DefaultSimilarity {
    @Override
    public byte encodeNormValue(float f) {
      return (byte) f;
    }
    
    @Override
    public float decodeNormValue(byte b) {
      return (float) b;
    }

    @Override
    public void computeNorm(FieldInvertState state, Norm norm) {
      norm.setByte(encodeNormValue((float) state.getLength()));
    }
  }
  
  // LUCENE-1260
  public void testCustomEncoder() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    config.setSimilarity(new CustomNormEncodingSimilarity());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    Document doc = new Document();
    Field foo = newField("foo", "", TextField.TYPE_UNSTORED);
    Field bar = newField("bar", "", TextField.TYPE_UNSTORED);
    doc.add(foo);
    doc.add(bar);
    
    for (int i = 0; i < 100; i++) {
      bar.setStringValue("singleton");
      writer.addDocument(doc);
    }
    
    IndexReader reader = writer.getReader();
    writer.close();
    
    byte fooNorms[] = (byte[]) MultiDocValues.getNormDocValues(reader, "foo").getSource().getArray();
    for (int i = 0; i < reader.maxDoc(); i++)
      assertEquals(0, fooNorms[i]);
    
    byte barNorms[] = (byte[]) MultiDocValues.getNormDocValues(reader, "bar").getSource().getArray();
    for (int i = 0; i < reader.maxDoc(); i++)
      assertEquals(1, barNorms[i]);
    
    reader.close();
    dir.close();
  }
  
  public void testMaxByteNorms() throws IOException {
    Directory dir = newDirectory();
    buildIndex(dir, true);
    AtomicReader open = SlowCompositeReaderWrapper.wrap(IndexReader.open(dir));
    DocValues normValues = open.normValues(byteTestField);
    assertNotNull(normValues);
    Source source = normValues.getSource();
    assertTrue(source.hasArray());
    assertEquals(Type.FIXED_INTS_8, normValues.getType());
    byte[] norms = (byte[]) source.getArray();
    for (int i = 0; i < open.maxDoc(); i++) {
      Document document = open.document(i);
      int expected = Integer.parseInt(document.get(byteTestField));
      assertEquals((byte)expected, norms[i]);
    }
    open.close();
    dir.close();
  }
  
  /**
   * this test randomly creates segments with or without norms but not omitting
   * norms. The similarity used doesn't write a norm value if writeNorms = false is
   * passed. This differs from omitNorm since norms are simply not written for this segment
   * while merging fills in default values based on the Norm {@link Type}
   */
  public void testNormsNotPresent() throws IOException {
    Directory dir = newDirectory();
    boolean firstWriteNorm = random().nextBoolean();
    buildIndex(dir, firstWriteNorm);

    Directory otherDir = newDirectory();
    boolean secondWriteNorm = random().nextBoolean();
    buildIndex(otherDir, secondWriteNorm);

    AtomicReader reader = SlowCompositeReaderWrapper.wrap(IndexReader.open(otherDir));
    FieldInfos fieldInfos = reader.getFieldInfos();
    FieldInfo fieldInfo = fieldInfos.fieldInfo(byteTestField);
    assertFalse(fieldInfo.omitNorms);
    assertTrue(fieldInfo.isIndexed);
    if (secondWriteNorm) {
      assertTrue(fieldInfo.hasNorms());
    } else {
      assertFalse(fieldInfo.hasNorms());  
    }
    
    IndexWriterConfig config = newIndexWriterConfig(TEST_VERSION_CURRENT,
        new MockAnalyzer(random()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    writer.addIndexes(reader);
    AtomicReader mergedReader = SlowCompositeReaderWrapper.wrap(writer.getReader());
    if (!firstWriteNorm && !secondWriteNorm) {
      DocValues normValues = mergedReader.normValues(byteTestField);
      assertNull(normValues);
      FieldInfo fi = mergedReader.getFieldInfos().fieldInfo(byteTestField);
      assertFalse(fi.omitNorms);
      assertTrue(fi.isIndexed);
      assertFalse(fi.hasNorms());
    } else {
      FieldInfo fi = mergedReader.getFieldInfos().fieldInfo(byteTestField);
      assertFalse(fi.omitNorms);
      assertTrue(fi.isIndexed);
      assertTrue(fi.hasNorms());
      
      DocValues normValues = mergedReader.normValues(byteTestField);
      assertNotNull(normValues);
      Source source = normValues.getSource();
      assertTrue(source.hasArray());
      assertEquals(Type.FIXED_INTS_8, normValues.getType());
      byte[] norms = (byte[]) source.getArray();
      for (int i = 0; i < mergedReader.maxDoc(); i++) {
        Document document = mergedReader.document(i);
        int expected = Integer.parseInt(document.get(byteTestField));
        assertEquals((byte) expected, norms[i]);
      }
    }
    mergedReader.close();
    reader.close();

    writer.close();
    dir.close();
    otherDir.close();
  }

  public void buildIndex(Directory dir, boolean writeNorms) throws IOException,
      CorruptIndexException {
    Random random = random();
    IndexWriterConfig config = newIndexWriterConfig(TEST_VERSION_CURRENT,
        new MockAnalyzer(random()));
    Similarity provider = new MySimProvider(writeNorms);
    config.setSimilarity(provider);
    RandomIndexWriter writer = new RandomIndexWriter(random, dir, config);
    final LineFileDocs docs = new LineFileDocs(random, defaultCodecSupportsDocValues());
    int num = atLeast(100);
    for (int i = 0; i < num; i++) {
      Document doc = docs.nextDoc();
      int boost = writeNorms ? 1 + random().nextInt(255) : 0;
      Field f = new Field(byteTestField, "" + boost,
          TextField.TYPE_STORED);
      f.setBoost(boost);
      doc.add(f);
      writer.addDocument(doc);
      doc.removeField(byteTestField);
      if (rarely()) {
        writer.commit();
      }
    }
    writer.commit();
    writer.close();
  }


  public class MySimProvider extends PerFieldSimilarityWrapper {
    Similarity delegate = new DefaultSimilarity();
    private boolean writeNorms;
    public MySimProvider(boolean writeNorms) {
      this.writeNorms = writeNorms;
    }
    @Override
    public float queryNorm(float sumOfSquaredWeights) {

      return delegate.queryNorm(sumOfSquaredWeights);
    }

    @Override
    public Similarity get(String field) {
      if (byteTestField.equals(field)) {
        return new ByteEncodingBoostSimilarity(writeNorms);
      } else {
        return delegate;
      }
    }

    @Override
    public float coord(int overlap, int maxOverlap) {
      return delegate.coord(overlap, maxOverlap);
    }
  }

  
  public static class ByteEncodingBoostSimilarity extends DefaultSimilarity {

    private boolean writeNorms;

    public ByteEncodingBoostSimilarity(boolean writeNorms) {
      this.writeNorms = writeNorms;
    }

    @Override
    public void computeNorm(FieldInvertState state, Norm norm) {
      if (writeNorms) {
        int boost = (int) state.getBoost();
        norm.setByte((byte) (0xFF & boost));
      }
    }
  }
 
}
