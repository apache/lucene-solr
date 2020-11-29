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


import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.VectorField;
import org.apache.lucene.index.VectorValues.SearchStrategy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.VectorUtil;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** Test Indexing/IndexWriter with vectors */
public class TestVectorValues extends LuceneTestCase {

  private IndexWriterConfig createIndexWriterConfig() {
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(Codec.forName("Lucene90"));
    return iwc;
  }

  // Suddenly add vectors to an existing field:
  public void testUpgradeFieldToVectors() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(newStringField("f", "foo", Store.NO));
        w.addDocument(doc);
      }
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("f", new float[4], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
        w.addDocument(doc);
      }
    }
  }

  public void testFieldConstructor() {
    float[] v = new float[1];
    VectorField field = new VectorField("f", v);
    assertEquals(1, field.fieldType().vectorDimension());
    assertEquals(VectorValues.SearchStrategy.EUCLIDEAN_HNSW, field.fieldType().vectorSearchStrategy());
    assertSame(v, field.vectorValue());
  }

  public void testFieldConstructorExceptions() {
    expectThrows(IllegalArgumentException.class, () -> new VectorField(null, new float[1]));
    expectThrows(IllegalArgumentException.class, () -> new VectorField("f", null));
    expectThrows(IllegalArgumentException.class, () -> new VectorField("f", new float[1], null));
    expectThrows(IllegalArgumentException.class, () -> new VectorField("f", new float[0]));
    expectThrows(IllegalArgumentException.class, () -> new VectorField("f", new float[VectorValues.MAX_DIMENSIONS + 1]));
  }

  public void testFieldSetValue() {
    VectorField field = new VectorField("f", new float[1]);
    float[] v1 = new float[1];
    field.setVectorValue(v1);
    assertSame(v1, field.vectorValue());
    expectThrows(IllegalArgumentException.class, () -> field.setVectorValue(new float[2]));
    expectThrows(IllegalArgumentException.class, () -> field.setVectorValue(null));
  }

  // Illegal schema change tests:

  public void testIllegalDimChangeTwoDocs() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new VectorField("f", new float[4], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
      w.addDocument(doc);
      if (random().nextBoolean()) {
        // sometimes test with two segments
        w.commit();
      }

      Document doc2 = new Document();
      doc2.add(new VectorField("f", new float[3], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
      IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
          () -> w.addDocument(doc2));
      assertEquals("cannot change vector dimension from 4 to 3 for field=\"f\"", expected.getMessage());
    }
  }

  public void testIllegalSearchStrategyChange() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new VectorField("f", new float[4], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
      w.addDocument(doc);
      if (random().nextBoolean()) {
        // sometimes test with two segments
        w.commit();
      }

      Document doc2 = new Document();
      doc2.add(new VectorField("f", new float[4], VectorValues.SearchStrategy.EUCLIDEAN_HNSW));
      IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
          () -> w.addDocument(doc2));
      assertEquals("cannot change vector search strategy from DOT_PRODUCT_HNSW to EUCLIDEAN_HNSW for field=\"f\"", expected.getMessage());
    }
  }

  public void testIllegalDimChangeTwoWriters() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("f", new float[4], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
        w.addDocument(doc);
      }

      try (IndexWriter w2 = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc2 = new Document();
        doc2.add(new VectorField("f", new float[1], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
            () -> w2.addDocument(doc2));
        assertEquals("cannot change vector dimension from 4 to 1 for field=\"f\"", expected.getMessage());
      }
    }
  }

  public void testIllegalSearchStrategyChangeTwoWriters() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("f", new float[4], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
        w.addDocument(doc);
      }

      try (IndexWriter w2 = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc2 = new Document();
        doc2.add(new VectorField("f", new float[4], VectorValues.SearchStrategy.EUCLIDEAN_HNSW));
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
            () -> w2.addDocument(doc2));
        assertEquals("cannot change vector search strategy from DOT_PRODUCT_HNSW to EUCLIDEAN_HNSW for field=\"f\"", expected.getMessage());
      }
    }
  }

  public void testAddIndexesDirectory0() throws Exception {
    String fieldName = "field";
    Document doc = new Document();
    doc.add(new VectorField(fieldName, new float[4], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        w2.addIndexes(dir);
        w2.forceMerge(1);
        try (IndexReader reader = w2.getReader()) {
          LeafReader r = getOnlyLeafReader(reader);
          VectorValues vectorValues = r.getVectorValues(fieldName);
          assertEquals(0, vectorValues.nextDoc());
          assertEquals(0, vectorValues.vectorValue()[0], 0);
          assertEquals(NO_MORE_DOCS, vectorValues.nextDoc());
        }
      }
    }
  }

  public void testAddIndexesDirectory1() throws Exception {
    String fieldName = "field";
    Document doc = new Document();
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        w.addDocument(doc);
      }
      doc.add(new VectorField(fieldName, new float[4], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        w2.addDocument(doc);
        w2.addIndexes(dir);
        w2.forceMerge(1);
        try (IndexReader reader = w2.getReader()) {
          LeafReader r = getOnlyLeafReader(reader);
          VectorValues vectorValues = r.getVectorValues(fieldName);
          assertNotEquals(NO_MORE_DOCS, vectorValues.nextDoc());
          assertEquals(0, vectorValues.vectorValue()[0], 0);
          assertEquals(NO_MORE_DOCS, vectorValues.nextDoc());
        }
      }
    }
  }

  public void testAddIndexesDirectory01() throws Exception {
    String fieldName = "field";
    float[] vector = new float[1];
    Document doc = new Document();
    doc.add(new VectorField(fieldName, vector, VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        vector[0] = 1;
        w2.addDocument(doc);
        w2.addIndexes(dir);
        w2.forceMerge(1);
        try (IndexReader reader = w2.getReader()) {
          LeafReader r = getOnlyLeafReader(reader);
          VectorValues vectorValues = r.getVectorValues(fieldName);
          assertEquals(0, vectorValues.nextDoc());
          // The merge order is randomized, we might get 0 first, or 1
          float value = vectorValues.vectorValue()[0];
          assertTrue(value == 0 || value == 1);
          assertEquals(1, vectorValues.nextDoc());
          value += vectorValues.vectorValue()[0];
          assertEquals(1, value, 0);
        }
      }
    }
  }

  public void testIllegalDimChangeViaAddIndexesDirectory() throws Exception {
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("f", new float[4], SearchStrategy.DOT_PRODUCT_HNSW));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("f", new float[5], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
        w2.addDocument(doc);
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
            () -> w2.addIndexes(new Directory[]{dir}));
        assertEquals("cannot change vector dimension from 5 to 4 for field=\"f\"", expected.getMessage());
      }
    }
  }

  public void testIllegalSearchStrategyChangeViaAddIndexesDirectory() throws Exception {
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("f", new float[4], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("f", new float[4], VectorValues.SearchStrategy.EUCLIDEAN_HNSW));
        w2.addDocument(doc);
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
            () -> w2.addIndexes(dir));
        assertEquals("cannot change vector search strategy from EUCLIDEAN_HNSW to DOT_PRODUCT_HNSW for field=\"f\"", expected.getMessage());
      }
    }
  }

  public void testIllegalDimChangeViaAddIndexesCodecReader() throws Exception {
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("f", new float[4], SearchStrategy.DOT_PRODUCT_HNSW));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("f", new float[5], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
              () -> w2.addIndexes(new CodecReader[]{(CodecReader) getOnlyLeafReader(r)}));
          assertEquals("cannot change vector dimension from 5 to 4 for field=\"f\"", expected.getMessage());
        }
      }
    }
  }

  public void testIllegalSearchStrategyChangeViaAddIndexesCodecReader() throws Exception {
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("f", new float[4], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("f", new float[4], VectorValues.SearchStrategy.EUCLIDEAN_HNSW));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
              () -> w2.addIndexes(new CodecReader[]{(CodecReader) getOnlyLeafReader(r)}));
          assertEquals("cannot change vector search strategy from EUCLIDEAN_HNSW to DOT_PRODUCT_HNSW for field=\"f\"", expected.getMessage());
        }
      }
    }
  }

  public void testIllegalDimChangeViaAddIndexesSlowCodecReader() throws Exception {
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("f", new float[4], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("f", new float[5], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
              () -> TestUtil.addIndexesSlowly(w2, r));
          assertEquals("cannot change vector dimension from 5 to 4 for field=\"f\"", expected.getMessage());
        }
      }
    }
  }

  public void testIllegalSearchStrategyChangeViaAddIndexesSlowCodecReader() throws Exception {
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("f", new float[4], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("f", new float[4], SearchStrategy.EUCLIDEAN_HNSW));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
              () -> TestUtil.addIndexesSlowly(w2, r));
          assertEquals("cannot change vector search strategy from EUCLIDEAN_HNSW to DOT_PRODUCT_HNSW for field=\"f\"", expected.getMessage());
        }
      }
    }
  }

  public void testIllegalMultipleValues() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new VectorField("f", new float[4], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
      doc.add(new VectorField("f", new float[4], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
      IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
          () -> w.addDocument(doc));
      assertEquals("VectorValuesField \"f\" appears more than once in this document (only one value is allowed per field)",
          expected.getMessage());
    }
  }

  public void testIllegalDimensionTooLarge() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc = new Document();
      expectThrows(IllegalArgumentException.class,
          () -> doc.add(new VectorField("f", new float[VectorValues.MAX_DIMENSIONS + 1], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW)));

      Document doc2 = new Document();
      doc2.add(new VectorField("f", new float[1], VectorValues.SearchStrategy.EUCLIDEAN_HNSW));
      w.addDocument(doc2);
    }
  }

  public void testIllegalEmptyVector() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc = new Document();
      Exception e = expectThrows(IllegalArgumentException.class,
          () -> doc.add(new VectorField("f", new float[0], SearchStrategy.NONE)));
      assertEquals("cannot index an empty vector", e.getMessage());

      Document doc2 = new Document();
      doc2.add(new VectorField("f", new float[1], VectorValues.SearchStrategy.NONE));
      w.addDocument(doc2);
    }
  }

  // Write vectors, one segment with default codec, another with SimpleText, then forceMerge
  public void testDifferentCodecs1() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("f", new float[4], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
        w.addDocument(doc);
      }
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setCodec(Codec.forName("SimpleText"));
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        doc.add(new VectorField("f", new float[4], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
        w.addDocument(doc);
        w.forceMerge(1);
      }
    }
  }

  // Write vectors, one segment with with SimpleText, another with default codec, then forceMerge
  public void testDifferentCodecs2() throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(Codec.forName("SimpleText"));
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        doc.add(new VectorField("f", new float[4], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
        w.addDocument(doc);
      }
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("f", new float[4], VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
        w.addDocument(doc);
        w.forceMerge(1);
      }
    }
  }

  public void testInvalidVectorFieldUsage() {
    VectorField field = new VectorField("field", new float[2], VectorValues.SearchStrategy.NONE);

    expectThrows(IllegalArgumentException.class, () -> field.setIntValue(14));

    expectThrows(IllegalArgumentException.class, () -> field.setVectorValue(new float[1]));

    assertNull(field.numericValue());
  }

  public void testDeleteAllVectorDocs() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new StringField("id", "0", Store.NO));
      doc.add(new VectorField("v", new float[]{2, 3, 5}, VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
      w.addDocument(doc);
      w.addDocument(new Document());
      w.commit();

      try (DirectoryReader r = w.getReader()) {
        assertNotNull(getOnlyLeafReader(r).getVectorValues("v"));
      }
      w.deleteDocuments(new Term("id", "0"));
      w.forceMerge(1);
      try (DirectoryReader r = w.getReader()) {
        assertNull(getOnlyLeafReader(r).getVectorValues("v"));
      }
    }
  }

  public void testVectorFieldMissingFromOneSegment() throws Exception {
    try (Directory dir = FSDirectory.open(createTempDir());
         IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new StringField("id", "0", Store.NO));
      doc.add(new VectorField("v0", new float[]{2, 3, 5}, VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
      w.addDocument(doc);
      w.commit();

      doc = new Document();
      doc.add(new VectorField("v1", new float[]{2, 3, 5}, VectorValues.SearchStrategy.DOT_PRODUCT_HNSW));
      w.addDocument(doc);
      w.forceMerge(1);
    }
  }

  public void testSparseVectors() throws Exception {
    int numDocs = atLeast(1000);
    int numFields = TestUtil.nextInt(random(), 1, 10);
    int[] fieldDocCounts = new int[numFields];
    float[] fieldTotals= new float[numFields];
    int[] fieldDims = new int[numFields];
    VectorValues.SearchStrategy[] fieldSearchStrategies = new VectorValues.SearchStrategy[numFields];
    for (int i = 0; i < numFields; i++) {
      fieldDims[i] = random().nextInt(20) + 1;
      fieldSearchStrategies[i] = VectorValues.SearchStrategy.values()[random().nextInt(VectorValues.SearchStrategy.values().length)];
    }
    try (Directory dir = newDirectory();
         RandomIndexWriter w = new RandomIndexWriter(random(), dir, createIndexWriterConfig())) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        for (int field = 0; field < numFields; field++) {
          String fieldName = "int" + field;
          if (random().nextInt(100) == 17) {
            float[] v = randomVector(fieldDims[field]);
            doc.add(new VectorField(fieldName, v, fieldSearchStrategies[field]));
            fieldDocCounts[field]++;
            fieldTotals[field] += v[0];
          }
        }
        w.addDocument(doc);
      }

      try (IndexReader r = w.getReader()) {
        for (int field = 0; field < numFields; field++) {
          int docCount = 0;
          float checksum = 0;
          String fieldName = "int" + field;
          for (LeafReaderContext ctx : r.leaves()) {
            VectorValues vectors = ctx.reader().getVectorValues(fieldName);
            if (vectors != null) {
              docCount += vectors.size();
              while (vectors.nextDoc() != NO_MORE_DOCS) {
                checksum += vectors.vectorValue()[0];
              }
            }
          }
          assertEquals(fieldDocCounts[field], docCount);
          assertEquals(fieldTotals[field], checksum, 1e-5);
        }
      }
    }
  }

  public void testIndexedValueNotAliased() throws Exception {
    // We copy indexed values (as for BinaryDocValues) so the input float[] can be reused across
    // calls to IndexWriter.addDocument.
    String fieldName = "field";
    float[] v = { 0 };
    try (Directory dir = newDirectory();
         IndexWriter iw = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc1 = new Document();
      doc1.add(new VectorField(fieldName, v, VectorValues.SearchStrategy.EUCLIDEAN_HNSW));
      v[0] = 1;
      Document doc2 = new Document();
      doc2.add(new VectorField(fieldName, v, VectorValues.SearchStrategy.EUCLIDEAN_HNSW));
      iw.addDocument(doc1);
      iw.addDocument(doc2);
      v[0] = 2;
      Document doc3 = new Document();
      doc3.add(new VectorField(fieldName, v, VectorValues.SearchStrategy.EUCLIDEAN_HNSW));
      iw.addDocument(doc3);
      iw.forceMerge(1);
      try (IndexReader reader = iw.getReader()) {
        LeafReader r = getOnlyLeafReader(reader);
        VectorValues vectorValues = r.getVectorValues(fieldName);
        vectorValues.nextDoc();
        assertEquals(1, vectorValues.vectorValue()[0], 0);
        vectorValues.nextDoc();
        assertEquals(1, vectorValues.vectorValue()[0], 0);
        vectorValues.nextDoc();
        assertEquals(2, vectorValues.vectorValue()[0], 0);
      }
    }
  }

  public void testSortedIndex() throws Exception {
    IndexWriterConfig iwc = createIndexWriterConfig();
    iwc.setIndexSort(new Sort(new SortField("sortkey", SortField.Type.INT)));
    String fieldName = "field";
    try (Directory dir = newDirectory();
         IndexWriter iw = new IndexWriter(dir, iwc)) {
      add(iw, fieldName, 1, 1, new float[]{-1, 0});
      add(iw, fieldName, 4, 4, new float[]{0, 1});
      add(iw, fieldName, 3, 3, null);
      add(iw, fieldName, 2, 2, new float[]{1, 0});
      iw.forceMerge(1);
      try (IndexReader reader = iw.getReader()) {
        LeafReader leaf = getOnlyLeafReader(reader);

        VectorValues vectorValues = leaf.getVectorValues(fieldName);
        assertEquals(2, vectorValues.dimension());
        assertEquals(3, vectorValues.size());
        assertEquals("1", leaf.document(vectorValues.nextDoc()).get("id"));
        assertEquals(-1f, vectorValues.vectorValue()[0], 0);
        assertEquals("2", leaf.document(vectorValues.nextDoc()).get("id"));
        assertEquals(1, vectorValues.vectorValue()[0], 0);
        assertEquals("4", leaf.document(vectorValues.nextDoc()).get("id"));
        assertEquals(0, vectorValues.vectorValue()[0], 0);
        assertEquals(NO_MORE_DOCS, vectorValues.nextDoc());

        RandomAccessVectorValues ra = ((RandomAccessVectorValuesProducer) vectorValues).randomAccess();
        assertEquals(-1f, ra.vectorValue(0)[0], 0);
        assertEquals(1f, ra.vectorValue(1)[0], 0);
        assertEquals(0f, ra.vectorValue(2)[0], 0);
      }
    }
  }

  public void testIndexMultipleVectorFields() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      float[] v = new float[]{1};
      doc.add(new VectorField("field1", v, SearchStrategy.EUCLIDEAN_HNSW));
      doc.add(new VectorField("field2", new float[]{1, 2, 3}, SearchStrategy.NONE));
      iw.addDocument(doc);
      v[0] = 2;
      iw.addDocument(doc);
      doc = new Document();
      doc.add(new VectorField("field3", new float[]{1, 2, 3}, SearchStrategy.DOT_PRODUCT_HNSW));
      iw.addDocument(doc);
      iw.forceMerge(1);
      try (IndexReader reader = iw.getReader()) {
        LeafReader leaf = reader.leaves().get(0).reader();

        VectorValues vectorValues = leaf.getVectorValues("field1");
        assertEquals(1, vectorValues.dimension());
        assertEquals(2, vectorValues.size());
        vectorValues.nextDoc();
        assertEquals(1f, vectorValues.vectorValue()[0], 0);
        vectorValues.nextDoc();
        assertEquals(2f, vectorValues.vectorValue()[0], 0);
        assertEquals(NO_MORE_DOCS, vectorValues.nextDoc());

        VectorValues vectorValues2 = leaf.getVectorValues("field2");
        assertEquals(3, vectorValues2.dimension());
        assertEquals(2, vectorValues2.size());
        vectorValues2.nextDoc();
        assertEquals(2f, vectorValues2.vectorValue()[1], 0);
        vectorValues2.nextDoc();
        assertEquals(2f, vectorValues2.vectorValue()[1], 0);
        assertEquals(NO_MORE_DOCS, vectorValues2.nextDoc());

        VectorValues vectorValues3 = leaf.getVectorValues("field3");
        assertEquals(3, vectorValues3.dimension());
        assertEquals(1, vectorValues3.size());
        vectorValues3.nextDoc();
        assertEquals(1f, vectorValues3.vectorValue()[0], 0);
        assertEquals(NO_MORE_DOCS, vectorValues3.nextDoc());
      }
    }
  }

  /**
   * Index random vectors, sometimes skipping documents, sometimes deleting a document,
   * sometimes merging, sometimes sorting the index,
   * and verify that the expected values can be read back consistently.
   */
  public void testRandom() throws Exception {
    IndexWriterConfig iwc = createIndexWriterConfig();
    if (random().nextBoolean()) {
      iwc.setIndexSort(new Sort(new SortField("sortkey", SortField.Type.INT)));
    }
    String fieldName = "field";
    try (Directory dir = newDirectory();
         IndexWriter iw = new IndexWriter(dir, iwc)) {
      int numDoc = atLeast(100);
      int dimension = atLeast(10);
      float[] scratch = new float[dimension];
      int numValues = 0;
      float[][] values = new float[numDoc][];
      for (int i = 0; i < numDoc; i++) {
        if (random().nextInt(7) != 3) {
          // usually index a vector value for a doc
          values[i] = randomVector(dimension);
          ++numValues;
        }
        if (random().nextBoolean() && values[i] != null) {
          // sometimes use a shared scratch array
          System.arraycopy(values[i], 0, scratch, 0, scratch.length);
          add(iw, fieldName, i, scratch);
        } else {
          add(iw, fieldName, i, values[i]);
        }
        if (random().nextInt(10) == 2) {
          // sometimes delete a random document
          int idToDelete = random().nextInt(i + 1);
          iw.deleteDocuments(new Term("id", Integer.toString(idToDelete)));
          // and remember that it was deleted
          if (values[idToDelete] != null) {
            values[idToDelete] = null;
            --numValues;
          }
        }
        if (random().nextInt(10) == 3) {
          iw.commit();
        }
      }
      iw.forceMerge(1);
      try (IndexReader reader = iw.getReader()) {
        int valueCount = 0, totalSize = 0;
        for (LeafReaderContext ctx : reader.leaves()) {
          VectorValues vectorValues = ctx.reader().getVectorValues(fieldName);
          if (vectorValues == null) {
            continue;
          }
          totalSize += vectorValues.size();
          int docId;
          while ((docId = vectorValues.nextDoc()) != NO_MORE_DOCS) {
            float[] v = vectorValues.vectorValue();
            assertEquals(dimension, v.length);
            String idString = ctx.reader().document(docId).getField("id").stringValue();
            int id = Integer.parseInt(idString);
            assertArrayEquals(idString, values[id], v, 0);
            ++valueCount;
          }
        }
        assertEquals(numValues, valueCount);
        assertEquals(numValues, totalSize);
      }
    }
  }

  private void add(IndexWriter iw, String field, int id, float[] vector) throws IOException {
    add(iw, field, id, random().nextInt(100), vector);
  }

  private void add(IndexWriter iw, String field, int id, int sortkey, float[] vector) throws IOException {
    Document doc = new Document();
    if (vector != null) {
      doc.add(new VectorField(field, vector));
    }
    doc.add(new NumericDocValuesField("sortkey", sortkey));
    doc.add(new StringField("id", Integer.toString(id), Field.Store.YES));
    iw.addDocument(doc);
  }

  private float[] randomVector(int dim) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = random().nextFloat();
    }
    VectorUtil.l2normalize(v);
    return v;
  }

  public void testCheckIndexIncludesVectors() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("v1", randomVector(3), VectorValues.SearchStrategy.NONE));
        w.addDocument(doc);

        doc.add(new VectorField("v2", randomVector(3), VectorValues.SearchStrategy.NONE));
        w.addDocument(doc);
      }

      ByteArrayOutputStream output = new ByteArrayOutputStream();
      CheckIndex.Status status = TestUtil.checkIndex(dir, false, true, output);
      assertEquals(1, status.segmentInfos.size());
      CheckIndex.Status.SegmentInfoStatus segStatus = status.segmentInfos.get(0);
      // total 3 vector values were indexed:
      assertEquals(3, segStatus.vectorValuesStatus.totalVectorValues);
      // ... across 2 fields:
      assertEquals(2, segStatus.vectorValuesStatus.totalVectorFields);

      // Make sure CheckIndex in fact declares that it is testing vectors!
      assertTrue(output.toString(IOUtils.UTF_8).contains("test: vectors..."));
    }
  }

  public void testSearchStrategyIdentifiers() {
    // make sure we don't accidentally mess up search strategy identifiers by re-ordering their enumerators
    assertEquals(0, VectorValues.SearchStrategy.NONE.ordinal());
    assertEquals(1, VectorValues.SearchStrategy.EUCLIDEAN_HNSW.ordinal());
    assertEquals(2, VectorValues.SearchStrategy.DOT_PRODUCT_HNSW.ordinal());
    assertEquals(3, VectorValues.SearchStrategy.values().length);
  }

}
