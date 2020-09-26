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


import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.VectorField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

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
        doc.add(newStringField("dim", "foo", Store.NO));
        w.addDocument(doc);
      }
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
    }
  }

  // Illegal schema change tests:

  public void testIllegalDimChangeTwoDocs() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.DOT_PRODUCT));
      w.addDocument(doc);
      if (random().nextBoolean()) {
        // sometimes test with two segments
        w.commit();
      }

      Document doc2 = new Document();
      doc2.add(new VectorField("dim", new float[3], VectorValues.ScoreFunction.DOT_PRODUCT));
      IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
        w.addDocument(doc2);
      });
      assertEquals("cannot change vector dimension from 4 to 3 for field=\"dim\"", expected.getMessage());
    }
  }

  public void testIllegalScoreFunctionChange() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.DOT_PRODUCT));
      w.addDocument(doc);
      if (random().nextBoolean()) {
        // sometimes test with two segments
        w.commit();
      }

      Document doc2 = new Document();
      doc2.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.EUCLIDEAN));
      IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
        w.addDocument(doc2);
      });
      assertEquals("cannot change vector score function from DOT_PRODUCT to EUCLIDEAN for field=\"dim\"", expected.getMessage());
    }
  }

  public void testIllegalDimChangeTwoWriters() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }

      try (IndexWriter w2 = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc2 = new Document();
        doc2.add(new VectorField("dim", new float[1], VectorValues.ScoreFunction.DOT_PRODUCT));
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
          w2.addDocument(doc2);
        });
        assertEquals("cannot change vector dimension from 4 to 1 for field=\"dim\"", expected.getMessage());
      }
    }
  }

  public void testIllegalScoreFunctionChangeTwoWriters() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }

      try (IndexWriter w2 = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc2 = new Document();
        doc2.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.EUCLIDEAN));
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
          w2.addDocument(doc2);
        });
        assertEquals("cannot change vector score function from DOT_PRODUCT to EUCLIDEAN for field=\"dim\"", expected.getMessage());
      }
    }
  }

  public void testIllegalDimChangeViaAddIndexesDirectory() throws Exception {
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[5], VectorValues.ScoreFunction.DOT_PRODUCT));
        w2.addDocument(doc);
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
          w2.addIndexes(new Directory[]{dir});
        });
        assertEquals("cannot change vector dimension from 5 to 4 for field=\"dim\"", expected.getMessage());
      }
    }
  }

  public void testIllegalScoreFunctionChangeViaAddIndexesDirectory() throws Exception {
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.EUCLIDEAN));
        w2.addDocument(doc);
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
          w2.addIndexes(new Directory[]{dir});
        });
        assertEquals("cannot change vector score function from EUCLIDEAN to DOT_PRODUCT for field=\"dim\"", expected.getMessage());
      }
    }
  }

  public void testIllegalDimChangeViaAddIndexesCodecReader() throws Exception {
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[5], VectorValues.ScoreFunction.DOT_PRODUCT));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
            w2.addIndexes(new CodecReader[]{(CodecReader) getOnlyLeafReader(r)});
          });
          assertEquals("cannot change vector dimension from 5 to 4 for field=\"dim\"", expected.getMessage());
        }
      }
    }
  }

  public void testIllegalScoreFunctionChangeViaAddIndexesCodecReader() throws Exception {
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.EUCLIDEAN));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
            w2.addIndexes(new CodecReader[]{(CodecReader) getOnlyLeafReader(r)});
          });
          assertEquals("cannot change vector score function from EUCLIDEAN to DOT_PRODUCT for field=\"dim\"", expected.getMessage());
        }
      }
    }
  }

  public void testIllegalDimChangeViaAddIndexesSlowCodecReader() throws Exception {
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[5], VectorValues.ScoreFunction.DOT_PRODUCT));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
            TestUtil.addIndexesSlowly(w2, r);
          });
          assertEquals("cannot change vector dimension from 5 to 4 for field=\"dim\"", expected.getMessage());
        }
      }
    }
  }

  public void testIllegalScoreFunctionChangeViaAddIndexesSlowCodecReader() throws Exception {
    try (Directory dir = newDirectory();
         Directory dir2 = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w2 = new IndexWriter(dir2, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.EUCLIDEAN));
        w2.addDocument(doc);
        try (DirectoryReader r = DirectoryReader.open(dir)) {
          IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
            TestUtil.addIndexesSlowly(w2, r);
          });
          assertEquals("cannot change vector score function from EUCLIDEAN to DOT_PRODUCT for field=\"dim\"", expected.getMessage());
        }
      }
    }
  }

  public void testIllegalMultipleValues() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.DOT_PRODUCT));
      doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.DOT_PRODUCT));
      IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
        w.addDocument(doc);
      });
      assertEquals("VectorValuesField \"dim\" appears more than once in this document (only one value is allowed per field)",
          expected.getMessage());
    }
  }

  public void testIllegalDimensionTooLarge() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc = new Document();
      expectThrows(IllegalArgumentException.class, () -> {
        doc.add(new VectorField("dim", new float[VectorValues.MAX_DIMENSIONS + 1], VectorValues.ScoreFunction.DOT_PRODUCT));
      });

      Document doc2 = new Document();
      doc2.add(new VectorField("dim", new float[1], VectorValues.ScoreFunction.EUCLIDEAN));
      w.addDocument(doc2);
    }
  }

  public void testIllegalEmptyVector() throws Exception {
    try (Directory dir = newDirectory();
         IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
      Document doc = new Document();
      Exception e = expectThrows(IllegalArgumentException.class, () -> {
        doc.add(new VectorField("dim", new float[0], VectorValues.ScoreFunction.NONE));
      });
      assertEquals("cannot index an empty vector", e.getMessage());

      Document doc2 = new Document();
      doc2.add(new VectorField("dim", new float[1], VectorValues.ScoreFunction.NONE));
      w.addDocument(doc2);
    }
  }

  // Write vectors, one segment with default codec, another with SimpleText, then forceMerge
  public void testDifferentCodecs1() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setCodec(Codec.forName("SimpleText"));
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.DOT_PRODUCT));
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
        doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      try (IndexWriter w = new IndexWriter(dir, createIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new VectorField("dim", new float[4], VectorValues.ScoreFunction.DOT_PRODUCT));
        w.addDocument(doc);
        w.forceMerge(1);
      }
    }
  }

  public void testInvalidVectorFieldUsage() {
    VectorField field = new VectorField("field", new float[2], VectorValues.ScoreFunction.NONE);

    expectThrows(IllegalArgumentException.class, () -> {
      field.setIntValue(14);
    });

    expectThrows(IllegalArgumentException.class, () -> {
      field.setVectorValue(new float[1]);
    });

    assertNull(field.numericValue());
  }

  /*
  public void testTieBreakByDocID() throws Exception {
    Directory dir = newFSDirectory(createTempDir());
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new IntPoint("int", 17));
    int numDocs = TEST_NIGHTLY ? 300000 : 3000;
    for(int i=0;i<numDocs;i++) {
      w.addDocument(doc);
      if (random().nextInt(1000) == 17) {
        w.commit();
      }
    }

    IndexReader r = DirectoryReader.open(w);

    for(LeafReaderContext ctx : r.leaves()) {
      PointValues vectors = ctx.reader().getPointValues("int");
      vectors.intersect(
                       new IntersectVisitor() {

                         int lastDocID = -1;

                         @Override
                         public void visit(int docID) {
                           if (docID < lastDocID) {
                             fail("docs out of order: docID=" + docID + " but lastDocID=" + lastDocID);
                           }
                           lastDocID = docID;
                         }

                         @Override
                         public void visit(int docID, byte[] packedValue) {
                           visit(docID);
                         }

                         @Override
                         public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                           if (random().nextBoolean()) {
                             return Relation.CELL_CROSSES_QUERY;
                           } else {
                             return Relation.CELL_INSIDE_QUERY;
                           }
                         }
                       });
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testDeleteAllPointDocs() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new StringField("id", "0", Store.NO));
    doc.add(new IntPoint("int", 17));
    w.addDocument(doc);
    w.addDocument(new Document());
    w.commit();

    w.deleteDocuments(new Term("id", "0"));

    w.forceMerge(1);
    DirectoryReader r = w.getReader();
    assertNull(r.leaves().get(0).reader().getPointValues("int"));
    w.close();
    r.close();
    dir.close();
  }

  public void testVectorsFieldMissingFromOneSegment() throws Exception {
    Directory dir = FSDirectory.open(createTempDir());
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new StringField("id", "0", Store.NO));
    doc.add(new IntPoint("int0", 0));
    w.addDocument(doc);
    w.commit();

    doc = new Document();
    doc.add(new IntPoint("int1", 17));
    w.addDocument(doc);
    w.forceMerge(1);

    w.close();
    dir.close();
  }

  public void testSparseVectors() throws Exception {
    Directory dir = newDirectory();
    int numDocs = atLeast(1000);
    int numFields = TestUtil.nextInt(random(), 1, 10);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    int[] fieldDocCounts = new int[numFields];
    int[] fieldSizes = new int[numFields];
    for(int i=0;i<numDocs;i++) {
      Document doc = new Document();
      for(int field=0;field<numFields;field++) {
        String fieldName = "int" + field;
        if (random().nextInt(100) == 17) {
          doc.add(new IntPoint(fieldName, random().nextInt()));
          fieldDocCounts[field]++;
          fieldSizes[field]++;

          if (random().nextInt(10) == 5) {
            // add same field again!
            doc.add(new IntPoint(fieldName, random().nextInt()));
            fieldSizes[field]++;
          }
        }
      }
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();
    for(int field=0;field<numFields;field++) {
      int docCount = 0;
      int size = 0;
      String fieldName = "int" + field;
      for(LeafReaderContext ctx : r.leaves()) {
        PointValues vectors = ctx.reader().getPointValues(fieldName);
        if (vectors != null) {
          docCount += vectors.getDocCount();
          size += vectors.size();
        }
      }
      assertEquals(fieldDocCounts[field], docCount);
      assertEquals(fieldSizes[field], size);
    }
    r.close();
    w.close();
    dir.close();
  }

  public void testCheckIndexIncludesVectors() throws Exception {
    Directory dir = new ByteBuffersDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new IntPoint("int1", 17));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new IntPoint("int1", 44));
    doc.add(new IntPoint("int2", -17));
    w.addDocument(doc);
    w.close();

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    CheckIndex.Status status = TestUtil.checkIndex(dir, false, true, output);
    assertEquals(1, status.segmentInfos.size());
    CheckIndex.Status.SegmentInfoStatus segStatus = status.segmentInfos.get(0);
    // total 3 point values were index:
    assertEquals(3, segStatus.vectorsStatus.totalValuevectors);
    // ... across 2 fields:
    assertEquals(2, segStatus.vectorsStatus.totalValueFields);

    // Make sure CheckIndex in fact declares that it is testing vectors!
    assertTrue(output.toString(IOUtils.UTF_8).contains("test: vectors..."));
    dir.close();
  }

  public void testMergedStatsEmptyReader() throws IOException {
    IndexReader reader = new MultiReader();
    assertNull(PointValues.getMinPackedValue(reader, "field"));
    assertNull(PointValues.getMaxPackedValue(reader, "field"));
    assertEquals(0, PointValues.getDocCount(reader, "field"));
    assertEquals(0, PointValues.size(reader, "field"));
  }

  public void testMergedStatsOneSegmentWithoutvectors() throws IOException {
    Directory dir = new ByteBuffersDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null).setMergePolicy(NoMergePolicy.INSTANCE));
    w.addDocument(new Document());
    DirectoryReader.open(w).close();
    Document doc = new Document();
    doc.add(new IntPoint("field", Integer.MIN_VALUE));
    w.addDocument(doc);
    IndexReader reader = DirectoryReader.open(w);

    assertArrayEquals(new byte[4], PointValues.getMinPackedValue(reader, "field"));
    assertArrayEquals(new byte[4], PointValues.getMaxPackedValue(reader, "field"));
    assertEquals(1, PointValues.getDocCount(reader, "field"));
    assertEquals(1, PointValues.size(reader, "field"));

    assertNull(PointValues.getMinPackedValue(reader, "field2"));
    assertNull(PointValues.getMaxPackedValue(reader, "field2"));
    assertEquals(0, PointValues.getDocCount(reader, "field2"));
    assertEquals(0, PointValues.size(reader, "field2"));
  }

  public void testMergedStatsAllvectorsDeleted() throws IOException {
    Directory dir = new ByteBuffersDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
    w.addDocument(new Document());
    Document doc = new Document();
    doc.add(new IntPoint("field", Integer.MIN_VALUE));
    doc.add(new StringField("delete", "yes", Store.NO));
    w.addDocument(doc);
    w.forceMerge(1);
    w.deleteDocuments(new Term("delete", "yes"));
    w.addDocument(new Document());
    w.forceMerge(1);
    IndexReader reader = DirectoryReader.open(w);

    assertNull(PointValues.getMinPackedValue(reader, "field"));
    assertNull(PointValues.getMaxPackedValue(reader, "field"));
    assertEquals(0, PointValues.getDocCount(reader, "field"));
    assertEquals(0, PointValues.size(reader, "field"));
  }

  public void testMergedStats() throws IOException {
    final int iters = atLeast(3);
    for (int iter = 0; iter < iters; ++iter) {
      doTestMergedStats();
    }
  }

  private static byte[][] randomBinaryValue(int numDims, int numBytesPerDim) {
    byte[][] bytes = new byte[numDims][];
    for (int i = 0; i < numDims; ++i) {
      bytes[i] = new byte[numBytesPerDim];
      random().nextBytes(bytes[i]);
    }
    return bytes;
  }

  private void doTestMergedStats() throws IOException {
    final int numDims = TestUtil.nextInt(random(), 1, 8);
    final int numBytesPerDim = TestUtil.nextInt(random(), 1, 16);
    Directory dir = new ByteBuffersDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
    final int numDocs = TestUtil.nextInt(random(), 10, 20);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      final int numvectors = random().nextInt(3);
      for (int j = 0; j < numvectors; ++j) {
        doc.add(new BinaryPoint("field", randomBinaryValue(numDims, numBytesPerDim)));
      }
      w.addDocument(doc);
      if (random().nextBoolean()) {
        DirectoryReader.open(w).close();
      }
    }

    final IndexReader reader1 = DirectoryReader.open(w);
    w.forceMerge(1);
    final IndexReader reader2 = DirectoryReader.open(w);
    final PointValues expected = getOnlyLeafReader(reader2).getPointValues("field");
    if (expected == null) {
      assertNull(PointValues.getMinPackedValue(reader1, "field"));
      assertNull(PointValues.getMaxPackedValue(reader1, "field"));
      assertEquals(0, PointValues.getDocCount(reader1, "field"));
      assertEquals(0, PointValues.size(reader1, "field"));
    } else {
      assertArrayEquals(
          expected.getMinPackedValue(),
          PointValues.getMinPackedValue(reader1, "field"));
      assertArrayEquals(
          expected.getMaxPackedValue(),
          PointValues.getMaxPackedValue(reader1, "field"));
      assertEquals(expected.getDocCount(), PointValues.getDocCount(reader1, "field"));
      assertEquals(expected.size(),  PointValues.size(reader1, "field"));
    }
    IOUtils.close(w, reader1, reader2, dir);
  }
  */

}
