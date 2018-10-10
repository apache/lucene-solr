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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/** Test Indexing/IndexWriter with points */
public class TestPointValues extends LuceneTestCase {

  // Suddenly add points to an existing field:
  public void testUpgradeFieldToPoints() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(newStringField("dim", "foo", Field.Store.NO));
    w.addDocument(doc);
    w.close();
    
    iwc = newIndexWriterConfig();
    w = new IndexWriter(dir, iwc);
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.close();
    dir.close();
  }

  // Illegal schema change tests:

  public void testIllegalDimChangeOneDoc() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      w.addDocument(doc);
    });
    assertEquals("cannot change point dimension count from 1 to 2 for field=\"dim\"", expected.getMessage());
    w.close();
    dir.close();
  }

  public void testIllegalDimChangeTwoDocs() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);

    Document doc2 = new Document();
    doc2.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      w.addDocument(doc2);
    });
    assertEquals("cannot change point dimension count from 1 to 2 for field=\"dim\"", expected.getMessage());

    w.close();
    dir.close();
  }

  public void testIllegalDimChangeTwoSegments() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.commit();

    Document doc2 = new Document();
    doc2.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      w.addDocument(doc2);
    });
    assertEquals("cannot change point dimension count from 1 to 2 for field=\"dim\"", expected.getMessage());

    w.close();
    dir.close();
  }

  public void testIllegalDimChangeTwoWriters() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));

    IndexWriter w2 = new IndexWriter(dir, iwc);
    Document doc2 = new Document();
    doc2.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      w2.addDocument(doc2);
    });
    assertEquals("cannot change point dimension count from 1 to 2 for field=\"dim\"", expected.getMessage());

    w2.close();
    dir.close();
  }

  public void testIllegalDimChangeViaAddIndexesDirectory() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = newDirectory();
    IndexWriter w2 = new IndexWriter(dir2, new IndexWriterConfig(new MockAnalyzer(random())));
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    w2.addDocument(doc);
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      w2.addIndexes(new Directory[] {dir});
    });
    assertEquals("cannot change point dimension count from 2 to 1 for field=\"dim\"", expected.getMessage());

    IOUtils.close(w2, dir, dir2);
  }

  public void testIllegalDimChangeViaAddIndexesCodecReader() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = newDirectory();
    IndexWriter w2 = new IndexWriter(dir2, new IndexWriterConfig(new MockAnalyzer(random())));
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    w2.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(dir);
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
        w2.addIndexes(new CodecReader[] {(CodecReader) getOnlyLeafReader(r)});
    });
    assertEquals("cannot change point dimension count from 2 to 1 for field=\"dim\"", expected.getMessage());

    IOUtils.close(r, w2, dir, dir2);
  }

  public void testIllegalDimChangeViaAddIndexesSlowCodecReader() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = newDirectory();
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w2 = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    w2.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(dir);
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      TestUtil.addIndexesSlowly(w2, r);
    });
    assertEquals("cannot change point dimension count from 2 to 1 for field=\"dim\"", expected.getMessage());

    IOUtils.close(r, w2, dir, dir2);
  }

  public void testIllegalNumBytesChangeOneDoc() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    doc.add(new BinaryPoint("dim", new byte[6]));
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      w.addDocument(doc);
    });
    assertEquals("cannot change point numBytes from 4 to 6 for field=\"dim\"", expected.getMessage());

    w.close();
    dir.close();
  }

  public void testIllegalNumBytesChangeTwoDocs() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);

    Document doc2 = new Document();
    doc2.add(new BinaryPoint("dim", new byte[6]));
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      w.addDocument(doc2);
    });
    assertEquals("cannot change point numBytes from 4 to 6 for field=\"dim\"", expected.getMessage());

    w.close();
    dir.close();
  }

  public void testIllegalNumBytesChangeTwoSegments() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.commit();
    
    Document doc2 = new Document();
    doc2.add(new BinaryPoint("dim", new byte[6]));
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      w.addDocument(doc2);
    });
    assertEquals("cannot change point numBytes from 4 to 6 for field=\"dim\"", expected.getMessage());

    w.close();
    dir.close();
  }

  public void testIllegalNumBytesChangeTwoWriters() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();
    
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w2 = new IndexWriter(dir, iwc);
    Document doc2 = new Document();
    doc2.add(new BinaryPoint("dim", new byte[6]));
    
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      w2.addDocument(doc2);
    });
    assertEquals("cannot change point numBytes from 4 to 6 for field=\"dim\"", expected.getMessage());

    w2.close();
    dir.close();
  }

  public void testIllegalNumBytesChangeViaAddIndexesDirectory() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = newDirectory();
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w2 = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[6]));
    w2.addDocument(doc);
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      w2.addIndexes(new Directory[] {dir});
    });
    assertEquals("cannot change point numBytes from 6 to 4 for field=\"dim\"", expected.getMessage());

    IOUtils.close(w2, dir, dir2);
  }

  public void testIllegalNumBytesChangeViaAddIndexesCodecReader() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = newDirectory();
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w2 = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[6]));
    w2.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(dir);
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
        w2.addIndexes(new CodecReader[] {(CodecReader) getOnlyLeafReader(r)});
    });
    assertEquals("cannot change point numBytes from 6 to 4 for field=\"dim\"", expected.getMessage());

    IOUtils.close(r, w2, dir, dir2);
  }

  public void testIllegalNumBytesChangeViaAddIndexesSlowCodecReader() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = newDirectory();
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w2 = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[6]));
    w2.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(dir);
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      TestUtil.addIndexesSlowly(w2, r);
    });
    assertEquals("cannot change point numBytes from 6 to 4 for field=\"dim\"", expected.getMessage());

    IOUtils.close(r, w2, dir, dir2);
  }

  public void testIllegalTooManyBytes() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    expectThrows(IllegalArgumentException.class, () -> {
      doc.add(new BinaryPoint("dim", new byte[PointValues.MAX_NUM_BYTES+1]));
    });

    Document doc2 = new Document();
    doc2.add(new IntPoint("dim", 17));
    w.addDocument(doc2);
    w.close();
    dir.close();
  }

  public void testIllegalTooManyDimensions() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    byte[][] values = new byte[PointValues.MAX_DIMENSIONS+1][];
    for(int i=0;i<values.length;i++) {
      values[i] = new byte[4];
    }
    expectThrows(IllegalArgumentException.class, () -> {
      doc.add(new BinaryPoint("dim", values));
    });

    Document doc2 = new Document();
    doc2.add(new IntPoint("dim", 17));
    w.addDocument(doc2);
    w.close();
    dir.close();
  }

  // Write point values, one segment with Lucene70, another with SimpleText, then forceMerge with SimpleText
  public void testDifferentCodecs1() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene70"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new IntPoint("int", 1));
    w.addDocument(doc);
    w.close();
    
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("SimpleText"));
    w = new IndexWriter(dir, iwc);
    doc = new Document();
    doc.add(new IntPoint("int", 1));
    w.addDocument(doc);

    w.forceMerge(1);
    w.close();
    dir.close();
  }

  // Write point values, one segment with Lucene70, another with SimpleText, then forceMerge with Lucene70
  public void testDifferentCodecs2() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("SimpleText"));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new IntPoint("int", 1));
    w.addDocument(doc);
    w.close();
    
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(Codec.forName("Lucene70"));
    w = new IndexWriter(dir, iwc);
    doc = new Document();
    doc.add(new IntPoint("int", 1));
    w.addDocument(doc);

    w.forceMerge(1);
    w.close();
    dir.close();
  }

  public void testInvalidIntPointUsage() throws Exception {
    IntPoint field = new IntPoint("field", 17, 42);

    expectThrows(IllegalArgumentException.class, () -> {
      field.setIntValue(14);
    });
    
    expectThrows(IllegalStateException.class, () -> {
      field.numericValue();
    });
  }

  public void testInvalidLongPointUsage() throws Exception {
    LongPoint field = new LongPoint("field", 17, 42);

    expectThrows(IllegalArgumentException.class, () -> {
      field.setLongValue(14);
    });

    expectThrows(IllegalStateException.class, () -> {
      field.numericValue();
    });
  }

  public void testInvalidFloatPointUsage() throws Exception {
    FloatPoint field = new FloatPoint("field", 17, 42);

    expectThrows(IllegalArgumentException.class, () -> {
      field.setFloatValue(14);
    });

    expectThrows(IllegalStateException.class, () -> {
      field.numericValue();
    });
  }

  public void testInvalidDoublePointUsage() throws Exception {
    DoublePoint field = new DoublePoint("field", 17, 42);

    expectThrows(IllegalArgumentException.class, () -> {
      field.setDoubleValue(14);
    });

    expectThrows(IllegalStateException.class, () -> {
      field.numericValue();
    });
  }

  public void testTieBreakByDocID() throws Exception {
    Directory dir = newFSDirectory(createTempDir());
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new IntPoint("int", 17));
    for(int i=0;i<300000;i++) {
      w.addDocument(doc);
      if (random().nextInt(1000) == 17) {
        w.commit();
      }
    }

    IndexReader r = DirectoryReader.open(w);

    for(LeafReaderContext ctx : r.leaves()) {
      PointValues points = ctx.reader().getPointValues("int");
      points.intersect(
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
    doc.add(new StringField("id", "0", Field.Store.NO));
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

  public void testPointsFieldMissingFromOneSegment() throws Exception {
    Directory dir = FSDirectory.open(createTempDir());
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.NO));
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

  public void testSparsePoints() throws Exception {
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
        PointValues points = ctx.reader().getPointValues(fieldName);
        if (points != null) {
          docCount += points.getDocCount();
          size += points.size();
        }
      }
      assertEquals(fieldDocCounts[field], docCount);
      assertEquals(fieldSizes[field], size);
    }
    r.close();
    w.close();
    dir.close();
  }

  public void testCheckIndexIncludesPoints() throws Exception {
    Directory dir = new RAMDirectory();
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
    assertEquals(3, segStatus.pointsStatus.totalValuePoints);
    // ... across 2 fields:
    assertEquals(2, segStatus.pointsStatus.totalValueFields);

    // Make sure CheckIndex in fact declares that it is testing points!
    assertTrue(output.toString(IOUtils.UTF_8).contains("test: points..."));
    dir.close();
  }

  public void testMergedStatsEmptyReader() throws IOException {
    IndexReader reader = new MultiReader();
    assertNull(PointValues.getMinPackedValue(reader, "field"));
    assertNull(PointValues.getMaxPackedValue(reader, "field"));
    assertEquals(0, PointValues.getDocCount(reader, "field"));
    assertEquals(0, PointValues.size(reader, "field"));
  }

  public void testMergedStatsOneSegmentWithoutPoints() throws IOException {
    Directory dir = new RAMDirectory();
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

  public void testMergedStatsAllPointsDeleted() throws IOException {
    Directory dir = new RAMDirectory();
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
    Directory dir = new RAMDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
    final int numDocs = TestUtil.nextInt(random(), 10, 20);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      final int numPoints = random().nextInt(3);
      for (int j = 0; j < numPoints; ++j) {
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
}
