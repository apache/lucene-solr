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
package org.apache.lucene.codecs.lucene86;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BasePointsFormatTestCase;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MockRandomMergePolicy;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.bkd.BKDConfig;

public class TestLucene86PointsFormat extends BasePointsFormatTestCase {

  private final Codec codec;
  private final int maxPointsInLeafNode;
  
  public TestLucene86PointsFormat() {
    // standard issue
    Codec defaultCodec = TestUtil.getDefaultCodec();
    if (random().nextBoolean()) {
      // randomize parameters
      maxPointsInLeafNode = TestUtil.nextInt(random(), 50, 500);
      double maxMBSortInHeap = 3.0 + (3*random().nextDouble());
      if (VERBOSE) {
        System.out.println("TEST: using Lucene60PointsFormat with maxPointsInLeafNode=" + maxPointsInLeafNode + " and maxMBSortInHeap=" + maxMBSortInHeap);
      }

      // sneaky impersonation!
      codec = new FilterCodec(defaultCodec.getName(), defaultCodec) {
        @Override
        public PointsFormat pointsFormat() {
          return new PointsFormat() {
            @Override
            public PointsWriter fieldsWriter(SegmentWriteState writeState) throws IOException {
              return new Lucene86PointsWriter(writeState, maxPointsInLeafNode, maxMBSortInHeap);
            }

            @Override
            public PointsReader fieldsReader(SegmentReadState readState) throws IOException {
              return new Lucene86PointsReader(readState);
            }
          };
        }
      };
    } else {
      // standard issue
      codec = defaultCodec;
      maxPointsInLeafNode = BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE;
    }
  }

  @Override
  protected Codec getCodec() {
    return codec;
  }

  @Override
  public void testMergeStability() throws Exception {
    assumeFalse("TODO: mess with the parameters and test gets angry!", codec instanceof FilterCodec);
    super.testMergeStability();
  }

  public void testEstimatePointCount() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    // Avoid mockRandomMP since it may cause non-optimal merges that make the
    // number of points per leaf hard to predict
    while (iwc.getMergePolicy() instanceof MockRandomMergePolicy) {
      iwc.setMergePolicy(newMergePolicy());
    }
    IndexWriter w = new IndexWriter(dir, iwc);
    byte[] pointValue = new byte[3];
    byte[] uniquePointValue = new byte[3];
    random().nextBytes(uniquePointValue);
    final int numDocs = TEST_NIGHTLY ? atLeast(10000) : atLeast(500); // at night, make sure we have several leaves
    final boolean multiValues = random().nextBoolean();
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      if (i == numDocs / 2) {
        doc.add(new BinaryPoint("f", uniquePointValue));
      } else {
        final int numValues = (multiValues) ? TestUtil.nextInt(random(), 2, 100) : 1;
        for (int j = 0; j < numValues; j ++) {
          do {
            random().nextBytes(pointValue);
          } while (Arrays.equals(pointValue, uniquePointValue));
          doc.add(new BinaryPoint("f", pointValue));
        }
      }
      w.addDocument(doc);
    }
    w.forceMerge(1);
    final IndexReader r = DirectoryReader.open(w);
    w.close();
    final LeafReader lr = getOnlyLeafReader(r);
    PointValues points = lr.getPointValues("f");

    // If all points match, then the point count is numLeaves * maxPointsInLeafNode
    final int numLeaves = (int) Math.ceil((double) points.size() / maxPointsInLeafNode);

    IntersectVisitor allPointsVisitor = new IntersectVisitor() {
      @Override
      public void visit(int docID, byte[] packedValue) throws IOException {}

      @Override
      public void visit(int docID) throws IOException {}

      @Override
      public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        return Relation.CELL_INSIDE_QUERY;
      }
    };

    assertEquals(numLeaves * maxPointsInLeafNode, points.estimatePointCount(allPointsVisitor));
    assertEquals(numDocs, points.estimateDocCount(allPointsVisitor));

    IntersectVisitor noPointsVisitor = new IntersectVisitor() {
      @Override
      public void visit(int docID, byte[] packedValue) throws IOException {}

      @Override
      public void visit(int docID) throws IOException {}

      @Override
      public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        return Relation.CELL_OUTSIDE_QUERY;
      }
    };

    // Return 0 if no points match
    assertEquals(0, points.estimatePointCount(noPointsVisitor));
    assertEquals(0, points.estimateDocCount(noPointsVisitor));

    IntersectVisitor onePointMatchVisitor = new IntersectVisitor() {
      @Override
      public void visit(int docID, byte[] packedValue) throws IOException {}

      @Override
      public void visit(int docID) throws IOException {}

      @Override
      public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        if (FutureArrays.compareUnsigned(uniquePointValue, 0, 3, maxPackedValue, 0, 3) > 0 ||
            FutureArrays.compareUnsigned(uniquePointValue, 0, 3, minPackedValue, 0, 3) < 0) {
          return Relation.CELL_OUTSIDE_QUERY;
        }
        return Relation.CELL_CROSSES_QUERY;
      }
    };

    // If only one point matches, then the point count is (maxPointsInLeafNode + 1) / 2
    // in general, or maybe 2x that if the point is a split value
    final long pointCount = points.estimatePointCount(onePointMatchVisitor);
    assertTrue(""+pointCount,
        pointCount == (maxPointsInLeafNode + 1) / 2 || // common case
            pointCount == 2*((maxPointsInLeafNode + 1) / 2)); // if the point is a split value

    final long docCount = points.estimateDocCount(onePointMatchVisitor);

    if (multiValues) {
      assertEquals(docCount, (long) (docCount * (1d - Math.pow( (numDocs -  pointCount) / points.size() , points.size() / docCount))));
    } else {
      assertEquals(Math.min(pointCount, numDocs), docCount);
    }
    r.close();
    dir.close();
  }

  // The tree is always balanced in the N dims case, and leaves are
  // not all full so things are a bit different
  public void testEstimatePointCount2Dims() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    byte[][] pointValue = new byte[2][];
    pointValue[0] = new byte[3];
    pointValue[1] = new byte[3];
    byte[][] uniquePointValue = new byte[2][];
    uniquePointValue[0] = new byte[3];
    uniquePointValue[1] = new byte[3];
    random().nextBytes(uniquePointValue[0]);
    random().nextBytes(uniquePointValue[1]);
    final int numDocs = TEST_NIGHTLY? atLeast(10000) : atLeast(1000); // in nightly, make sure we have several leaves
    final boolean multiValues = random().nextBoolean();
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      if (i == numDocs / 2) {
        doc.add(new BinaryPoint("f", uniquePointValue));
      } else {
        final int numValues = (multiValues) ? TestUtil.nextInt(random(), 2, 100) : 1;
        for (int j = 0; j < numValues; j ++) {
          do {
            random().nextBytes(pointValue[0]);
            random().nextBytes(pointValue[1]);
          } while (Arrays.equals(pointValue[0], uniquePointValue[0]) || Arrays.equals(pointValue[1], uniquePointValue[1]));
          doc.add(new BinaryPoint("f", pointValue));
        }
      }
      w.addDocument(doc);
    }
    w.forceMerge(1);
    final IndexReader r = DirectoryReader.open(w);
    w.close();
    final LeafReader lr = getOnlyLeafReader(r);
    PointValues points = lr.getPointValues("f");

    IntersectVisitor allPointsVisitor = new IntersectVisitor() {
      @Override
      public void visit(int docID, byte[] packedValue) throws IOException {}

      @Override
      public void visit(int docID) throws IOException {}

      @Override
      public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        return Relation.CELL_INSIDE_QUERY;
      }
    };

    // If all points match, then the point count is numLeaves * maxPointsInLeafNode
    final int numLeaves = (int) Math.ceil((double) points.size() / maxPointsInLeafNode);

    assertEquals(numLeaves * maxPointsInLeafNode, points.estimatePointCount(allPointsVisitor));
    assertEquals(numDocs, points.estimateDocCount(allPointsVisitor));

    IntersectVisitor noPointsVisitor = new IntersectVisitor() {
      @Override
      public void visit(int docID, byte[] packedValue) throws IOException {}

      @Override
      public void visit(int docID) throws IOException {}

      @Override
      public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        return Relation.CELL_OUTSIDE_QUERY;
      }
    };

    // Return 0 if no points match
    assertEquals(0, points.estimatePointCount(noPointsVisitor));
    assertEquals(0, points.estimateDocCount(noPointsVisitor));

    IntersectVisitor onePointMatchVisitor = new IntersectVisitor() {
      @Override
      public void visit(int docID, byte[] packedValue) throws IOException {}

      @Override
      public void visit(int docID) throws IOException {}

      @Override
      public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        for (int dim = 0; dim < 2; ++dim) {
          if (FutureArrays.compareUnsigned(uniquePointValue[dim], 0, 3, maxPackedValue, dim * 3, dim * 3 + 3) > 0 ||
              FutureArrays.compareUnsigned(uniquePointValue[dim], 0, 3, minPackedValue, dim * 3, dim * 3 + 3) < 0) {
            return Relation.CELL_OUTSIDE_QUERY;
          }
        }
        return Relation.CELL_CROSSES_QUERY;
      }
    };

    final long pointCount = points.estimatePointCount(onePointMatchVisitor);
    // The number of matches needs to be multiple of count per leaf
    final long countPerLeaf = (maxPointsInLeafNode + 1) / 2;
    assertTrue(""+pointCount, pointCount % countPerLeaf == 0);
    // in extreme cases, a point can be be shared by 4 leaves
    assertTrue(""+pointCount, pointCount / countPerLeaf <= 4 && pointCount / countPerLeaf >= 1);

    final long docCount = points.estimateDocCount(onePointMatchVisitor);
    if (multiValues) {
      assertEquals(docCount, (long) (docCount * (1d - Math.pow( (numDocs -  pointCount) / points.size() , points.size() / docCount))));
    } else {
      assertEquals(Math.min(pointCount, numDocs), docCount);
    }
    r.close();
    dir.close();
  }

  public void testDocCountEdgeCases() {
    PointValues values = getPointValues(Long.MAX_VALUE, 1, Long.MAX_VALUE);
    long docs = values.estimateDocCount(null);
    assertEquals(1, docs);
    values = getPointValues(Long.MAX_VALUE, 1, 1);
    docs = values.estimateDocCount(null);
    assertEquals(1, docs);
    values = getPointValues(Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE);
    docs = values.estimateDocCount(null);
    assertEquals(Integer.MAX_VALUE, docs);
    values = getPointValues(Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE / 2);
    docs = values.estimateDocCount(null);
    assertEquals(Integer.MAX_VALUE, docs);
    values = getPointValues(Long.MAX_VALUE, Integer.MAX_VALUE, 1);
    docs = values.estimateDocCount(null);
    assertEquals(1, docs);
  }

  public void testRandomDocCount() {
    for (int i = 0; i < 100; i++) {
      long size = TestUtil.nextLong(random(), 1, Long.MAX_VALUE);
      int maxDoc = (size > Integer.MAX_VALUE) ? Integer.MAX_VALUE : Math.toIntExact(size);
      int docCount = TestUtil.nextInt(random(), 1, maxDoc);
      long estimatedPointCount = TestUtil.nextLong(random(), 0, size);
      PointValues values = getPointValues(size, docCount, estimatedPointCount);
      long docs = values.estimateDocCount(null);
      assertTrue(docs <= estimatedPointCount);
      assertTrue(docs <= maxDoc);
      assertTrue(docs >= estimatedPointCount / (size/docCount));
    }
  }


  private PointValues getPointValues(long size, int docCount, long estimatedPointCount) {
    return new PointValues() {
      @Override
      public void intersect(IntersectVisitor visitor) {
        throw new UnsupportedOperationException();
      }

      @Override
      public long estimatePointCount(IntersectVisitor visitor) {
        return estimatedPointCount;
      }

      @Override
      public byte[] getMinPackedValue() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public byte[] getMaxPackedValue() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getNumDimensions() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getNumIndexDimensions() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getBytesPerDimension() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public long size() {
        return size;
      }

      @Override
      public int getDocCount() {
        return docCount;
      }
    };
  }

}
