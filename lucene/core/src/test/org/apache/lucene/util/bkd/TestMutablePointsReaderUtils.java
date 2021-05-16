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
package org.apache.lucene.util.bkd;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestMutablePointsReaderUtils extends LuceneTestCase {

  public void testSort() {
    for (int iter = 0; iter < 10; ++iter) {
      doTestSort(false);
    }
  }

  public void testSortWithIncrementalDocId() {
    for (int iter = 0; iter < 10; ++iter) {
      doTestSort(true);
    }
  }

  private void doTestSort(boolean isDocIdIncremental) {
    final int bytesPerDim = TestUtil.nextInt(random(), 1, 16);
    final int maxDoc = TestUtil.nextInt(random(), 1, 1 << random().nextInt(30));
    BKDConfig config = new BKDConfig(1, 1, bytesPerDim, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
    Point[] points = createRandomPoints(config, maxDoc, new int[1], isDocIdIncremental);
    DummyPointsReader reader = new DummyPointsReader(points);
    MutablePointsReaderUtils.sort(config, maxDoc, reader, 0, points.length);
    Arrays.sort(points, new Comparator<Point>() {
      @Override
      public int compare(Point o1, Point o2) {
        int cmp = o1.packedValue.compareTo(o2.packedValue);
        if (cmp == 0) {
          cmp = Integer.compare(o1.doc, o2.doc);
        }
        return cmp;
      }
    });
    assertNotSame(points, reader.points);
    assertEquals(points.length, reader.points.length);

    // Check doc IDs are in ascending order.
    // If doc IDs are already increasing, StableMSBRadixSorter should keep doc ID's ordering.
    // If doc IDs are not ordered, StableMSBRadixSorter should compare doc ID to guarantee the
    // ordering.
    Point prevPoint = null;
    for (int i = 0; i < points.length; i++) {
      assertEquals(points[i].packedValue, reader.points[i].packedValue);
      assertSame(points[i].packedValue, reader.points[i].packedValue);
      if (prevPoint != null) {
        if (reader.points[i].packedValue.equals(prevPoint.packedValue)) {
          assertTrue(reader.points[i].doc >= prevPoint.doc);
        }
      }
      prevPoint = reader.points[i];
    }
  }

  public void testSortByDim() {
    for (int iter = 0; iter < 5; ++iter) {
      doTestSortByDim();
    }
  }

  private void doTestSortByDim() {
    BKDConfig config = createRandomConfig();
    final int maxDoc = TestUtil.nextInt(random(), 1, 1 << random().nextInt(30));
    int[] commonPrefixLengths = new int[config.numDims];
    Point[] points = createRandomPoints(config, maxDoc, commonPrefixLengths, false);
    DummyPointsReader reader = new DummyPointsReader(points);
    final int sortedDim = random().nextInt(config.numIndexDims);
    MutablePointsReaderUtils.sortByDim(config, sortedDim, commonPrefixLengths, reader, 0, points.length,
            new BytesRef(), new BytesRef());
    for (int i = 1; i < points.length; ++i) {
      final int offset = sortedDim * config.bytesPerDim;
      BytesRef previousValue = reader.points[i-1].packedValue;
      BytesRef currentValue = reader.points[i].packedValue;
      int cmp = FutureArrays.compareUnsigned(previousValue.bytes, previousValue.offset + offset, previousValue.offset + offset + config.bytesPerDim, currentValue.bytes, currentValue.offset + offset, currentValue.offset + offset + config.bytesPerDim);
      if (cmp == 0) {
        int dataDimOffset = config.packedIndexBytesLength;
        int dataDimsLength = (config.numDims - config.numIndexDims) * config.bytesPerDim;
        cmp = FutureArrays.compareUnsigned(previousValue.bytes, previousValue.offset + dataDimOffset, previousValue.offset + dataDimOffset + dataDimsLength,
                currentValue.bytes, currentValue.offset + dataDimOffset, currentValue.offset + dataDimOffset + dataDimsLength);
        if (cmp == 0) {
          cmp = reader.points[i - 1].doc - reader.points[i].doc;
        }
      }
      assertTrue(cmp <= 0);
    }
  }

  public void testPartition() {
    for (int iter = 0; iter < 5; ++iter) {
      doTestPartition();
    }
  }

  private void doTestPartition() {
    BKDConfig config = createRandomConfig();
    int[] commonPrefixLengths  = new int[config.numDims];
    final int maxDoc = TestUtil.nextInt(random(), 1, 1 << random().nextInt(30));
    Point[] points = createRandomPoints(config, maxDoc, commonPrefixLengths, false);
    final int splitDim =  random().nextInt(config.numIndexDims);
    DummyPointsReader reader = new DummyPointsReader(points);
    final int pivot = TestUtil.nextInt(random(), 0, points.length - 1);
    MutablePointsReaderUtils.partition(config, maxDoc, splitDim, commonPrefixLengths[splitDim], reader, 0, points.length, pivot,
            new BytesRef(), new BytesRef());
    BytesRef pivotValue = reader.points[pivot].packedValue;
    int offset = splitDim * config.bytesPerDim;
    for (int i = 0; i < points.length; ++i) {
      BytesRef value = reader.points[i].packedValue;
      int cmp = FutureArrays.compareUnsigned(value.bytes, value.offset + offset, value.offset + offset + config.bytesPerDim,
              pivotValue.bytes, pivotValue.offset + offset, pivotValue.offset + offset + config.bytesPerDim);
      if (cmp == 0) {
        int dataDimOffset = config.packedIndexBytesLength;
        int dataDimsLength = (config.numDims - config.numIndexDims) * config.bytesPerDim;
        cmp = FutureArrays.compareUnsigned(value.bytes, value.offset + dataDimOffset, value.offset + dataDimOffset + dataDimsLength,
                pivotValue.bytes, pivotValue.offset + dataDimOffset, pivotValue.offset + dataDimOffset + dataDimsLength);
        if (cmp == 0) {
          cmp = reader.points[i].doc - reader.points[pivot].doc;
        }
      }
      if (i < pivot) {
        assertTrue(cmp <= 0);
      } else if (i > pivot) {
        assertTrue(cmp >= 0);
      } else {
        assertEquals(0, cmp);
      }
    }
  }

  private static BKDConfig createRandomConfig() {
    final int numIndexDims = TestUtil.nextInt(random(), 1, BKDConfig.MAX_INDEX_DIMS);
    final int numDims = TestUtil.nextInt(random(), numIndexDims, BKDConfig.MAX_DIMS);
    final int bytesPerDim = TestUtil.nextInt(random(), 1, 16);
    final int maxPointsInLeafNode = TestUtil.nextInt(random(), 50, 2000);
    return new BKDConfig(numDims, numIndexDims, bytesPerDim, maxPointsInLeafNode);
  }

  private static Point[] createRandomPoints(BKDConfig config, int maxDoc, int[] commonPrefixLengths, boolean isDocIdIncremental) {
    assertTrue(commonPrefixLengths.length == config.numDims);
    final int numPoints = TestUtil.nextInt(random(), 1, 100000);
    Point[] points = new Point[numPoints];
    if (random().nextInt(10) != 0) {
      for (int i = 0; i < numPoints; ++i) {
        byte[] value = new byte[config.packedBytesLength];
        random().nextBytes(value);
        points[i] = new Point(value, isDocIdIncremental ? Math.min(i, maxDoc - 1) : random().nextInt(maxDoc));
      }
      for (int i = 0; i < config.numDims; ++i) {
        commonPrefixLengths[i] = TestUtil.nextInt(random(), 0, config.bytesPerDim);
      }
      BytesRef firstValue = points[0].packedValue;
      for (int i = 1; i < points.length; ++i) {
        for (int dim = 0; dim < config.numDims; ++dim) {
          int offset = dim * config.bytesPerDim;
          BytesRef packedValue = points[i].packedValue;
          System.arraycopy(firstValue.bytes, firstValue.offset + offset, packedValue.bytes, packedValue.offset + offset, commonPrefixLengths[dim]);
        }
      }
    } else {
      //index dim are equal, data dims different
      int numDataDims = config.numDims - config.numIndexDims;
      byte[] indexDims = new byte[config.packedIndexBytesLength];
      random().nextBytes(indexDims);
      byte[] dataDims = new byte[numDataDims * config.bytesPerDim];
      for (int i = 0; i < numPoints; ++i) {
        byte[] value = new byte[config.packedBytesLength];
        System.arraycopy(indexDims, 0, value, 0, config.packedIndexBytesLength);
        random().nextBytes(dataDims);
        System.arraycopy(dataDims, 0, value, config.packedIndexBytesLength, numDataDims * config.bytesPerDim);
        points[i] = new Point(value, isDocIdIncremental ? Math.min(i, maxDoc - 1) : random().nextInt(maxDoc));
      }
      for (int i = 0; i < config.numIndexDims; ++i) {
        commonPrefixLengths[i] = config.bytesPerDim;
      }
      for (int i = config.numIndexDims; i < config.numDims; ++i) {
        commonPrefixLengths[i] = TestUtil.nextInt(random(), 0, config.bytesPerDim);
      }
      BytesRef firstValue = points[0].packedValue;
      for (int i = 1; i < points.length; ++i) {
        for (int dim = config.numIndexDims; dim < config.numDims; ++dim) {
          int offset = dim * config.bytesPerDim;
          BytesRef packedValue = points[i].packedValue;
          System.arraycopy(firstValue.bytes, firstValue.offset + offset, packedValue.bytes, packedValue.offset + offset, commonPrefixLengths[dim]);
        }
      }
    }
    return points;
  }

  private static class Point {
    final BytesRef packedValue;
    final int doc;

    Point(byte[] packedValue, int doc) {
      // use a non-null offset to make sure MutablePointsReaderUtils does not ignore it
      this.packedValue = new BytesRef(packedValue.length + 1);
      this.packedValue.bytes[0] = (byte) random().nextInt(256);
      this.packedValue.offset = 1;
      this.packedValue.length = packedValue.length;
      System.arraycopy(packedValue, 0, this.packedValue.bytes, 1, packedValue.length);
      this.doc = doc;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || obj instanceof Point == false) {
        return false;
      }
      Point that = (Point) obj;
      return packedValue.equals(that.packedValue) && doc == that.doc;
    }

    @Override
    public int hashCode() {
      return 31 * packedValue.hashCode() + doc;
    }

    @Override
    public String toString() {
      return "value=" + packedValue + " doc=" + doc;
    }
  }

  private static class DummyPointsReader extends MutablePointValues {

    private final Point[] points;

    private Point[] temp;

    DummyPointsReader(Point[] points) {
      this.points = points.clone();
    }

    @Override
    public void getValue(int i, BytesRef packedValue) {
      packedValue.bytes = points[i].packedValue.bytes;
      packedValue.offset = points[i].packedValue.offset;
      packedValue.length = points[i].packedValue.length;
    }

    @Override
    public byte getByteAt(int i, int k) {
      BytesRef packedValue = points[i].packedValue;
      return packedValue.bytes[packedValue.offset + k];
    }

    @Override
    public int getDocID(int i) {
      return points[i].doc;
    }

    @Override
    public void swap(int i, int j) {
      ArrayUtil.swap(points, i, j);
    }

    @Override
    public void intersect(IntersectVisitor visitor) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long estimatePointCount(IntersectVisitor visitor) {
      throw new UnsupportedOperationException();
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
      throw new UnsupportedOperationException();
    }

    @Override
    public int getDocCount() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void save(int i, int j) {
      if (temp == null) {
        temp = new Point[points.length];
      }
      temp[j] = points[i];
    }

    @Override
    public void restore(int i, int j) {
      if (temp != null) {
        System.arraycopy(temp, i, points, i, j - i);
      }
    }
  }

}
