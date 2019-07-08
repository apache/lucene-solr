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
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestMutablePointsReaderUtils extends LuceneTestCase {

  public void testSort() {
    for (int iter = 0; iter < 5; ++iter) {
      doTestSort();
    }
  }

  private void doTestSort() {
    final int bytesPerDim = TestUtil.nextInt(random(), 1, 16);
    final int maxDoc = TestUtil.nextInt(random(), 1, 1 << random().nextInt(30));
    Point[] points = createRandomPoints(1, 1, bytesPerDim, maxDoc, new int[1]);
    DummyPointsReader reader = new DummyPointsReader(points);
    MutablePointsReaderUtils.sort(maxDoc, bytesPerDim, reader, 0, points.length);
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
    assertArrayEquals(points, reader.points);
  }

  public void testSortByDim() {
    for (int iter = 0; iter < 5; ++iter) {
      doTestSortByDim();
    }
  }

  private void doTestSortByDim() {
    final int numIndexDims = TestUtil.nextInt(random(), 1, 8);
    final int numDataDims = TestUtil.nextInt(random(), numIndexDims, 8);
    final int bytesPerDim = TestUtil.nextInt(random(), 1, 16);
    final int maxDoc = TestUtil.nextInt(random(), 1, 1 << random().nextInt(30));
    int[] commonPrefixLengths = new int[numDataDims];
    Point[] points = createRandomPoints(numDataDims, numIndexDims, bytesPerDim, maxDoc, commonPrefixLengths);
    DummyPointsReader reader = new DummyPointsReader(points);
    final int sortedDim = random().nextInt(numIndexDims);
    MutablePointsReaderUtils.sortByDim(numDataDims, numIndexDims, sortedDim, bytesPerDim, commonPrefixLengths, reader, 0, points.length,
        new BytesRef(), new BytesRef());
    for (int i = 1; i < points.length; ++i) {
      final int offset = sortedDim * bytesPerDim;
      BytesRef previousValue = reader.points[i-1].packedValue;
      BytesRef currentValue = reader.points[i].packedValue;
      int cmp = Arrays.compareUnsigned(previousValue.bytes, previousValue.offset + offset, previousValue.offset + offset + bytesPerDim, currentValue.bytes, currentValue.offset + offset, currentValue.offset + offset + bytesPerDim);
      if (cmp == 0) {
        int dataDimOffset = numIndexDims * bytesPerDim;
        int dataDimsLength = (numDataDims - numIndexDims) * bytesPerDim;
        cmp = Arrays.compareUnsigned(previousValue.bytes, previousValue.offset + dataDimOffset, previousValue.offset + dataDimOffset + dataDimsLength,
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
    final int numIndexDims = TestUtil.nextInt(random(), 1, 8);
    final int numDataDims = TestUtil.nextInt(random(), numIndexDims, 8);
    final int bytesPerDim = TestUtil.nextInt(random(), 1, 16);
    int[] commonPrefixLengths  = new int[numDataDims];
    final int maxDoc = TestUtil.nextInt(random(), 1, 1 << random().nextInt(30));
    Point[] points = createRandomPoints(numDataDims, numIndexDims, bytesPerDim, maxDoc, commonPrefixLengths);
    final int splitDim =  random().nextInt(numIndexDims);
    DummyPointsReader reader = new DummyPointsReader(points);
    final int pivot = TestUtil.nextInt(random(), 0, points.length - 1);
    MutablePointsReaderUtils.partition(numDataDims, numIndexDims, maxDoc, splitDim, bytesPerDim, commonPrefixLengths[splitDim], reader, 0, points.length, pivot,
        new BytesRef(), new BytesRef());
    BytesRef pivotValue = reader.points[pivot].packedValue;
    int offset = splitDim * bytesPerDim;
    for (int i = 0; i < points.length; ++i) {
      BytesRef value = reader.points[i].packedValue;
      int cmp = Arrays.compareUnsigned(value.bytes, value.offset + offset, value.offset + offset + bytesPerDim,
          pivotValue.bytes, pivotValue.offset + offset, pivotValue.offset + offset + bytesPerDim);
      if (cmp == 0) {
        int dataDimOffset = numIndexDims * bytesPerDim;
        int dataDimsLength = (numDataDims - numIndexDims) * bytesPerDim;
        cmp = Arrays.compareUnsigned(value.bytes, value.offset + dataDimOffset, value.offset + dataDimOffset + dataDimsLength,
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

  private static Point[] createRandomPoints(int numDataDims, int numIndexdims, int bytesPerDim, int maxDoc, int[] commonPrefixLengths) {
    assertTrue(commonPrefixLengths.length == numDataDims);
    final int packedBytesLength = numDataDims * bytesPerDim;
    final int numPoints = TestUtil.nextInt(random(), 1, 100000);
    Point[] points = new Point[numPoints];
    if (random().nextInt(5) != 0) {
      for (int i = 0; i < numPoints; ++i) {
        byte[] value = new byte[packedBytesLength];
        random().nextBytes(value);
        points[i] = new Point(value, random().nextInt(maxDoc));
      }
      for (int i = 0; i < numDataDims; ++i) {
        commonPrefixLengths[i] = TestUtil.nextInt(random(), 0, bytesPerDim);
      }
      BytesRef firstValue = points[0].packedValue;
      for (int i = 1; i < points.length; ++i) {
        for (int dim = 0; dim < numDataDims; ++dim) {
          int offset = dim * bytesPerDim;
          BytesRef packedValue = points[i].packedValue;
          System.arraycopy(firstValue.bytes, firstValue.offset + offset, packedValue.bytes, packedValue.offset + offset, commonPrefixLengths[dim]);
        }
      }
    } else {
      //index dim are equal, data dims different
      byte[] indexDims = new byte[numIndexdims * bytesPerDim];
      random().nextBytes(indexDims);
      byte[] dataDims = new byte[(numDataDims - numIndexdims) * bytesPerDim];
      for (int i = 0; i < numPoints; ++i) {
        byte[] value = new byte[packedBytesLength];
        System.arraycopy(indexDims, 0, value, 0, numIndexdims * bytesPerDim);
        random().nextBytes(dataDims);
        System.arraycopy(dataDims, 0, value, numIndexdims * bytesPerDim, (numDataDims - numIndexdims) * bytesPerDim);
        points[i] = new Point(value, random().nextInt(maxDoc));
      }
      for (int i = 0; i < numIndexdims; ++i) {
        commonPrefixLengths[i] = bytesPerDim;
      }
      for (int i = numDataDims; i < numDataDims; ++i) {
        commonPrefixLengths[i] = TestUtil.nextInt(random(), 0, bytesPerDim);
      }
      BytesRef firstValue = points[0].packedValue;
      for (int i = 1; i < points.length; ++i) {
        for (int dim = numIndexdims; dim < numDataDims; ++dim) {
          int offset = dim * bytesPerDim;
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
    public int getNumDataDimensions() throws IOException {
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

  }

}
