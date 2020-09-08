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

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;

public class TestBKDRadixSelector extends LuceneTestCase {

  public void testBasic() throws IOException {
    int values = 4;
    Directory dir = getDirectory(values);
    int middle = 2;
    int dimensions = 1;
    int bytesPerDimensions = Integer.BYTES;
    BKDConfig config = new BKDConfig(dimensions, dimensions, bytesPerDimensions, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
    PointWriter points = getRandomPointWriter(config, dir, values);
    byte[] value = new byte[config.packedBytesLength];
    NumericUtils.intToSortableBytes(1, value, 0);
    points.append(value, 0);
    NumericUtils.intToSortableBytes(2, value, 0);
    points.append(value, 1);
    NumericUtils.intToSortableBytes(3, value, 0);
    points.append(value, 2);
    NumericUtils.intToSortableBytes(4, value, 0);
    points.append(value, 3);
    points.close();
    PointWriter copy = copyPoints(config, dir,points);
    verify(config, dir, copy, 0, values, middle, 0);
    dir.close();
  }

  public void testRandomBinaryTiny() throws Exception {
    doTestRandomBinary(10);
  }

  public void testRandomBinaryMedium() throws Exception {
    doTestRandomBinary(25000);
  }

  @Nightly
  public void testRandomBinaryBig() throws Exception {
    doTestRandomBinary(500000);
  }

  private void doTestRandomBinary(int count) throws IOException {
    BKDConfig config = getRandomConfig();
    int values = TestUtil.nextInt(random(), count, count*2);
    Directory dir = getDirectory(values);
    int start;
    int end;
    if (random().nextBoolean()) {
      start = 0;
      end = values;
    } else  {
      start = TestUtil.nextInt(random(), 0, values -3);
      end = TestUtil.nextInt(random(), start  + 2, values);
    }
    int partitionPoint = TestUtil.nextInt(random(), start + 1, end - 1);
    int sortedOnHeap = random().nextInt(5000);
    PointWriter points = getRandomPointWriter(config, dir, values);
    byte[] value = new byte[config.packedBytesLength];
    for (int i =0; i < values; i++) {
      random().nextBytes(value);
      points.append(value, i);
    }
    points.close();
    verify(config, dir, points,start, end, partitionPoint, sortedOnHeap);
    dir.close();
  }

  public void testRandomAllDimensionsEquals() throws IOException {
    int dimensions =  TestUtil.nextInt(random(), 1, BKDConfig.MAX_INDEX_DIMS);
    int bytesPerDimensions = TestUtil.nextInt(random(), 2, 30);
    BKDConfig config = new BKDConfig(dimensions, dimensions, bytesPerDimensions, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
    int values =  TestUtil.nextInt(random(), 15000, 20000);
    Directory dir = getDirectory(values);
    int partitionPoint = random().nextInt(values);
    int sortedOnHeap = random().nextInt(5000);
    PointWriter points = getRandomPointWriter(config, dir, values);
    byte[] value = new byte[config.packedBytesLength];
    random().nextBytes(value);
    for (int i =0; i < values; i++) {
      if (random().nextBoolean()) {
        points.append(value, i);
      } else {
        points.append(value, random().nextInt(values));
      }
    }
    points.close();
    verify(config, dir, points, 0, values, partitionPoint, sortedOnHeap);
    dir.close();
  }

  public void testRandomLastByteTwoValues() throws IOException {
    int values = random().nextInt(15000) + 1;
    Directory dir = getDirectory(values);
    int partitionPoint = random().nextInt(values);
    int sortedOnHeap = random().nextInt(5000);
    BKDConfig config = getRandomConfig();
    PointWriter points = getRandomPointWriter(config, dir, values);
    byte[] value = new byte[config.packedBytesLength];
    random().nextBytes(value);
    for (int i =0; i < values; i++) {
      if (random().nextBoolean()) {
        points.append(value, 1);
      } else {
        points.append(value, 2);
      }
    }
    points.close();
    verify(config, dir, points, 0, values, partitionPoint, sortedOnHeap);
    dir.close();
  }

  public void testRandomAllDocsEquals() throws IOException {
    int values = random().nextInt(15000) + 1;
    Directory dir = getDirectory(values);
    int partitionPoint = random().nextInt(values);
    int sortedOnHeap = random().nextInt(5000);
    BKDConfig config = getRandomConfig();
    PointWriter points = getRandomPointWriter(config, dir, values);
    byte[] value = new byte[config.packedBytesLength];
    random().nextBytes(value);
    for (int i =0; i < values; i++) {
      points.append(value, 0);
    }
    points.close();
    verify(config, dir, points, 0, values, partitionPoint, sortedOnHeap);
    dir.close();
  }

  public void testRandomFewDifferentValues() throws IOException {
    BKDConfig config = getRandomConfig();
    int values = atLeast(15000);
    Directory dir = getDirectory(values);
    int partitionPoint = random().nextInt(values);
    int sortedOnHeap = random().nextInt(5000);
    PointWriter points = getRandomPointWriter(config, dir, values);
    int numberValues = random().nextInt(8) + 2;
    byte[][] differentValues = new byte[numberValues][config.packedBytesLength];
    for (int i =0; i < numberValues; i++) {
      random().nextBytes(differentValues[i]);
    }
    for (int i =0; i < values; i++) {
      points.append(differentValues[random().nextInt(numberValues)], i);
    }
    points.close();
    verify(config, dir, points, 0, values, partitionPoint, sortedOnHeap);
    dir.close();
  }

  public void testRandomDataDimDiffValues() throws IOException {
    BKDConfig config = getRandomConfig();
    int values = atLeast(15000);
    Directory dir = getDirectory(values);
    int partitionPoint = random().nextInt(values);
    int sortedOnHeap = random().nextInt(5000);
    PointWriter points = getRandomPointWriter(config, dir, values);
    byte[] value = new byte[config.packedBytesLength];
    int dataOnlyDims = config.numDims - config.numIndexDims;
    byte[] dataValue = new byte[dataOnlyDims * config.bytesPerDim];
    random().nextBytes(value);
    for (int i =0; i < values; i++) {
      random().nextBytes(dataValue);
      System.arraycopy(dataValue, 0, value, config.numIndexDims * config.bytesPerDim, dataOnlyDims * config.bytesPerDim);
      points.append(value, i);
    }
    points.close();
    verify(config, dir, points, 0, values, partitionPoint, sortedOnHeap);
    dir.close();
  }

  private void verify(BKDConfig config, Directory dir, PointWriter points, long start, long end, long middle, int sortedOnHeap) throws IOException{
    BKDRadixSelector radixSelector = new BKDRadixSelector(config, sortedOnHeap, dir, "test");
    int dataOnlyDims = config.numDims - config.numIndexDims;
    // we only split by indexed dimension so we check for each only those dimension
    for (int splitDim = 0; splitDim < config.numIndexDims; splitDim++) {
      // We need to make a copy of the data as it is deleted in the process
      BKDRadixSelector.PathSlice inputSlice = new BKDRadixSelector.PathSlice(copyPoints(config, dir, points), 0, points.count());
      int commonPrefixLengthInput = getRandomCommonPrefix(config, inputSlice, splitDim);
      BKDRadixSelector.PathSlice[] slices = new BKDRadixSelector.PathSlice[2];
      byte[] partitionPoint = radixSelector.select(inputSlice, slices, start, end, middle, splitDim, commonPrefixLengthInput);
      assertEquals(middle - start, slices[0].count);
      assertEquals(end - middle, slices[1].count);
      // check that left and right slices contain the correct points
      byte[] max = getMax(config, slices[0], splitDim);
      byte[] min = getMin(config, slices[1], splitDim);
      int cmp = FutureArrays.compareUnsigned(max, 0, config.bytesPerDim, min, 0, config.bytesPerDim);
      assertTrue(cmp <= 0);
      if (cmp == 0) {
        byte[] maxDataDim = getMaxDataDimension(config, slices[0], max, splitDim);
        byte[] minDataDim = getMinDataDimension(config, slices[1], min, splitDim);
        cmp = FutureArrays.compareUnsigned(maxDataDim, 0, dataOnlyDims * config.bytesPerDim, minDataDim, 0, dataOnlyDims * config.bytesPerDim);
        assertTrue(cmp <= 0);
        if (cmp == 0) {
          int maxDocID = getMaxDocId(config, slices[0], splitDim, partitionPoint, maxDataDim);
          int minDocId = getMinDocId(config, slices[1], splitDim, partitionPoint, minDataDim);
          assertTrue(minDocId >= maxDocID);
        }
      }
      assertTrue(Arrays.equals(partitionPoint, min));
      slices[0].writer.destroy();
      slices[1].writer.destroy();
    }
    points.destroy();
  }

  private PointWriter copyPoints(BKDConfig config, Directory dir, PointWriter points) throws IOException {
    try (PointWriter copy  = getRandomPointWriter(config, dir, points.count());
         PointReader reader = points.getReader(0, points.count())) {
      while (reader.next()) {
        copy.append(reader.pointValue());
      }
      return copy;
    }
  }

  /** returns a common prefix length equal or lower than the current one */
  private int getRandomCommonPrefix(BKDConfig config, BKDRadixSelector.PathSlice inputSlice, int splitDim) throws IOException {
    byte[] pointsMax = getMax(config, inputSlice, splitDim);
    byte[] pointsMin = getMin(config, inputSlice, splitDim);
    int commonPrefixLength = FutureArrays.mismatch(pointsMin, 0, config.bytesPerDim, pointsMax, 0, config.bytesPerDim);
    if (commonPrefixLength == -1) {
      commonPrefixLength = config.bytesPerDim;
    }
    return (random().nextBoolean()) ? commonPrefixLength : commonPrefixLength == 0 ? 0 : random().nextInt(commonPrefixLength);
  }

  private PointWriter getRandomPointWriter(BKDConfig config, Directory dir, long numPoints) throws IOException {
    if (numPoints < 4096 && random().nextBoolean()) {
      return new HeapPointWriter(config, Math.toIntExact(numPoints));
    } else {
      return new OfflinePointWriter(config, dir, "test", "data", numPoints);
    }
  }

  private Directory getDirectory(int numPoints) {
    Directory dir;
    if (numPoints > 100000) {
      dir = newFSDirectory(createTempDir("TestBKDTRadixSelector"));
    } else {
      dir = newDirectory();
    }
    return dir;
  }

  private byte[] getMin(BKDConfig config, BKDRadixSelector.PathSlice pathSlice, int dimension) throws  IOException {
    byte[] min = new byte[config.bytesPerDim];
    Arrays.fill(min, (byte) 0xff);
    try (PointReader reader = pathSlice.writer.getReader(pathSlice.start, pathSlice.count)) {
      byte[] value = new byte[config.bytesPerDim];

      while (reader.next()) {
        PointValue pointValue = reader.pointValue();
        BytesRef packedValue = pointValue.packedValue();
        System.arraycopy(packedValue.bytes, packedValue.offset + dimension * config.bytesPerDim, value, 0, config.bytesPerDim);
        if (FutureArrays.compareUnsigned(min, 0, config.bytesPerDim, value, 0, config.bytesPerDim) > 0) {
          System.arraycopy(value, 0, min, 0, config.bytesPerDim);
        }
      }
    }
    return min;
  }

  private int getMinDocId(BKDConfig config, BKDRadixSelector.PathSlice p, int dimension, byte[] partitionPoint, byte[] dataDim) throws  IOException {
    int docID = Integer.MAX_VALUE;
    try (PointReader reader = p.writer.getReader(p.start, p.count)) {
      while (reader.next()) {
        PointValue pointValue = reader.pointValue();
        BytesRef packedValue = pointValue.packedValue();
        int offset = dimension * config.bytesPerDim;
        int dataOffset = config.packedIndexBytesLength;
        int dataLength = (config.numDims - config.numIndexDims) * config.bytesPerDim;
        if (FutureArrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + config.bytesPerDim, partitionPoint, 0, config.bytesPerDim) == 0
                && FutureArrays.compareUnsigned(packedValue.bytes, packedValue.offset + dataOffset, packedValue.offset + dataOffset + dataLength, dataDim, 0, dataLength) == 0) {
          int newDocID = pointValue.docID();
          if (newDocID < docID) {
            docID = newDocID;
          }
        }
      }
    }
    return docID;
  }

  private byte[] getMinDataDimension(BKDConfig config, BKDRadixSelector.PathSlice p, byte[] minDim, int splitDim) throws  IOException {
    final int numDataDims = config.numDims - config.numIndexDims;
    byte[] min = new byte[numDataDims * config.bytesPerDim];
    Arrays.fill(min, (byte) 0xff);
    int offset = splitDim * config.bytesPerDim;
    try (PointReader reader = p.writer.getReader(p.start, p.count)) {
      byte[] value = new byte[numDataDims * config.bytesPerDim];
      while (reader.next()) {
        PointValue pointValue = reader.pointValue();
        BytesRef packedValue = pointValue.packedValue();
        if (FutureArrays.mismatch(minDim, 0, config.bytesPerDim, packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + config.bytesPerDim) == -1) {
          System.arraycopy(packedValue.bytes, packedValue.offset + config.numIndexDims * config.bytesPerDim, value, 0, numDataDims * config.bytesPerDim);
          if (FutureArrays.compareUnsigned(min, 0, numDataDims * config.bytesPerDim, value, 0, numDataDims * config.bytesPerDim) > 0) {
            System.arraycopy(value, 0, min, 0, numDataDims * config.bytesPerDim);
          }
        }
      }
    }
    return min;
  }

  private byte[] getMax(BKDConfig config, BKDRadixSelector.PathSlice p, int dimension) throws  IOException {
    byte[] max = new byte[config.bytesPerDim];
    Arrays.fill(max, (byte) 0);
    try (PointReader reader = p.writer.getReader(p.start, p.count)) {
      byte[] value = new byte[config.bytesPerDim];
      while (reader.next()) {
        PointValue pointValue = reader.pointValue();
        BytesRef packedValue = pointValue.packedValue();
        System.arraycopy(packedValue.bytes, packedValue.offset + dimension * config.bytesPerDim, value, 0, config.bytesPerDim);
        if (FutureArrays.compareUnsigned(max, 0, config.bytesPerDim, value, 0, config.bytesPerDim) < 0) {
          System.arraycopy(value, 0, max, 0, config.bytesPerDim);
        }
      }
    }
    return max;
  }

  private byte[] getMaxDataDimension(BKDConfig config, BKDRadixSelector.PathSlice p, byte[] maxDim, int splitDim) throws  IOException {
    final int numDataDims = config.numDims - config.numIndexDims;
    byte[] max = new byte[numDataDims * config.bytesPerDim];
    Arrays.fill(max, (byte) 0);
    int offset = splitDim * config.bytesPerDim;
    try (PointReader reader = p.writer.getReader(p.start, p.count)) {
      byte[] value = new byte[numDataDims * config.bytesPerDim];
      while (reader.next()) {
        PointValue pointValue = reader.pointValue();
        BytesRef packedValue = pointValue.packedValue();
        if (FutureArrays.mismatch(maxDim, 0, config.bytesPerDim, packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + config.bytesPerDim) == -1) {
          System.arraycopy(packedValue.bytes, packedValue.offset + config.packedIndexBytesLength, value, 0, numDataDims * config.bytesPerDim);
          if (FutureArrays.compareUnsigned(max, 0, numDataDims * config.bytesPerDim, value, 0, numDataDims * config.bytesPerDim) < 0) {
            System.arraycopy(value, 0, max, 0, numDataDims * config.bytesPerDim);
          }
        }
      }
    }
    return max;
  }

  private int getMaxDocId(BKDConfig config, BKDRadixSelector.PathSlice p, int dimension, byte[] partitionPoint, byte[] dataDim) throws  IOException {
    int docID = Integer.MIN_VALUE;
    try (PointReader reader = p.writer.getReader(p.start, p.count)) {
      while (reader.next()) {
        PointValue pointValue = reader.pointValue();
        BytesRef packedValue = pointValue.packedValue();
        int offset = dimension * config.bytesPerDim;
        int dataOffset = config.packedIndexBytesLength;
        int dataLength = (config.numDims - config.numIndexDims) * config.bytesPerDim;
        if (FutureArrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + config.bytesPerDim, partitionPoint, 0, config.bytesPerDim) == 0
                && FutureArrays.compareUnsigned(packedValue.bytes, packedValue.offset + dataOffset, packedValue.offset + dataOffset + dataLength, dataDim, 0, dataLength) == 0) {
          int newDocID = pointValue.docID();
          if (newDocID > docID) {
            docID = newDocID;
          }
        }
      }
    }
    return docID;
  }

  private BKDConfig getRandomConfig() {
    int numIndexDims = TestUtil.nextInt(random(), 1, BKDConfig.MAX_INDEX_DIMS);
    int numDims = TestUtil.nextInt(random(), numIndexDims, BKDConfig.MAX_DIMS);
    int bytesPerDim = TestUtil.nextInt(random(), 2, 30);
    int maxPointsInLeafNode = TestUtil.nextInt(random(), 50, 2000);
    return new BKDConfig(numDims, numIndexDims, bytesPerDim, maxPointsInLeafNode);
  }
}
