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
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;

public class TestBKDRadixSelector extends LuceneTestCase {

  public void testBasic() throws IOException {
    int values = 4;
    Directory dir = getDirectory(values);
    int middle = 2;
    int dimensions =1;
    int bytesPerDimensions = Integer.BYTES;
    int packedLength = dimensions * bytesPerDimensions;
    PointWriter points = getRandomPointWriter(dir, values, packedLength);
    byte[] value = new byte[packedLength];
    NumericUtils.intToSortableBytes(1, value, 0);
    points.append(value, 0);
    NumericUtils.intToSortableBytes(2, value, 0);
    points.append(value, 1);
    NumericUtils.intToSortableBytes(3, value, 0);
    points.append(value, 2);
    NumericUtils.intToSortableBytes(4, value, 0);
    points.append(value, 3);
    points.close();
    PointWriter copy = copyPoints(dir,points, packedLength);
    verify(dir, copy, dimensions, dimensions, 0, values, middle, packedLength, bytesPerDimensions, 0);
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
    int indexDimensions =  TestUtil.nextInt(random(), 1, 8);
    int dataDimensions =  TestUtil.nextInt(random(), indexDimensions, 8);
    int bytesPerDimensions = TestUtil.nextInt(random(), 2, 30);
    int packedLength = dataDimensions * bytesPerDimensions;
    PointWriter points = getRandomPointWriter(dir, values, packedLength);
    byte[] value = new byte[packedLength];
    for (int i =0; i < values; i++) {
      random().nextBytes(value);
      points.append(value, i);
    }
    points.close();
    verify(dir, points, dataDimensions, indexDimensions, start, end, partitionPoint, packedLength, bytesPerDimensions, sortedOnHeap);
    dir.close();
  }

  public void testRandomAllDimensionsEquals() throws IOException {
    int values =  TestUtil.nextInt(random(), 15000, 20000);
    Directory dir = getDirectory(values);
    int partitionPoint = random().nextInt(values);
    int sortedOnHeap = random().nextInt(5000);
    int dimensions =  TestUtil.nextInt(random(), 1, 8);
    int bytesPerDimensions = TestUtil.nextInt(random(), 2, 30);
    int packedLength = dimensions * bytesPerDimensions;
    PointWriter points = getRandomPointWriter(dir, values, packedLength);
    byte[] value = new byte[packedLength];
    random().nextBytes(value);
    for (int i =0; i < values; i++) {
      if (random().nextBoolean()) {
        points.append(value, i);
      } else {
        points.append(value, random().nextInt(values));
      }
    }
    points.close();
    verify(dir, points, dimensions, dimensions, 0, values, partitionPoint, packedLength, bytesPerDimensions, sortedOnHeap);
    dir.close();
  }

  public void testRandomLastByteTwoValues() throws IOException {
    int values = random().nextInt(15000) + 1;
    Directory dir = getDirectory(values);
    int partitionPoint = random().nextInt(values);
    int sortedOnHeap = random().nextInt(5000);
    int indexDimensions =  TestUtil.nextInt(random(), 1, 8);
    int dataDimensions =  TestUtil.nextInt(random(), indexDimensions, 8);
    int bytesPerDimensions = TestUtil.nextInt(random(), 2, 30);
    int packedLength = dataDimensions * bytesPerDimensions;
    PointWriter points = getRandomPointWriter(dir, values, packedLength);
    byte[] value = new byte[packedLength];
    random().nextBytes(value);
    for (int i =0; i < values; i++) {
      if (random().nextBoolean()) {
        points.append(value, 1);
      } else {
        points.append(value, 2);
      }
    }
    points.close();
    verify(dir, points, dataDimensions, indexDimensions, 0, values, partitionPoint, packedLength, bytesPerDimensions, sortedOnHeap);
    dir.close();
  }

  public void testRandomAllDocsEquals() throws IOException {
    int values = random().nextInt(15000) + 1;
    Directory dir = getDirectory(values);
    int partitionPoint = random().nextInt(values);
    int sortedOnHeap = random().nextInt(5000);
    int indexDimensions =  TestUtil.nextInt(random(), 1, 8);
    int dataDimensions =  TestUtil.nextInt(random(), indexDimensions, 8);
    int bytesPerDimensions = TestUtil.nextInt(random(), 2, 30);
    int packedLength = dataDimensions * bytesPerDimensions;
    PointWriter points = getRandomPointWriter(dir, values, packedLength);
    byte[] value = new byte[packedLength];
    random().nextBytes(value);
    for (int i =0; i < values; i++) {
      points.append(value, 0);
    }
    points.close();
    verify(dir, points, dataDimensions, indexDimensions, 0, values, partitionPoint, packedLength, bytesPerDimensions, sortedOnHeap);
    dir.close();
  }

  public void testRandomFewDifferentValues() throws IOException {
    int values = atLeast(15000);
    Directory dir = getDirectory(values);
    int partitionPoint = random().nextInt(values);
    int sortedOnHeap = random().nextInt(5000);
    int indexDimensions =  TestUtil.nextInt(random(), 1, 8);
    int dataDimensions =  TestUtil.nextInt(random(), indexDimensions, 8);
    int bytesPerDimensions = TestUtil.nextInt(random(), 2, 30);
    int packedLength = dataDimensions * bytesPerDimensions;
    PointWriter points = getRandomPointWriter(dir, values, packedLength);
    int numberValues = random().nextInt(8) + 2;
    byte[][] differentValues = new byte[numberValues][packedLength];
    for (int i =0; i < numberValues; i++) {
      random().nextBytes(differentValues[i]);
    }
    for (int i =0; i < values; i++) {
      points.append(differentValues[random().nextInt(numberValues)], i);
    }
    points.close();
    verify(dir, points, dataDimensions, indexDimensions, 0, values, partitionPoint, packedLength, bytesPerDimensions, sortedOnHeap);
    dir.close();
  }

  public void testRandomDataDimDiffValues() throws IOException {
    int values = atLeast(15000);
    Directory dir = getDirectory(values);
    int partitionPoint = random().nextInt(values);
    int sortedOnHeap = random().nextInt(5000);
    int indexDimensions =  TestUtil.nextInt(random(), 1, 8);
    int dataDimensions =  TestUtil.nextInt(random(), indexDimensions, 8);
    int bytesPerDimensions = TestUtil.nextInt(random(), 2, 30);
    int packedLength = dataDimensions * bytesPerDimensions;
    PointWriter points = getRandomPointWriter(dir, values, packedLength);
    byte[] value = new byte[packedLength];
    byte[] dataValue = new byte[(dataDimensions - indexDimensions) * bytesPerDimensions];
    random().nextBytes(value);
    for (int i =0; i < values; i++) {
      random().nextBytes(dataValue);
      System.arraycopy(dataValue, 0, value, indexDimensions * bytesPerDimensions, (dataDimensions - indexDimensions) * bytesPerDimensions);
      points.append(value, i);
    }
    points.close();
    verify(dir, points, dataDimensions, indexDimensions, 0, values, partitionPoint, packedLength, bytesPerDimensions, sortedOnHeap);
    dir.close();
  }

  private void verify(Directory dir, PointWriter points, int dataDimensions, int indexDimensions, long start, long end, long middle, int packedLength, int bytesPerDimensions, int sortedOnHeap) throws IOException{
    BKDRadixSelector radixSelector = new BKDRadixSelector(dataDimensions, indexDimensions, bytesPerDimensions, sortedOnHeap, dir, "test");
    //we only split by indexed dimension so we check for each only those dimension
    for (int splitDim = 0; splitDim < indexDimensions; splitDim++) {
      //We need to make a copy of the data as it is deleted in the process
      BKDRadixSelector.PathSlice inputSlice = new BKDRadixSelector.PathSlice(copyPoints(dir, points, packedLength), 0, points.count());
      int commonPrefixLengthInput = getRandomCommonPrefix(inputSlice, bytesPerDimensions, splitDim);
      BKDRadixSelector.PathSlice[] slices = new BKDRadixSelector.PathSlice[2];
      byte[] partitionPoint = radixSelector.select(inputSlice, slices, start, end, middle, splitDim, commonPrefixLengthInput);
      assertEquals(middle - start, slices[0].count);
      assertEquals(end - middle, slices[1].count);
      //check that left and right slices contain the correct points
      byte[] max = getMax(slices[0], bytesPerDimensions, splitDim);
      byte[] min = getMin(slices[1], bytesPerDimensions, splitDim);
      int cmp = Arrays.compareUnsigned(max, 0, bytesPerDimensions, min, 0, bytesPerDimensions);
      assertTrue(cmp <= 0);
      if (cmp == 0) {
        byte[] maxDataDim = getMaxDataDimension(slices[0], bytesPerDimensions, dataDimensions, indexDimensions, max, splitDim);
        byte[] minDataDim = getMinDataDimension(slices[1], bytesPerDimensions, dataDimensions, indexDimensions, min, splitDim);
        cmp = Arrays.compareUnsigned(maxDataDim, 0, (dataDimensions - indexDimensions) * bytesPerDimensions, minDataDim, 0, (dataDimensions - indexDimensions) * bytesPerDimensions);
        assertTrue(cmp <= 0);
        if (cmp == 0) {
          int maxDocID = getMaxDocId(slices[0], bytesPerDimensions, splitDim, partitionPoint, dataDimensions, indexDimensions,maxDataDim);
          int minDocId = getMinDocId(slices[1], bytesPerDimensions, splitDim, partitionPoint, dataDimensions, indexDimensions,minDataDim);
          assertTrue(minDocId >= maxDocID);
        }
      }
      assertTrue(Arrays.equals(partitionPoint, min));
      slices[0].writer.destroy();
      slices[1].writer.destroy();
    }
    points.destroy();
  }

  private PointWriter copyPoints(Directory dir, PointWriter points, int packedLength) throws IOException {
    try (PointWriter copy  = getRandomPointWriter(dir, points.count(), packedLength);
         PointReader reader = points.getReader(0, points.count())) {
      while (reader.next()) {
        copy.append(reader.pointValue());
      }
      return copy;
    }
  }

  /** returns a common prefix length equal or lower than the current one */
  private int getRandomCommonPrefix(BKDRadixSelector.PathSlice inputSlice, int bytesPerDimension, int splitDim) throws IOException {
    byte[] pointsMax = getMax(inputSlice, bytesPerDimension, splitDim);
    byte[] pointsMin = getMin(inputSlice, bytesPerDimension, splitDim);
    int commonPrefixLength = Arrays.mismatch(pointsMin, 0, bytesPerDimension, pointsMax, 0, bytesPerDimension);
    if (commonPrefixLength == -1) {
      commonPrefixLength = bytesPerDimension;
    }
    return (random().nextBoolean()) ? commonPrefixLength : commonPrefixLength == 0 ? 0 : random().nextInt(commonPrefixLength);
  }

  private PointWriter getRandomPointWriter(Directory dir, long numPoints, int packedBytesLength) throws IOException {
    if (numPoints < 4096 && random().nextBoolean()) {
      return new HeapPointWriter(Math.toIntExact(numPoints), packedBytesLength);
    } else {
      return new OfflinePointWriter(dir, "test", packedBytesLength, "data", numPoints);
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

  private byte[] getMin(BKDRadixSelector.PathSlice p, int bytesPerDimension, int dimension) throws  IOException {
    byte[] min = new byte[bytesPerDimension];
    Arrays.fill(min, (byte) 0xff);
    try (PointReader reader = p.writer.getReader(p.start, p.count)) {
      byte[] value = new byte[bytesPerDimension];

      while (reader.next()) {
        PointValue pointValue = reader.pointValue();
        BytesRef packedValue = pointValue.packedValue();
        System.arraycopy(packedValue.bytes, packedValue.offset + dimension * bytesPerDimension, value, 0, bytesPerDimension);
        if (Arrays.compareUnsigned(min, 0, bytesPerDimension, value, 0, bytesPerDimension) > 0) {
          System.arraycopy(value, 0, min, 0, bytesPerDimension);
        }
      }
    }
    return min;
  }

  private int getMinDocId(BKDRadixSelector.PathSlice p, int bytesPerDimension, int dimension, byte[] partitionPoint, int dataDims, int indexDims, byte[] dataDim) throws  IOException {
   int docID = Integer.MAX_VALUE;
    try (PointReader reader = p.writer.getReader(p.start, p.count)) {
      while (reader.next()) {
        PointValue pointValue = reader.pointValue();
        BytesRef packedValue = pointValue.packedValue();
        int offset = dimension * bytesPerDimension;
        int dataOffset = indexDims * bytesPerDimension;
        int dataLength = (dataDims - indexDims) * bytesPerDimension;
        if (Arrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + bytesPerDimension, partitionPoint, 0, bytesPerDimension) == 0
          && Arrays.compareUnsigned(packedValue.bytes, packedValue.offset + dataOffset, packedValue.offset + dataOffset + dataLength, dataDim, 0, dataLength) == 0) {
          int newDocID = pointValue.docID();
          if (newDocID < docID) {
            docID = newDocID;
          }
        }
      }
    }
    return docID;
  }

  private byte[] getMinDataDimension(BKDRadixSelector.PathSlice p, int bytesPerDimension, int dataDims, int indexDims, byte[] minDim, int splitDim) throws  IOException {
    byte[] min = new byte[(dataDims - indexDims) * bytesPerDimension];
    Arrays.fill(min, (byte) 0xff);
    int offset = splitDim * bytesPerDimension;
    try (PointReader reader = p.writer.getReader(p.start, p.count)) {
      byte[] value = new byte[(dataDims - indexDims) * bytesPerDimension];
      while (reader.next()) {
        PointValue pointValue = reader.pointValue();
        BytesRef packedValue = pointValue.packedValue();
        if (Arrays.mismatch(minDim, 0, bytesPerDimension, packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + bytesPerDimension) == -1) {
          System.arraycopy(packedValue.bytes, packedValue.offset + indexDims * bytesPerDimension, value, 0, (dataDims - indexDims) * bytesPerDimension);
          if (Arrays.compareUnsigned(min, 0, (dataDims - indexDims) * bytesPerDimension, value, 0, (dataDims - indexDims) * bytesPerDimension) > 0) {
            System.arraycopy(value, 0, min, 0, (dataDims - indexDims) * bytesPerDimension);
          }
        }
      }
    }
    return min;
  }

  private byte[] getMax(BKDRadixSelector.PathSlice p, int bytesPerDimension, int dimension) throws  IOException {
    byte[] max = new byte[bytesPerDimension];
    Arrays.fill(max, (byte) 0);
    try (PointReader reader = p.writer.getReader(p.start, p.count)) {
      byte[] value = new byte[bytesPerDimension];
      while (reader.next()) {
        PointValue pointValue = reader.pointValue();
        BytesRef packedValue = pointValue.packedValue();
        System.arraycopy(packedValue.bytes, packedValue.offset + dimension * bytesPerDimension, value, 0, bytesPerDimension);
        if (Arrays.compareUnsigned(max, 0, bytesPerDimension, value, 0, bytesPerDimension) < 0) {
          System.arraycopy(value, 0, max, 0, bytesPerDimension);
        }
      }
    }
    return max;
  }

  private byte[] getMaxDataDimension(BKDRadixSelector.PathSlice p, int bytesPerDimension, int dataDims, int indexDims, byte[] maxDim, int splitDim) throws  IOException {
    byte[] max = new byte[(dataDims - indexDims) * bytesPerDimension];
    Arrays.fill(max, (byte) 0);
    int offset = splitDim * bytesPerDimension;
    try (PointReader reader = p.writer.getReader(p.start, p.count)) {
      byte[] value = new byte[(dataDims - indexDims) * bytesPerDimension];
      while (reader.next()) {
        PointValue pointValue = reader.pointValue();
        BytesRef packedValue = pointValue.packedValue();
        if (Arrays.mismatch(maxDim, 0, bytesPerDimension, packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + bytesPerDimension) == -1) {
          System.arraycopy(packedValue.bytes, packedValue.offset + indexDims * bytesPerDimension, value, 0, (dataDims - indexDims) * bytesPerDimension);
          if (Arrays.compareUnsigned(max, 0, (dataDims - indexDims) * bytesPerDimension, value, 0, (dataDims - indexDims) * bytesPerDimension) < 0) {
            System.arraycopy(value, 0, max, 0, (dataDims - indexDims) * bytesPerDimension);
          }
        }
      }
    }
    return max;
  }

  private int getMaxDocId(BKDRadixSelector.PathSlice p, int bytesPerDimension, int dimension, byte[] partitionPoint, int dataDims, int indexDims, byte[] dataDim) throws  IOException {
    int docID = Integer.MIN_VALUE;
    try (PointReader reader = p.writer.getReader(p.start, p.count)) {
      while (reader.next()) {
        PointValue pointValue = reader.pointValue();
        BytesRef packedValue = pointValue.packedValue();
        int offset = dimension * bytesPerDimension;
        int dataOffset = indexDims * bytesPerDimension;
        int dataLength = (dataDims - indexDims) * bytesPerDimension;
        if (Arrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + bytesPerDimension, partitionPoint, 0, bytesPerDimension) == 0
            && Arrays.compareUnsigned(packedValue.bytes, packedValue.offset + dataOffset, packedValue.offset + dataOffset + dataLength, dataDim, 0, dataLength) == 0) {
          int newDocID = pointValue.docID();
          if (newDocID > docID) {
            docID = newDocID;
          }
        }
      }
    }
    return docID;
  }
}
