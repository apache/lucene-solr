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
import org.apache.lucene.util.IntroSelector;

/**
 *
 * Offline Radix selector for BKD tree.
 *
 *  @lucene.internal
 * */
public final class BKDRadixSelector {
  //size of the histogram
  private static final int HISTOGRAM_SIZE = 256;
  // we store one histogram per recursion level
  private final int[][] histogram;
  //bytes we are sorting
  private final int bytesPerDim;
  // number of bytes to be sorted: bytesPerDim + Integer.BYTES
  private final int bytesSorted;
  //data dimensions size
  private final int packedBytesLength;
  //flag to when we are moving to sort on heap
  private final int maxPointsSortedOffHeap;
  //reusable buffer
  private final byte[] offlineBuffer;
  //holder for partition points
  private final int[] partitionBucket;
  //holder for partition bytes
  private final byte[] partitionBytes;
  //re-usable on-heap selector
  private final HeapSelector heapSelector;
  // scratch object to move bytes around
  private final BytesRef bytesRef1 = new BytesRef();
  // scratch object to move bytes around
  private final BytesRef bytesRef2 = new BytesRef();
  //Directory to create new Offline writer
  private final Directory tempDir;
  // prefix for temp files
  private final String tempFileNamePrefix;



  /**
   * Sole constructor.
   */
  public BKDRadixSelector(int numDim, int bytesPerDim, int maxPointsSortedOffHeap, Directory tempDir, String tempFileNamePrefix) {
    this.bytesPerDim = bytesPerDim;
    this.packedBytesLength = numDim * bytesPerDim;
    this.bytesSorted = bytesPerDim + Integer.BYTES;
    this.maxPointsSortedOffHeap = maxPointsSortedOffHeap;
    this.offlineBuffer = new byte[maxPointsSortedOffHeap * (packedBytesLength + Integer.BYTES)];
    this.partitionBucket = new int[bytesSorted];
    this.partitionBytes =  new byte[bytesSorted];
    this.histogram = new int[bytesSorted][HISTOGRAM_SIZE];
    this.bytesRef1.length = numDim * bytesPerDim;
    this.heapSelector = new HeapSelector(numDim, bytesPerDim);
    this.tempDir = tempDir;
    this.tempFileNamePrefix = tempFileNamePrefix;
  }

  /**
   * Method to partition the input data. It returns the value of the dimension where
   * the split happens.
   */
  public byte[] select(PointWriter points, PointWriter left, PointWriter right, int from, int to, int middle, int dim) throws IOException {
    checkArgs(from, to, middle);

    //If we are on heap then we just select on heap
    if (points instanceof HeapPointWriter) {
      return heapSelect((HeapPointWriter) points, left, right, dim, middle, 0, 0);
    }

    //reset histogram
    for (int i = 0; i < bytesSorted; i++) {
      Arrays.fill(histogram[i], 0);
    }
    OfflinePointWriter offlinePointWriter = (OfflinePointWriter) points;

    //find common prefix, it does already set histogram values if needed
    int commonPrefix = findCommonPrefix(offlinePointWriter, from, to, dim);

    //if all equals we just partition the data
    if (commonPrefix ==  bytesSorted) {
      return partition(offlinePointWriter, left, right, from, to, middle, dim, null, commonPrefix - 1, middle);
    }
    //let's rock'n'roll
    return buildHistogramAndPartition(offlinePointWriter, null, left, right, from, to, middle, 0, commonPrefix, dim,0, 0);
  }

  void checkArgs(int from, int to, int middle) {
    if (middle < from) {
      throw new IllegalArgumentException("middle must be >= from");
    }
    if (middle >= to) {
      throw new IllegalArgumentException("middle must be < to");
    }
  }

  private int findCommonPrefix(OfflinePointWriter points, int from, int to, int dim) throws IOException{
    //find common prefix
    byte[] commonPrefix = new byte[bytesSorted];
    int commonPrefixPosition = bytesSorted;
    try (OfflinePointReader reader = points.getReader(from, to, maxPointsSortedOffHeap, offlineBuffer)) {
      reader.next();
      reader.docValue(bytesRef1);
      // copy dimension
      System.arraycopy(bytesRef1.bytes, bytesRef1.offset + dim * bytesPerDim, commonPrefix, 0, bytesPerDim);
      // copy docID
      System.arraycopy(bytesRef1.bytes, bytesRef1.offset + packedBytesLength, commonPrefix, bytesPerDim, Integer.BYTES);
      for (int i =from + 1; i< to; i++) {
        reader.next();
        reader.docValue(bytesRef1);
        int startIndex =  dim * bytesPerDim;
        int endIndex  = (commonPrefixPosition > bytesPerDim) ? startIndex + bytesPerDim :  startIndex + commonPrefixPosition;
        int j = FutureArrays.mismatch(commonPrefix, 0, endIndex - startIndex, bytesRef1.bytes, bytesRef1.offset + startIndex, bytesRef1.offset + endIndex);
        if (j == 0) {
          return 0;
        } else if (j == -1) {
          if (commonPrefixPosition > bytesPerDim) {
            //tie-break on docID
            int k = FutureArrays.mismatch(commonPrefix, bytesPerDim, commonPrefixPosition, bytesRef1.bytes, bytesRef1.offset + packedBytesLength, bytesRef1.offset + packedBytesLength + commonPrefixPosition - bytesPerDim );
            if (k != -1) {
              commonPrefixPosition = bytesPerDim + k;
            }
          }
        } else {
          commonPrefixPosition = j;
        }
      }
    }

    //build histogram up to the common prefix
    for (int i=0; i < commonPrefixPosition; i++) {
      partitionBucket[i] = commonPrefix[i] & 0xff;
      partitionBytes[i] = commonPrefix[i];
      histogram[i][partitionBucket[i]] = to - from;
    }
    return commonPrefixPosition;
  }

  private byte[] buildHistogramAndPartition(OfflinePointWriter points, OfflinePointWriter deltaPoints, PointWriter left, PointWriter right, int from, int to, int middle,
                                            int iteration,  int commonPrefix, int dim, int leftCount, int rightCount) throws IOException {

    OfflinePointWriter currentPoints = (deltaPoints == null) ? points : deltaPoints;
    //build histogram at the commonPrefix byte
    try (OfflinePointReader reader = currentPoints.getReader(0, currentPoints.count(), maxPointsSortedOffHeap, offlineBuffer);
         OfflinePointWriter deltaPointsWriter = (iteration == 0) ? null : new OfflinePointWriter(tempDir, tempFileNamePrefix, packedBytesLength, "delta", 0)) {
      while (reader.next()) {
        reader.docValue(bytesRef1);
        if (iteration == 0 || hasCommonPrefix(bytesRef1, dim, commonPrefix)) {
          int bucket;
          if (commonPrefix < bytesPerDim) {
            bucket = bytesRef1.bytes[bytesRef1.offset + dim * bytesPerDim + commonPrefix] & 0xff;
          } else {
            bucket = bytesRef1.bytes[bytesRef1.offset + packedBytesLength + commonPrefix - bytesPerDim] & 0xff;
          }
          histogram[commonPrefix][bucket]++;
          if (deltaPointsWriter != null) {
            reader.packedValue(bytesRef2);
            deltaPointsWriter.append(bytesRef2, reader.docID());
          }
        }
      }
      if (deltaPoints != null) {
        deltaPoints.destroy();
      }
      deltaPoints = deltaPointsWriter;
    }
    //Count left points and record the partition point
    for(int i = 0; i < HISTOGRAM_SIZE; i++) {
      int size = histogram[commonPrefix][i];
      if (leftCount + size > middle) {
        partitionBucket[commonPrefix] = i;
        partitionBytes[commonPrefix] = (byte) i;
        break;
      }
      leftCount += size;
    }
    //Count right points
    for(int i = partitionBucket[commonPrefix] + 1; i < HISTOGRAM_SIZE; i++) {
      rightCount += histogram[commonPrefix][i];;
    }

    assert leftCount + rightCount + histogram[commonPrefix][partitionBucket[commonPrefix]] == to - from;

    if (commonPrefix == bytesSorted - 1) {
      if (deltaPoints != null) {
        deltaPoints.destroy();
      }
      // we are done, lets break data around. No need to sort on heap
      return partition(points, left, right, from, to, middle, dim, null, commonPrefix, middle - leftCount);
    } else if (histogram[commonPrefix][partitionBucket[commonPrefix]] <= maxPointsSortedOffHeap) {
      if (deltaPoints != null) {
        deltaPoints.destroy();
      }
      // last points are done on heap
      int size = histogram[commonPrefix][partitionBucket[commonPrefix]];
      HeapPointWriter writer = new HeapPointWriter(size, size, packedBytesLength);
      return partition(points, left, right, from, to, middle, dim, writer, commonPrefix, 0);
    } else {
      // iterate next common prefix
      return buildHistogramAndPartition(points, deltaPoints, left, right, from, to, middle, ++iteration, ++commonPrefix, dim,  leftCount, rightCount);
    }
  }

  private byte[] partition(OfflinePointWriter points, PointWriter left, PointWriter right, int from, int to, int middle, int dim,
                           HeapPointWriter sorted, int commonPrefix, int numDocsTiebreak) throws IOException {
    int leftCounter = 0;
    int tiebreakCounter = 0;

    try (OfflinePointReader reader = points.getReader(from, to, maxPointsSortedOffHeap, offlineBuffer)) {
      while(reader.next()) {
        assert leftCounter <= middle;
       reader.docValue(bytesRef1);
       reader.packedValue(bytesRef2);
        int docID = reader.docID();
        int thisCommonPrefix = getCommonPrefix(bytesRef1, dim, commonPrefix);
        int bucket = getBucket(bytesRef1, dim, thisCommonPrefix);

        if (bucket < this.partitionBucket[thisCommonPrefix]) {
          // to the left side
          left.append(bytesRef2, docID);
          leftCounter++;
        } else if (bucket > this.partitionBucket[thisCommonPrefix]) {
          // to the right side
          right.append(bytesRef2, docID);
        } else {
          assert thisCommonPrefix == commonPrefix;
          //we should be at the common prefix, if we are at the end of the array
          //then use the  tie-break value, else store for sorting later
          if (thisCommonPrefix == bytesSorted - 1) {
            if (tiebreakCounter < numDocsTiebreak) {
              left.append(bytesRef2, docID);
              tiebreakCounter++;
            } else {
              right.append(bytesRef2, docID);
            }
          } else {
            sorted.append(bytesRef2, docID);
          }
        }
      }
    }
    // do have work to do?
    if (sorted != null) {
      // make sure we are just not soring all data
      assert sorted.count() != points.count();
      return heapSelect(sorted, left, right, dim, middle, leftCounter, commonPrefix);
    }
    // We did not have any points to sort on memory,
    // compute the partition point from the common prefix
    byte[] partition = new byte[bytesPerDim];
    for (int i = 0; i < bytesPerDim; i++) {
      partition[i] = (byte)partitionBucket[i];
    }
    return partition;
  }

  private byte[] heapSelect(HeapPointWriter writer, PointWriter left, PointWriter right, int dim, int k, int leftCounter, int commonPrefix) throws IOException {

    assert writer.count() != k - leftCounter;
    heapSelector.setHeapPointWriter(writer, dim, commonPrefix);
    heapSelector.select(0, Math.toIntExact(writer.count()), k - leftCounter);

    byte[] partition = new byte[bytesPerDim];
    Arrays.fill(partition, (byte) 0xff);
    for (int i = 0; i < writer.count(); i++) {
      writer.getPackedValueSlice(i, bytesRef1);
      int docID = writer.docIDs[i];
      if (leftCounter < k) {
        left.append(bytesRef1, docID);
        leftCounter++;
      } else {
        right.append(bytesRef1, docID);
        if (FutureArrays.compareUnsigned(bytesRef1.bytes, bytesRef1.offset + dim * bytesPerDim, bytesRef1.offset + (dim + 1) * bytesPerDim, partition, 0, bytesPerDim) < 0) {
          System.arraycopy(bytesRef1.bytes, bytesRef1.offset + dim * bytesPerDim, partition, 0, bytesPerDim);
        }
      }
    }
    assert k == leftCounter;
    return partition;
  }

  private boolean hasCommonPrefix(BytesRef packedValue, int dim, int commonPrefix) {
    if (commonPrefix < bytesPerDim) {
      return FutureArrays.compareUnsigned(partitionBytes, 0, commonPrefix, packedValue.bytes, packedValue.offset + dim * bytesPerDim, packedValue.offset + dim * bytesPerDim + commonPrefix) == 0;
    } else {
      if (FutureArrays.compareUnsigned(partitionBytes, 0, bytesPerDim, packedValue.bytes, packedValue.offset + dim * bytesPerDim, packedValue.offset + dim * bytesPerDim + bytesPerDim) == 0) {
        int docIdBytes = commonPrefix - bytesPerDim;
        return FutureArrays.compareUnsigned(partitionBytes, bytesPerDim, bytesPerDim + docIdBytes, packedValue.bytes, packedValue.offset + packedBytesLength, packedValue.offset + packedBytesLength + docIdBytes) == 0;
      }
      return false;
    }
  }

  private int getCommonPrefix(BytesRef packedValue, int dim, int maxCommmonPrefix) {
    if (maxCommmonPrefix < bytesPerDim) {
      int commonPrefix = FutureArrays.mismatch(packedValue.bytes, packedValue.offset + dim * bytesPerDim, packedValue.offset + dim * bytesPerDim + maxCommmonPrefix, partitionBytes, 0, maxCommmonPrefix);
      if (commonPrefix == -1) {
        return maxCommmonPrefix;
      }
      return commonPrefix;
    } else {
      int commonPrefix = FutureArrays.mismatch(packedValue.bytes, packedValue.offset + dim * bytesPerDim, packedValue.offset + dim * bytesPerDim + bytesPerDim, partitionBytes, 0, bytesPerDim);
      if (commonPrefix != -1) {
        return commonPrefix;
      }
      int docBytes = maxCommmonPrefix - bytesPerDim;
      int docCommonPrefix = FutureArrays.mismatch(packedValue.bytes, packedValue.offset + packedBytesLength, packedValue.offset + packedBytesLength + docBytes, partitionBytes, bytesPerDim, bytesPerDim + docBytes);
      if (docCommonPrefix != -1) {
        return bytesPerDim + docCommonPrefix;
      }
      return maxCommmonPrefix;
    }
  }

  private int getBucket(BytesRef packedValue, int dim, int commonPrefix) {
    if (commonPrefix < bytesPerDim) {
      return packedValue.bytes[packedValue.offset + dim * bytesPerDim + commonPrefix] & 0xff;
    } else {
      return packedValue.bytes[packedValue.offset + packedBytesLength + commonPrefix - bytesPerDim] & 0xff;
    }
  }

  /**
   * Reusable on-heap intro  selector.
   */
  private static class HeapSelector extends IntroSelector {
    //fixed from the beginning
    private final int bytesPerDim;
    //writer that must be set before running
    private HeapPointWriter writer;
    //byte start position for comparisons
    private int start;
    //byte end position for comparisons
    private int end;
    //common prefix
    private int commonPrefix;
    //scratch object
    private final BytesRef bytesRef1 = new BytesRef();
    //scratch object
    private final BytesRef bytesRef2 = new BytesRef();
    //holds the current pivot
    private final BytesRef pivot = new BytesRef();
    //holds the current pivot docID
    private int pivotDoc;

    public HeapSelector(int numDim, int bytesPerDim) {
      this.bytesPerDim = bytesPerDim;
      this.bytesRef1.length = numDim * bytesPerDim;
      this.bytesRef2.length = numDim * bytesPerDim;
      this.pivot.length = numDim * bytesPerDim;
    }

    /**
     * Set the data to be sorted. It must be done before calling select.
     * @param writer The data to be sorted
     * @param dim the dimension to sort from
     * @param commonPrefix byte we need to start from the comparisons
     */
    public void setHeapPointWriter(HeapPointWriter writer, int dim, int commonPrefix) {
      this.writer = writer;
      this.commonPrefix = commonPrefix;
      start = dim * bytesPerDim + commonPrefix;
      end = dim * bytesPerDim + bytesPerDim;
    }

    @Override
    protected void swap(int i, int j){
      writer.swap(i, j);
    }

    @Override
    protected void setPivot(int i) {
      writer.getPackedValueSlice(i, pivot);
      pivotDoc = writer.docIDs[i];
    }

    @Override
    protected int compare(int i, int j) {
      if (commonPrefix < bytesPerDim) {
        writer.getPackedValueSlice(i, bytesRef1);
        writer.getPackedValueSlice(j, bytesRef2);
        int cmp = FutureArrays.compareUnsigned(bytesRef1.bytes, bytesRef1.offset + start, bytesRef1.offset + end, bytesRef2.bytes, bytesRef2.offset + start, bytesRef2.offset + end);
        if (cmp != 0) {
          return cmp;
        }
      }
      return writer.docIDs[i] - writer.docIDs[j];
    }

    @Override
    protected int comparePivot(int j) {
      if (commonPrefix < bytesPerDim) {
        writer.getPackedValueSlice(j, bytesRef1);
        int cmp = FutureArrays.compareUnsigned(pivot.bytes, pivot.offset + start, pivot.offset + end, bytesRef1.bytes, bytesRef1.offset + start, bytesRef1.offset + end);
        if (cmp != 0) {
          return cmp;
        }
      }
      return pivotDoc - writer.docIDs[j];
    }
  }
}
