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
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.MSBRadixSorter;
import org.apache.lucene.util.RadixSelector;
import org.apache.lucene.util.Selector;
import org.apache.lucene.util.Sorter;

/**
 *
 * Offline Radix selector for BKD tree.
 *
 *  @lucene.internal
 * */
public final class BKDRadixSelector {
  //size of the histogram
  private static final int HISTOGRAM_SIZE = 256;
  //size of the online buffer: 8 KB
  private static final int MAX_SIZE_OFFLINE_BUFFER = 1024 * 8;
  //histogram array
  private final long[] histogram;
  //bytes per dimension
  private final int bytesPerDim;
  // number of bytes to be sorted: bytesPerDim + Integer.BYTES
  private final int bytesSorted;
  //data dimensions size
  private final int packedBytesLength;
  //flag to when we are moving to sort on heap
  private final int maxPointsSortInHeap;
  //reusable buffer
  private final byte[] offlineBuffer;
  //holder for partition points
  private final int[] partitionBucket;
  // scratch array to hold temporary data
  private final byte[] scratch;
  //Directory to create new Offline writer
  private final Directory tempDir;
  // prefix for temp files
  private final String tempFileNamePrefix;


  /**
   * Sole constructor.
   */
  public BKDRadixSelector(int numDim, int bytesPerDim, int maxPointsSortInHeap, Directory tempDir, String tempFileNamePrefix) {
    this.bytesPerDim = bytesPerDim;
    this.packedBytesLength = numDim * bytesPerDim;
    this.bytesSorted = bytesPerDim + Integer.BYTES;
    this.maxPointsSortInHeap = maxPointsSortInHeap;
    int numberOfPointsOffline  = MAX_SIZE_OFFLINE_BUFFER / (packedBytesLength + Integer.BYTES);
    this.offlineBuffer = new byte[numberOfPointsOffline * (packedBytesLength + Integer.BYTES)];
    this.partitionBucket = new int[bytesSorted];
    this.histogram = new long[HISTOGRAM_SIZE];
    this.scratch = new byte[bytesSorted];
    this.tempDir = tempDir;
    this.tempFileNamePrefix = tempFileNamePrefix;
  }

  /**
   *  It uses the provided {@code points} from the given {@code from} to the given {@code to}
   *  to populate the {@code partitionSlices} array holder (length &gt; 1) with two path slices
   *  so the path slice at position 0 contains {@code partition - from} points
   *  where the value of the {@code dim} is lower or equal to the {@code to -from}
   *  points on the slice at position 1.
   *
   *  The {@code dimCommonPrefix} provides a hint for the length of the common prefix length for
   *  the {@code dim} where are partitioning the points.
   *
   *  It return the value of the {@code dim} at the partition point.
   *
   *  If the provided {@code points} is wrapping an {@link OfflinePointWriter}, the
   *  writer is destroyed in the process to save disk space.
   */
  public byte[] select(PathSlice points, PathSlice[] partitionSlices, long from, long to, long partitionPoint, int dim, int dimCommonPrefix) throws IOException {
    checkArgs(from, to, partitionPoint);

    assert partitionSlices.length > 1 : "[partition alices] must be > 1, got " + partitionSlices.length;

    //If we are on heap then we just select on heap
    if (points.writer instanceof HeapPointWriter) {
      byte[] partition = heapRadixSelect((HeapPointWriter) points.writer, dim, Math.toIntExact(from), Math.toIntExact(to),  Math.toIntExact(partitionPoint), dimCommonPrefix);
      partitionSlices[0] = new PathSlice(points.writer, from, partitionPoint - from);
      partitionSlices[1] = new PathSlice(points.writer, partitionPoint, to - partitionPoint);
      return partition;
    }

    OfflinePointWriter offlinePointWriter = (OfflinePointWriter) points.writer;

    try (PointWriter left = getPointWriter(partitionPoint - from, "left" + dim);
         PointWriter right = getPointWriter(to - partitionPoint, "right" + dim)) {
      partitionSlices[0] = new PathSlice(left, 0, partitionPoint - from);
      partitionSlices[1] = new PathSlice(right, 0, to - partitionPoint);
      return buildHistogramAndPartition(offlinePointWriter, left, right, from, to, partitionPoint, 0, dimCommonPrefix, dim);
    }
  }

  void checkArgs(long from, long to, long partitionPoint) {
    if (partitionPoint < from) {
      throw new IllegalArgumentException("partitionPoint must be >= from");
    }
    if (partitionPoint >= to) {
      throw new IllegalArgumentException("partitionPoint must be < to");
    }
  }

  private int findCommonPrefixAndHistogram(OfflinePointWriter points, long from, long to, int dim, int dimCommonPrefix) throws IOException{
    //find common prefix
    int commonPrefixPosition = bytesSorted;
    final int offset = dim * bytesPerDim;
    try (OfflinePointReader reader = points.getReader(from, to - from, offlineBuffer)) {
      assert commonPrefixPosition > dimCommonPrefix;
      reader.next();
      PointValue pointValue = reader.pointValue();
      // copy dimension
      BytesRef packedValue = pointValue.packedValue();
      System.arraycopy(packedValue.bytes, packedValue.offset + offset, scratch, 0, bytesPerDim);
      // copy docID
      BytesRef docIDBytes = pointValue.docIDBytes();
      System.arraycopy(docIDBytes.bytes, docIDBytes.offset, scratch, bytesPerDim, Integer.BYTES);
      for (long i = from + 1; i < to; i++) {
        reader.next();
        pointValue = reader.pointValue();
        if (commonPrefixPosition == dimCommonPrefix) {
          histogram[getBucket(offset, commonPrefixPosition, pointValue)]++;
          // we do not need to check for common prefix anymore,
          // just finish the histogram and break
          for (long j = i + 1; j < to; j++) {
            reader.next();
            pointValue = reader.pointValue();
            histogram[getBucket(offset, commonPrefixPosition, pointValue)]++;
          }
          break;
        } else {
          //check common prefix and adjust histogram
          final int startIndex = (dimCommonPrefix > bytesPerDim) ? bytesPerDim : dimCommonPrefix;
          final int endIndex = (commonPrefixPosition > bytesPerDim) ? bytesPerDim : commonPrefixPosition;
          packedValue = pointValue.packedValue();
          int j = FutureArrays.mismatch(scratch, startIndex, endIndex, packedValue.bytes, packedValue.offset + offset + startIndex, packedValue.offset + offset + endIndex);
          if (j == -1) {
            if (commonPrefixPosition > bytesPerDim) {
              //tie-break on docID
              docIDBytes = pointValue.docIDBytes();
              int k = FutureArrays.mismatch(scratch, bytesPerDim, commonPrefixPosition, docIDBytes.bytes, docIDBytes.offset, docIDBytes.offset + commonPrefixPosition - bytesPerDim);
              if (k != -1) {
                commonPrefixPosition = bytesPerDim + k;
                Arrays.fill(histogram, 0);
                histogram[scratch[commonPrefixPosition] & 0xff] = i - from;
              }
            }
          } else {
            commonPrefixPosition = dimCommonPrefix + j;
            Arrays.fill(histogram, 0);
            histogram[scratch[commonPrefixPosition] & 0xff] = i - from;
          }
          if (commonPrefixPosition != bytesSorted) {
            histogram[getBucket(offset, commonPrefixPosition, pointValue)]++;
          }
        }
      }
    }

    //build partition buckets up to commonPrefix
    for (int i = 0; i < commonPrefixPosition; i++) {
      partitionBucket[i] = scratch[i] & 0xff;
    }
    return commonPrefixPosition;
  }

  private int getBucket(int offset, int commonPrefixPosition, PointValue pointValue) {
    int bucket;
    if (commonPrefixPosition < bytesPerDim) {
      BytesRef packedValue = pointValue.packedValue();
      bucket = packedValue.bytes[packedValue.offset + offset + commonPrefixPosition] & 0xff;
    } else {
      BytesRef docIDValue = pointValue.docIDBytes();
      bucket = docIDValue.bytes[docIDValue.offset + commonPrefixPosition - bytesPerDim] & 0xff;
    }
    return bucket;
  }

  private byte[] buildHistogramAndPartition(OfflinePointWriter points, PointWriter left, PointWriter right,
                                            long from, long to, long partitionPoint, int iteration,  int baseCommonPrefix, int dim) throws IOException {
    //find common prefix from baseCommonPrefix and build histogram
    int commonPrefix = findCommonPrefixAndHistogram(points, from, to, dim, baseCommonPrefix);

    //if all equals we just partition the points
    if (commonPrefix == bytesSorted) {
      offlinePartition(points, left, right, null, from, to, dim, commonPrefix - 1, partitionPoint);
      return partitionPointFromCommonPrefix();
    }

    long leftCount = 0;
    long rightCount = 0;

    //Count left points and record the partition point
    for(int i = 0; i < HISTOGRAM_SIZE; i++) {
      long size = histogram[i];
      if (leftCount + size > partitionPoint - from) {
        partitionBucket[commonPrefix] = i;
        break;
      }
      leftCount += size;
    }
    //Count right points
    for(int i = partitionBucket[commonPrefix] + 1; i < HISTOGRAM_SIZE; i++) {
      rightCount += histogram[i];
    }

    long delta = histogram[partitionBucket[commonPrefix]];
    assert leftCount + rightCount + delta == to - from : (leftCount + rightCount + delta) + " / " + (to - from);

    //special case when points are equal except last byte, we can just tie-break
    if (commonPrefix == bytesSorted - 1) {
      long tieBreakCount =(partitionPoint - from - leftCount);
      offlinePartition(points, left,  right, null, from, to, dim, commonPrefix, tieBreakCount);
      return partitionPointFromCommonPrefix();
    }

    //create the delta points writer
    PointWriter deltaPoints;
    try (PointWriter tempDeltaPoints = getDeltaPointWriter(left, right, delta, iteration)) {
      //divide the points. This actually destroys the current writer
      offlinePartition(points, left, right, tempDeltaPoints, from, to, dim, commonPrefix, 0);
      deltaPoints = tempDeltaPoints;
    }

    long newPartitionPoint = partitionPoint - from - leftCount;

    if (deltaPoints instanceof HeapPointWriter) {
      return heapPartition((HeapPointWriter) deltaPoints, left, right, dim, 0, (int) deltaPoints.count(), Math.toIntExact(newPartitionPoint), ++commonPrefix);
    } else {
      return buildHistogramAndPartition((OfflinePointWriter) deltaPoints, left, right, 0, deltaPoints.count(), newPartitionPoint, ++iteration, ++commonPrefix, dim);
    }
  }

  private void offlinePartition(OfflinePointWriter points, PointWriter left, PointWriter right, PointWriter deltaPoints,
                                long from, long to, int dim, int bytePosition, long numDocsTiebreak) throws IOException {
    assert bytePosition == bytesSorted -1 || deltaPoints != null;
    int offset =  dim * bytesPerDim;
    long tiebreakCounter = 0;
    try (OfflinePointReader reader = points.getReader(from, to - from, offlineBuffer)) {
      while (reader.next()) {
        PointValue pointValue = reader.pointValue();
        int bucket = getBucket(offset, bytePosition, pointValue);
        if (bucket < this.partitionBucket[bytePosition]) {
          // to the left side
          left.append(pointValue);
        } else if (bucket > this.partitionBucket[bytePosition]) {
          // to the right side
          right.append(pointValue);
        } else {
          if (bytePosition == bytesSorted - 1) {
            if (tiebreakCounter < numDocsTiebreak) {
              left.append(pointValue);
              tiebreakCounter++;
            } else {
              right.append(pointValue);
            }
          } else {
            deltaPoints.append(pointValue);
          }
        }
      }
    }
    //Delete original file
    points.destroy();
  }

  private byte[] partitionPointFromCommonPrefix() {
    byte[] partition = new byte[bytesPerDim];
    for (int i = 0; i < bytesPerDim; i++) {
      partition[i] = (byte)partitionBucket[i];
    }
    return partition;
  }

  private byte[] heapPartition(HeapPointWriter points, PointWriter left, PointWriter right, int dim, int from, int to, int partitionPoint, int commonPrefix) throws IOException {
    byte[] partition = heapRadixSelect(points, dim, from, to, partitionPoint, commonPrefix);
    for (int i = from; i < to; i++) {
      PointValue value = points.getPackedValueSlice(i);
      if (i < partitionPoint) {
        left.append(value);
      } else {
        right.append(value);
      }
    }
    return partition;
  }

  private byte[] heapRadixSelect(HeapPointWriter points, int dim, int from, int to, int partitionPoint, int commonPrefix) {
    final int offset = dim * bytesPerDim + commonPrefix;
    final int dimCmpBytes = bytesPerDim - commonPrefix;
    new RadixSelector(bytesSorted - commonPrefix) {

      @Override
      protected void swap(int i, int j) {
        points.swap(i, j);
      }

      @Override
      protected int byteAt(int i, int k) {
        assert k >= 0 : "negative prefix " + k;
        if (k  < dimCmpBytes) {
          // dim bytes
          return points.block[i * packedBytesLength + offset + k] & 0xff;
        } else {
          // doc id
          int s = 3 - (k - dimCmpBytes);
          return (points.docIDs[i] >>> (s * 8)) & 0xff;
        }
      }

      @Override
      protected Selector getFallbackSelector(int d) {
        int skypedBytes = d + commonPrefix;
        final int start = dim * bytesPerDim + skypedBytes;
        final int end =  dim * bytesPerDim + bytesPerDim;
        return new IntroSelector() {

          int pivotDoc = -1;

          @Override
          protected void swap(int i, int j) {
            points.swap(i, j);
          }

          @Override
          protected void setPivot(int i) {
            if (skypedBytes < bytesPerDim) {
              System.arraycopy(points.block, i * packedBytesLength + dim * bytesPerDim, scratch, 0, bytesPerDim);
            }
            pivotDoc = points.docIDs[i];
          }

          @Override
          protected int compare(int i, int j) {
            if (skypedBytes < bytesPerDim) {
              int iOffset = i * packedBytesLength;
              int jOffset = j * packedBytesLength;
              int cmp = FutureArrays.compareUnsigned(points.block, iOffset + start, iOffset + end,
                  points.block, jOffset + start, jOffset + end);
              if (cmp != 0) {
                return cmp;
              }
            }
            return points.docIDs[i] - points.docIDs[j];
          }

          @Override
          protected int comparePivot(int j) {
            if (skypedBytes < bytesPerDim) {
              int jOffset = j * packedBytesLength;
              int cmp = FutureArrays.compareUnsigned(scratch, skypedBytes, bytesPerDim,
                  points.block, jOffset + start, jOffset + end);
              if (cmp != 0) {
                return cmp;
              }
            }
            return pivotDoc - points.docIDs[j];
          }
        };
      }
    }.select(from, to, partitionPoint);

    byte[] partition = new byte[bytesPerDim];
    PointValue pointValue = points.getPackedValueSlice(partitionPoint);
    BytesRef packedValue = pointValue.packedValue();
    System.arraycopy(packedValue.bytes, packedValue.offset + dim * bytesPerDim, partition, 0, bytesPerDim);
    return partition;
  }

  /** Sort the heap writer by the specified dim. It is used to sort the leaves of the tree */
  public void heapRadixSort(final HeapPointWriter points, int from, int to, int dim, int commonPrefixLength) {
    final int offset = dim * bytesPerDim + commonPrefixLength;
    final int dimCmpBytes = bytesPerDim - commonPrefixLength;
    new MSBRadixSorter(bytesSorted - commonPrefixLength) {

      @Override
      protected int byteAt(int i, int k) {
        assert k >= 0 : "negative prefix " + k;
        if (k  < dimCmpBytes) {
          // dim bytes
          return points.block[i * packedBytesLength + offset + k] & 0xff;
        } else {
          // doc id
          int s = 3 - (k - dimCmpBytes);
          return (points.docIDs[i] >>> (s * 8)) & 0xff;
        }
      }

      @Override
      protected void swap(int i, int j) {
        points.swap(i, j);
      }

      @Override
      protected Sorter getFallbackSorter(int k) {
        int skypedBytes = k + commonPrefixLength;
        final int start = dim * bytesPerDim + skypedBytes;
        final int end =  dim * bytesPerDim + bytesPerDim;
        return new IntroSorter() {

          int pivotDoc = -1;

          @Override
          protected void swap(int i, int j) {
            points.swap(i, j);
          }

          @Override
          protected void setPivot(int i) {
            if (skypedBytes < bytesPerDim) {
              System.arraycopy(points.block, i * packedBytesLength + dim * bytesPerDim, scratch, 0, bytesPerDim);
            }
            pivotDoc = points.docIDs[i];
          }

          @Override
          protected int compare(int i, int j) {
            if (skypedBytes < bytesPerDim) {
              int iOffset = i * packedBytesLength;
              int jOffset = j * packedBytesLength;
              int cmp = FutureArrays.compareUnsigned(points.block, iOffset + start, iOffset + end,
                  points.block, jOffset + start, jOffset + end);
              if (cmp != 0) {
                return cmp;
              }
            }
            return points.docIDs[i] - points.docIDs[j];
          }

          @Override
          protected int comparePivot(int j) {
            if (skypedBytes < bytesPerDim) {
              int jOffset = j * packedBytesLength;
              int cmp = FutureArrays.compareUnsigned(scratch, skypedBytes, bytesPerDim,
                  points.block, jOffset + start, jOffset + end);
              if (cmp != 0) {
                return cmp;
              }
            }
            return pivotDoc - points.docIDs[j];
          }
        };
      }
    }.sort(from, to);
  }

  private PointWriter getDeltaPointWriter(PointWriter left, PointWriter right, long delta, int iteration) throws IOException {
    if (delta <= getMaxPointsSortInHeap(left, right)) {
      return  new HeapPointWriter(Math.toIntExact(delta), packedBytesLength);
    } else {
      return new OfflinePointWriter(tempDir, tempFileNamePrefix, packedBytesLength, "delta" + iteration, delta);
    }
  }

  private int getMaxPointsSortInHeap(PointWriter left, PointWriter right) {
    int pointsUsed = 0;
    if (left instanceof HeapPointWriter) {
      pointsUsed += ((HeapPointWriter) left).size;
    }
    if (right instanceof HeapPointWriter) {
      pointsUsed += ((HeapPointWriter) right).size;
    }
    assert maxPointsSortInHeap >= pointsUsed;
    return maxPointsSortInHeap - pointsUsed;
  }

  PointWriter getPointWriter(long count, String desc) throws IOException {
    //As we recurse, we hold two on-heap point writers at any point. Therefore the
    //max size for these objects is half of the total points we can have on-heap.
    if (count <= maxPointsSortInHeap / 2) {
      int size = Math.toIntExact(count);
      return new HeapPointWriter(size, packedBytesLength);
    } else {
      return new OfflinePointWriter(tempDir, tempFileNamePrefix, packedBytesLength, desc, count);
    }
  }

  /** Sliced reference to points in an PointWriter. */
  public static final class PathSlice {
    public final PointWriter writer;
    public final long start;
    public final long count;

    public PathSlice(PointWriter writer, long start, long count) {
      this.writer = writer;
      this.start = start;
      this.count = count;
    }

    @Override
    public String toString() {
      return "PathSlice(start=" + start + " count=" + count + " writer=" + writer + ")";
    }
  }
}
