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
package org.apache.lucene.codecs.simpletext;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.bkd.BKDRadixSelector;
import org.apache.lucene.util.bkd.BKDWriter;
import org.apache.lucene.util.bkd.HeapPointWriter;
import org.apache.lucene.util.bkd.MutablePointsReaderUtils;
import org.apache.lucene.util.bkd.OfflinePointWriter;
import org.apache.lucene.util.bkd.PointReader;
import org.apache.lucene.util.bkd.PointValue;
import org.apache.lucene.util.bkd.PointWriter;

import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.BLOCK_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.BLOCK_DOC_ID;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.BLOCK_FP;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.BLOCK_VALUE;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.BYTES_PER_DIM;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.DOC_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.INDEX_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.MAX_LEAF_POINTS;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.MAX_VALUE;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.MIN_VALUE;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.NUM_DATA_DIMS;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.NUM_INDEX_DIMS;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.POINT_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.SPLIT_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.SPLIT_DIM;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.SPLIT_VALUE;


// TODO
//   - allow variable length byte[] (across docs and dims), but this is quite a bit more hairy
//   - we could also index "auto-prefix terms" here, and use better compression, and maybe only use for the "fully contained" case so we'd
//     only index docIDs
//   - the index could be efficiently encoded as an FST, so we don't have wasteful
//     (monotonic) long[] leafBlockFPs; or we could use MonotonicLongValues ... but then
//     the index is already plenty small: 60M OSM points --> 1.1 MB with 128 points
//     per leaf, and you can reduce that by putting more points per leaf
//   - we could use threads while building; the higher nodes are very parallelizable

/** Forked from {@link BKDWriter} and simplified/specialized for SimpleText's usage */

final class SimpleTextBKDWriter implements Closeable {

  public static final String CODEC_NAME = "BKD";
  public static final int VERSION_START = 0;
  public static final int VERSION_COMPRESSED_DOC_IDS = 1;
  public static final int VERSION_COMPRESSED_VALUES = 2;
  public static final int VERSION_IMPLICIT_SPLIT_DIM_1D = 3;
  public static final int VERSION_CURRENT = VERSION_IMPLICIT_SPLIT_DIM_1D;

  /** How many bytes each docs takes in the fixed-width offline format */
  private final int bytesPerDoc;

  /** Default maximum number of point in each leaf block */
  public static final int DEFAULT_MAX_POINTS_IN_LEAF_NODE = 1024;

  /** Default maximum heap to use, before spilling to (slower) disk */
  public static final float DEFAULT_MAX_MB_SORT_IN_HEAP = 16.0f;

  /** Maximum number of dimensions */
  public static final int MAX_DIMS = 8;

  /** How many dimensions we are storing at the leaf (data) nodes */
  protected final int numDataDims;

  /** How many dimensions we are indexing in the internal nodes */
  protected final int numIndexDims;

  /** How many bytes each value in each dimension takes. */
  protected final int bytesPerDim;

  /** numDataDims * bytesPerDim */
  protected final int packedBytesLength;

  /** numIndexDims * bytesPerDim */
  protected final int packedIndexBytesLength;

  final BytesRefBuilder scratch = new BytesRefBuilder();

  final TrackingDirectoryWrapper tempDir;
  final String tempFileNamePrefix;
  final double maxMBSortInHeap;

  final byte[] scratchDiff;
  final byte[] scratch1;
  final byte[] scratch2;
  final BytesRef scratchBytesRef1 = new BytesRef();
  final BytesRef scratchBytesRef2 = new BytesRef();
  final int[] commonPrefixLengths;

  protected final FixedBitSet docsSeen;

  private PointWriter pointWriter;
  private boolean finished;

  private IndexOutput tempInput;
  protected final int maxPointsInLeafNode;
  private final int maxPointsSortInHeap;

  /** Minimum per-dim values, packed */
  protected final byte[] minPackedValue;

  /** Maximum per-dim values, packed */
  protected final byte[] maxPackedValue;

  protected long pointCount;

  /** An upper bound on how many points the caller will add (includes deletions) */
  private final long totalPointCount;

  private final int maxDoc;


  public SimpleTextBKDWriter(int maxDoc, Directory tempDir, String tempFileNamePrefix, int numDataDims, int numIndexDims, int bytesPerDim,
                              int maxPointsInLeafNode, double maxMBSortInHeap, long totalPointCount) throws IOException {
    verifyParams(numDataDims, numIndexDims, maxPointsInLeafNode, maxMBSortInHeap, totalPointCount);
    // We use tracking dir to deal with removing files on exception, so each place that
    // creates temp files doesn't need crazy try/finally/sucess logic:
    this.tempDir = new TrackingDirectoryWrapper(tempDir);
    this.tempFileNamePrefix = tempFileNamePrefix;
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.numDataDims = numDataDims;
    this.numIndexDims = numIndexDims;
    this.bytesPerDim = bytesPerDim;
    this.totalPointCount = totalPointCount;
    this.maxDoc = maxDoc;
    docsSeen = new FixedBitSet(maxDoc);
    packedBytesLength = numDataDims * bytesPerDim;
    packedIndexBytesLength = numIndexDims * bytesPerDim;

    scratchDiff = new byte[bytesPerDim];
    scratch1 = new byte[packedBytesLength];
    scratch2 = new byte[packedBytesLength];
    commonPrefixLengths = new int[numDataDims];

    minPackedValue = new byte[packedIndexBytesLength];
    maxPackedValue = new byte[packedIndexBytesLength];

    // dimensional values (numDims * bytesPerDim) +  docID (int)
    bytesPerDoc = packedBytesLength + Integer.BYTES;

    // Maximum number of points we hold in memory at any time
    maxPointsSortInHeap = (int) ((maxMBSortInHeap * 1024 * 1024) / (bytesPerDoc * numDataDims));

    // Finally, we must be able to hold at least the leaf node in heap during build:
    if (maxPointsSortInHeap < maxPointsInLeafNode) {
      throw new IllegalArgumentException("maxMBSortInHeap=" + maxMBSortInHeap + " only allows for maxPointsSortInHeap=" + maxPointsSortInHeap + ", but this is less than maxPointsInLeafNode=" + maxPointsInLeafNode + "; either increase maxMBSortInHeap or decrease maxPointsInLeafNode");
    }

    this.maxMBSortInHeap = maxMBSortInHeap;
  }

  public static void verifyParams(int numDataDims, int numIndexDims, int maxPointsInLeafNode, double maxMBSortInHeap, long totalPointCount) {
    // We encode dim in a single byte in the splitPackedValues, but we only expose 4 bits for it now, in case we want to use
    // remaining 4 bits for another purpose later
    if (numDataDims < 1 || numDataDims > MAX_DIMS) {
      throw new IllegalArgumentException("numDataDims must be 1 .. " + MAX_DIMS + " (got: " + numDataDims + ")");
    }
    if (numIndexDims < 1 || numIndexDims > numDataDims) {
      throw new IllegalArgumentException("numIndexDims must be 1 .. " + numDataDims + " (got: " + numIndexDims + ")");
    }
    if (maxPointsInLeafNode <= 0) {
      throw new IllegalArgumentException("maxPointsInLeafNode must be > 0; got " + maxPointsInLeafNode);
    }
    if (maxPointsInLeafNode > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalArgumentException("maxPointsInLeafNode must be <= ArrayUtil.MAX_ARRAY_LENGTH (= " + ArrayUtil.MAX_ARRAY_LENGTH + "); got " + maxPointsInLeafNode);
    }
    if (maxMBSortInHeap < 0.0) {
      throw new IllegalArgumentException("maxMBSortInHeap must be >= 0.0 (got: " + maxMBSortInHeap + ")");
    }
    if (totalPointCount < 0) {
      throw new IllegalArgumentException("totalPointCount must be >=0 (got: " + totalPointCount + ")");
    }
  }

  public void add(byte[] packedValue, int docID) throws IOException {
    if (packedValue.length != packedBytesLength) {
      throw new IllegalArgumentException("packedValue should be length=" + packedBytesLength + " (got: " + packedValue.length + ")");
    }
    if (pointCount >= totalPointCount) {
      throw new IllegalStateException("totalPointCount=" + totalPointCount + " was passed when we were created, but we just hit " + (pointCount + 1) + " values");
    }
    if (pointCount == 0) {
      assert pointWriter == null : "Point writer is already initialized";
      //total point count is an estimation but the final point count must be equal or lower to that number.
      if (totalPointCount > maxPointsSortInHeap) {
        pointWriter = new OfflinePointWriter(tempDir, tempFileNamePrefix, packedBytesLength, "spill", 0);
        tempInput = ((OfflinePointWriter)pointWriter).out;
      } else {
        pointWriter = new HeapPointWriter(Math.toIntExact(totalPointCount), packedBytesLength);
      }
      System.arraycopy(packedValue, 0, minPackedValue, 0, packedIndexBytesLength);
      System.arraycopy(packedValue, 0, maxPackedValue, 0, packedIndexBytesLength);
    } else {
      for(int dim=0;dim<numIndexDims;dim++) {
        int offset = dim*bytesPerDim;
        if (Arrays.compareUnsigned(packedValue, offset, offset + bytesPerDim, minPackedValue, offset, offset + bytesPerDim) < 0) {
          System.arraycopy(packedValue, offset, minPackedValue, offset, bytesPerDim);
        }
        if (Arrays.compareUnsigned(packedValue, offset, offset + bytesPerDim, maxPackedValue, offset, offset + bytesPerDim) > 0) {
          System.arraycopy(packedValue, offset, maxPackedValue, offset, bytesPerDim);
        }
      }
    }
    pointWriter.append(packedValue, docID);
    pointCount++;
    docsSeen.set(docID);
  }

  /** How many points have been added so far */
  public long getPointCount() {
    return pointCount;
  }

  /** Write a field from a {@link MutablePointValues}. This way of writing
   *  points is faster than regular writes with {@link BKDWriter#add} since
   *  there is opportunity for reordering points before writing them to
   *  disk. This method does not use transient disk in order to reorder points.
   */
  public long writeField(IndexOutput out, String fieldName, MutablePointValues reader) throws IOException {
    if (numIndexDims == 1) {
      return writeField1Dim(out, fieldName, reader);
    } else {
      return writeFieldNDims(out, fieldName, reader);
    }
  }


  /* In the 2+D case, we recursively pick the split dimension, compute the
   * median value and partition other values around it. */
  private long writeFieldNDims(IndexOutput out, String fieldName, MutablePointValues values) throws IOException {
    if (pointCount != 0) {
      throw new IllegalStateException("cannot mix add and writeField");
    }

    // Catch user silliness:
    if (finished == true) {
      throw new IllegalStateException("already finished");
    }

    // Mark that we already finished:
    finished = true;

    long countPerLeaf = pointCount = values.size();
    long innerNodeCount = 1;

    while (countPerLeaf > maxPointsInLeafNode) {
      countPerLeaf = (countPerLeaf+1)/2;
      innerNodeCount *= 2;
    }

    int numLeaves = Math.toIntExact(innerNodeCount);

    checkMaxLeafNodeCount(numLeaves);

    final byte[] splitPackedValues = new byte[numLeaves * (bytesPerDim + 1)];
    final long[] leafBlockFPs = new long[numLeaves];

    // compute the min/max for this slice
    Arrays.fill(minPackedValue, (byte) 0xff);
    Arrays.fill(maxPackedValue, (byte) 0);
    for (int i = 0; i < Math.toIntExact(pointCount); ++i) {
      values.getValue(i, scratchBytesRef1);
      for(int dim=0;dim<numIndexDims;dim++) {
        int offset = dim*bytesPerDim;
        if (Arrays.compareUnsigned(scratchBytesRef1.bytes, scratchBytesRef1.offset + offset, scratchBytesRef1.offset + offset + bytesPerDim, minPackedValue, offset, offset + bytesPerDim) < 0) {
          System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + offset, minPackedValue, offset, bytesPerDim);
        }
        if (Arrays.compareUnsigned(scratchBytesRef1.bytes, scratchBytesRef1.offset + offset, scratchBytesRef1.offset + offset + bytesPerDim, maxPackedValue, offset, offset + bytesPerDim) > 0) {
          System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + offset, maxPackedValue, offset, bytesPerDim);
        }
      }

      docsSeen.set(values.getDocID(i));
    }

    build(1, numLeaves, values, 0, Math.toIntExact(pointCount), out,
        minPackedValue, maxPackedValue, splitPackedValues, leafBlockFPs,
        new int[maxPointsInLeafNode]);

    long indexFP = out.getFilePointer();
    writeIndex(out, leafBlockFPs, splitPackedValues);
    return indexFP;
  }


  /* In the 1D case, we can simply sort points in ascending order and use the
   * same writing logic as we use at merge time. */
  private long writeField1Dim(IndexOutput out, String fieldName, MutablePointValues reader) throws IOException {
    MutablePointsReaderUtils.sort(maxDoc, packedIndexBytesLength, reader, 0, Math.toIntExact(reader.size()));

    final OneDimensionBKDWriter oneDimWriter = new OneDimensionBKDWriter(out);

    reader.intersect(new IntersectVisitor() {

      @Override
      public void visit(int docID, byte[] packedValue) throws IOException {
        oneDimWriter.add(packedValue, docID);
      }

      @Override
      public void visit(int docID) throws IOException {
        throw new IllegalStateException();
      }

      @Override
      public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        return Relation.CELL_CROSSES_QUERY;
      }
    });

    return oneDimWriter.finish();
  }

  private class OneDimensionBKDWriter {

    final IndexOutput out;
    final List<Long> leafBlockFPs = new ArrayList<>();
    final List<byte[]> leafBlockStartValues = new ArrayList<>();
    final byte[] leafValues = new byte[maxPointsInLeafNode * packedBytesLength];
    final int[] leafDocs = new int[maxPointsInLeafNode];
    long valueCount;
    int leafCount;

    OneDimensionBKDWriter(IndexOutput out) {
      if (numIndexDims != 1) {
        throw new UnsupportedOperationException("numIndexDims must be 1 but got " + numIndexDims);
      }
      if (pointCount != 0) {
        throw new IllegalStateException("cannot mix add and merge");
      }

      // Catch user silliness:
      if (finished == true) {
        throw new IllegalStateException("already finished");
      }

      // Mark that we already finished:
      finished = true;

      this.out = out;

      lastPackedValue = new byte[packedBytesLength];
    }

    // for asserts
    final byte[] lastPackedValue;
    int lastDocID;

    void add(byte[] packedValue, int docID) throws IOException {
      assert valueInOrder(valueCount + leafCount,
          0, lastPackedValue, packedValue, 0, docID, lastDocID);

      System.arraycopy(packedValue, 0, leafValues, leafCount * packedBytesLength, packedBytesLength);
      leafDocs[leafCount] = docID;
      docsSeen.set(docID);
      leafCount++;

      if (valueCount > totalPointCount) {
        throw new IllegalStateException("totalPointCount=" + totalPointCount + " was passed when we were created, but we just hit " + pointCount + " values");
      }

      if (leafCount == maxPointsInLeafNode) {
        // We write a block once we hit exactly the max count ... this is different from
        // when we flush a new segment, where we write between max/2 and max per leaf block,
        // so merged segments will behave differently from newly flushed segments:
        writeLeafBlock();
        leafCount = 0;
      }

      assert (lastDocID = docID) >= 0; // only assign when asserts are enabled
    }

    public long finish() throws IOException {
      if (leafCount > 0) {
        writeLeafBlock();
        leafCount = 0;
      }

      if (valueCount == 0) {
        return -1;
      }

      pointCount = valueCount;

      long indexFP = out.getFilePointer();

      int numInnerNodes = leafBlockStartValues.size();

      //System.out.println("BKDW: now rotate numInnerNodes=" + numInnerNodes + " leafBlockStarts=" + leafBlockStartValues.size());

      byte[] index = new byte[(1+numInnerNodes) * (1+bytesPerDim)];
      rotateToTree(1, 0, numInnerNodes, index, leafBlockStartValues);
      long[] arr = new long[leafBlockFPs.size()];
      for(int i=0;i<leafBlockFPs.size();i++) {
        arr[i] = leafBlockFPs.get(i);
      }
      writeIndex(out, arr, index);
      return indexFP;
    }

    private void writeLeafBlock() throws IOException {
      assert leafCount != 0;
      if (valueCount == 0) {
        System.arraycopy(leafValues, 0, minPackedValue, 0, packedIndexBytesLength);
      }
      System.arraycopy(leafValues, (leafCount - 1) * packedBytesLength, maxPackedValue, 0, packedIndexBytesLength);

      valueCount += leafCount;

      if (leafBlockFPs.size() > 0) {
        // Save the first (minimum) value in each leaf block except the first, to build the split value index in the end:
        leafBlockStartValues.add(ArrayUtil.copyOfSubArray(leafValues, 0, packedBytesLength));
      }
      leafBlockFPs.add(out.getFilePointer());
      checkMaxLeafNodeCount(leafBlockFPs.size());

      Arrays.fill(commonPrefixLengths, bytesPerDim);
      // Find per-dim common prefix:
      for(int dim=0;dim<numDataDims;dim++) {
        int offset1 = dim * bytesPerDim;
        int offset2 = (leafCount - 1) * packedBytesLength + offset1;
        for(int j=0;j<commonPrefixLengths[dim];j++) {
          if (leafValues[offset1+j] != leafValues[offset2+j]) {
            commonPrefixLengths[dim] = j;
            break;
          }
        }
      }

      writeLeafBlockDocs(out, leafDocs, 0, leafCount);

      final IntFunction<BytesRef> packedValues = new IntFunction<BytesRef>() {
        final BytesRef scratch = new BytesRef();

        {
          scratch.length = packedBytesLength;
          scratch.bytes = leafValues;
        }

        @Override
        public BytesRef apply(int i) {
          scratch.offset = packedBytesLength * i;
          return scratch;
        }
      };
      assert valuesInOrderAndBounds(leafCount, 0, ArrayUtil.copyOfSubArray(leafValues, 0, packedBytesLength),
          ArrayUtil.copyOfSubArray(leafValues, (leafCount - 1) * packedBytesLength, leafCount * packedBytesLength),
          packedValues, leafDocs, 0);
      writeLeafBlockPackedValues(out, commonPrefixLengths, leafCount, 0, packedValues);
    }

  }

  // TODO: there must be a simpler way?
  private void rotateToTree(int nodeID, int offset, int count, byte[] index, List<byte[]> leafBlockStartValues) {
    //System.out.println("ROTATE: nodeID=" + nodeID + " offset=" + offset + " count=" + count + " bpd=" + bytesPerDim + " index.length=" + index.length);
    if (count == 1) {
      // Leaf index node
      //System.out.println("  leaf index node");
      //System.out.println("  index[" + nodeID + "] = blockStartValues[" + offset + "]");
      System.arraycopy(leafBlockStartValues.get(offset), 0, index, nodeID*(1+bytesPerDim)+1, bytesPerDim);
    } else if (count > 1) {
      // Internal index node: binary partition of count
      int countAtLevel = 1;
      int totalCount = 0;
      while (true) {
        int countLeft = count - totalCount;
        //System.out.println("    cycle countLeft=" + countLeft + " coutAtLevel=" + countAtLevel);
        if (countLeft <= countAtLevel) {
          // This is the last level, possibly partially filled:
          int lastLeftCount = Math.min(countAtLevel/2, countLeft);
          assert lastLeftCount >= 0;
          int leftHalf = (totalCount-1)/2 + lastLeftCount;

          int rootOffset = offset + leftHalf;
          /*
          System.out.println("  last left count " + lastLeftCount);
          System.out.println("  leftHalf " + leftHalf + " rightHalf=" + (count-leftHalf-1));
          System.out.println("  rootOffset=" + rootOffset);
          */

          System.arraycopy(leafBlockStartValues.get(rootOffset), 0, index, nodeID*(1+bytesPerDim)+1, bytesPerDim);
          //System.out.println("  index[" + nodeID + "] = blockStartValues[" + rootOffset + "]");

          // TODO: we could optimize/specialize, when we know it's simply fully balanced binary tree
          // under here, to save this while loop on each recursion

          // Recurse left
          rotateToTree(2*nodeID, offset, leftHalf, index, leafBlockStartValues);

          // Recurse right
          rotateToTree(2*nodeID+1, rootOffset+1, count-leftHalf-1, index, leafBlockStartValues);
          return;
        }
        totalCount += countAtLevel;
        countAtLevel *= 2;
      }
    } else {
      assert count == 0;
    }
  }

  private void checkMaxLeafNodeCount(int numLeaves) {
    if ((1+bytesPerDim) * (long) numLeaves > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalStateException("too many nodes; increase maxPointsInLeafNode (currently " + maxPointsInLeafNode + ") and reindex");
    }
  }

  /** Writes the BKD tree to the provided {@link IndexOutput} and returns the file offset where index was written. */
  public long finish(IndexOutput out) throws IOException {
    // System.out.println("\nBKDTreeWriter.finish pointCount=" + pointCount + " out=" + out + " heapWriter=" + heapPointWriter);

    // TODO: specialize the 1D case?  it's much faster at indexing time (no partitioning on recurse...)

    // Catch user silliness:
    if (pointCount == 0) {
      throw new IllegalStateException("must index at least one point");
    }

    // Catch user silliness:
    if (finished == true) {
      throw new IllegalStateException("already finished");
    }

    //mark as finished
    finished = true;

    pointWriter.close();
    BKDRadixSelector.PathSlice points = new BKDRadixSelector.PathSlice(pointWriter, 0, pointCount);
    //clean up pointers
    tempInput = null;
    pointWriter = null;


    long countPerLeaf = pointCount;
    long innerNodeCount = 1;

    while (countPerLeaf > maxPointsInLeafNode) {
      countPerLeaf = (countPerLeaf+1)/2;
      innerNodeCount *= 2;
    }

    int numLeaves = (int) innerNodeCount;

    checkMaxLeafNodeCount(numLeaves);

    // NOTE: we could save the 1+ here, to use a bit less heap at search time, but then we'd need a somewhat costly check at each
    // step of the recursion to recompute the split dim:

    // Indexed by nodeID, but first (root) nodeID is 1.  We do 1+ because the lead byte at each recursion says which dim we split on.
    byte[] splitPackedValues = new byte[Math.toIntExact(numLeaves*(1+bytesPerDim))];

    // +1 because leaf count is power of 2 (e.g. 8), and innerNodeCount is power of 2 minus 1 (e.g. 7)
    long[] leafBlockFPs = new long[numLeaves];

    // Make sure the math above "worked":
    assert pointCount / numLeaves <= maxPointsInLeafNode: "pointCount=" + pointCount + " numLeaves=" + numLeaves + " maxPointsInLeafNode=" + maxPointsInLeafNode;

    //We re-use the selector so we do not need to create an object every time.
    BKDRadixSelector radixSelector = new BKDRadixSelector(numDataDims, numIndexDims, bytesPerDim, maxPointsSortInHeap, tempDir, tempFileNamePrefix);

    boolean success = false;
    try {


      build(1, numLeaves, points, out,
          radixSelector, minPackedValue, maxPackedValue,
            splitPackedValues, leafBlockFPs, new int[maxPointsInLeafNode]);


      // If no exception, we should have cleaned everything up:
      assert tempDir.getCreatedFiles().isEmpty();
      //long t2 = System.nanoTime();
      //System.out.println("write time: " + ((t2-t1)/1000000.0) + " msec");

      success = true;
    } finally {
      if (success == false) {
        IOUtils.deleteFilesIgnoringExceptions(tempDir, tempDir.getCreatedFiles());
      }
    }

    //System.out.println("Total nodes: " + innerNodeCount);

    // Write index:
    long indexFP = out.getFilePointer();
    writeIndex(out, leafBlockFPs, splitPackedValues);
    return indexFP;
  }

  /** Subclass can change how it writes the index. */
  private void writeIndex(IndexOutput out, long[] leafBlockFPs, byte[] splitPackedValues) throws IOException {
    write(out, NUM_DATA_DIMS);
    writeInt(out, numDataDims);
    newline(out);

    write(out, NUM_INDEX_DIMS);
    writeInt(out, numIndexDims);
    newline(out);

    write(out, BYTES_PER_DIM);
    writeInt(out, bytesPerDim);
    newline(out);

    write(out, MAX_LEAF_POINTS);
    writeInt(out, maxPointsInLeafNode);
    newline(out);

    write(out, INDEX_COUNT);
    writeInt(out, leafBlockFPs.length);
    newline(out);

    write(out, MIN_VALUE);
    BytesRef br = new BytesRef(minPackedValue, 0, minPackedValue.length);
    write(out, br.toString());
    newline(out);

    write(out, MAX_VALUE);
    br = new BytesRef(maxPackedValue, 0, maxPackedValue.length);
    write(out, br.toString());
    newline(out);

    write(out, POINT_COUNT);
    writeLong(out, pointCount);
    newline(out);

    write(out, DOC_COUNT);
    writeInt(out, docsSeen.cardinality());
    newline(out);

    for(int i=0;i<leafBlockFPs.length;i++) {
      write(out, BLOCK_FP);
      writeLong(out, leafBlockFPs[i]);
      newline(out);
    }

    assert (splitPackedValues.length % (1 + bytesPerDim)) == 0;
    int count = splitPackedValues.length / (1 + bytesPerDim);
    assert count == leafBlockFPs.length;

    write(out, SPLIT_COUNT);
    writeInt(out, count);
    newline(out);

    for(int i=0;i<count;i++) {
      write(out, SPLIT_DIM);
      writeInt(out, splitPackedValues[i * (1 + bytesPerDim)] & 0xff);
      newline(out);
      write(out, SPLIT_VALUE);
      br = new BytesRef(splitPackedValues, 1+(i * (1+bytesPerDim)), bytesPerDim);
      write(out, br.toString());
      newline(out);
    }
  }

  protected void writeLeafBlockDocs(IndexOutput out, int[] docIDs, int start, int count) throws IOException {
    write(out, BLOCK_COUNT);
    writeInt(out, count);
    newline(out);
    for(int i=0;i<count;i++) {
      write(out, BLOCK_DOC_ID);
      writeInt(out, docIDs[start+i]);
      newline(out);
    }
  }

  protected void writeLeafBlockPackedValues(IndexOutput out, int[] commonPrefixLengths, int count, int sortedDim, IntFunction<BytesRef> packedValues) throws IOException {
    for (int i = 0; i < count; ++i) {
      BytesRef packedValue = packedValues.apply(i);
      // NOTE: we don't do prefix coding, so we ignore commonPrefixLengths
      write(out, BLOCK_VALUE);
      write(out, packedValue.toString());
      newline(out);
    }
  }

  private void writeLeafBlockPackedValuesRange(IndexOutput out, int[] commonPrefixLengths, int start, int end, IntFunction<BytesRef> packedValues) throws IOException {
    for (int i = start; i < end; ++i) {
      BytesRef ref = packedValues.apply(i);
      assert ref.length == packedBytesLength;

      for(int dim=0;dim<numDataDims;dim++) {
        int prefix = commonPrefixLengths[dim];
        out.writeBytes(ref.bytes, ref.offset + dim*bytesPerDim + prefix, bytesPerDim-prefix);
      }
    }
  }

  private static int runLen(IntFunction<BytesRef> packedValues, int start, int end, int byteOffset) {
    BytesRef first = packedValues.apply(start);
    byte b = first.bytes[first.offset + byteOffset];
    for (int i = start + 1; i < end; ++i) {
      BytesRef ref = packedValues.apply(i);
      byte b2 = ref.bytes[ref.offset + byteOffset];
      assert Byte.toUnsignedInt(b2) >= Byte.toUnsignedInt(b);
      if (b != b2) {
        return i - start;
      }
    }
    return end - start;
  }

  @Override
  public void close() throws IOException {
    if (tempInput != null) {
      // NOTE: this should only happen on exception, e.g. caller calls close w/o calling finish:
      try {
        tempInput.close();
      } finally {
        tempDir.deleteFile(tempInput.getName());
        tempInput = null;
      }
    }
  }

  /** Called on exception, to check whether the checksum is also corrupt in this source, and add that
   *  information (checksum matched or didn't) as a suppressed exception. */
  private Error verifyChecksum(Throwable priorException, PointWriter writer) throws IOException {
    assert priorException != null;
    // TODO: we could improve this, to always validate checksum as we recurse, if we shared left and
    // right reader after recursing to children, and possibly within recursed children,
    // since all together they make a single pass through the file.  But this is a sizable re-org,
    // and would mean leaving readers (IndexInputs) open for longer:
    if (writer instanceof OfflinePointWriter) {
      // We are reading from a temp file; go verify the checksum:
      String tempFileName = ((OfflinePointWriter) writer).name;
      try (ChecksumIndexInput in = tempDir.openChecksumInput(tempFileName, IOContext.READONCE)) {
        CodecUtil.checkFooter(in, priorException);
      }
    }

    // We are reading from heap; nothing to add:
    throw IOUtils.rethrowAlways(priorException);
  }

  /** Called only in assert */
  private boolean valueInBounds(BytesRef packedValue, byte[] minPackedValue, byte[] maxPackedValue) {
    for(int dim=0;dim<numIndexDims;dim++) {
      int offset = bytesPerDim*dim;
      if (Arrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + bytesPerDim, minPackedValue, offset, offset + bytesPerDim) < 0) {
        return false;
      }
      if (Arrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + bytesPerDim, maxPackedValue, offset, offset + bytesPerDim) > 0) {
        return false;
      }
    }

    return true;
  }

  protected int split(byte[] minPackedValue, byte[] maxPackedValue) {
    // Find which dim has the largest span so we can split on it:
    int splitDim = -1;
    for(int dim=0;dim<numIndexDims;dim++) {
      NumericUtils.subtract(bytesPerDim, dim, maxPackedValue, minPackedValue, scratchDiff);
      if (splitDim == -1 || Arrays.compareUnsigned(scratchDiff, 0, bytesPerDim, scratch1, 0, bytesPerDim) > 0) {
        System.arraycopy(scratchDiff, 0, scratch1, 0, bytesPerDim);
        splitDim = dim;
      }
    }

    //System.out.println("SPLIT: " + splitDim);
    return splitDim;
  }

  /** Pull a partition back into heap once the point count is low enough while recursing. */
  private HeapPointWriter switchToHeap(PointWriter source) throws IOException {
    int count = Math.toIntExact(source.count());
    try (PointReader reader = source.getReader(0, count);
        HeapPointWriter writer = new HeapPointWriter(count, packedBytesLength)) {
      for(int i=0;i<count;i++) {
        boolean hasNext = reader.next();
        assert hasNext;
        writer.append(reader.pointValue());
      }
      return writer;
    } catch (Throwable t) {
      throw verifyChecksum(t, source);
    }
  }

  /* Recursively reorders the provided reader and writes the bkd-tree on the fly. */
  private void build(int nodeID, int leafNodeOffset,
      MutablePointValues reader, int from, int to,
      IndexOutput out,
      byte[] minPackedValue, byte[] maxPackedValue,
      byte[] splitPackedValues,
      long[] leafBlockFPs,
      int[] spareDocIds) throws IOException {

    if (nodeID >= leafNodeOffset) {
      // leaf node
      final int count = to - from;
      assert count <= maxPointsInLeafNode;

      // Compute common prefixes
      Arrays.fill(commonPrefixLengths, bytesPerDim);
      reader.getValue(from, scratchBytesRef1);
      for (int i = from + 1; i < to; ++i) {
        reader.getValue(i, scratchBytesRef2);
        for (int dim=0;dim<numDataDims;dim++) {
          final int offset = dim * bytesPerDim;
          for(int j=0;j<commonPrefixLengths[dim];j++) {
            if (scratchBytesRef1.bytes[scratchBytesRef1.offset+offset+j] != scratchBytesRef2.bytes[scratchBytesRef2.offset+offset+j]) {
              commonPrefixLengths[dim] = j;
              break;
            }
          }
        }
      }

      // Find the dimension that has the least number of unique bytes at commonPrefixLengths[dim]
      FixedBitSet[] usedBytes = new FixedBitSet[numDataDims];
      for (int dim = 0; dim < numDataDims; ++dim) {
        if (commonPrefixLengths[dim] < bytesPerDim) {
          usedBytes[dim] = new FixedBitSet(256);
        }
      }
      for (int i = from + 1; i < to; ++i) {
        for (int dim=0;dim<numDataDims;dim++) {
          if (usedBytes[dim] != null) {
            byte b = reader.getByteAt(i, dim * bytesPerDim + commonPrefixLengths[dim]);
            usedBytes[dim].set(Byte.toUnsignedInt(b));
          }
        }
      }
      int sortedDim = 0;
      int sortedDimCardinality = Integer.MAX_VALUE;
      for (int dim = 0; dim < numDataDims; ++dim) {
        if (usedBytes[dim] != null) {
          final int cardinality = usedBytes[dim].cardinality();
          if (cardinality < sortedDimCardinality) {
            sortedDim = dim;
            sortedDimCardinality = cardinality;
          }
        }
      }

      // sort by sortedDim
      MutablePointsReaderUtils.sortByDim(numDataDims, numIndexDims, sortedDim, bytesPerDim, commonPrefixLengths,
                                         reader, from, to, scratchBytesRef1, scratchBytesRef2);

      // Save the block file pointer:
      leafBlockFPs[nodeID - leafNodeOffset] = out.getFilePointer();

      // Write doc IDs
      int[] docIDs = spareDocIds;
      for (int i = from; i < to; ++i) {
        docIDs[i - from] = reader.getDocID(i);
      }
      writeLeafBlockDocs(out, docIDs, 0, count);

      // Write the common prefixes:
      reader.getValue(from, scratchBytesRef1);
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset, scratch1, 0, packedBytesLength);

      // Write the full values:
      IntFunction<BytesRef> packedValues = new IntFunction<BytesRef>() {
        @Override
        public BytesRef apply(int i) {
          reader.getValue(from + i, scratchBytesRef1);
          return scratchBytesRef1;
        }
      };
      assert valuesInOrderAndBounds(count, sortedDim, minPackedValue, maxPackedValue, packedValues,
          docIDs, 0);
      writeLeafBlockPackedValues(out, commonPrefixLengths, count, sortedDim, packedValues);

    } else {
      // inner node

      // compute the split dimension and partition around it
      final int splitDim = split(minPackedValue, maxPackedValue);
      final int mid = (from + to + 1) >>> 1;

      int commonPrefixLen = bytesPerDim;
      for (int i = 0; i < bytesPerDim; ++i) {
        if (minPackedValue[splitDim * bytesPerDim + i] != maxPackedValue[splitDim * bytesPerDim + i]) {
          commonPrefixLen = i;
          break;
        }
      }
      MutablePointsReaderUtils.partition(numDataDims, numIndexDims, maxDoc, splitDim, bytesPerDim, commonPrefixLen,
          reader, from, to, mid, scratchBytesRef1, scratchBytesRef2);

      // set the split value
      final int address = nodeID * (1+bytesPerDim);
      splitPackedValues[address] = (byte) splitDim;
      reader.getValue(mid, scratchBytesRef1);
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + splitDim * bytesPerDim, splitPackedValues, address + 1, bytesPerDim);

      byte[] minSplitPackedValue = ArrayUtil.copyOfSubArray(minPackedValue, 0, packedIndexBytesLength);
      byte[] maxSplitPackedValue = ArrayUtil.copyOfSubArray(maxPackedValue, 0, packedIndexBytesLength);
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + splitDim * bytesPerDim,
          minSplitPackedValue, splitDim * bytesPerDim, bytesPerDim);
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + splitDim * bytesPerDim,
          maxSplitPackedValue, splitDim * bytesPerDim, bytesPerDim);

      // recurse
      build(nodeID * 2, leafNodeOffset, reader, from, mid, out,
          minPackedValue, maxSplitPackedValue, splitPackedValues, leafBlockFPs, spareDocIds);
      build(nodeID * 2 + 1, leafNodeOffset, reader, mid, to, out,
          minSplitPackedValue, maxPackedValue, splitPackedValues, leafBlockFPs, spareDocIds);
    }
  }

  /** The array (sized numDims) of PathSlice describe the cell we have currently recursed to. */
  private void build(int nodeID, int leafNodeOffset,
                     BKDRadixSelector.PathSlice points,
                     IndexOutput out,
                     BKDRadixSelector radixSelector,
                     byte[] minPackedValue, byte[] maxPackedValue,
                     byte[] splitPackedValues,
                     long[] leafBlockFPs,
                     int[] spareDocIds) throws IOException {

    if (nodeID >= leafNodeOffset) {

      // Leaf node: write block
      // We can write the block in any order so by default we write it sorted by the dimension that has the
      // least number of unique bytes at commonPrefixLengths[dim], which makes compression more efficient
      HeapPointWriter heapSource;
      if (points.writer instanceof HeapPointWriter == false) {
        // Adversarial cases can cause this, e.g. merging big segments with most of the points deleted
        heapSource  = switchToHeap(points.writer);
      } else {
        heapSource = (HeapPointWriter) points.writer;
      }

      int from = Math.toIntExact(points.start);
      int to = Math.toIntExact(points.start + points.count);

      //we store common prefix on scratch1
      computeCommonPrefixLength(heapSource, scratch1);

      int sortedDim = 0;
      int sortedDimCardinality = Integer.MAX_VALUE;
      FixedBitSet[] usedBytes = new FixedBitSet[numDataDims];
      for (int dim = 0; dim < numDataDims; ++dim) {
        if (commonPrefixLengths[dim] < bytesPerDim) {
          usedBytes[dim] = new FixedBitSet(256);
        }
      }
      //Find the dimension to compress
      for (int dim = 0; dim < numDataDims; dim++) {
        int prefix = commonPrefixLengths[dim];
        if (prefix < bytesPerDim) {
          int offset = dim * bytesPerDim;
          for (int i = 0; i < heapSource.count(); ++i) {
            PointValue value = heapSource.getPackedValueSlice(i);
            BytesRef packedValue = value.packedValue();
            int bucket = packedValue.bytes[packedValue.offset + offset + prefix] & 0xff;
            usedBytes[dim].set(bucket);
          }
          int cardinality = usedBytes[dim].cardinality();
          if (cardinality < sortedDimCardinality) {
            sortedDim = dim;
            sortedDimCardinality = cardinality;
          }
        }
      }

      // sort the chosen dimension
      radixSelector.heapRadixSort(heapSource, from, to, sortedDim, commonPrefixLengths[sortedDim]);

      // Save the block file pointer:
      leafBlockFPs[nodeID - leafNodeOffset] = out.getFilePointer();
      //System.out.println("  write leaf block @ fp=" + out.getFilePointer());

      // Write docIDs first, as their own chunk, so that at intersect time we can add all docIDs w/o
      // loading the values:
      int count = to - from;
      assert count > 0: "nodeID=" + nodeID + " leafNodeOffset=" + leafNodeOffset;
      // Write doc IDs
      int[] docIDs = spareDocIds;
      for (int i = 0; i < count; i++) {
        docIDs[i] = heapSource.getPackedValueSlice(from + i).docID();
      }
      writeLeafBlockDocs(out, spareDocIds, 0, count);

      // TODO: minor opto: we don't really have to write the actual common prefixes, because BKDReader on recursing can regenerate it for us
      // from the index, much like how terms dict does so from the FST:

      // Write the full values:
      IntFunction<BytesRef> packedValues = new IntFunction<BytesRef>() {
        final BytesRef scratch = new BytesRef();

        {
          scratch.length = packedBytesLength;
        }

        @Override
        public BytesRef apply(int i) {
          PointValue value = heapSource.getPackedValueSlice(from + i);
          return value.packedValue();
        }
      };
      assert valuesInOrderAndBounds(count, sortedDim, minPackedValue, maxPackedValue, packedValues,
          docIDs, 0);
      writeLeafBlockPackedValues(out, commonPrefixLengths, count, sortedDim, packedValues);

    } else {
      // Inner node: partition/recurse

      int splitDim;
      if (numIndexDims > 1) {
        splitDim = split(minPackedValue, maxPackedValue);
      } else {
        splitDim = 0;
      }

      assert nodeID < splitPackedValues.length : "nodeID=" + nodeID + " splitValues.length=" + splitPackedValues.length;

      // How many points will be in the left tree:
      long rightCount = points.count / 2;
      long leftCount = points.count - rightCount;

      int commonPrefixLen = Arrays.mismatch(minPackedValue, splitDim * bytesPerDim,
          splitDim * bytesPerDim + bytesPerDim, maxPackedValue, splitDim * bytesPerDim,
          splitDim * bytesPerDim + bytesPerDim);
      if (commonPrefixLen == -1) {
        commonPrefixLen = bytesPerDim;
      }

      BKDRadixSelector.PathSlice[] pathSlices = new BKDRadixSelector.PathSlice[2];

      byte[] splitValue =  radixSelector.select(points, pathSlices, points.start, points.start + points.count,  points.start + leftCount, splitDim, commonPrefixLen);

      int address = nodeID * (1 + bytesPerDim);
      splitPackedValues[address] = (byte) splitDim;
      System.arraycopy(splitValue, 0, splitPackedValues, address + 1, bytesPerDim);

      byte[] minSplitPackedValue = new byte[packedIndexBytesLength];
      System.arraycopy(minPackedValue, 0, minSplitPackedValue, 0, packedIndexBytesLength);

      byte[] maxSplitPackedValue = new byte[packedIndexBytesLength];
      System.arraycopy(maxPackedValue, 0, maxSplitPackedValue, 0, packedIndexBytesLength);

      System.arraycopy(splitValue, 0, minSplitPackedValue, splitDim * bytesPerDim, bytesPerDim);
      System.arraycopy(splitValue, 0, maxSplitPackedValue, splitDim * bytesPerDim, bytesPerDim);

      // Recurse on left tree:
      build(2*nodeID, leafNodeOffset, pathSlices[0], out, radixSelector,
            minPackedValue, maxSplitPackedValue, splitPackedValues, leafBlockFPs, spareDocIds);

      // TODO: we could "tail recurse" here?  have our parent discard its refs as we recurse right?
      // Recurse on right tree:
      build(2*nodeID+1, leafNodeOffset, pathSlices[1], out, radixSelector,
            minSplitPackedValue, maxPackedValue, splitPackedValues, leafBlockFPs, spareDocIds);
    }
  }

  private void computeCommonPrefixLength(HeapPointWriter heapPointWriter, byte[] commonPrefix) {
    Arrays.fill(commonPrefixLengths, bytesPerDim);
    PointValue value = heapPointWriter.getPackedValueSlice(0);
    BytesRef packedValue = value.packedValue();
    for (int dim = 0; dim < numDataDims; dim++) {
      System.arraycopy(packedValue.bytes, packedValue.offset + dim * bytesPerDim, commonPrefix, dim * bytesPerDim, bytesPerDim);
    }
    for (int i = 1; i < heapPointWriter.count(); i++) {
      value = heapPointWriter.getPackedValueSlice(i);
      packedValue = value.packedValue();
      for (int dim = 0; dim < numDataDims; dim++) {
        if (commonPrefixLengths[dim] != 0) {
          int j = Arrays.mismatch(commonPrefix, dim * bytesPerDim, dim * bytesPerDim + commonPrefixLengths[dim], packedValue.bytes, packedValue.offset + dim * bytesPerDim, packedValue.offset + dim * bytesPerDim + commonPrefixLengths[dim]);
          if (j != -1) {
            commonPrefixLengths[dim] = j;
          }
        }
      }
    }
  }

  // only called from assert
  private boolean valuesInOrderAndBounds(int count, int sortedDim, byte[] minPackedValue, byte[] maxPackedValue,
      IntFunction<BytesRef> values, int[] docs, int docsOffset) throws IOException {
    byte[] lastPackedValue = new byte[packedBytesLength];
    int lastDoc = -1;
    for (int i=0;i<count;i++) {
      BytesRef packedValue = values.apply(i);
      assert packedValue.length == packedBytesLength;
      assert valueInOrder(i, sortedDim, lastPackedValue, packedValue.bytes, packedValue.offset,
          docs[docsOffset + i], lastDoc);
      lastDoc = docs[docsOffset + i];

      // Make sure this value does in fact fall within this leaf cell:
      assert valueInBounds(packedValue, minPackedValue, maxPackedValue);
    }
    return true;
  }

  // only called from assert
  private boolean valueInOrder(long ord, int sortedDim, byte[] lastPackedValue, byte[] packedValue, int packedValueOffset,
      int doc, int lastDoc) {
    int dimOffset = sortedDim * bytesPerDim;
    if (ord > 0) {
      int cmp = Arrays.compareUnsigned(lastPackedValue, dimOffset, dimOffset + bytesPerDim, packedValue, packedValueOffset + dimOffset, packedValueOffset + dimOffset + bytesPerDim);
      if (cmp > 0) {
        throw new AssertionError("values out of order: last value=" + new BytesRef(lastPackedValue) + " current value=" + new BytesRef(packedValue, packedValueOffset, packedBytesLength) + " ord=" + ord + " sortedDim=" + sortedDim);
      }
      if (cmp == 0  && numDataDims > numIndexDims) {
        int dataOffset = numIndexDims * bytesPerDim;
        cmp = Arrays.compareUnsigned(lastPackedValue, dataOffset, packedBytesLength, packedValue, packedValueOffset + dataOffset, packedValueOffset + packedBytesLength);
        if (cmp > 0) {
          throw new AssertionError("data values out of order: last value=" + new BytesRef(lastPackedValue) + " current value=" + new BytesRef(packedValue, packedValueOffset, packedBytesLength) + " ord=" + ord);
        }
      }
      if (cmp == 0 && doc < lastDoc) {
        throw new AssertionError("docs out of order: last doc=" + lastDoc + " current doc=" + doc + " ord=" + ord + " sortedDim=" + sortedDim);
      }
    }
    System.arraycopy(packedValue, packedValueOffset, lastPackedValue, 0, packedBytesLength);
    return true;
  }

  private void write(IndexOutput out, String s) throws IOException {
    SimpleTextUtil.write(out, s, scratch);
  }

  private void writeInt(IndexOutput out, int x) throws IOException {
    SimpleTextUtil.write(out, Integer.toString(x), scratch);
  }

  private void writeLong(IndexOutput out, long x) throws IOException {
    SimpleTextUtil.write(out, Long.toString(x), scratch);
  }

  private void write(IndexOutput out, BytesRef b) throws IOException {
    SimpleTextUtil.write(out, b);
  }

  private void newline(IndexOutput out) throws IOException {
    SimpleTextUtil.writeNewline(out);
  }
}
