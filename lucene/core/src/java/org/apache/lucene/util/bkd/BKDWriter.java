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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.IntFunction;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.GrowableByteArrayDataOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefComparator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.MSBRadixSorter;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.OfflineSorter;
import org.apache.lucene.util.PriorityQueue;

// TODO
//   - allow variable length byte[] (across docs and dims), but this is quite a bit more hairy
//   - we could also index "auto-prefix terms" here, and use better compression, and maybe only use for the "fully contained" case so we'd
//     only index docIDs
//   - the index could be efficiently encoded as an FST, so we don't have wasteful
//     (monotonic) long[] leafBlockFPs; or we could use MonotonicLongValues ... but then
//     the index is already plenty small: 60M OSM points --> 1.1 MB with 128 points
//     per leaf, and you can reduce that by putting more points per leaf
//   - we could use threads while building; the higher nodes are very parallelizable

/** Recursively builds a block KD-tree to assign all incoming points in N-dim space to smaller
 *  and smaller N-dim rectangles (cells) until the number of points in a given
 *  rectangle is &lt;= <code>maxPointsInLeafNode</code>.  The tree is
 *  fully balanced, which means the leaf nodes will have between 50% and 100% of
 *  the requested <code>maxPointsInLeafNode</code>.  Values that fall exactly
 *  on a cell boundary may be in either cell.
 *
 *  <p>The number of dimensions can be 1 to 8, but every byte[] value is fixed length.
 *
 *  <p>
 *  See <a href="https://www.cs.duke.edu/~pankaj/publications/papers/bkd-sstd.pdf">this paper</a> for details.
 *
 *  <p>This consumes heap during writing: it allocates a <code>LongBitSet(numPoints)</code>,
 *  and then uses up to the specified {@code maxMBSortInHeap} heap space for writing.
 *
 *  <p>
 *  <b>NOTE</b>: This can write at most Integer.MAX_VALUE * <code>maxPointsInLeafNode</code> total points.
 *
 * @lucene.experimental */

public class BKDWriter implements Closeable {

  public static final String CODEC_NAME = "BKD";
  public static final int VERSION_START = 4; // version used by Lucene 7.0
  //public static final int VERSION_CURRENT = VERSION_START;
  public static final int VERSION_LEAF_STORES_BOUNDS = 5;
  public static final int VERSION_SELECTIVE_INDEXING = 6;
  public static final int VERSION_CURRENT = VERSION_SELECTIVE_INDEXING;

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

  private OfflinePointWriter offlinePointWriter;
  private HeapPointWriter heapPointWriter;

  private IndexOutput tempInput;
  protected final int maxPointsInLeafNode;
  private final int maxPointsSortInHeap;

  /** Minimum per-dim values, packed */
  protected final byte[] minPackedValue;

  /** Maximum per-dim values, packed */
  protected final byte[] maxPackedValue;

  protected long pointCount;

  /** true if we have so many values that we must write ords using long (8 bytes) instead of int (4 bytes) */
  protected final boolean longOrds;

  /** An upper bound on how many points the caller will add (includes deletions) */
  private final long totalPointCount;

  /** True if every document has at most one value.  We specialize this case by not bothering to store the ord since it's redundant with docID.  */
  protected final boolean singleValuePerDoc;

  /** How much heap OfflineSorter is allowed to use */
  protected final OfflineSorter.BufferSize offlineSorterBufferMB;

  /** How much heap OfflineSorter is allowed to use */
  protected final int offlineSorterMaxTempFiles;

  private final int maxDoc;

  public BKDWriter(int maxDoc, Directory tempDir, String tempFileNamePrefix, int numDataDims, int numIndexDims, int bytesPerDim,
                   int maxPointsInLeafNode, double maxMBSortInHeap, long totalPointCount, boolean singleValuePerDoc) throws IOException {
    this(maxDoc, tempDir, tempFileNamePrefix, numDataDims, numIndexDims, bytesPerDim, maxPointsInLeafNode, maxMBSortInHeap, totalPointCount, singleValuePerDoc,
         totalPointCount > Integer.MAX_VALUE, Math.max(1, (long) maxMBSortInHeap), OfflineSorter.MAX_TEMPFILES);
  }

  protected BKDWriter(int maxDoc, Directory tempDir, String tempFileNamePrefix, int numDataDims, int numIndexDims, int bytesPerDim,
                      int maxPointsInLeafNode, double maxMBSortInHeap, long totalPointCount,
                      boolean singleValuePerDoc, boolean longOrds, long offlineSorterBufferMB, int offlineSorterMaxTempFiles) throws IOException {
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
    this.offlineSorterBufferMB = OfflineSorter.BufferSize.megabytes(offlineSorterBufferMB);
    this.offlineSorterMaxTempFiles = offlineSorterMaxTempFiles;
    docsSeen = new FixedBitSet(maxDoc);
    packedBytesLength = numDataDims * bytesPerDim;
    packedIndexBytesLength = numIndexDims * bytesPerDim;

    scratchDiff = new byte[bytesPerDim];
    scratch1 = new byte[packedBytesLength];
    scratch2 = new byte[packedBytesLength];
    commonPrefixLengths = new int[numDataDims];

    minPackedValue = new byte[packedIndexBytesLength];
    maxPackedValue = new byte[packedIndexBytesLength];

    // If we may have more than 1+Integer.MAX_VALUE values, then we must encode ords with long (8 bytes), else we can use int (4 bytes).
    this.longOrds = longOrds;

    this.singleValuePerDoc = singleValuePerDoc;

    // dimensional values (numDims * bytesPerDim) + ord (int or long) + docID (int)
    if (singleValuePerDoc) {
      // Lucene only supports up to 2.1 docs, so we better not need longOrds in this case:
      assert longOrds == false;
      bytesPerDoc = packedBytesLength + Integer.BYTES;
    } else if (longOrds) {
      bytesPerDoc = packedBytesLength + Long.BYTES + Integer.BYTES;
    } else {
      bytesPerDoc = packedBytesLength + Integer.BYTES + Integer.BYTES;
    }

    // As we recurse, we compute temporary partitions of the data, halving the
    // number of points at each recursion.  Once there are few enough points,
    // we can switch to sorting in heap instead of offline (on disk).  At any
    // time in the recursion, we hold the number of points at that level, plus
    // all recursive halves (i.e. 16 + 8 + 4 + 2) so the memory usage is 2X
    // what that level would consume, so we multiply by 0.5 to convert from
    // bytes to points here.  Each dimension has its own sorted partition, so
    // we must divide by numDims as wel.

    maxPointsSortInHeap = (int) (0.5 * (maxMBSortInHeap * 1024 * 1024) / (bytesPerDoc * numDataDims));

    // Finally, we must be able to hold at least the leaf node in heap during build:
    if (maxPointsSortInHeap < maxPointsInLeafNode) {
      throw new IllegalArgumentException("maxMBSortInHeap=" + maxMBSortInHeap + " only allows for maxPointsSortInHeap=" + maxPointsSortInHeap + ", but this is less than maxPointsInLeafNode=" + maxPointsInLeafNode + "; either increase maxMBSortInHeap or decrease maxPointsInLeafNode");
    }

    // We write first maxPointsSortInHeap in heap, then cutover to offline for additional points:
    heapPointWriter = new HeapPointWriter(16, maxPointsSortInHeap, packedBytesLength, longOrds, singleValuePerDoc);

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

  /** If the current segment has too many points then we spill over to temp files / offline sort. */
  private void spillToOffline() throws IOException {

    // For each .add we just append to this input file, then in .finish we sort this input and resursively build the tree:
    offlinePointWriter = new OfflinePointWriter(tempDir, tempFileNamePrefix, packedBytesLength, longOrds, "spill", 0, singleValuePerDoc);
    tempInput = offlinePointWriter.out;
    PointReader reader = heapPointWriter.getReader(0, pointCount);
    for(int i=0;i<pointCount;i++) {
      boolean hasNext = reader.next();
      assert hasNext;
      offlinePointWriter.append(reader.packedValue(), i, heapPointWriter.docIDs[i]);
    }

    heapPointWriter = null;
  }

  public void add(byte[] packedValue, int docID) throws IOException {
    if (packedValue.length != packedBytesLength) {
      throw new IllegalArgumentException("packedValue should be length=" + packedBytesLength + " (got: " + packedValue.length + ")");
    }

    if (pointCount >= maxPointsSortInHeap) {
      if (offlinePointWriter == null) {
        spillToOffline();
      }
      offlinePointWriter.append(packedValue, pointCount, docID);
    } else {
      // Not too many points added yet, continue using heap:
      heapPointWriter.append(packedValue, pointCount, docID);
    }

    // TODO: we could specialize for the 1D case:
    if (pointCount == 0) {
      System.arraycopy(packedValue, 0, minPackedValue, 0, packedIndexBytesLength);
      System.arraycopy(packedValue, 0, maxPackedValue, 0, packedIndexBytesLength);
    } else {
      for(int dim=0;dim<numIndexDims;dim++) {
        int offset = dim*bytesPerDim;
        if (FutureArrays.compareUnsigned(packedValue, offset, offset + bytesPerDim, minPackedValue, offset, offset + bytesPerDim) < 0) {
          System.arraycopy(packedValue, offset, minPackedValue, offset, bytesPerDim);
        }
        if (FutureArrays.compareUnsigned(packedValue, offset, offset + bytesPerDim, maxPackedValue, offset, offset + bytesPerDim) > 0) {
          System.arraycopy(packedValue, offset, maxPackedValue, offset, bytesPerDim);
        }
      }
    }

    pointCount++;
    if (pointCount > totalPointCount) {
      throw new IllegalStateException("totalPointCount=" + totalPointCount + " was passed when we were created, but we just hit " + pointCount + " values");
    }
    docsSeen.set(docID);
  }

  /** How many points have been added so far */
  public long getPointCount() {
    return pointCount;
  }

  private static class MergeReader {
    final BKDReader bkd;
    final BKDReader.IntersectState state;
    final MergeState.DocMap docMap;

    /** Current doc ID */
    public int docID;

    /** Which doc in this block we are up to */
    private int docBlockUpto;

    /** How many docs in the current block */
    private int docsInBlock;

    /** Which leaf block we are up to */
    private int blockID;

    private final byte[] packedValues;

    public MergeReader(BKDReader bkd, MergeState.DocMap docMap) throws IOException {
      this.bkd = bkd;
      state = new BKDReader.IntersectState(bkd.in.clone(),
                                           bkd.numDataDims,
                                           bkd.packedBytesLength,
                                           bkd.packedIndexBytesLength,
                                           bkd.maxPointsInLeafNode,
                                           null,
                                           null);
      this.docMap = docMap;
      state.in.seek(bkd.getMinLeafBlockFP());
      this.packedValues = new byte[bkd.maxPointsInLeafNode * bkd.packedBytesLength];
    }

    public boolean next() throws IOException {
      //System.out.println("MR.next this=" + this);
      while (true) {
        if (docBlockUpto == docsInBlock) {
          if (blockID == bkd.leafNodeOffset) {
            //System.out.println("  done!");
            return false;
          }
          //System.out.println("  new block @ fp=" + state.in.getFilePointer());
          docsInBlock = bkd.readDocIDs(state.in, state.in.getFilePointer(), state.scratchDocIDs);
          assert docsInBlock > 0;
          docBlockUpto = 0;
          bkd.visitDocValues(state.commonPrefixLengths, state.scratchDataPackedValue, state.scratchMinIndexPackedValue, state.scratchMaxIndexPackedValue, state.in, state.scratchDocIDs, docsInBlock, new IntersectVisitor() {
            int i = 0;

            @Override
            public void visit(int docID) {
              throw new UnsupportedOperationException();
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
              assert docID == state.scratchDocIDs[i];
              System.arraycopy(packedValue, 0, packedValues, i * bkd.packedBytesLength, bkd.packedBytesLength);
              i++;
            }

            @Override
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
              return Relation.CELL_CROSSES_QUERY;
            }

          });

          blockID++;
        }

        final int index = docBlockUpto++;
        int oldDocID = state.scratchDocIDs[index];

        int mappedDocID;
        if (docMap == null) {
          mappedDocID = oldDocID;
        } else {
          mappedDocID = docMap.get(oldDocID);
        }

        if (mappedDocID != -1) {
          // Not deleted!
          docID = mappedDocID;
          System.arraycopy(packedValues, index * bkd.packedBytesLength, state.scratchDataPackedValue, 0, bkd.packedBytesLength);
          return true;
        }
      }
    }
  }

  private static class BKDMergeQueue extends PriorityQueue<MergeReader> {
    private final int bytesPerDim;

    public BKDMergeQueue(int bytesPerDim, int maxSize) {
      super(maxSize);
      this.bytesPerDim = bytesPerDim;
    }

    @Override
    public boolean lessThan(MergeReader a, MergeReader b) {
      assert a != b;

      int cmp = FutureArrays.compareUnsigned(a.state.scratchDataPackedValue, 0, bytesPerDim, b.state.scratchDataPackedValue, 0, bytesPerDim);
      if (cmp < 0) {
        return true;
      } else if (cmp > 0) {
        return false;
      }

      // Tie break by sorting smaller docIDs earlier:
      return a.docID < b.docID;
    }
  }

  /** Write a field from a {@link MutablePointValues}. This way of writing
   *  points is faster than regular writes with {@link BKDWriter#add} since
   *  there is opportunity for reordering points before writing them to
   *  disk. This method does not use transient disk in order to reorder points.
   */
  public long writeField(IndexOutput out, String fieldName, MutablePointValues reader) throws IOException {
    if (numDataDims == 1) {
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
    if (heapPointWriter == null && tempInput == null) {
      throw new IllegalStateException("already finished");
    }

    // Mark that we already finished:
    heapPointWriter = null;

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
        if (FutureArrays.compareUnsigned(scratchBytesRef1.bytes, scratchBytesRef1.offset + offset, scratchBytesRef1.offset + offset + bytesPerDim, minPackedValue, offset, offset + bytesPerDim) < 0) {
          System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + offset, minPackedValue, offset, bytesPerDim);
        }
        if (FutureArrays.compareUnsigned(scratchBytesRef1.bytes, scratchBytesRef1.offset + offset, scratchBytesRef1.offset + offset + bytesPerDim, maxPackedValue, offset, offset + bytesPerDim) > 0) {
          System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + offset, maxPackedValue, offset, bytesPerDim);
        }
      }

      docsSeen.set(values.getDocID(i));
    }

    final int[] parentSplits = new int[numIndexDims];
    build(1, numLeaves, values, 0, Math.toIntExact(pointCount), out,
          minPackedValue, maxPackedValue, parentSplits,
          splitPackedValues, leafBlockFPs,
          new int[maxPointsInLeafNode]);
    assert Arrays.equals(parentSplits, new int[numIndexDims]);

    long indexFP = out.getFilePointer();
    writeIndex(out, Math.toIntExact(countPerLeaf), leafBlockFPs, splitPackedValues);
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

  /** More efficient bulk-add for incoming {@link BKDReader}s.  This does a merge sort of the already
   *  sorted values and currently only works when numDims==1.  This returns -1 if all documents containing
   *  dimensional values were deleted. */
  public long merge(IndexOutput out, List<MergeState.DocMap> docMaps, List<BKDReader> readers) throws IOException {
    assert docMaps == null || readers.size() == docMaps.size();

    BKDMergeQueue queue = new BKDMergeQueue(bytesPerDim, readers.size());

    for(int i=0;i<readers.size();i++) {
      BKDReader bkd = readers.get(i);
      MergeState.DocMap docMap;
      if (docMaps == null) {
        docMap = null;
      } else {
        docMap = docMaps.get(i);
      }
      MergeReader reader = new MergeReader(bkd, docMap);
      if (reader.next()) {
        queue.add(reader);
      }
    }

    OneDimensionBKDWriter oneDimWriter = new OneDimensionBKDWriter(out);

    while (queue.size() != 0) {
      MergeReader reader = queue.top();
      // System.out.println("iter reader=" + reader);

      oneDimWriter.add(reader.state.scratchDataPackedValue, reader.docID);

      if (reader.next()) {
        queue.updateTop();
      } else {
        // This segment was exhausted
        queue.pop();
      }
    }

    return oneDimWriter.finish();
  }

  // reused when writing leaf blocks
  private final GrowableByteArrayDataOutput scratchOut = new GrowableByteArrayDataOutput(32*1024);

  private class OneDimensionBKDWriter {

    final IndexOutput out;
    final List<Long> leafBlockFPs = new ArrayList<>();
    final List<byte[]> leafBlockStartValues = new ArrayList<>();
    final byte[] leafValues = new byte[maxPointsInLeafNode * packedBytesLength];
    final int[] leafDocs = new int[maxPointsInLeafNode];
    private long valueCount;
    private int leafCount;

    OneDimensionBKDWriter(IndexOutput out) {
      if (numIndexDims != 1) {
        throw new UnsupportedOperationException("numIndexDims must be 1 but got " + numIndexDims);
      }
      if (pointCount != 0) {
        throw new IllegalStateException("cannot mix add and merge");
      }

      // Catch user silliness:
      if (heapPointWriter == null && tempInput == null) {
        throw new IllegalStateException("already finished");
      }

      // Mark that we already finished:
      heapPointWriter = null;

      this.out = out;

      lastPackedValue = new byte[packedBytesLength];
    }

    // for asserts
    final byte[] lastPackedValue;
    private int lastDocID;

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
        // when we write N > 1 dimensional points where we write between max/2 and max per leaf block
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
      writeIndex(out, maxPointsInLeafNode, arr, index);
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

      // Find per-dim common prefix:
      int offset = (leafCount - 1) * packedBytesLength;
      int prefix = FutureArrays.mismatch(leafValues, 0, bytesPerDim, leafValues, offset, offset + bytesPerDim);
      if (prefix == -1) {
          prefix = bytesPerDim;
      }

      commonPrefixLengths[0] = prefix;

      assert scratchOut.getPosition() == 0;
      writeLeafBlockDocs(scratchOut, leafDocs, 0, leafCount);
      writeCommonPrefixes(scratchOut, commonPrefixLengths, leafValues);

      scratchBytesRef1.length = packedBytesLength;
      scratchBytesRef1.bytes = leafValues;
      
      final IntFunction<BytesRef> packedValues = new IntFunction<BytesRef>() {
        @Override
        public BytesRef apply(int i) {
          scratchBytesRef1.offset = packedBytesLength * i;
          return scratchBytesRef1;
        }
      };
      assert valuesInOrderAndBounds(leafCount, 0, ArrayUtil.copyOfSubArray(leafValues, 0, packedBytesLength),
          ArrayUtil.copyOfSubArray(leafValues, (leafCount - 1) * packedBytesLength, leafCount * packedBytesLength),
          packedValues, leafDocs, 0);
      writeLeafBlockPackedValues(scratchOut, commonPrefixLengths, leafCount, 0, packedValues);
      out.writeBytes(scratchOut.getBytes(), 0, scratchOut.getPosition());
      scratchOut.reset();
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

  // TODO: if we fixed each partition step to just record the file offset at the "split point", we could probably handle variable length
  // encoding and not have our own ByteSequencesReader/Writer

  /** Sort the heap writer by the specified dim */
  private void sortHeapPointWriter(final HeapPointWriter writer, int pointCount, int dim) {
    // Tie-break by docID:

    // No need to tie break on ord, for the case where the same doc has the same value in a given dimension indexed more than once: it
    // can't matter at search time since we don't write ords into the index:
    new MSBRadixSorter(bytesPerDim + Integer.BYTES) {

      @Override
      protected int byteAt(int i, int k) {
        assert k >= 0;
        if (k < bytesPerDim) {
          // dim bytes
          int block = i / writer.valuesPerBlock;
          int index = i % writer.valuesPerBlock;
          return writer.blocks.get(block)[index * packedBytesLength + dim * bytesPerDim + k] & 0xff;
        } else {
          // doc id
          int s = 3 - (k - bytesPerDim);
          return (writer.docIDs[i] >>> (s * 8)) & 0xff;
        }
      }

      @Override
      protected void swap(int i, int j) {
        int docID = writer.docIDs[i];
        writer.docIDs[i] = writer.docIDs[j];
        writer.docIDs[j] = docID;

        if (singleValuePerDoc == false) {
          if (longOrds) {
            long ord = writer.ordsLong[i];
            writer.ordsLong[i] = writer.ordsLong[j];
            writer.ordsLong[j] = ord;
          } else {
            int ord = writer.ords[i];
            writer.ords[i] = writer.ords[j];
            writer.ords[j] = ord;
          }
        }

        byte[] blockI = writer.blocks.get(i / writer.valuesPerBlock);
        int indexI = (i % writer.valuesPerBlock) * packedBytesLength;
        byte[] blockJ = writer.blocks.get(j / writer.valuesPerBlock);
        int indexJ = (j % writer.valuesPerBlock) * packedBytesLength;

        // scratch1 = values[i]
        System.arraycopy(blockI, indexI, scratch1, 0, packedBytesLength);
        // values[i] = values[j]
        System.arraycopy(blockJ, indexJ, blockI, indexI, packedBytesLength);
        // values[j] = scratch1
        System.arraycopy(scratch1, 0, blockJ, indexJ, packedBytesLength);
      }

    }.sort(0, pointCount);
  }

  // useful for debugging:
  /*
  private void printPathSlice(String desc, PathSlice slice, int dim) throws IOException {
    System.out.println("    " + desc + " dim=" + dim + " count=" + slice.count + ":");    
    try(PointReader r = slice.writer.getReader(slice.start, slice.count)) {
      int count = 0;
      while (r.next()) {
        byte[] v = r.packedValue();
        System.out.println("      " + count + ": " + new BytesRef(v, dim*bytesPerDim, bytesPerDim));
        count++;
        if (count == slice.count) {
          break;
        }
      }
    }
  }
  */

  //return a new point writer sort by the provided dimension from input data
  private PointWriter sort(int dim) throws IOException {
    assert dim >= 0 && dim < numDataDims;

    if (heapPointWriter != null) {
      assert tempInput == null;
      // We never spilled the incoming points to disk, so now we sort in heap:
      HeapPointWriter sorted = heapPointWriter;
      //long t0 = System.nanoTime();
      sortHeapPointWriter(sorted, Math.toIntExact(this.pointCount), dim);
      //long t1 = System.nanoTime();
      //System.out.println("BKD: sort took " + ((t1-t0)/1000000.0) + " msec");
      sorted.close();
      heapPointWriter = null;
      return sorted;
    } else {
      // Offline sort:
      assert tempInput != null;
      OfflinePointWriter sorted = sortOffLine(dim, tempInput.getName(), 0, pointCount);
      tempDir.deleteFile(tempInput.getName());
      tempInput = null;
      return sorted;
    }
  }

  //return a new point writer sort by the provided dimension from start to start + pointCount
  private PointWriter sort(int dim, PointWriter writer, final long start, final long pointCount) throws IOException {
    assert dim >= 0 && dim < numDataDims;

    if (writer instanceof HeapPointWriter) {
      HeapPointWriter heapPointWriter = createHeapPointWriterCopy((HeapPointWriter) writer, start, pointCount);
      sortHeapPointWriter(heapPointWriter, Math.toIntExact(pointCount), dim);
      return heapPointWriter;
    } else {
      OfflinePointWriter offlinePointWriter = (OfflinePointWriter) writer;
      return sortOffLine(dim, offlinePointWriter.name, start, pointCount);
    }
  }

  // sort a given file on a given dimension for start to start + point count
  private OfflinePointWriter sortOffLine(int dim, String inputName, final long start, final long pointCount) throws IOException {

    final int offset = bytesPerDim * dim;

    Comparator<BytesRef> cmp;
    if (dim == numDataDims - 1) {
      // in that case the bytes for the dimension and for the doc id are contiguous,
      // so we don't need a branch
      cmp = new BytesRefComparator(bytesPerDim + Integer.BYTES) {
        @Override
        protected int byteAt(BytesRef ref, int i) {
          return ref.bytes[ref.offset + offset + i] & 0xff;
        }
      };
    } else {
      cmp = new BytesRefComparator(bytesPerDim + Integer.BYTES) {
        @Override
        protected int byteAt(BytesRef ref, int i) {
          if (i < bytesPerDim) {
            return ref.bytes[ref.offset + offset + i] & 0xff;
          } else {
            return ref.bytes[ref.offset + packedBytesLength + i - bytesPerDim] & 0xff;
          }
        }
      };
    }

    OfflineSorter sorter = new OfflineSorter(tempDir, tempFileNamePrefix + "_bkd" + dim, cmp, offlineSorterBufferMB, offlineSorterMaxTempFiles, bytesPerDoc, null, 0) {
      /**
       * We write/read fixed-byte-width file that {@link OfflinePointReader} can read.
       */
      @Override
      protected ByteSequencesWriter getWriter(IndexOutput out, long count) {
        return new ByteSequencesWriter(out) {
          @Override
          public void write(byte[] bytes, int off, int len) throws IOException {
            assert len == bytesPerDoc : "len=" + len + " bytesPerDoc=" + bytesPerDoc;
            out.writeBytes(bytes, off, len);
          }
        };
      }

      /**
       * We write/read fixed-byte-width file that {@link OfflinePointReader} can read.
       */
      @Override
      protected ByteSequencesReader getReader(ChecksumIndexInput in, String name) throws IOException {
        //This allows to read only a subset of the original file
        long startPointer = (name.equals(inputName)) ? bytesPerDoc * start : in.getFilePointer();
        long endPointer = (name.equals(inputName)) ? startPointer + bytesPerDoc * pointCount : Long.MAX_VALUE;
        in.seek(startPointer);
        return new ByteSequencesReader(in, name) {
          final BytesRef scratch = new BytesRef(new byte[bytesPerDoc]);

          @Override
          public BytesRef next() throws IOException {
            if (in.getFilePointer() >= end) {
              return null;
            } else if (in.getFilePointer() >= endPointer) {
              in.seek(end);
              return null;
            }
            in.readBytes(scratch.bytes, 0, bytesPerDoc);
            return scratch;
          }
        };
      }
    };

    String name = sorter.sort(inputName);
    return new OfflinePointWriter(tempDir, name, packedBytesLength, pointCount, longOrds, singleValuePerDoc);
  }

  private HeapPointWriter createHeapPointWriterCopy(HeapPointWriter writer, long start, long count) throws IOException {
    //TODO: Can we do this faster?
    int size = Math.toIntExact(count);
    try (HeapPointWriter pointWriter = new HeapPointWriter(size, size, packedBytesLength, longOrds, singleValuePerDoc);
         PointReader reader = writer.getReader(start, count)) {
     for (long i =0; i < count; i++) {
       reader.next();
       pointWriter.append(reader.packedValue(), reader.ord(), reader.docID());
     }
     return pointWriter;
    } catch (Throwable t) {
      throw verifyChecksum(t, writer);
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
    if (heapPointWriter == null && tempInput == null) {
      throw new IllegalStateException("already finished");
    }

    if (offlinePointWriter != null) {
      offlinePointWriter.close();
    }

    if (pointCount == 0) {
      throw new IllegalStateException("must index at least one point");
    }

    LongBitSet ordBitSet;
    if (numIndexDims > 1) {
      if (singleValuePerDoc) {
        ordBitSet = new LongBitSet(maxDoc);
      } else {
        ordBitSet = new LongBitSet(pointCount);
      }
    } else {
      ordBitSet = null;
    }

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

    // Slices are created as they are needed
    PathSlice[] sortedPointWriters = new PathSlice[numIndexDims];

    // This is only used on exception; on normal code paths we close all files we opened:
    List<Closeable> toCloseHeroically = new ArrayList<>();

    boolean success = false;
    try {

      final int[] parentSplits = new int[numIndexDims];
      build(1, numLeaves, sortedPointWriters,
            ordBitSet, out,
            minPackedValue, maxPackedValue,
            parentSplits,
            splitPackedValues,
            leafBlockFPs,
            toCloseHeroically);
      assert Arrays.equals(parentSplits, new int[numIndexDims]);

      // If no exception, we should have cleaned everything up:
      assert tempDir.getCreatedFiles().isEmpty();
      //long t2 = System.nanoTime();
      //System.out.println("write time: " + ((t2-t1)/1000000.0) + " msec");

      success = true;
    } finally {
      if (success == false) {
        IOUtils.deleteFilesIgnoringExceptions(tempDir, tempDir.getCreatedFiles());
        IOUtils.closeWhileHandlingException(toCloseHeroically);
      }
    }

    //System.out.println("Total nodes: " + innerNodeCount);

    // Write index:
    long indexFP = out.getFilePointer();
    writeIndex(out, Math.toIntExact(countPerLeaf), leafBlockFPs, splitPackedValues);
    return indexFP;
  }

  /** Packs the two arrays, representing a balanced binary tree, into a compact byte[] structure. */
  private byte[] packIndex(long[] leafBlockFPs, byte[] splitPackedValues) throws IOException {

    int numLeaves = leafBlockFPs.length;

    // Possibly rotate the leaf block FPs, if the index not fully balanced binary tree (only happens
    // if it was created by OneDimensionBKDWriter).  In this case the leaf nodes may straddle the two bottom
    // levels of the binary tree:
    if (numIndexDims == 1 && numLeaves > 1) {
      int levelCount = 2;
      while (true) {
        if (numLeaves >= levelCount && numLeaves <= 2*levelCount) {
          int lastLevel = 2*(numLeaves - levelCount);
          assert lastLevel >= 0;
          if (lastLevel != 0) {
            // Last level is partially filled, so we must rotate the leaf FPs to match.  We do this here, after loading
            // at read-time, so that we can still delta code them on disk at write:
            long[] newLeafBlockFPs = new long[numLeaves];
            System.arraycopy(leafBlockFPs, lastLevel, newLeafBlockFPs, 0, leafBlockFPs.length - lastLevel);
            System.arraycopy(leafBlockFPs, 0, newLeafBlockFPs, leafBlockFPs.length - lastLevel, lastLevel);
            leafBlockFPs = newLeafBlockFPs;
          }
          break;
        }

        levelCount *= 2;
      }
    }

    /** Reused while packing the index */
    RAMOutputStream writeBuffer = new RAMOutputStream();

    // This is the "file" we append the byte[] to:
    List<byte[]> blocks = new ArrayList<>();
    byte[] lastSplitValues = new byte[bytesPerDim * numIndexDims];
    //System.out.println("\npack index");
    int totalSize = recursePackIndex(writeBuffer, leafBlockFPs, splitPackedValues, 0l, blocks, 1, lastSplitValues, new boolean[numIndexDims], false);

    // Compact the byte[] blocks into single byte index:
    byte[] index = new byte[totalSize];
    int upto = 0;
    for(byte[] block : blocks) {
      System.arraycopy(block, 0, index, upto, block.length);
      upto += block.length;
    }
    assert upto == totalSize;

    return index;
  }

  /** Appends the current contents of writeBuffer as another block on the growing in-memory file */
  private int appendBlock(RAMOutputStream writeBuffer, List<byte[]> blocks) throws IOException {
    int pos = Math.toIntExact(writeBuffer.getFilePointer());
    byte[] bytes = new byte[pos];
    writeBuffer.writeTo(bytes, 0);
    writeBuffer.reset();
    blocks.add(bytes);
    return pos;
  }

  /**
   * lastSplitValues is per-dimension split value previously seen; we use this to prefix-code the split byte[] on each inner node
   */
  private int recursePackIndex(RAMOutputStream writeBuffer, long[] leafBlockFPs, byte[] splitPackedValues, long minBlockFP, List<byte[]> blocks,
                               int nodeID, byte[] lastSplitValues, boolean[] negativeDeltas, boolean isLeft) throws IOException {
    if (nodeID >= leafBlockFPs.length) {
      int leafID = nodeID - leafBlockFPs.length;
      //System.out.println("recursePack leaf nodeID=" + nodeID);

      // In the unbalanced case it's possible the left most node only has one child:
      if (leafID < leafBlockFPs.length) {
        long delta = leafBlockFPs[leafID] - minBlockFP;
        if (isLeft) {
          assert delta == 0;
          return 0;
        } else {
          assert nodeID == 1 || delta > 0: "nodeID=" + nodeID;
          writeBuffer.writeVLong(delta);
          return appendBlock(writeBuffer, blocks);
        }
      } else {
        return 0;
      }
    } else {
      long leftBlockFP;
      if (isLeft == false) {
        leftBlockFP = getLeftMostLeafBlockFP(leafBlockFPs, nodeID);
        long delta = leftBlockFP - minBlockFP;
        assert nodeID == 1 || delta > 0 : "expected nodeID=1 or delta > 0; got nodeID=" + nodeID + " and delta=" + delta;
        writeBuffer.writeVLong(delta);
      } else {
        // The left tree's left most leaf block FP is always the minimal FP:
        leftBlockFP = minBlockFP;
      }

      int address = nodeID * (1+bytesPerDim);
      int splitDim = splitPackedValues[address++] & 0xff;

      //System.out.println("recursePack inner nodeID=" + nodeID + " splitDim=" + splitDim + " splitValue=" + new BytesRef(splitPackedValues, address, bytesPerDim));

      // find common prefix with last split value in this dim:
      int prefix = FutureArrays.mismatch(splitPackedValues, address, address + bytesPerDim, lastSplitValues,
          splitDim * bytesPerDim, splitDim * bytesPerDim + bytesPerDim);
      if (prefix == -1) {
        prefix = bytesPerDim;
      }

      //System.out.println("writeNodeData nodeID=" + nodeID + " splitDim=" + splitDim + " numDims=" + numDims + " bytesPerDim=" + bytesPerDim + " prefix=" + prefix);

      int firstDiffByteDelta;
      if (prefix < bytesPerDim) {
        //System.out.println("  delta byte cur=" + Integer.toHexString(splitPackedValues[address+prefix]&0xFF) + " prev=" + Integer.toHexString(lastSplitValues[splitDim * bytesPerDim + prefix]&0xFF) + " negated?=" + negativeDeltas[splitDim]);
        firstDiffByteDelta = (splitPackedValues[address+prefix]&0xFF) - (lastSplitValues[splitDim * bytesPerDim + prefix]&0xFF);
        if (negativeDeltas[splitDim]) {
          firstDiffByteDelta = -firstDiffByteDelta;
        }
        //System.out.println("  delta=" + firstDiffByteDelta);
        assert firstDiffByteDelta > 0;
      } else {
        firstDiffByteDelta = 0;
      }

      // pack the prefix, splitDim and delta first diff byte into a single vInt:
      int code = (firstDiffByteDelta * (1+bytesPerDim) + prefix) * numIndexDims + splitDim;

      //System.out.println("  code=" + code);
      //System.out.println("  splitValue=" + new BytesRef(splitPackedValues, address, bytesPerDim));

      writeBuffer.writeVInt(code);

      // write the split value, prefix coded vs. our parent's split value:
      int suffix = bytesPerDim - prefix;
      byte[] savSplitValue = new byte[suffix];
      if (suffix > 1) {
        writeBuffer.writeBytes(splitPackedValues, address+prefix+1, suffix-1);
      }

      byte[] cmp = lastSplitValues.clone();

      System.arraycopy(lastSplitValues, splitDim * bytesPerDim + prefix, savSplitValue, 0, suffix);

      // copy our split value into lastSplitValues for our children to prefix-code against
      System.arraycopy(splitPackedValues, address+prefix, lastSplitValues, splitDim * bytesPerDim + prefix, suffix);

      int numBytes = appendBlock(writeBuffer, blocks);

      // placeholder for left-tree numBytes; we need this so that at search time if we only need to recurse into the right sub-tree we can
      // quickly seek to its starting point
      int idxSav = blocks.size();
      blocks.add(null);

      boolean savNegativeDelta = negativeDeltas[splitDim];
      negativeDeltas[splitDim] = true;

      int leftNumBytes = recursePackIndex(writeBuffer, leafBlockFPs, splitPackedValues, leftBlockFP, blocks, 2*nodeID, lastSplitValues, negativeDeltas, true);

      if (nodeID * 2 < leafBlockFPs.length) {
        writeBuffer.writeVInt(leftNumBytes);
      } else {
        assert leftNumBytes == 0: "leftNumBytes=" + leftNumBytes;
      }
      int numBytes2 = Math.toIntExact(writeBuffer.getFilePointer());
      byte[] bytes2 = new byte[numBytes2];
      writeBuffer.writeTo(bytes2, 0);
      writeBuffer.reset();
      // replace our placeholder:
      blocks.set(idxSav, bytes2);

      negativeDeltas[splitDim] = false;
      int rightNumBytes = recursePackIndex(writeBuffer, leafBlockFPs, splitPackedValues, leftBlockFP, blocks, 2*nodeID+1, lastSplitValues, negativeDeltas, false);

      negativeDeltas[splitDim] = savNegativeDelta;

      // restore lastSplitValues to what caller originally passed us:
      System.arraycopy(savSplitValue, 0, lastSplitValues, splitDim * bytesPerDim + prefix, suffix);

      assert Arrays.equals(lastSplitValues, cmp);
      
      return numBytes + numBytes2 + leftNumBytes + rightNumBytes;
    }
  }

  private long getLeftMostLeafBlockFP(long[] leafBlockFPs, int nodeID) {
    int nodeIDIn = nodeID;
    // TODO: can we do this cheaper, e.g. a closed form solution instead of while loop?  Or
    // change the recursion while packing the index to return this left-most leaf block FP
    // from each recursion instead?
    //
    // Still, the overall cost here is minor: this method's cost is O(log(N)), and while writing
    // we call it O(N) times (N = number of leaf blocks)
    while (nodeID < leafBlockFPs.length) {
      nodeID *= 2;
    }
    int leafID = nodeID - leafBlockFPs.length;
    long result = leafBlockFPs[leafID];
    if (result < 0) {
      throw new AssertionError(result + " for leaf " + leafID);
    }
    return result;
  }

  private void writeIndex(IndexOutput out, int countPerLeaf, long[] leafBlockFPs, byte[] splitPackedValues) throws IOException {
    byte[] packedIndex = packIndex(leafBlockFPs, splitPackedValues);
    writeIndex(out, countPerLeaf, leafBlockFPs.length, packedIndex);
  }
  
  private void writeIndex(IndexOutput out, int countPerLeaf, int numLeaves, byte[] packedIndex) throws IOException {
    
    CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT);
    out.writeVInt(numDataDims);
    out.writeVInt(numIndexDims);
    out.writeVInt(countPerLeaf);
    out.writeVInt(bytesPerDim);

    assert numLeaves > 0;
    out.writeVInt(numLeaves);
    out.writeBytes(minPackedValue, 0, packedIndexBytesLength);
    out.writeBytes(maxPackedValue, 0, packedIndexBytesLength);

    out.writeVLong(pointCount);
    out.writeVInt(docsSeen.cardinality());
    out.writeVInt(packedIndex.length);
    out.writeBytes(packedIndex, 0, packedIndex.length);
  }

  private void writeLeafBlockDocs(DataOutput out, int[] docIDs, int start, int count) throws IOException {
    assert count > 0: "maxPointsInLeafNode=" + maxPointsInLeafNode;
    out.writeVInt(count);
    DocIdsWriter.writeDocIds(docIDs, start, count, out);
  }

  private void writeLeafBlockPackedValues(DataOutput out, int[] commonPrefixLengths, int count, int sortedDim, IntFunction<BytesRef> packedValues) throws IOException {
    int prefixLenSum = Arrays.stream(commonPrefixLengths).sum();
    if (prefixLenSum == packedBytesLength) {
      // all values in this block are equal
      out.writeByte((byte) -1);
    } else {
      if (numIndexDims != 1) {
        writeActualBounds(out, commonPrefixLengths, count, packedValues);
      }
      assert commonPrefixLengths[sortedDim] < bytesPerDim;
      out.writeByte((byte) sortedDim);
      int compressedByteOffset = sortedDim * bytesPerDim + commonPrefixLengths[sortedDim];
      commonPrefixLengths[sortedDim]++;
      for (int i = 0; i < count; ) {
        // do run-length compression on the byte at compressedByteOffset
        int runLen = runLen(packedValues, i, Math.min(i + 0xff, count), compressedByteOffset);
        assert runLen <= 0xff;
        BytesRef first = packedValues.apply(i);
        byte prefixByte = first.bytes[first.offset + compressedByteOffset];
        out.writeByte(prefixByte);
        out.writeByte((byte) runLen);
        writeLeafBlockPackedValuesRange(out, commonPrefixLengths, i, i + runLen, packedValues);
        i += runLen;
        assert i <= count;
      }
    }
  }

  private void writeActualBounds(DataOutput out, int[] commonPrefixLengths, int count, IntFunction<BytesRef> packedValues) throws IOException {
    for (int dim = 0; dim < numIndexDims; ++dim) {
      int commonPrefixLength = commonPrefixLengths[dim];
      int suffixLength = bytesPerDim - commonPrefixLength;
      if (suffixLength > 0) {
        BytesRef[] minMax = computeMinMax(count, packedValues, dim * bytesPerDim + commonPrefixLength, suffixLength);
        BytesRef min = minMax[0];
        BytesRef max = minMax[1];
        out.writeBytes(min.bytes, min.offset, min.length);
        out.writeBytes(max.bytes, max.offset, max.length);
      }
    }
  }

  /** Return an array that contains the min and max values for the [offset, offset+length] interval
   *  of the given {@link BytesRef}s. */
  private static BytesRef[] computeMinMax(int count, IntFunction<BytesRef> packedValues, int offset, int length) {
    assert length > 0;
    BytesRefBuilder min = new BytesRefBuilder();
    BytesRefBuilder max = new BytesRefBuilder();
    BytesRef first = packedValues.apply(0);
    min.copyBytes(first.bytes, first.offset + offset, length);
    max.copyBytes(first.bytes, first.offset + offset, length);
    for (int i = 1; i < count; ++i) {
      BytesRef candidate = packedValues.apply(i);
      if (FutureArrays.compareUnsigned(min.bytes(), 0, length, candidate.bytes, candidate.offset + offset, candidate.offset + offset + length) > 0) {
        min.copyBytes(candidate.bytes, candidate.offset + offset, length);
      } else if (FutureArrays.compareUnsigned(max.bytes(), 0, length, candidate.bytes, candidate.offset + offset, candidate.offset + offset + length) < 0) {
        max.copyBytes(candidate.bytes, candidate.offset + offset, length);
      }
    }
    return new BytesRef[]{min.get(), max.get()};
  }

  private void writeLeafBlockPackedValuesRange(DataOutput out, int[] commonPrefixLengths, int start, int end, IntFunction<BytesRef> packedValues) throws IOException {
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

  private void writeCommonPrefixes(DataOutput out, int[] commonPrefixes, byte[] packedValue) throws IOException {
    for(int dim=0;dim<numDataDims;dim++) {
      out.writeVInt(commonPrefixes[dim]);
      //System.out.println(commonPrefixes[dim] + " of " + bytesPerDim);
      out.writeBytes(packedValue, dim*bytesPerDim, commonPrefixes[dim]);
    }
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

  /** Sliced reference to points in an OfflineSorter.ByteSequencesWriter file. */
  private static final class PathSlice {
    final PointWriter writer;
    final long start;
    final long count;

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

  /** Marks bits for the ords (points) that belong in the right sub tree (those docs that have values >= the splitValue). */
  private byte[] markRightTree(long rightCount, int splitDim, PathSlice source, LongBitSet ordBitSet) throws IOException {

    // Now we mark ords that fall into the right half, so we can partition on all other dims that are not the split dim:

    // Read the split value, then mark all ords in the right tree (larger than the split value):

    // TODO: find a way to also checksum this reader?  If we changed to markLeftTree, and scanned the final chunk, it could work?
    try (PointReader reader = source.writer.getReader(source.start + source.count - rightCount, rightCount)) {
      boolean result = reader.next();
      assert result: "rightCount=" + rightCount + " source.count=" + source.count + " source.writer=" + source.writer;
      System.arraycopy(reader.packedValue(), splitDim*bytesPerDim, scratch1, 0, bytesPerDim);
      if (numIndexDims > 1 && ordBitSet != null) {
        assert ordBitSet.get(reader.ord()) == false;
        ordBitSet.set(reader.ord());
        // Subtract 1 from rightCount because we already did the first value above (so we could record the split value):
        reader.markOrds(rightCount-1, ordBitSet);
      }
    } catch (Throwable t) {
      throw verifyChecksum(t, source.writer);
    }

    return scratch1;
  }

  /** Called only in assert */
  private boolean valueInBounds(BytesRef packedValue, byte[] minPackedValue, byte[] maxPackedValue) {
    for(int dim=0;dim<numIndexDims;dim++) {
      int offset = bytesPerDim*dim;
      if (FutureArrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + bytesPerDim, minPackedValue, offset, offset + bytesPerDim) < 0) {
        return false;
      }
      if (FutureArrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + bytesPerDim, maxPackedValue, offset, offset + bytesPerDim) > 0) {
        return false;
      }
    }

    return true;
  }

  /**
   * Pick the next dimension to split.
   * @param minPackedValue the min values for all dimensions
   * @param maxPackedValue the max values for all dimensions
   * @param parentSplits how many times each dim has been split on the parent levels
   * @return the dimension to split
   */
  protected int split(byte[] minPackedValue, byte[] maxPackedValue, int[] parentSplits) {
    // First look at whether there is a dimension that has split less than 2x less than
    // the dim that has most splits, and return it if there is such a dimension and it
    // does not only have equals values. This helps ensure all dimensions are indexed.
    int maxNumSplits = 0;
    for (int numSplits : parentSplits) {
      maxNumSplits = Math.max(maxNumSplits, numSplits);
    }
    for (int dim = 0; dim < numIndexDims; ++dim) {
      final int offset = dim * bytesPerDim;
      if (parentSplits[dim] < maxNumSplits / 2 &&
          FutureArrays.compareUnsigned(minPackedValue, offset, offset + bytesPerDim, maxPackedValue, offset, offset + bytesPerDim) != 0) {
        return dim;
      }
    }

    // Find which dim has the largest span so we can split on it:
    int splitDim = -1;
    for(int dim=0;dim<numIndexDims;dim++) {
      NumericUtils.subtract(bytesPerDim, dim, maxPackedValue, minPackedValue, scratchDiff);
      if (splitDim == -1 || FutureArrays.compareUnsigned(scratchDiff, 0, bytesPerDim, scratch1, 0, bytesPerDim) > 0) {
        System.arraycopy(scratchDiff, 0, scratch1, 0, bytesPerDim);
        splitDim = dim;
      }
    }

    //System.out.println("SPLIT: " + splitDim);
    return splitDim;
  }

  /** Pull a partition back into heap once the point count is low enough while recursing. */
  private PathSlice switchToHeap(PathSlice source, List<Closeable> toCloseHeroically) throws IOException {
    int count = Math.toIntExact(source.count);
    // Not inside the try because we don't want to close it here:
    PointReader reader = source.writer.getSharedReader(source.start, source.count, toCloseHeroically);
    try (PointWriter writer = new HeapPointWriter(count, count, packedBytesLength, longOrds, singleValuePerDoc)) {
      for(int i=0;i<count;i++) {
        boolean hasNext = reader.next();
        assert hasNext;
        writer.append(reader.packedValue(), reader.ord(), reader.docID());
      }
      return new PathSlice(writer, 0, count);
    } catch (Throwable t) {
      throw verifyChecksum(t, source.writer);
    }
  }

  /* Recursively reorders the provided reader and writes the bkd-tree on the fly; this method is used
   * when we are writing a new segment directly from IndexWriter's indexing buffer (MutablePointsReader). */
  private void build(int nodeID, int leafNodeOffset,
                     MutablePointValues reader, int from, int to,
                     IndexOutput out,
                     byte[] minPackedValue, byte[] maxPackedValue,
                     int[] parentSplits,
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
          int dimensionPrefixLength = commonPrefixLengths[dim];
          commonPrefixLengths[dim] = FutureArrays.mismatch(scratchBytesRef1.bytes, scratchBytesRef1.offset + offset,
              scratchBytesRef1.offset + offset + dimensionPrefixLength,
              scratchBytesRef2.bytes, scratchBytesRef2.offset + offset,
              scratchBytesRef2.offset + offset + dimensionPrefixLength);
          if (commonPrefixLengths[dim] == -1) {
            commonPrefixLengths[dim] = dimensionPrefixLength;
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
      MutablePointsReaderUtils.sortByDim(sortedDim, bytesPerDim, commonPrefixLengths,
          reader, from, to, scratchBytesRef1, scratchBytesRef2);

      // Save the block file pointer:
      leafBlockFPs[nodeID - leafNodeOffset] = out.getFilePointer();

      assert scratchOut.getPosition() == 0;

      // Write doc IDs
      int[] docIDs = spareDocIds;
      for (int i = from; i < to; ++i) {
        docIDs[i - from] = reader.getDocID(i);
      }
      //System.out.println("writeLeafBlock pos=" + out.getFilePointer());
      writeLeafBlockDocs(scratchOut, docIDs, 0, count);

      // Write the common prefixes:
      reader.getValue(from, scratchBytesRef1);
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset, scratch1, 0, packedBytesLength);
      writeCommonPrefixes(scratchOut, commonPrefixLengths, scratch1);

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
      writeLeafBlockPackedValues(scratchOut, commonPrefixLengths, count, sortedDim, packedValues);

      out.writeBytes(scratchOut.getBytes(), 0, scratchOut.getPosition());
      scratchOut.reset();

    } else {
      // inner node

      // compute the split dimension and partition around it
      final int splitDim = split(minPackedValue, maxPackedValue, parentSplits);
      final int mid = (from + to + 1) >>> 1;

      int commonPrefixLen = FutureArrays.mismatch(minPackedValue, splitDim * bytesPerDim,
          splitDim * bytesPerDim + bytesPerDim, maxPackedValue, splitDim * bytesPerDim,
          splitDim * bytesPerDim + bytesPerDim);
      if (commonPrefixLen == -1) {
        commonPrefixLen = bytesPerDim;
      }

      MutablePointsReaderUtils.partition(maxDoc, splitDim, bytesPerDim, commonPrefixLen,
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
      parentSplits[splitDim]++;
      build(nodeID * 2, leafNodeOffset, reader, from, mid, out,
          minPackedValue, maxSplitPackedValue, parentSplits,
          splitPackedValues, leafBlockFPs, spareDocIds);
      build(nodeID * 2 + 1, leafNodeOffset, reader, mid, to, out,
          minSplitPackedValue, maxPackedValue, parentSplits,
          splitPackedValues, leafBlockFPs, spareDocIds);
      parentSplits[splitDim]--;
    }
  }

  /** The array (sized numDims) of PathSlice describe the cell we have currently recursed to.
  /*  This method is used when we are merging previously written segments, in the numDims > 1 case. */
  private void build(int nodeID, int leafNodeOffset,
                     PathSlice[] slices,
                     LongBitSet ordBitSet,
                     IndexOutput out,
                     byte[] minPackedValue, byte[] maxPackedValue,
                     int[] parentSplits,
                     byte[] splitPackedValues,
                     long[] leafBlockFPs,
                     List<Closeable> toCloseHeroically) throws IOException {

    if (nodeID >= leafNodeOffset) {

      // Leaf node: write block
      // We can write the block in any order so by default we write it sorted by the dimension that has the
      // least number of unique bytes at commonPrefixLengths[dim], which makes compression more efficient
      int sortedDim = 0;
      int sortedDimCardinality = Integer.MAX_VALUE;

      for (int dim=0;dim<numIndexDims;dim++) {
        //create a slice if it does not exist
        boolean created = false;
        if (slices[dim] == null) {
          createPathSlice(slices, dim);
          created = true;
        }
        if (slices[dim].writer instanceof HeapPointWriter == false) {
          // Adversarial cases can cause this, e.g. very lopsided data, all equal points, such that we started
          // offline, but then kept splitting only in one dimension, and so never had to rewrite into heap writer
          PathSlice slice = slices[dim];
          slices[dim] = switchToHeap(slices[dim], toCloseHeroically);
          if (created) {
            slice.writer.destroy();
          }
        }

        PathSlice source = slices[dim];

        HeapPointWriter heapSource = (HeapPointWriter) source.writer;

        // Find common prefix by comparing first and last values, already sorted in this dimension:
        heapSource.readPackedValue(Math.toIntExact(source.start), scratch1);
        heapSource.readPackedValue(Math.toIntExact(source.start + source.count - 1), scratch2);

        int offset = dim * bytesPerDim;
        commonPrefixLengths[dim] = FutureArrays.mismatch(scratch1, offset, offset + bytesPerDim, scratch2, offset, offset + bytesPerDim);
        if (commonPrefixLengths[dim] == -1) {
          commonPrefixLengths[dim] = bytesPerDim;
        }

        int prefix = commonPrefixLengths[dim];
        if (prefix < bytesPerDim) {
          int cardinality = 1;
          byte previous = scratch1[offset + prefix];
          for (long i = 1; i < source.count; ++i) {
            heapSource.readPackedValue(Math.toIntExact(source.start + i), scratch2);
            byte b = scratch2[offset + prefix];
            assert Byte.toUnsignedInt(previous) <= Byte.toUnsignedInt(b);
            if (b != previous) {
              cardinality++;
              previous = b;
            }
          }
          assert cardinality <= 256;
          if (cardinality < sortedDimCardinality) {
            sortedDim = dim;
            sortedDimCardinality = cardinality;
          }
        }
      }

      PathSlice dataDimPathSlice = null;

      if (numDataDims != numIndexDims) {
        HeapPointWriter heapSource = (HeapPointWriter) slices[0].writer;
        int from = (int) slices[0].start;
        int to = from + (int) slices[0].count;
        Arrays.fill(commonPrefixLengths, numIndexDims, numDataDims, bytesPerDim);
        heapSource.readPackedValue(from, scratch1);
        for (int i = from + 1; i < to; ++i) {
          heapSource.readPackedValue(i, scratch2);
          for (int dim = numIndexDims; dim < numDataDims; dim++) {
            final int offset = dim * bytesPerDim;
            int dimensionPrefixLength = commonPrefixLengths[dim];
            commonPrefixLengths[dim] = FutureArrays.mismatch(scratch1, offset, offset + dimensionPrefixLength,
                scratch2, offset, offset + dimensionPrefixLength);
            if (commonPrefixLengths[dim] == -1) {
                commonPrefixLengths[dim] = dimensionPrefixLength;
            }
          }
        }
        //handle case when all index dimensions contain the same value but not the data dimensions
        if (commonPrefixLengths[sortedDim] == bytesPerDim) {
          for (int dim = numIndexDims; dim < numDataDims; ++dim) {
            if (commonPrefixLengths[dim] != bytesPerDim) {
              sortedDim = dim;
              //create a new slice in memory
              dataDimPathSlice = switchToHeap(slices[0], toCloseHeroically);
              sortHeapPointWriter((HeapPointWriter) dataDimPathSlice.writer, (int) dataDimPathSlice.count, sortedDim);
              break;
            }
          }
        }
      }

      PathSlice source = (dataDimPathSlice != null) ? dataDimPathSlice : slices[sortedDim];

      // We ensured that maxPointsSortInHeap was >= maxPointsInLeafNode, so we better be in heap at this point:
      HeapPointWriter heapSource = (HeapPointWriter) source.writer;

      // Save the block file pointer:
      leafBlockFPs[nodeID - leafNodeOffset] = out.getFilePointer();
      //System.out.println("  write leaf block @ fp=" + out.getFilePointer());

      // Write docIDs first, as their own chunk, so that at intersect time we can add all docIDs w/o
      // loading the values:
      int count = Math.toIntExact(source.count);
      assert count > 0: "nodeID=" + nodeID + " leafNodeOffset=" + leafNodeOffset;
      writeLeafBlockDocs(out, heapSource.docIDs, Math.toIntExact(source.start), count);

      // TODO: minor opto: we don't really have to write the actual common prefixes, because BKDReader on recursing can regenerate it for us
      // from the index, much like how terms dict does so from the FST:

      // Write the common prefixes:
      writeCommonPrefixes(out, commonPrefixLengths, scratch1);

      // Write the full values:
      IntFunction<BytesRef> packedValues = new IntFunction<BytesRef>() {
        final BytesRef scratch = new BytesRef();

        {
          scratch.length = packedBytesLength;
        }

        @Override
        public BytesRef apply(int i) {
          heapSource.getPackedValueSlice(Math.toIntExact(source.start + i), scratch);
          return scratch;
        }
      };
      assert valuesInOrderAndBounds(count, sortedDim, minPackedValue, maxPackedValue, packedValues,
          heapSource.docIDs, Math.toIntExact(source.start));
      writeLeafBlockPackedValues(out, commonPrefixLengths, count, sortedDim, packedValues);

    } else {
      // Inner node: partition/recurse

      int splitDim;
      if (numIndexDims > 1) {
        splitDim = split(minPackedValue, maxPackedValue, parentSplits);
      } else {
        splitDim = 0;
      }

      //We delete the created path slices at the same level
      boolean deleteSplitDim = false;
      if (slices[splitDim] == null) {
        createPathSlice(slices, splitDim);
        deleteSplitDim = true;
      }
      PathSlice source = slices[splitDim];

      assert nodeID < splitPackedValues.length: "nodeID=" + nodeID + " splitValues.length=" + splitPackedValues.length;

      // How many points will be in the left tree:
      long rightCount = source.count / 2;
      long leftCount = source.count - rightCount;

      // When we are on this dim, below, we clear the ordBitSet:
      int dimToClear = numIndexDims - 1;
      while (dimToClear >= 0) {
        if (slices[dimToClear] != null && splitDim != dimToClear) {
          break;
        }
        dimToClear--;
      }

      byte[] splitValue = (dimToClear == -1) ? markRightTree(rightCount, splitDim, source, null) : markRightTree(rightCount, splitDim, source, ordBitSet);
      int address = nodeID * (1+bytesPerDim);
      splitPackedValues[address] = (byte) splitDim;
      System.arraycopy(splitValue, 0, splitPackedValues, address + 1, bytesPerDim);

      // Partition all PathSlice that are not the split dim into sorted left and right sets, so we can recurse:

      PathSlice[] leftSlices = new PathSlice[numIndexDims];
      PathSlice[] rightSlices = new PathSlice[numIndexDims];

      byte[] minSplitPackedValue = new byte[packedIndexBytesLength];
      System.arraycopy(minPackedValue, 0, minSplitPackedValue, 0, packedIndexBytesLength);

      byte[] maxSplitPackedValue = new byte[packedIndexBytesLength];
      System.arraycopy(maxPackedValue, 0, maxSplitPackedValue, 0, packedIndexBytesLength);


      for(int dim=0;dim<numIndexDims;dim++) {
        if (slices[dim] == null) {
          continue;
        }
        if (dim == splitDim) {
          // No need to partition on this dim since it's a simple slice of the incoming already sorted slice, and we
          // will re-use its shared reader when visiting it as we recurse:
          leftSlices[dim] = new PathSlice(source.writer, source.start, leftCount);
          rightSlices[dim] = new PathSlice(source.writer, source.start + leftCount, rightCount);
          System.arraycopy(splitValue, 0, minSplitPackedValue, dim*bytesPerDim, bytesPerDim);
          System.arraycopy(splitValue, 0, maxSplitPackedValue, dim*bytesPerDim, bytesPerDim);
          continue;
        }

        // Not inside the try because we don't want to close this one now, so that after recursion is done,
        // we will have done a singel full sweep of the file:
        PointReader reader = slices[dim].writer.getSharedReader(slices[dim].start, slices[dim].count, toCloseHeroically);

        try (PointWriter leftPointWriter = getPointWriter(leftCount, "left" + dim);
             PointWriter rightPointWriter = getPointWriter(source.count - leftCount, "right" + dim)) {

          long nextRightCount = reader.split(source.count, ordBitSet, leftPointWriter, rightPointWriter, dim == dimToClear);
          if (rightCount != nextRightCount) {
            throw new IllegalStateException("wrong number of points in split: expected=" + rightCount + " but actual=" + nextRightCount);
          }

          leftSlices[dim] = new PathSlice(leftPointWriter, 0, leftCount);
          rightSlices[dim] = new PathSlice(rightPointWriter, 0, rightCount);
        } catch (Throwable t) {
          throw verifyChecksum(t, slices[dim].writer);
        }
      }

      parentSplits[splitDim]++;
      // Recurse on left tree:
      build(2*nodeID, leafNodeOffset, leftSlices,
            ordBitSet, out,
            minPackedValue, maxSplitPackedValue, parentSplits,
            splitPackedValues, leafBlockFPs, toCloseHeroically);
      for(int dim=0;dim<numIndexDims;dim++) {
        // Don't destroy the dim we split on because we just re-used what our caller above gave us for that dim:
        if (dim != splitDim && slices[dim] != null) {
          leftSlices[dim].writer.destroy();
        }
      }

      // TODO: we could "tail recurse" here?  have our parent discard its refs as we recurse right?
      // Recurse on right tree:
      build(2*nodeID+1, leafNodeOffset, rightSlices,
            ordBitSet, out,
            minSplitPackedValue, maxPackedValue, parentSplits,
            splitPackedValues, leafBlockFPs, toCloseHeroically);
      for(int dim=0;dim<numIndexDims;dim++) {
        // Don't destroy the dim we split on because we just re-used what our caller above gave us for that dim:
        if (dim != splitDim && slices[dim] != null) {
          rightSlices[dim].writer.destroy();
        }
      }
      parentSplits[splitDim]--;
      if (deleteSplitDim) {
        slices[splitDim].writer.destroy();
      }
    }
  }

  private void createPathSlice(PathSlice[] slices, int dim) throws IOException{
    assert slices[dim] == null;
    PathSlice current = null;
    for(PathSlice slice : slices) {
      if (slice != null) {
        current = slice;
        break;
      }
    }
    if (current == null) {
      slices[dim] =  new PathSlice(sort(dim), 0, pointCount);
    } else {
      slices[dim] =  new PathSlice(sort(dim, current.writer, current.start, current.count), 0, current.count);
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
      int cmp = FutureArrays.compareUnsigned(lastPackedValue, dimOffset, dimOffset + bytesPerDim, packedValue, packedValueOffset + dimOffset, packedValueOffset + dimOffset + bytesPerDim);
      if (cmp > 0) {
        throw new AssertionError("values out of order: last value=" + new BytesRef(lastPackedValue) + " current value=" + new BytesRef(packedValue, packedValueOffset, packedBytesLength) + " ord=" + ord);
      }
      if (cmp == 0 && doc < lastDoc) {
        throw new AssertionError("docs out of order: last doc=" + lastDoc + " current doc=" + doc + " ord=" + ord);
      }
    }
    System.arraycopy(packedValue, packedValueOffset, lastPackedValue, 0, packedBytesLength);
    return true;
  }

  PointWriter getPointWriter(long count, String desc) throws IOException {
    if (count <= maxPointsSortInHeap) {
      int size = Math.toIntExact(count);
      return new HeapPointWriter(size, size, packedBytesLength, longOrds, singleValuePerDoc);
    } else {
      return new OfflinePointWriter(tempDir, tempFileNamePrefix, packedBytesLength, longOrds, desc, count, singleValuePerDoc);
    }
  }

}
