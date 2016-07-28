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
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefComparator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.MSBRadixSorter;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.OfflineSorter;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.StringHelper;

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
  public static final int VERSION_START = 0;
  public static final int VERSION_COMPRESSED_DOC_IDS = 1;
  public static final int VERSION_COMPRESSED_VALUES = 2;
  public static final int VERSION_CURRENT = VERSION_COMPRESSED_VALUES;

  /** How many bytes each docs takes in the fixed-width offline format */
  private final int bytesPerDoc;

  /** Default maximum number of point in each leaf block */
  public static final int DEFAULT_MAX_POINTS_IN_LEAF_NODE = 1024;

  /** Default maximum heap to use, before spilling to (slower) disk */
  public static final float DEFAULT_MAX_MB_SORT_IN_HEAP = 16.0f;

  /** Maximum number of dimensions */
  public static final int MAX_DIMS = 8;

  /** How many dimensions we are indexing */
  protected final int numDims;

  /** How many bytes each value in each dimension takes. */
  protected final int bytesPerDim;

  /** numDims * bytesPerDim */
  protected final int packedBytesLength;

  final TrackingDirectoryWrapper tempDir;
  final String tempFileNamePrefix;
  final double maxMBSortInHeap;

  final byte[] scratchDiff;
  final byte[] scratch1;
  final byte[] scratch2;
  final BytesRef scratchBytesRef = new BytesRef();
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

  public BKDWriter(int maxDoc, Directory tempDir, String tempFileNamePrefix, int numDims, int bytesPerDim,
                   int maxPointsInLeafNode, double maxMBSortInHeap, long totalPointCount, boolean singleValuePerDoc) throws IOException {
    this(maxDoc, tempDir, tempFileNamePrefix, numDims, bytesPerDim, maxPointsInLeafNode, maxMBSortInHeap, totalPointCount, singleValuePerDoc,
         totalPointCount > Integer.MAX_VALUE, Math.max(1, (long) maxMBSortInHeap), OfflineSorter.MAX_TEMPFILES);
  }

  protected BKDWriter(int maxDoc, Directory tempDir, String tempFileNamePrefix, int numDims, int bytesPerDim,
                      int maxPointsInLeafNode, double maxMBSortInHeap, long totalPointCount,
                      boolean singleValuePerDoc, boolean longOrds, long offlineSorterBufferMB, int offlineSorterMaxTempFiles) throws IOException {
    verifyParams(numDims, maxPointsInLeafNode, maxMBSortInHeap, totalPointCount);
    // We use tracking dir to deal with removing files on exception, so each place that
    // creates temp files doesn't need crazy try/finally/sucess logic:
    this.tempDir = new TrackingDirectoryWrapper(tempDir);
    this.tempFileNamePrefix = tempFileNamePrefix;
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.numDims = numDims;
    this.bytesPerDim = bytesPerDim;
    this.totalPointCount = totalPointCount;
    this.maxDoc = maxDoc;
    this.offlineSorterBufferMB = OfflineSorter.BufferSize.megabytes(offlineSorterBufferMB);
    this.offlineSorterMaxTempFiles = offlineSorterMaxTempFiles;
    docsSeen = new FixedBitSet(maxDoc);
    packedBytesLength = numDims * bytesPerDim;

    scratchDiff = new byte[bytesPerDim];
    scratchBytesRef.length = packedBytesLength;
    scratch1 = new byte[packedBytesLength];
    scratch2 = new byte[packedBytesLength];
    commonPrefixLengths = new int[numDims];

    minPackedValue = new byte[packedBytesLength];
    maxPackedValue = new byte[packedBytesLength];

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
    // we must divide by numDims as well.

    maxPointsSortInHeap = (int) (0.5 * (maxMBSortInHeap * 1024 * 1024) / (bytesPerDoc * numDims));

    // Finally, we must be able to hold at least the leaf node in heap during build:
    if (maxPointsSortInHeap < maxPointsInLeafNode) {
      throw new IllegalArgumentException("maxMBSortInHeap=" + maxMBSortInHeap + " only allows for maxPointsSortInHeap=" + maxPointsSortInHeap + ", but this is less than maxPointsInLeafNode=" + maxPointsInLeafNode + "; either increase maxMBSortInHeap or decrease maxPointsInLeafNode");
    }

    // We write first maxPointsSortInHeap in heap, then cutover to offline for additional points:
    heapPointWriter = new HeapPointWriter(16, maxPointsSortInHeap, packedBytesLength, longOrds, singleValuePerDoc);

    this.maxMBSortInHeap = maxMBSortInHeap;
  }

  public static void verifyParams(int numDims, int maxPointsInLeafNode, double maxMBSortInHeap, long totalPointCount) {
    // We encode dim in a single byte in the splitPackedValues, but we only expose 4 bits for it now, in case we want to use
    // remaining 4 bits for another purpose later
    if (numDims < 1 || numDims > MAX_DIMS) {
      throw new IllegalArgumentException("numDims must be 1 .. " + MAX_DIMS + " (got: " + numDims + ")");
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
      System.arraycopy(packedValue, 0, minPackedValue, 0, packedBytesLength);
      System.arraycopy(packedValue, 0, maxPackedValue, 0, packedBytesLength);
    } else {
      for(int dim=0;dim<numDims;dim++) {
        int offset = dim*bytesPerDim;
        if (StringHelper.compare(bytesPerDim, packedValue, offset, minPackedValue, offset) < 0) {
          System.arraycopy(packedValue, offset, minPackedValue, offset, bytesPerDim);
        }
        if (StringHelper.compare(bytesPerDim, packedValue, offset, maxPackedValue, offset) > 0) {
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
                                           bkd.numDims,
                                           bkd.packedBytesLength,
                                           bkd.maxPointsInLeafNode,
                                           null);
      this.docMap = docMap;
      long minFP = Long.MAX_VALUE;
      //System.out.println("MR.init " + this + " bkdreader=" + bkd + " leafBlockFPs.length=" + bkd.leafBlockFPs.length);
      for(long fp : bkd.leafBlockFPs) {
        minFP = Math.min(minFP, fp);
        //System.out.println("  leaf fp=" + fp);
      }
      state.in.seek(minFP);
      this.packedValues = new byte[bkd.maxPointsInLeafNode * bkd.packedBytesLength];
    }

    public boolean next() throws IOException {
      //System.out.println("MR.next this=" + this);
      while (true) {
        if (docBlockUpto == docsInBlock) {
          if (blockID == bkd.leafBlockFPs.length) {
            //System.out.println("  done!");
            return false;
          }
          //System.out.println("  new block @ fp=" + state.in.getFilePointer());
          docsInBlock = bkd.readDocIDs(state.in, state.in.getFilePointer(), state.scratchDocIDs);
          assert docsInBlock > 0;
          docBlockUpto = 0;
          bkd.visitDocValues(state.commonPrefixLengths, state.scratchPackedValue, state.in, state.scratchDocIDs, docsInBlock, new IntersectVisitor() {
            int i = 0;

            @Override
            public void visit(int docID) throws IOException {
              throw new UnsupportedOperationException();
            }

            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
              assert docID == state.scratchDocIDs[i];
              System.arraycopy(packedValue, 0, packedValues, i * bkd.packedBytesLength, bkd.packedBytesLength);
              i++;
            }

            @Override
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
              throw new UnsupportedOperationException();
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
          System.arraycopy(packedValues, index * bkd.packedBytesLength, state.scratchPackedValue, 0, bkd.packedBytesLength);
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

      int cmp = StringHelper.compare(bytesPerDim, a.state.scratchPackedValue, 0, b.state.scratchPackedValue, 0);
      if (cmp < 0) {
        return true;
      } else if (cmp > 0) {
        return false;
      }

      // Tie break by sorting smaller docIDs earlier:
      return a.docID < b.docID;
    }
  }

  /** More efficient bulk-add for incoming {@link BKDReader}s.  This does a merge sort of the already
   *  sorted values and currently only works when numDims==1.  This returns -1 if all documents containing
   *  dimensional values were deleted. */
  public long merge(IndexOutput out, List<MergeState.DocMap> docMaps, List<BKDReader> readers) throws IOException {
    if (numDims != 1) {
      throw new UnsupportedOperationException("numDims must be 1 but got " + numDims);
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

    if (queue.size() == 0) {
      return -1;
    }

    int leafCount = 0;
    List<Long> leafBlockFPs = new ArrayList<>();
    List<byte[]> leafBlockStartValues = new ArrayList<>();

    // Target halfway between min and max allowed for the leaf:
    int pointsPerLeafBlock = (int) (0.75 * maxPointsInLeafNode);
    //System.out.println("POINTS PER: " + pointsPerLeafBlock);

    byte[] lastPackedValue = new byte[bytesPerDim];
    byte[] firstPackedValue = new byte[bytesPerDim];
    long valueCount = 0;

    // Buffer up each leaf block's docs and values
    int[] leafBlockDocIDs = new int[maxPointsInLeafNode];
    byte[][] leafBlockPackedValues = new byte[maxPointsInLeafNode][];
    for(int i=0;i<maxPointsInLeafNode;i++) {
      leafBlockPackedValues[i] = new byte[packedBytesLength];
    }
    Arrays.fill(commonPrefixLengths, bytesPerDim);

    while (queue.size() != 0) {
      MergeReader reader = queue.top();
      // System.out.println("iter reader=" + reader);

      // NOTE: doesn't work with subclasses (e.g. SimpleText!)
      int docID = reader.docID;
      leafBlockDocIDs[leafCount] = docID;
      System.arraycopy(reader.state.scratchPackedValue, 0, leafBlockPackedValues[leafCount], 0, packedBytesLength);
      docsSeen.set(docID);

      if (valueCount == 0) {
        System.arraycopy(reader.state.scratchPackedValue, 0, minPackedValue, 0, packedBytesLength);
      }
      System.arraycopy(reader.state.scratchPackedValue, 0, maxPackedValue, 0, packedBytesLength);

      assert numDims > 1 || valueInOrder(valueCount, lastPackedValue, reader.state.scratchPackedValue, 0);
      valueCount++;
      if (pointCount > totalPointCount) {
        throw new IllegalStateException("totalPointCount=" + totalPointCount + " was passed when we were created, but we just hit " + pointCount + " values");
      }

      if (leafCount == 0) {
        if (leafBlockFPs.size() > 0) {
          // Save the first (minimum) value in each leaf block except the first, to build the split value index in the end:
          leafBlockStartValues.add(Arrays.copyOf(reader.state.scratchPackedValue, bytesPerDim));
        }
        Arrays.fill(commonPrefixLengths, bytesPerDim);
        System.arraycopy(reader.state.scratchPackedValue, 0, firstPackedValue, 0, bytesPerDim);
      } else {
        // Find per-dim common prefix:
        for(int dim=0;dim<numDims;dim++) {
          int offset = dim * bytesPerDim;
          for(int j=0;j<commonPrefixLengths[dim];j++) {
            if (firstPackedValue[offset+j] != reader.state.scratchPackedValue[offset+j]) {
              commonPrefixLengths[dim] = j;
              break;
            }
          }
        }
      }

      leafCount++;

      if (reader.next()) {
        queue.updateTop();
      } else {
        // This segment was exhausted
        queue.pop();
      }

      // We write a block once we hit exactly the max count ... this is different from
      // when we flush a new segment, where we write between max/2 and max per leaf block,
      // so merged segments will behave differently from newly flushed segments:
      if (leafCount == pointsPerLeafBlock || queue.size() == 0) {
        leafBlockFPs.add(out.getFilePointer());
        checkMaxLeafNodeCount(leafBlockFPs.size());

        writeLeafBlockDocs(out, leafBlockDocIDs, 0, leafCount);
        writeCommonPrefixes(out, commonPrefixLengths, firstPackedValue);

        final IntFunction<BytesRef> packedValues = new IntFunction<BytesRef>() {
          final BytesRef scratch = new BytesRef();

          {
            scratch.length = packedBytesLength;
            scratch.offset = 0;
          }

          @Override
          public BytesRef apply(int i) {
            scratch.bytes = leafBlockPackedValues[i];
            return scratch;
          }
        };
        writeLeafBlockPackedValues(out, commonPrefixLengths, leafCount, 0, packedValues);

        leafCount = 0;
      }
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
  private void sortHeapPointWriter(final HeapPointWriter writer, int dim) {
    final int pointCount = Math.toIntExact(this.pointCount);
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

  private PointWriter sort(int dim) throws IOException {

    if (heapPointWriter != null) {

      assert tempInput == null;

      // We never spilled the incoming points to disk, so now we sort in heap:
      HeapPointWriter sorted;

      if (dim == 0) {
        // First dim can re-use the current heap writer
        sorted = heapPointWriter;
      } else {
        // Subsequent dims need a private copy
        sorted = new HeapPointWriter((int) pointCount, (int) pointCount, packedBytesLength, longOrds, singleValuePerDoc);
        sorted.copyFrom(heapPointWriter);
      }

      //long t0 = System.nanoTime();
      sortHeapPointWriter(sorted, dim);
      //long t1 = System.nanoTime();
      //System.out.println("BKD: sort took " + ((t1-t0)/1000000.0) + " msec");

      sorted.close();
      return sorted;
    } else {

      // Offline sort:
      assert tempInput != null;

      final int offset = bytesPerDim * dim;

      Comparator<BytesRef> cmp;
      if (dim == numDims - 1) {
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

      OfflineSorter sorter = new OfflineSorter(tempDir, tempFileNamePrefix + "_bkd" + dim, cmp, offlineSorterBufferMB, offlineSorterMaxTempFiles, bytesPerDoc) {

          /** We write/read fixed-byte-width file that {@link OfflinePointReader} can read. */
          @Override
          protected ByteSequencesWriter getWriter(IndexOutput out) {
            return new ByteSequencesWriter(out) {
              @Override
              public void write(byte[] bytes, int off, int len) throws IOException {
                assert len == bytesPerDoc: "len=" + len + " bytesPerDoc=" + bytesPerDoc;
                out.writeBytes(bytes, off, len);
              }
            };
          }

          /** We write/read fixed-byte-width file that {@link OfflinePointReader} can read. */
          @Override
          protected ByteSequencesReader getReader(ChecksumIndexInput in, String name) throws IOException {
            return new ByteSequencesReader(in, name) {
              final BytesRef scratch = new BytesRef(new byte[bytesPerDoc]);
              @Override
              public BytesRef next() throws IOException {
                if (in.getFilePointer() >= end) {
                  return null;
                }
                in.readBytes(scratch.bytes, 0, bytesPerDoc);
                return scratch;
              }
            };
          }
        };

      String name = sorter.sort(tempInput.getName());

      return new OfflinePointWriter(tempDir, name, packedBytesLength, pointCount, longOrds, singleValuePerDoc);
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
    if (numDims > 1) {
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

    // Sort all docs once by each dimension:
    PathSlice[] sortedPointWriters = new PathSlice[numDims];

    // This is only used on exception; on normal code paths we close all files we opened:
    List<Closeable> toCloseHeroically = new ArrayList<>();

    boolean success = false;
    try {
      //long t0 = System.nanoTime();
      for(int dim=0;dim<numDims;dim++) {
        sortedPointWriters[dim] = new PathSlice(sort(dim), 0, pointCount);
      }
      //long t1 = System.nanoTime();
      //System.out.println("sort time: " + ((t1-t0)/1000000.0) + " msec");

      if (tempInput != null) {
        tempDir.deleteFile(tempInput.getName());
        tempInput = null;
      } else {
        assert heapPointWriter != null;
        heapPointWriter = null;
      }

      build(1, numLeaves, sortedPointWriters,
            ordBitSet, out,
            minPackedValue, maxPackedValue,
            splitPackedValues,
            leafBlockFPs,
            toCloseHeroically);

      for(PathSlice slice : sortedPointWriters) {
        slice.writer.destroy();
      }

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
    writeIndex(out, leafBlockFPs, splitPackedValues);
    return indexFP;
  }

  /** Subclass can change how it writes the index. */
  protected void writeIndex(IndexOutput out, long[] leafBlockFPs, byte[] splitPackedValues) throws IOException {
    CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT);
    out.writeVInt(numDims);
    out.writeVInt(maxPointsInLeafNode);
    out.writeVInt(bytesPerDim);

    assert leafBlockFPs.length > 0;
    out.writeVInt(leafBlockFPs.length);
    out.writeBytes(minPackedValue, 0, packedBytesLength);
    out.writeBytes(maxPackedValue, 0, packedBytesLength);

    out.writeVLong(pointCount);
    out.writeVInt(docsSeen.cardinality());

    // TODO: for 1D case, don't waste the first byte of each split value (it's always 0)

    // NOTE: splitPackedValues[0] is unused, because nodeID is 1-based:
    out.writeBytes(splitPackedValues, 0, splitPackedValues.length);

    long lastFP = 0;
    for (int i=0;i<leafBlockFPs.length;i++) {
      long delta = leafBlockFPs[i]-lastFP;
      out.writeVLong(delta);
      lastFP = leafBlockFPs[i];
    }
  }

  protected void writeLeafBlockDocs(IndexOutput out, int[] docIDs, int start, int count) throws IOException {
    assert count > 0: "maxPointsInLeafNode=" + maxPointsInLeafNode;
    out.writeVInt(count);
    DocIdsWriter.writeDocIds(docIDs, start, count, out);
  }

  protected void writeLeafBlockPackedValues(IndexOutput out, int[] commonPrefixLengths, int count, int sortedDim, IntFunction<BytesRef> packedValues) throws IOException {
    int prefixLenSum = Arrays.stream(commonPrefixLengths).sum();
    if (prefixLenSum == packedBytesLength) {
      // all values in this block are equal
      out.writeByte((byte) -1);
    } else {
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

  private void writeLeafBlockPackedValuesRange(IndexOutput out, int[] commonPrefixLengths, int start, int end, IntFunction<BytesRef> packedValues) throws IOException {
    for (int i = start; i < end; ++i) {
      BytesRef ref = packedValues.apply(i);
      assert ref.length == packedBytesLength;

      for(int dim=0;dim<numDims;dim++) {
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

  protected void writeCommonPrefixes(IndexOutput out, int[] commonPrefixes, byte[] packedValue) throws IOException {
    for(int dim=0;dim<numDims;dim++) {
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
  private void verifyChecksum(Throwable priorException, PointWriter writer) throws IOException {
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
    } else {
      // We are reading from heap; nothing to add:
      IOUtils.reThrow(priorException);
    }
  }

  /** Marks bits for the ords (points) that belong in the right sub tree (those docs that have values >= the splitValue). */
  private byte[] markRightTree(long rightCount, int splitDim, PathSlice source, LongBitSet ordBitSet) throws IOException {

    // Now we mark ords that fall into the right half, so we can partition on all other dims that are not the split dim:

    // Read the split value, then mark all ords in the right tree (larger than the split value):

    // TODO: find a way to also checksum this reader?  If we changed to markLeftTree, and scanned the final chunk, it could work?
    try (PointReader reader = source.writer.getReader(source.start + source.count - rightCount, rightCount)) {
      boolean result = reader.next();
      assert result;
      System.arraycopy(reader.packedValue(), splitDim*bytesPerDim, scratch1, 0, bytesPerDim);
      if (numDims > 1) {
        assert ordBitSet.get(reader.ord()) == false;
        ordBitSet.set(reader.ord());
        // Subtract 1 from rightCount because we already did the first value above (so we could record the split value):
        reader.markOrds(rightCount-1, ordBitSet);
      }
    } catch (Throwable t) {
      verifyChecksum(t, source.writer);
    }

    return scratch1;
  }

  /** Called only in assert */
  private boolean valueInBounds(BytesRef packedValue, byte[] minPackedValue, byte[] maxPackedValue) {
    for(int dim=0;dim<numDims;dim++) {
      int offset = bytesPerDim*dim;
      if (StringHelper.compare(bytesPerDim, packedValue.bytes, packedValue.offset + offset, minPackedValue, offset) < 0) {
        return false;
      }
      if (StringHelper.compare(bytesPerDim, packedValue.bytes, packedValue.offset + offset, maxPackedValue, offset) > 0) {
        return false;
      }
    }

    return true;
  }

  protected int split(byte[] minPackedValue, byte[] maxPackedValue) {
    // Find which dim has the largest span so we can split on it:
    int splitDim = -1;
    for(int dim=0;dim<numDims;dim++) {
      NumericUtils.subtract(bytesPerDim, dim, maxPackedValue, minPackedValue, scratchDiff);
      if (splitDim == -1 || StringHelper.compare(bytesPerDim, scratchDiff, 0, scratch1, 0) > 0) {
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
      verifyChecksum(t, source.writer);

      // Dead code but javac disagrees:
      return null;
    }
  }

  /** The array (sized numDims) of PathSlice describe the cell we have currently recursed to. */
  private void build(int nodeID, int leafNodeOffset,
                     PathSlice[] slices,
                     LongBitSet ordBitSet,
                     IndexOutput out,
                     byte[] minPackedValue, byte[] maxPackedValue,
                     byte[] splitPackedValues,
                     long[] leafBlockFPs,
                     List<Closeable> toCloseHeroically) throws IOException {

    for(PathSlice slice : slices) {
      assert slice.count == slices[0].count;
    }

    if (numDims == 1 && slices[0].writer instanceof OfflinePointWriter && slices[0].count <= maxPointsSortInHeap) {
      // Special case for 1D, to cutover to heap once we recurse deeply enough:
      slices[0] = switchToHeap(slices[0], toCloseHeroically);
    }

    if (nodeID >= leafNodeOffset) {

      // Leaf node: write block
      // We can write the block in any order so by default we write it sorted by the dimension that has the
      // least number of unique bytes at commonPrefixLengths[dim], which makes compression more efficient
      int sortedDim = 0;
      int sortedDimCardinality = Integer.MAX_VALUE;

      for (int dim=0;dim<numDims;dim++) {
        if (slices[dim].writer instanceof HeapPointWriter == false) {
          // Adversarial cases can cause this, e.g. very lopsided data, all equal points, such that we started
          // offline, but then kept splitting only in one dimension, and so never had to rewrite into heap writer
          slices[dim] = switchToHeap(slices[dim], toCloseHeroically);
        }

        PathSlice source = slices[dim];

        HeapPointWriter heapSource = (HeapPointWriter) source.writer;

        // Find common prefix by comparing first and last values, already sorted in this dimension:
        heapSource.readPackedValue(Math.toIntExact(source.start), scratch1);
        heapSource.readPackedValue(Math.toIntExact(source.start + source.count - 1), scratch2);

        int offset = dim * bytesPerDim;
        commonPrefixLengths[dim] = bytesPerDim;
        for(int j=0;j<bytesPerDim;j++) {
          if (scratch1[offset+j] != scratch2[offset+j]) {
            commonPrefixLengths[dim] = j;
            break;
          }
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

      PathSlice source = slices[sortedDim];

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
      assert valuesInOrderAndBounds(count, minPackedValue, maxPackedValue, packedValues);
      writeLeafBlockPackedValues(out, commonPrefixLengths, count, sortedDim, packedValues);

    } else {
      // Inner node: partition/recurse

      int splitDim;
      if (numDims > 1) {
        splitDim = split(minPackedValue, maxPackedValue);
      } else {
        splitDim = 0;
      }

      PathSlice source = slices[splitDim];

      assert nodeID < splitPackedValues.length: "nodeID=" + nodeID + " splitValues.length=" + splitPackedValues.length;

      // How many points will be in the left tree:
      long rightCount = source.count / 2;
      long leftCount = source.count - rightCount;

      byte[] splitValue = markRightTree(rightCount, splitDim, source, ordBitSet);
      int address = nodeID * (1+bytesPerDim);
      splitPackedValues[address] = (byte) splitDim;
      System.arraycopy(splitValue, 0, splitPackedValues, address + 1, bytesPerDim);

      // Partition all PathSlice that are not the split dim into sorted left and right sets, so we can recurse:

      PathSlice[] leftSlices = new PathSlice[numDims];
      PathSlice[] rightSlices = new PathSlice[numDims];

      byte[] minSplitPackedValue = new byte[packedBytesLength];
      System.arraycopy(minPackedValue, 0, minSplitPackedValue, 0, packedBytesLength);

      byte[] maxSplitPackedValue = new byte[packedBytesLength];
      System.arraycopy(maxPackedValue, 0, maxSplitPackedValue, 0, packedBytesLength);

      // When we are on this dim, below, we clear the ordBitSet:
      int dimToClear;
      if (numDims - 1 == splitDim) {
        dimToClear = numDims - 2;
      } else {
        dimToClear = numDims - 1;
      }

      for(int dim=0;dim<numDims;dim++) {

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
          verifyChecksum(t, slices[dim].writer);
        }
      }

      // Recurse on left tree:
      build(2*nodeID, leafNodeOffset, leftSlices,
            ordBitSet, out,
            minPackedValue, maxSplitPackedValue,
            splitPackedValues, leafBlockFPs, toCloseHeroically);
      for(int dim=0;dim<numDims;dim++) {
        // Don't destroy the dim we split on because we just re-used what our caller above gave us for that dim:
        if (dim != splitDim) {
          leftSlices[dim].writer.destroy();
        }
      }

      // TODO: we could "tail recurse" here?  have our parent discard its refs as we recurse right?
      // Recurse on right tree:
      build(2*nodeID+1, leafNodeOffset, rightSlices,
            ordBitSet, out,
            minSplitPackedValue, maxPackedValue,
            splitPackedValues, leafBlockFPs, toCloseHeroically);
      for(int dim=0;dim<numDims;dim++) {
        // Don't destroy the dim we split on because we just re-used what our caller above gave us for that dim:
        if (dim != splitDim) {
          rightSlices[dim].writer.destroy();
        }
      }
    }
  }

  // only called from assert
  private boolean valuesInOrderAndBounds(int count, byte[] minPackedValue, byte[] maxPackedValue, IntFunction<BytesRef> values) throws IOException {
    byte[] lastPackedValue = new byte[bytesPerDim];
    for (int i=0;i<count;i++) {
      BytesRef packedValue = values.apply(i);
      assert packedValue.length == packedBytesLength;
      assert numDims != 1 || valueInOrder(i, lastPackedValue, packedValue.bytes, packedValue.offset);

      // Make sure this value does in fact fall within this leaf cell:
      assert valueInBounds(packedValue, minPackedValue, maxPackedValue);
    }
    return true;
  }

  // only called from assert
  private boolean valueInOrder(long ord, byte[] lastPackedValue, byte[] packedValue, int packedValueOffset) {
    if (ord > 0 && StringHelper.compare(bytesPerDim, lastPackedValue, 0, packedValue, packedValueOffset) > 0) {
      throw new AssertionError("values out of order: last value=" + new BytesRef(lastPackedValue) + " current value=" + new BytesRef(packedValue, packedValueOffset, packedBytesLength) + " ord=" + ord);
    }
    System.arraycopy(packedValue, packedValueOffset, lastPackedValue, 0, bytesPerDim);
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
