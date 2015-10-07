package org.apache.lucene.util.bkd;

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

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.OfflineSorter.ByteSequencesWriter;
import org.apache.lucene.util.OfflineSorter;
import org.apache.lucene.util.RamUsageEstimator;

// TODO
//   - the compression is somewhat stupid now (delta vInt for 1024 docIDs, no compression for the byte[] values even though they have high locality)
//   - allow variable length byte[], but this is quite a bit more hairy
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
 *  <p>The number of dimensions can be 1 to 255, but every byte[] value is fixed length.
 *
 *  <p>
 *  See <a href="https://www.cs.duke.edu/~pankaj/publications/papers/bkd-sstd.pdf">this paper</a> for details.
 *
 *  <p>This consumes heap during writing: it allocates a <code>LongBitSet(numPoints)</code>, 
 *  and then uses up to the specified {@code maxMBSortInHeap} heap space for writing.
 *
 *  <p>
 *  <b>NOTE</b>: This can write at most Integer.MAX_VALUE * <code>maxPointsInLeafNode</code> total points, and
 *
 * @lucene.experimental */

public final class BKDWriter {

  static final String CODEC_NAME = "BKD";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  /** How many bytes each docs takes in the fixed-width offline format */
  private final int bytesPerDoc;

  static final boolean DEBUG = true;

  public static final int DEFAULT_MAX_POINTS_IN_LEAF_NODE = 1024;

  public static final float DEFAULT_MAX_MB_SORT_IN_HEAP = 16.0f;

  private final byte[] scratchBytes;
  private final ByteArrayDataOutput scratchBytesOutput;

  /** How many dimensions we are indexing */
  final int numDims;

  /** How many bytes each value in each dimension takes. */
  final int bytesPerDim;

  /** numDims * bytesPerDim */
  final int packedBytesLength;

  final byte[] scratchMax;
  final byte[] scratchDiff;
  final byte[] scratchPackedValue;
  final byte[] scratch1;
  final byte[] scratch2;

  // nocommit grep for lat / lon and fix!

  private OfflineSorter.ByteSequencesWriter offlinePointWriter;
  private HeapPointWriter heapPointWriter;

  private Path tempInput;
  private final int maxPointsInLeafNode;
  private final int maxPointsSortInHeap;

  private long pointCount;

  public BKDWriter(int numDims, int bytesPerDim) throws IOException {
    this(numDims, bytesPerDim, DEFAULT_MAX_POINTS_IN_LEAF_NODE, DEFAULT_MAX_MB_SORT_IN_HEAP);
  }

  public BKDWriter(int numDims, int bytesPerDim, int maxPointsInLeafNode, float maxMBSortInHeap) throws IOException {
    verifyParams(numDims, maxPointsInLeafNode, maxMBSortInHeap);
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.numDims = numDims;
    this.bytesPerDim = bytesPerDim;
    packedBytesLength = numDims * bytesPerDim;

    scratchMax = new byte[bytesPerDim];
    scratchDiff = new byte[bytesPerDim];
    scratchPackedValue = new byte[packedBytesLength];
    scratch1 = new byte[packedBytesLength];
    scratch2 = new byte[packedBytesLength];

    // dimensional values (numDims * bytesPerDim) + ord (long) + docID (int)
    bytesPerDoc = packedBytesLength + RamUsageEstimator.NUM_BYTES_LONG + RamUsageEstimator.NUM_BYTES_INT;
    scratchBytes = new byte[bytesPerDoc];
    scratchBytesOutput = new ByteArrayDataOutput(scratchBytes);

    // nocommit verify this is correct!  see how much heap we actually use
    // We must be able to hold at least the leaf node in heap at write:
    maxPointsSortInHeap = Math.max(maxPointsInLeafNode, (int) (0.25 * (maxMBSortInHeap * 1024 * 1024) / bytesPerDoc));

    // We write first maxPointsSortInHeap in heap, then cutover to offline for additional points:
    heapPointWriter = new HeapPointWriter(16, maxPointsSortInHeap, packedBytesLength);

    // nocommit use TrackingDirWrapper here to do file deletion cleanup in one place...
  }

  public static void verifyParams(int numDims, int maxPointsInLeafNode, float maxMBSortInHeap) {
    // We encode dim in a single byte in the splitPackedValues, but we only expose 4 bits for it now, in case we want to use
    // remaining 4 bits for another purpose later
    if (numDims < 1 || numDims > 15) {
      throw new IllegalArgumentException("numDims must be 1 .. 15 (got: " + numDims + ")");
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
  }

  /** If the current segment has too many points then we switchover to temp files / offline sort. */
  private void switchToOffline() throws IOException {
    System.out.println("W: switchToOffline");

    // For each .add we just append to this input file, then in .finish we sort this input and resursively build the tree:
    tempInput = Files.createTempFile(OfflineSorter.getDefaultTempDir(), "in", "");
    offlinePointWriter = new OfflineSorter.ByteSequencesWriter(tempInput);
    PointReader reader = heapPointWriter.getReader(0);
    for(int i=0;i<pointCount;i++) {
      boolean hasNext = reader.next();
      assert hasNext;

      byte[] packedValue = reader.packedValue();

      scratchBytesOutput.reset(scratchBytes);
      scratchBytesOutput.writeBytes(packedValue, 0, packedValue.length);
      scratchBytesOutput.writeVInt(heapPointWriter.docIDs[i]);
      scratchBytesOutput.writeVLong(i);
      // TODO: can/should OfflineSorter optimize the fixed-width case?
      offlinePointWriter.write(scratchBytes, 0, scratchBytes.length);
    }

    heapPointWriter = null;
  }

  public void add(byte[] packedValue, int docID) throws IOException {
    if (packedValue.length != packedBytesLength) {
      throw new IllegalArgumentException("packedValue should be length=" + packedBytesLength + " (got: " + packedValue.length + ")");
    }

    if (pointCount >= maxPointsSortInHeap) {
      if (offlinePointWriter == null) {
        switchToOffline();
      }
      scratchBytesOutput.reset(scratchBytes);
      scratchBytesOutput.writeBytes(packedValue, 0, packedValue.length);
      scratchBytesOutput.writeVInt(docID);
      scratchBytesOutput.writeVLong(pointCount);
      offlinePointWriter.write(scratchBytes, 0, scratchBytes.length);
    } else {
      // Not too many points added yet, continue using heap:
      heapPointWriter.append(packedValue, pointCount, docID);
    }

    pointCount++;
  }

  // TODO: if we fixed each partition step to just record the file offset at the "split point", we could probably handle variable length
  // encoding?

  /** Changes incoming {@link ByteSequencesWriter} file to to fixed-width-per-entry file, because we need to be able to slice
   *  as we recurse in {@link #build}. */
  private PointWriter convertToFixedWidth(Path in) throws IOException {
    BytesRefBuilder scratch = new BytesRefBuilder();
    scratch.grow(bytesPerDoc);
    BytesRef bytes = scratch.get();
    ByteArrayDataInput dataReader = new ByteArrayDataInput();

    OfflineSorter.ByteSequencesReader reader = null;
    PointWriter sortedPointWriter = null;
    boolean success = false;
    byte[] packedValue = new byte[packedBytesLength];
    try {
      reader = new OfflineSorter.ByteSequencesReader(in);
      sortedPointWriter = getPointWriter(pointCount);
      for (long i=0;i<pointCount;i++) {
        boolean result = reader.read(scratch);
        assert result;
        dataReader.reset(bytes.bytes, bytes.offset, bytes.length);
        dataReader.readBytes(packedValue, 0, packedValue.length);
        int docID = dataReader.readVInt();
        long ord = dataReader.readVLong();
        assert docID >= 0: "docID=" + docID;
        sortedPointWriter.append(packedValue, ord, docID);
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(sortedPointWriter, reader);
      } else {
        IOUtils.closeWhileHandlingException(sortedPointWriter, reader);
        try {
          sortedPointWriter.destroy();
        } catch (Throwable t) {
          // Suppress to keep throwing original exc
        }
      }
    }

    return sortedPointWriter;
  }

  private int compare(byte[] a, byte[] b, int dim) {
    int start = dim * bytesPerDim;
    int end = start + bytesPerDim;
    for(int i=start;i<end;i++) {
      int diff = (a[i]&0xff) - (b[i]&0xff);
      if (diff != 0) {
        return diff;
      }
    }

    return 0;
  }

  /** If dim=-1 we sort by docID, else by that dim. */
  private void sortHeapPointWriter(final HeapPointWriter writer, int start, int length, int dim) {

    assert pointCount < Integer.MAX_VALUE;

    // All buffered points are still in heap; just do in-place sort:
    new InPlaceMergeSorter() {
      @Override
      protected void swap(int i, int j) {
        int docID = writer.docIDs[i];
        writer.docIDs[i] = writer.docIDs[j];
        writer.docIDs[j] = docID;

        long ord = writer.ords[i];
        writer.ords[i] = writer.ords[j];
        writer.ords[j] = ord;

        // scratch1 = values[i]
        writer.readPackedValue(i, scratch1);
        // scratch2 = values[j]
        writer.readPackedValue(j, scratch2);
        // values[i] = scratch2
        writer.writePackedValue(i, scratch2);
        // values[j] = scratch1
        writer.writePackedValue(j, scratch1);
      }

      @Override
      protected int compare(int i, int j) {
        if (dim != -1) {
          writer.readPackedValue(i, scratch1);
          writer.readPackedValue(j, scratch2);
          int cmp = BKDUtil.compare(bytesPerDim, scratch1, dim, scratch2, dim);
          if (cmp != 0) {
            return cmp;
          }
        }

        // Tie-break
        int cmp = Integer.compare(writer.docIDs[i], writer.docIDs[j]);
        if (cmp != 0) {
          return cmp;
        }

        return Long.compare(writer.ords[i], writer.ords[j]);
      }
    }.sort(start, start+length);
  }

  private PointWriter sort(int dim) throws IOException {

    if (heapPointWriter != null) {

      sortHeapPointWriter(heapPointWriter, 0, (int) pointCount, dim);

      // nocommit shrink wrap?
      heapPointWriter.close();
      return heapPointWriter;
    } else {

      // Offline sort:
      assert tempInput != null;

      final ByteArrayDataInput reader = new ByteArrayDataInput();
      Comparator<BytesRef> cmp = new Comparator<BytesRef>() {
        private final ByteArrayDataInput readerB = new ByteArrayDataInput();

        @Override
        public int compare(BytesRef a, BytesRef b) {
          reader.reset(a.bytes, a.offset, a.length);
          reader.readBytes(scratch1, 0, scratch1.length);
          final int docIDA = reader.readVInt();
          final long ordA = reader.readVLong();

          reader.reset(b.bytes, b.offset, b.length);
          reader.readBytes(scratch2, 0, scratch2.length);
          final int docIDB = reader.readVInt();
          final long ordB = reader.readVLong();

          int cmp = BKDUtil.compare(bytesPerDim, scratch1, dim, scratch2, dim);

          if (cmp != 0) {
            return cmp;
          }

          // Tie-break
          cmp = Integer.compare(docIDA, docIDB);
          if (cmp != 0) {
            return cmp;
          }

          return Long.compare(ordA, ordB);
        }
      };

      Path sorted = Files.createTempFile(OfflineSorter.getDefaultTempDir(), "sorted", "");
      boolean success = false;
      
      try {
        OfflineSorter sorter = new OfflineSorter(cmp);
        sorter.sort(tempInput, sorted);
        PointWriter writer = convertToFixedWidth(sorted);
        success = true;
        return writer;
      } finally {
        if (success) {
          IOUtils.rm(sorted);
        } else {
          IOUtils.deleteFilesIgnoringExceptions(sorted);
        }
      }
    }
  }

  /** Writes the BKD tree to the provided {@link IndexOutput} and returns the file offset where index was written. */
  public long finish(IndexOutput out) throws IOException {
    //System.out.println("\nBKDTreeWriter.finish pointCount=" + pointCount + " out=" + out + " heapWriter=" + heapWriter);

    if (offlinePointWriter != null) {
      offlinePointWriter.close();
    }

    LongBitSet ordBitSet = new LongBitSet(pointCount);

    long countPerLeaf = pointCount;
    long innerNodeCount = 1;

    while (countPerLeaf > maxPointsInLeafNode) {
      countPerLeaf = (countPerLeaf+1)/2;
      innerNodeCount *= 2;
    }

    //System.out.println("innerNodeCount=" + innerNodeCount);

    if (1+2*innerNodeCount >= Integer.MAX_VALUE) {
      throw new IllegalStateException("too many nodes; increase maxPointsInLeafNode (currently " + maxPointsInLeafNode + ") and reindex");
    }

    innerNodeCount--;

    int numLeaves = (int) (innerNodeCount+1);

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

    for(int dim=0;dim<numDims;dim++) {
      sortedPointWriters[dim] = new PathSlice(sort(dim), 0, pointCount);
    }

    // nocommit make sure this happens on exc:
    IOUtils.rm(tempInput);
    heapPointWriter = null;

    byte[] minPacked = new byte[packedBytesLength];
    byte[] maxPacked = new byte[packedBytesLength];
    Arrays.fill(maxPacked, (byte) 0xff);

    build(1, numLeaves, sortedPointWriters,
          ordBitSet, out,
          minPacked, maxPacked,
          splitPackedValues,
          leafBlockFPs);

    // nocommit make sure this happens on exc:
    for(PathSlice slice : sortedPointWriters) {
      slice.writer.destroy();
    }

    //System.out.println("Total nodes: " + innerNodeCount);

    // Write index:
    long indexFP = out.getFilePointer();
    CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT);
    out.writeVInt(numDims);
    out.writeVInt(maxPointsInLeafNode);
    out.writeVInt(bytesPerDim);

    out.writeVInt(numLeaves);

    // NOTE: splitPackedValues[0] is unused, because nodeID is 1-based:
    out.writeBytes(splitPackedValues, 0, splitPackedValues.length);

    for (int i=0;i<leafBlockFPs.length;i++) {
      out.writeVLong(leafBlockFPs[i]);
    }

    return indexFP;
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

  /** Marks bits for the ords (points) that belong in the right sub tree (those docs that have values >= the splitValue). */
  private byte[] markRightTree(long rightCount, int splitDim, PathSlice source, LongBitSet ordBitSet) throws IOException {

    // nocommit instead of partitioning to disk can't we just alloc new bitsets and pass those down?

    // Now we mark ords that fall into the right half, so we can partition on all other dims that are not the split dim:
    assert ordBitSet.cardinality() == 0: "cardinality=" + ordBitSet.cardinality();

    // Read the split value: just open a reader, seek'd to the next value after leftCount, then read its value:
    PointReader reader = source.writer.getReader(source.start + source.count - rightCount);
    boolean success = false;
    try {
      boolean result = reader.next();
      assert result;

      System.arraycopy(reader.packedValue(), splitDim*bytesPerDim, scratchMax, 0, bytesPerDim);

      ordBitSet.set(reader.ord());

      // Start at 1 because we already did the first value above (so we could keep the split value):
      for(int i=1;i<rightCount;i++) {
        result = reader.next();
        assert result;
        ordBitSet.set(reader.ord());
      }

      success = true;
    } finally {
      if (success) {
        IOUtils.close(reader);
      } else {
        IOUtils.closeWhileHandlingException(reader);
      }
    }

    assert rightCount == ordBitSet.cardinality(): "rightCount=" + rightCount + " cardinality=" + ordBitSet.cardinality();

    // nocommit rename scratchMax
    return scratchMax;
  }

  /** Called only in assert */
  private boolean valueInBounds(byte[] packedValue, byte[] minPackedValue, byte[] maxPackedValue) {
    for(int dim=0;dim<numDims;dim++) {
      if (BKDUtil.compare(bytesPerDim, packedValue, dim, minPackedValue, dim) < 0) {
        return false;
      }
      if (BKDUtil.compare(bytesPerDim, packedValue, dim, maxPackedValue, dim) > 0) {
        return false;
      }
    }

    return true;
  }

  // TODO: make this protected when we want to subclass to play with different splitting criteria
  private int split(byte[] minPackedValue, byte[] maxPackedValue) {
    // Find which dim has the largest span so we can split on it:
    int splitDim = -1;
    for(int dim=0;dim<numDims;dim++) {
      BKDUtil.subtract(bytesPerDim, dim, maxPackedValue, minPackedValue, scratchDiff);
      if (splitDim == -1 || BKDUtil.compare(bytesPerDim, scratchDiff, 0, scratchMax, 0) > 0) {
        System.arraycopy(scratchDiff, 0, scratchMax, 0, bytesPerDim);
        splitDim = dim;
      }
    }

    return splitDim;
  }

  /** The array (sized numDims) of PathSlice describe the cell we have currently recursed to. */
  private void build(int nodeID, int leafNodeOffset,
                     PathSlice[] slices,
                     LongBitSet ordBitSet,
                     IndexOutput out,
                     byte[] minPackedValue, byte[] maxPackedValue,
                     byte[] splitPackedValues,
                     long[] leafBlockFPs) throws IOException {

    int splitDim = split(minPackedValue, maxPackedValue);

    for(PathSlice slice : slices) {
      assert slice.count == slices[0].count;
    }

    PathSlice source = slices[splitDim];

    if (DEBUG) System.out.println("\nBUILD: nodeID=" + nodeID + " leafNodeOffset=" + leafNodeOffset + " splitDim=" + splitDim + "\n  count=" + source.count + "\n  source=" + source +
                                  "\n  min=" + BKDUtil.bytesToInt(minPackedValue, 0) + " max=" + BKDUtil.bytesToInt(maxPackedValue, 0));

    if (nodeID >= leafNodeOffset) {
      // Leaf node: write block
      if (DEBUG) System.out.println("  leaf");

      // We ensured that maxPointsSortInHeap was >= maxPointsInLeafNode, so we better be in heap at this point:
      assert source.writer instanceof HeapPointWriter;
      HeapPointWriter heapSource = (HeapPointWriter) source.writer;

      // Sort by docID in the leaf so we can delta-vInt encode:
      sortHeapPointWriter(heapSource, Math.toIntExact(source.start), Math.toIntExact(source.count), -1);

      int lastDocID = 0;

      // Save the block file pointer:
      leafBlockFPs[nodeID - leafNodeOffset] = out.getFilePointer();

      out.writeVInt(Math.toIntExact(source.count));

      // Write docIDs first, as their own chunk, so that at intersect time we can add all docIDs w/o
      // loading the values:
      for (int i=0;i<source.count;i++) {
        int docID = heapSource.docIDs[Math.toIntExact(source.start + i)];
        System.out.println("    docID=" + docID);
        out.writeVInt(docID - lastDocID);
        lastDocID = docID;
      }

      // TODO: we should delta compress / only write suffix bytes, like terms dict (the values will all be "close together" since we are at
      // a leaf cell):

      // Now write the full values:
      for (int i=0;i<source.count;i++) {
        // TODO: we could do bulk copying here, avoiding the intermediate copy:
        heapSource.readPackedValue(Math.toIntExact(source.start + i), scratchPackedValue);

        // Make sure this value does in fact fall within this leaf cell:
        assert valueInBounds(scratchPackedValue, minPackedValue, maxPackedValue);
        out.writeBytes(scratchPackedValue, 0, scratchPackedValue.length);
      }

    } else {
      // Inner node: partition/recurse

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

      for(int dim=0;dim<numDims;dim++) {
        if (dim == splitDim) {
          // No need to partition on this dim since it's a simple slice of the incoming already sorted slice.
          leftSlices[dim] = new PathSlice(source.writer, source.start, leftCount);
          rightSlices[dim] = new PathSlice(source.writer, source.start + leftCount, rightCount);
          System.arraycopy(splitValue, 0, minSplitPackedValue, dim*bytesPerDim, bytesPerDim);
          System.arraycopy(splitValue, 0, maxSplitPackedValue, dim*bytesPerDim, bytesPerDim);
          continue;
        }

        PointWriter leftPointWriter = null;
        PointWriter rightPointWriter = null;
        PointReader reader = null;

        boolean success = false;

        int nextRightCount = 0;

        try {
          leftPointWriter = getPointWriter(leftCount);
          rightPointWriter = getPointWriter(source.count - leftCount);

          //if (DEBUG) System.out.println("  partition:\n    splitValueEnc=" + splitValue + "\n    " + nextSource + "\n      --> leftSorted=" + leftPointWriter + "\n      --> rightSorted=" + rightPointWriter + ")");
          reader = slices[dim].writer.getReader(slices[dim].start);

          // Partition this source according to how the splitDim split the values:
          for (int i=0;i<source.count;i++) {
            boolean result = reader.next();
            assert result;
            byte[] packedValue = reader.packedValue();
            long ord = reader.ord();
            int docID = reader.docID();
            if (ordBitSet.get(ord)) {
              rightPointWriter.append(packedValue, ord, docID);
              nextRightCount++;
            } else {
              leftPointWriter.append(packedValue, ord, docID);
            }
          }
          success = true;
        } finally {
          if (success) {
            IOUtils.close(reader, leftPointWriter, rightPointWriter);
          } else {
            IOUtils.closeWhileHandlingException(reader, leftPointWriter, rightPointWriter);
            // nocommit we must also destroy all prior writers here (so temp files are removed)
          }
        }

        leftSlices[dim] = new PathSlice(leftPointWriter, 0, leftCount);
        rightSlices[dim] = new PathSlice(rightPointWriter, 0, rightCount);

        assert rightCount == nextRightCount: "rightCount=" + rightCount + " nextRightCount=" + nextRightCount;
      }

      System.out.println("pointCount=" + pointCount);
      ordBitSet.clear(0, pointCount);

      // Recurse on left tree:
      build(2*nodeID, leafNodeOffset, leftSlices,
            ordBitSet, out,
            minPackedValue, maxSplitPackedValue,
            splitPackedValues, leafBlockFPs);
      for(int dim=0;dim<numDims;dim++) {
        if (dim != splitDim) {
          // nocommit need try/finally monster around this?
          leftSlices[dim].writer.destroy();
        }
      }

      // Recurse on right tree:
      build(2*nodeID+1, leafNodeOffset, rightSlices,
            ordBitSet, out,
            minSplitPackedValue, maxPackedValue,
            splitPackedValues, leafBlockFPs);
      for(int dim=0;dim<numDims;dim++) {
        if (dim != splitDim) {
          // nocommit need try/finally monster around this?
          rightSlices[dim].writer.destroy();
        }
      }
      
      // nocommit make sure we destroy on exception
    }
  }

  PointWriter getPointWriter(long count) throws IOException {
    System.out.println("W: getPointWriter count=" + count);
    if (count <= maxPointsSortInHeap) {
      System.out.println("  heap");
      int size = Math.toIntExact(count);
      return new HeapPointWriter(size, size, packedBytesLength);
    } else {
      System.out.println("  offline");
      return new OfflinePointWriter(count, packedBytesLength);
    }
  }
}
