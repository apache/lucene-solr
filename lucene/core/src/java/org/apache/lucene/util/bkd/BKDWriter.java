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

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.OfflineSorter;
import org.apache.lucene.util.OfflineSorter.ByteSequencesWriter;
import org.apache.lucene.util.RamUsageEstimator;

// TODO
//   - the compression is somewhat stupid now (delta vInt for 1024 docIDs, no compression for the byte[] values even though they have high locality)
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

public class BKDWriter implements Closeable {

  public static final String CODEC_NAME = "BKD";
  public static final int VERSION_START = 0;
  public static final int VERSION_CURRENT = VERSION_START;

  /** How many bytes each docs takes in the fixed-width offline format */
  private final int bytesPerDoc;

  public static final int DEFAULT_MAX_POINTS_IN_LEAF_NODE = 1024;

  public static final float DEFAULT_MAX_MB_SORT_IN_HEAP = 16.0f;

  /** Maximum number of dimensions */
  public static final int MAX_DIMS = 255;

  /** How many dimensions we are indexing */
  protected final int numDims;

  /** How many bytes each value in each dimension takes. */
  protected final int bytesPerDim;

  /** numDims * bytesPerDim */
  protected final int packedBytesLength;

  final TrackingDirectoryWrapper tempDir;
  final String tempFileNamePrefix;

  final byte[] scratchDiff;
  final byte[] scratchPackedValue;
  final byte[] scratch1;
  final byte[] scratch2;

  private OfflinePointWriter offlinePointWriter;
  private HeapPointWriter heapPointWriter;

  private IndexOutput tempInput;
  protected final int maxPointsInLeafNode;
  private final int maxPointsSortInHeap;

  private long pointCount;

  public BKDWriter(Directory tempDir, String tempFileNamePrefix, int numDims, int bytesPerDim) throws IOException {
    this(tempDir, tempFileNamePrefix, numDims, bytesPerDim, DEFAULT_MAX_POINTS_IN_LEAF_NODE, DEFAULT_MAX_MB_SORT_IN_HEAP);
  }

  public BKDWriter(Directory tempDir, String tempFileNamePrefix, int numDims, int bytesPerDim, int maxPointsInLeafNode, double maxMBSortInHeap) throws IOException {
    verifyParams(numDims, maxPointsInLeafNode, maxMBSortInHeap);
    // We use tracking dir to deal with removing files on exception, so each place that
    // creates temp files doesn't need crazy try/finally/sucess logic:
    this.tempDir = new TrackingDirectoryWrapper(tempDir);
    this.tempFileNamePrefix = tempFileNamePrefix;
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.numDims = numDims;
    this.bytesPerDim = bytesPerDim;
    packedBytesLength = numDims * bytesPerDim;

    scratchDiff = new byte[bytesPerDim];
    scratchPackedValue = new byte[packedBytesLength];
    scratch1 = new byte[packedBytesLength];
    scratch2 = new byte[packedBytesLength];

    // dimensional values (numDims * bytesPerDim) + ord (long) + docID (int)
    bytesPerDoc = packedBytesLength + RamUsageEstimator.NUM_BYTES_LONG + RamUsageEstimator.NUM_BYTES_INT;

    // As we recurse, we compute temporary partitions of the data, halving the
    // number of points at each recursion.  Once there are few enough points,
    // we can switch to sorting in heap instead of offline (on disk).  At any
    // time in the recursion, we hold the number of points at that level, plus
    // all recursive halves (i.e. 16 + 8 + 4 + 2) so the memory usage is 2X
    // what that level would consume, so we multiply by 0.5 to convert from
    // bytes to points here.  Each dimension has its own sorted partition, so
    // we must divide by numDims as wel.

    maxPointsSortInHeap = (int) (0.5 * (maxMBSortInHeap * 1024 * 1024) / (bytesPerDoc * numDims));

    // Finally, we must be able to hold at least the leaf node in heap during build:
    if (maxPointsSortInHeap < maxPointsInLeafNode) {
      throw new IllegalArgumentException("maxMBSortInHeap=" + maxMBSortInHeap + " only allows for maxPointsSortInHeap=" + maxPointsSortInHeap + ", but this is less than maxPointsInLeafNode=" + maxPointsInLeafNode + "; either increase maxMBSortInHeap or decrease maxPointsInLeafNode");
    }

    // We write first maxPointsSortInHeap in heap, then cutover to offline for additional points:
    heapPointWriter = new HeapPointWriter(16, maxPointsSortInHeap, packedBytesLength);
  }

  public static void verifyParams(int numDims, int maxPointsInLeafNode, double maxMBSortInHeap) {
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
  }

  /** If the current segment has too many points then we switchover to temp files / offline sort. */
  private void switchToOffline() throws IOException {

    // For each .add we just append to this input file, then in .finish we sort this input and resursively build the tree:
    offlinePointWriter = new OfflinePointWriter(tempDir, tempFileNamePrefix, packedBytesLength);
    tempInput = offlinePointWriter.out;
    PointReader reader = heapPointWriter.getReader(0);
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
        switchToOffline();
      }
      offlinePointWriter.append(packedValue, pointCount, docID);
    } else {
      // Not too many points added yet, continue using heap:
      heapPointWriter.append(packedValue, pointCount, docID);
    }

    pointCount++;
  }

  // TODO: if we fixed each partition step to just record the file offset at the "split point", we could probably handle variable length
  // encoding and not have our own ByteSequencesReader/Writer

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

      assert tempInput == null;

      // We never spilled the incoming points to disk, so now we sort in heap:
      HeapPointWriter sorted;

      if (dim == 0) {
        // First dim can re-use the current heap writer
        sorted = heapPointWriter;
      } else {
        // Subsequent dims need a private copy
        sorted = new HeapPointWriter((int) pointCount, (int) pointCount, packedBytesLength);
        sorted.copyFrom(heapPointWriter);
      }

      sortHeapPointWriter(sorted, 0, (int) pointCount, dim);

      sorted.close();
      return sorted;
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

      // TODO: this is sort of sneaky way to get the final OfflinePointWriter from OfflineSorter:
      IndexOutput[] lastWriter = new IndexOutput[1];

      OfflineSorter sorter = new OfflineSorter(tempDir, tempFileNamePrefix, cmp) {

          /** We write/read fixed-byte-width file that {@link OfflinePointReader} can read. */
          @Override
          protected ByteSequencesWriter getWriter(IndexOutput out) {
            lastWriter[0] = out;
            return new ByteSequencesWriter(out) {
              @Override
              public void write(byte[] bytes, int off, int len) throws IOException {
                if (len != bytesPerDoc) {
                  throw new IllegalArgumentException("len=" + len + " bytesPerDoc=" + bytesPerDoc);
                }
                out.writeBytes(bytes, off, len);
              }
            };
          }

          /** We write/read fixed-byte-width file that {@link OfflinePointReader} can read. */
          @Override
          protected ByteSequencesReader getReader(IndexInput in) throws IOException {
            return new ByteSequencesReader(in) {
              @Override
              public boolean read(BytesRefBuilder ref) throws IOException {
                ref.grow(bytesPerDoc);
                try {
                  in.readBytes(ref.bytes(), 0, bytesPerDoc);
                } catch (EOFException eofe) {
                  return false;
                }
                ref.setLength(bytesPerDoc);
                return true;
              }
            };
          }
        };

      sorter.sort(tempInput.getName());

      assert lastWriter[0] != null;

      return new OfflinePointWriter(tempDir, lastWriter[0], packedBytesLength, pointCount);
    }
  }

  /** Writes the BKD tree to the provided {@link IndexOutput} and returns the file offset where index was written. */
  public long finish(IndexOutput out) throws IOException {
    //System.out.println("\nBKDTreeWriter.finish pointCount=" + pointCount + " out=" + out + " heapWriter=" + heapWriter);

    // TODO: specialize the 1D case?  it's much faster at indexing time (no partitioning on recruse...)

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

    byte[] minPacked = new byte[packedBytesLength];
    byte[] maxPacked = new byte[packedBytesLength];
    Arrays.fill(maxPacked, (byte) 0xff);

    boolean success = false;
    try {
      for(int dim=0;dim<numDims;dim++) {
        sortedPointWriters[dim] = new PathSlice(sort(dim), 0, pointCount);
      }

      if (tempInput != null) {
        tempDir.deleteFile(tempInput.getName());
        tempInput = null;
      } else {
        assert heapPointWriter != null;
        heapPointWriter = null;
      }

      build(1, numLeaves, sortedPointWriters,
            ordBitSet, out,
            minPacked, maxPacked,
            splitPackedValues,
            leafBlockFPs);

      for(PathSlice slice : sortedPointWriters) {
        slice.writer.destroy();
      }

      // If no exception, we should have cleaned everything up:
      assert tempDir.getCreatedFiles().isEmpty();

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
  protected void writeIndex(IndexOutput out, long[] leafBlockFPs, byte[] splitPackedValues) throws IOException {
    CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT);
    out.writeVInt(numDims);
    out.writeVInt(maxPointsInLeafNode);
    out.writeVInt(bytesPerDim);

    out.writeVInt(leafBlockFPs.length);

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
    out.writeVInt(count);

    int lastDocID = 0;
    for (int i=0;i<count;i++) {
      int docID = docIDs[start + i];
      out.writeVInt(docID - lastDocID);
      lastDocID = docID;
    }
  }

  protected void writeLeafBlockPackedValue(IndexOutput out, byte[] bytes, int offset, int length) throws IOException {
    out.writeBytes(bytes, 0, length);
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

  /** Marks bits for the ords (points) that belong in the right sub tree (those docs that have values >= the splitValue). */
  private byte[] markRightTree(long rightCount, int splitDim, PathSlice source, LongBitSet ordBitSet) throws IOException {

    // Now we mark ords that fall into the right half, so we can partition on all other dims that are not the split dim:
    assert ordBitSet.cardinality() == 0: "cardinality=" + ordBitSet.cardinality();

    // Read the split value, then mark all ords in the right tree (larger than the split value):
    try (PointReader reader = source.writer.getReader(source.start + source.count - rightCount)) {
      boolean result = reader.next();
      assert result;

      System.arraycopy(reader.packedValue(), splitDim*bytesPerDim, scratch1, 0, bytesPerDim);

      ordBitSet.set(reader.ord());

      // Start at 1 because we already did the first value above (so we could keep the split value):
      for(int i=1;i<rightCount;i++) {
        result = reader.next();
        assert result;
        ordBitSet.set(reader.ord());
      }

      assert rightCount == ordBitSet.cardinality(): "rightCount=" + rightCount + " cardinality=" + ordBitSet.cardinality();
    }

    return scratch1;
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
      if (splitDim == -1 || BKDUtil.compare(bytesPerDim, scratchDiff, 0, scratch1, 0) > 0) {
        System.arraycopy(scratchDiff, 0, scratch1, 0, bytesPerDim);
        splitDim = dim;
      }
    }

    return splitDim;
  }

  /** Only called in the 1D case, to pull a partition back into heap once
   *  the point count is low enough while recursing. */
  private PathSlice switchToHeap(PathSlice source) throws IOException {
    int count = Math.toIntExact(source.count);
    try (
       PointWriter writer = new HeapPointWriter(count, count, packedBytesLength);
       PointReader reader = source.writer.getReader(source.start);
       ) {
      for(int i=0;i<count;i++) {
        boolean hasNext = reader.next();
        assert hasNext;
        writer.append(reader.packedValue(), reader.ord(), reader.docID());
      }
      return new PathSlice(writer, 0, count);
    }
  }

  /** The array (sized numDims) of PathSlice describe the cell we have currently recursed to. */
  private void build(int nodeID, int leafNodeOffset,
                     PathSlice[] slices,
                     LongBitSet ordBitSet,
                     IndexOutput out,
                     byte[] minPackedValue, byte[] maxPackedValue,
                     byte[] splitPackedValues,
                     long[] leafBlockFPs) throws IOException {

    for(PathSlice slice : slices) {
      assert slice.count == slices[0].count;
    }

    if (numDims == 1 && slices[0].writer instanceof OfflinePointWriter && slices[0].count <= maxPointsSortInHeap) {
      // Special case for 1D, to cutover to heap once we recurse deeply enough:
      slices[0] = switchToHeap(slices[0]);
    }

    if (nodeID >= leafNodeOffset) {
      // Leaf node: write block

      PathSlice source = slices[0];

      if (source.writer instanceof HeapPointWriter == false) {
        // Adversarial cases can cause this, e.g. very lopsided data, all equal points
        source = switchToHeap(source);
      }

      // We ensured that maxPointsSortInHeap was >= maxPointsInLeafNode, so we better be in heap at this point:
      HeapPointWriter heapSource = (HeapPointWriter) source.writer;

      // Sort by docID in the leaf so we can delta-vInt encode:
      sortHeapPointWriter(heapSource, Math.toIntExact(source.start), Math.toIntExact(source.count), -1);

      // Save the block file pointer:
      leafBlockFPs[nodeID - leafNodeOffset] = out.getFilePointer();

      // Write docIDs first, as their own chunk, so that at intersect time we can add all docIDs w/o
      // loading the values:
      writeLeafBlockDocs(out, heapSource.docIDs, Math.toIntExact(source.start), Math.toIntExact(source.count));

      // TODO: we should delta compress / only write suffix bytes, like terms dict (the values will all be "close together" since we are at
      // a leaf cell):

      // Now write the full values:
      for (int i=0;i<source.count;i++) {
        // TODO: we could do bulk copying here, avoiding the intermediate copy:
        heapSource.readPackedValue(Math.toIntExact(source.start + i), scratchPackedValue);

        // Make sure this value does in fact fall within this leaf cell:
        assert valueInBounds(scratchPackedValue, minPackedValue, maxPackedValue);
        writeLeafBlockPackedValue(out, scratchPackedValue, 0, scratchPackedValue.length);
      }

    } else {
      // Inner node: partition/recurse

      int splitDim = split(minPackedValue, maxPackedValue);

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

      for(int dim=0;dim<numDims;dim++) {

        if (dim == splitDim) {
          // No need to partition on this dim since it's a simple slice of the incoming already sorted slice.
          leftSlices[dim] = new PathSlice(source.writer, source.start, leftCount);
          rightSlices[dim] = new PathSlice(source.writer, source.start + leftCount, rightCount);
          System.arraycopy(splitValue, 0, minSplitPackedValue, dim*bytesPerDim, bytesPerDim);
          System.arraycopy(splitValue, 0, maxSplitPackedValue, dim*bytesPerDim, bytesPerDim);
          continue;
        }

        try (PointWriter leftPointWriter = getPointWriter(leftCount);
             PointWriter rightPointWriter = getPointWriter(source.count - leftCount);
             PointReader reader = slices[dim].writer.getReader(slices[dim].start);) {

          // Partition this source according to how the splitDim split the values:
          int nextRightCount = 0;
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

          leftSlices[dim] = new PathSlice(leftPointWriter, 0, leftCount);
          rightSlices[dim] = new PathSlice(rightPointWriter, 0, rightCount);

          assert rightCount == nextRightCount: "rightCount=" + rightCount + " nextRightCount=" + nextRightCount;
        }
      }

      ordBitSet.clear(0, pointCount);

      // Recurse on left tree:
      build(2*nodeID, leafNodeOffset, leftSlices,
            ordBitSet, out,
            minPackedValue, maxSplitPackedValue,
            splitPackedValues, leafBlockFPs);
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
            splitPackedValues, leafBlockFPs);
      for(int dim=0;dim<numDims;dim++) {
        // Don't destroy the dim we split on because we just re-used what our caller above gave us for that dim:
        if (dim != splitDim) {
          rightSlices[dim].writer.destroy();
        }
      }
    }
  }

  PointWriter getPointWriter(long count) throws IOException {
    if (count <= maxPointsSortInHeap) {
      int size = Math.toIntExact(count);
      return new HeapPointWriter(size, size, packedBytesLength);
    } else {
      return new OfflinePointWriter(tempDir, tempFileNamePrefix, packedBytesLength);
    }
  }
}
