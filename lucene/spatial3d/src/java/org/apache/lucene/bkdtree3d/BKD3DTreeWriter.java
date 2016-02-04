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
package org.apache.lucene.bkdtree3d;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.IndexOutput;
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
//   - we could also index "auto-prefix terms" here, and use better compression, and maybe only use for the "fully contained" case so we'd
//     only index docIDs
//   - the index could be efficiently encoded as an FST, so we don't have wasteful
//     (monotonic) long[] leafBlockFPs; or we could use MonotonicLongValues ... but then
//     the index is already plenty small: 60M OSM points --> 1.1 MB with 128 points
//     per leaf, and you can reduce that by putting more points per leaf
//   - we can quantize the split values to 2 bytes (short): http://people.csail.mit.edu/tmertens/papers/qkdtree.pdf
//   - we could use threads while building; the higher nodes are very parallelizable
//   - generalize to N dimenions? i think there are reasonable use cases here, e.g.
//     2 dimensional points to store houses, plus e.g. 3rd dimension for "household income"

/** Recursively builds a BKD tree to assign all incoming points to smaller
 *  and smaller rectangles until the number of points in a given
 *  rectangle is &lt= the <code>maxPointsInLeafNode</code>.  The tree is
 *  fully balanced, which means the leaf nodes will have between 50% and 100% of
 *  the requested <code>maxPointsInLeafNode</code>, except for the adversarial case
 *  of indexing exactly the same point many times.
 *
 *  <p>
 *  See <a href="https://www.cs.duke.edu/~pankaj/publications/papers/bkd-sstd.pdf">this paper</a> for details.
 *
 *  <p>This consumes heap during writing: it allocates a <code>LongBitSet(numPoints)</code>, 
 *  and for any nodes with fewer than <code>maxPointsSortInHeap</code>, it holds
 *  the points in memory as simple java arrays.
 *
 *  <p>
 *  <b>NOTE</b>: This can write at most Integer.MAX_VALUE * <code>maxPointsInLeafNode</code> total points.
 *
 * @lucene.experimental
 *
 * @deprecated Use dimensional values in Lucene 6.0 instead */
@Deprecated
class BKD3DTreeWriter {

  // x (int), y (int), z (int) + ord (long) + docID (int)
  static final int BYTES_PER_DOC = RamUsageEstimator.NUM_BYTES_LONG + 4 * RamUsageEstimator.NUM_BYTES_INT;

  //static final boolean DEBUG = false;

  public static final int DEFAULT_MAX_POINTS_IN_LEAF_NODE = 1024;

  /** This works out to max of ~10 MB peak heap tied up during writing: */
  public static final int DEFAULT_MAX_POINTS_SORT_IN_HEAP = 128*1024;;

  private final byte[] scratchBytes = new byte[BYTES_PER_DOC];
  private final ByteArrayDataOutput scratchBytesOutput = new ByteArrayDataOutput(scratchBytes);

  private OfflineSorter.ByteSequencesWriter writer;
  private GrowingHeapWriter heapWriter;

  private Path tempInput;
  private final int maxPointsInLeafNode;
  private final int maxPointsSortInHeap;

  private long pointCount;

  private final int[] scratchDocIDs;

  public BKD3DTreeWriter() throws IOException {
    this(DEFAULT_MAX_POINTS_IN_LEAF_NODE, DEFAULT_MAX_POINTS_SORT_IN_HEAP);
  }

  // TODO: instead of maxPointsSortInHeap, change to maxMBHeap ... the mapping is non-obvious:
  public BKD3DTreeWriter(int maxPointsInLeafNode, int maxPointsSortInHeap) throws IOException {
    verifyParams(maxPointsInLeafNode, maxPointsSortInHeap);
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.maxPointsSortInHeap = maxPointsSortInHeap;
    scratchDocIDs = new int[maxPointsInLeafNode];

    // We write first maxPointsSortInHeap in heap, then cutover to offline for additional points:
    heapWriter = new GrowingHeapWriter(maxPointsSortInHeap);
  }

  public static void verifyParams(int maxPointsInLeafNode, int maxPointsSortInHeap) {
    if (maxPointsInLeafNode <= 0) {
      throw new IllegalArgumentException("maxPointsInLeafNode must be > 0; got " + maxPointsInLeafNode);
    }
    if (maxPointsInLeafNode > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalArgumentException("maxPointsInLeafNode must be <= ArrayUtil.MAX_ARRAY_LENGTH (= " + ArrayUtil.MAX_ARRAY_LENGTH + "); got " + maxPointsInLeafNode);
    }
    if (maxPointsSortInHeap < maxPointsInLeafNode) {
      throw new IllegalArgumentException("maxPointsSortInHeap must be >= maxPointsInLeafNode; got " + maxPointsSortInHeap + " vs maxPointsInLeafNode="+ maxPointsInLeafNode);
    }
    if (maxPointsSortInHeap > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalArgumentException("maxPointsSortInHeap must be <= ArrayUtil.MAX_ARRAY_LENGTH (= " + ArrayUtil.MAX_ARRAY_LENGTH + "); got " + maxPointsSortInHeap);
    }
  }

  /** If the current segment has too many points then we switchover to temp files / offline sort. */
  private void switchToOffline() throws IOException {

    // For each .add we just append to this input file, then in .finish we sort this input and resursively build the tree:
    tempInput = Files.createTempFile(OfflineSorter.getDefaultTempDir(), "in", "");
    writer = new OfflineSorter.ByteSequencesWriter(tempInput);
    for(int i=0;i<pointCount;i++) {
      scratchBytesOutput.reset(scratchBytes);
      scratchBytesOutput.writeInt(heapWriter.xs[i]);
      scratchBytesOutput.writeInt(heapWriter.ys[i]);
      scratchBytesOutput.writeInt(heapWriter.zs[i]);
      scratchBytesOutput.writeVInt(heapWriter.docIDs[i]);
      scratchBytesOutput.writeVLong(i);
      // TODO: can/should OfflineSorter optimize the fixed-width case?
      writer.write(scratchBytes, 0, scratchBytes.length);
    }

    heapWriter = null;
  }

  public void add(int x, int y, int z, int docID) throws IOException {

    if (pointCount >= maxPointsSortInHeap) {
      if (writer == null) {
        switchToOffline();
      }
      scratchBytesOutput.reset(scratchBytes);
      scratchBytesOutput.writeInt(x);
      scratchBytesOutput.writeInt(y);
      scratchBytesOutput.writeInt(z);
      scratchBytesOutput.writeVInt(docID);
      scratchBytesOutput.writeVLong(pointCount);
      writer.write(scratchBytes, 0, scratchBytes.length);
    } else {
      // Not too many points added yet, continue using heap:
      heapWriter.append(x, y, z, pointCount, docID);
    }

    pointCount++;
  }

  /** Changes incoming {@link ByteSequencesWriter} file to to fixed-width-per-entry file, because we need to be able to slice
   *  as we recurse in {@link #build}. */
  private Writer convertToFixedWidth(Path in) throws IOException {
    BytesRefBuilder scratch = new BytesRefBuilder();
    scratch.grow(BYTES_PER_DOC);
    BytesRef bytes = scratch.get();
    ByteArrayDataInput dataReader = new ByteArrayDataInput();

    OfflineSorter.ByteSequencesReader reader = null;
    Writer sortedWriter = null;
    boolean success = false;
    try {
      reader = new OfflineSorter.ByteSequencesReader(in);
      sortedWriter = getWriter(pointCount);
      for (long i=0;i<pointCount;i++) {
        boolean result = reader.read(scratch);
        assert result;
        dataReader.reset(bytes.bytes, bytes.offset, bytes.length);
        int x = dataReader.readInt();
        int y = dataReader.readInt();
        int z = dataReader.readInt();
        int docID = dataReader.readVInt();
        long ord = dataReader.readVLong();
        assert docID >= 0: "docID=" + docID;
        sortedWriter.append(x, y, z, ord, docID);
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(sortedWriter, reader);
      } else {
        IOUtils.closeWhileHandlingException(sortedWriter, reader);
        try {
          sortedWriter.destroy();
        } catch (Throwable t) {
          // Suppress to keep throwing original exc
        }
      }
    }

    return sortedWriter;
  }

  /** dim: 0=x, 1=y, 2=z */
  private Writer sort(final int dim) throws IOException {
    if (heapWriter != null) {

      assert pointCount < Integer.MAX_VALUE;

      // All buffered points are still in heap
      new InPlaceMergeSorter() {
        @Override
        protected void swap(int i, int j) {
          int docID = heapWriter.docIDs[i];
          heapWriter.docIDs[i] = heapWriter.docIDs[j];
          heapWriter.docIDs[j] = docID;

          long ord = heapWriter.ords[i];
          heapWriter.ords[i] = heapWriter.ords[j];
          heapWriter.ords[j] = ord;

          int x = heapWriter.xs[i];
          heapWriter.xs[i] = heapWriter.xs[j];
          heapWriter.xs[j] = x;

          int y = heapWriter.ys[i];
          heapWriter.ys[i] = heapWriter.ys[j];
          heapWriter.ys[j] = y;

          int z = heapWriter.zs[i];
          heapWriter.zs[i] = heapWriter.zs[j];
          heapWriter.zs[j] = z;
        }

        @Override
        protected int compare(int i, int j) {
          int cmp;
          if (dim == 0) {
            cmp = Integer.compare(heapWriter.xs[i], heapWriter.xs[j]);
          } else if (dim == 1) {
            cmp = Integer.compare(heapWriter.ys[i], heapWriter.ys[j]);
          } else {
            cmp = Integer.compare(heapWriter.zs[i], heapWriter.zs[j]);
          }
          if (cmp != 0) {
            return cmp;
          }

          // Tie-break
          cmp = Integer.compare(heapWriter.docIDs[i], heapWriter.docIDs[j]);
          if (cmp != 0) {
            return cmp;
          }

          return Long.compare(heapWriter.ords[i], heapWriter.ords[j]);
        }
      }.sort(0, (int) pointCount);

      HeapWriter sorted = new HeapWriter((int) pointCount);
      //System.out.println("sorted dim=" + dim);
      for(int i=0;i<pointCount;i++) {
        /*
        System.out.println("  docID=" + heapWriter.docIDs[i] + 
                           " x=" + heapWriter.xs[i] +
                           " y=" + heapWriter.ys[i] +
                           " z=" + heapWriter.zs[i]);
        */
        sorted.append(heapWriter.xs[i],
                      heapWriter.ys[i],
                      heapWriter.zs[i],
                      heapWriter.ords[i],
                      heapWriter.docIDs[i]);
      }
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
          final int xa = reader.readInt();
          final int ya = reader.readInt();
          final int za = reader.readInt();
          final int docIDA = reader.readVInt();
          final long ordA = reader.readVLong();

          reader.reset(b.bytes, b.offset, b.length);
          final int xb = reader.readInt();
          final int yb = reader.readInt();
          final int zb = reader.readInt();
          final int docIDB = reader.readVInt();
          final long ordB = reader.readVLong();

          int cmp;
          if (dim == 0) {
            cmp = Integer.compare(xa, xb);
          } else if (dim == 1) {
            cmp = Integer.compare(ya, yb);
          } else {
            cmp = Integer.compare(za, zb);
          }
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
        Writer writer = convertToFixedWidth(sorted);
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
    //System.out.println("\nBKDTreeWriter.finish pointCount=" + pointCount + " out=" + out + " heapWriter=" + heapWriter + " maxPointsInLeafNode=" + maxPointsInLeafNode);

    if (writer != null) {
      writer.close();
    }

    LongBitSet bitSet = new LongBitSet(pointCount);

    long countPerLeaf = pointCount;
    long innerNodeCount = 1;

    while (countPerLeaf > maxPointsInLeafNode) {
      countPerLeaf = (countPerLeaf+1)/2;
      innerNodeCount *= 2;
    }

    //System.out.println("innerNodeCount=" + innerNodeCount + " countPerLeaf=" + countPerLeaf);

    if (1+2*innerNodeCount >= Integer.MAX_VALUE) {
      throw new IllegalStateException("too many nodes; increase maxPointsInLeafNode (currently " + maxPointsInLeafNode + ") and reindex");
    }

    innerNodeCount--;

    int numLeaves = (int) (innerNodeCount+1);
    //System.out.println("  numLeaves=" + numLeaves);

    // Indexed by nodeID, but first (root) nodeID is 1
    int[] splitValues = new int[numLeaves];

    // +1 because leaf count is power of 2 (e.g. 8), and innerNodeCount is power of 2 minus 1 (e.g. 7)
    long[] leafBlockFPs = new long[numLeaves];

    // Make sure the math above "worked":
    assert pointCount / splitValues.length <= maxPointsInLeafNode: "pointCount=" + pointCount + " splitValues.length=" + splitValues.length + " maxPointsInLeafNode=" + maxPointsInLeafNode;
    //System.out.println("  avg pointsPerLeaf=" + (pointCount/splitValues.length));

    // Sort all docs once by x, once by y, once by z:
    Writer xSortedWriter = null;
    Writer ySortedWriter = null;
    Writer zSortedWriter = null;

    boolean success = false;
    try {
      xSortedWriter = sort(0);
      ySortedWriter = sort(1);
      zSortedWriter = sort(2);
      heapWriter = null;

      build(1, numLeaves,
            new PathSlice(xSortedWriter, 0, pointCount),
            new PathSlice(ySortedWriter, 0, pointCount),
            new PathSlice(zSortedWriter, 0, pointCount),
            bitSet, out,
            Integer.MIN_VALUE, Integer.MAX_VALUE,
            Integer.MIN_VALUE, Integer.MAX_VALUE,
            Integer.MIN_VALUE, Integer.MAX_VALUE,
            splitValues,
            leafBlockFPs);
      success = true;
    } finally {
      if (success) {
        xSortedWriter.destroy();
        ySortedWriter.destroy();
        zSortedWriter.destroy();
        IOUtils.rm(tempInput);
      } else {
        try {
          xSortedWriter.destroy();
        } catch (Throwable t) {
          // Suppress to keep throwing original exc
        }
        try {
          ySortedWriter.destroy();
        } catch (Throwable t) {
          // Suppress to keep throwing original exc
        }
        try {
          zSortedWriter.destroy();
        } catch (Throwable t) {
          // Suppress to keep throwing original exc
        }
        IOUtils.deleteFilesIgnoringExceptions(tempInput);
      }
    }

    //System.out.println("Total nodes: " + innerNodeCount);

    // Write index:
    long indexFP = out.getFilePointer();
    //System.out.println("indexFP=" + indexFP);
    out.writeVInt(numLeaves);

    // NOTE: splitValues[0] is unused, because nodeID is 1-based:
    for (int i=0;i<splitValues.length;i++) {
      out.writeInt(splitValues[i]);
    }
    for (int i=0;i<leafBlockFPs.length;i++) {
      out.writeVLong(leafBlockFPs[i]);
    }

    return indexFP;
  }

  /** Sliced reference to points in an OfflineSorter.ByteSequencesWriter file. */
  private static final class PathSlice {
    final Writer writer;
    final long start;
    final long count;

    public PathSlice(Writer writer, long start, long count) {
      this.writer = writer;
      this.start = start;
      this.count = count;
    }

    @Override
    public String toString() {
      return "PathSlice(start=" + start + " count=" + count + " writer=" + writer + ")";
    }
  }

  /** Marks bits for the ords (points) that belong in the left sub tree. */
  private int markLeftTree(int splitDim, PathSlice source, LongBitSet bitSet,
                           int minX, int maxX,
                           int minY, int maxY,
                           int minZ, int maxZ) throws IOException {

    // This is the size of our left tree
    long leftCount = source.count / 2;

    // Read the split value:
    //if (DEBUG) System.out.println("  leftCount=" + leftCount + " vs " + source.count);
    Reader reader = source.writer.getReader(source.start + leftCount);
    boolean success = false;
    int splitValue;
    try {
      boolean result = reader.next();
      assert result;

      int x = reader.x();
      assert x >= minX && x <= maxX: "x=" + x + " minX=" + minX + " maxX=" + maxX;

      int y = reader.y();
      assert y >= minY && y <= maxY: "y=" + y + " minY=" + minY + " maxY=" + maxY;

      int z = reader.z();
      assert z >= minZ && z <= maxZ: "z=" + z + " minZ=" + minZ + " maxZ=" + maxZ;

      if (splitDim == 0) {
        splitValue = x;
      } else if (splitDim == 1) {
        splitValue = y;
      } else {
        splitValue = z;
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(reader);
      } else {
        IOUtils.closeWhileHandlingException(reader);
      }
    }

    // Mark ords that fall into the left half, and also handle the == boundary case:
    assert bitSet.cardinality() == 0: "cardinality=" + bitSet.cardinality();

    success = false;
    reader = source.writer.getReader(source.start);
    try {
      int lastValue = Integer.MIN_VALUE;
      for (int i=0;i<leftCount;i++) {
        boolean result = reader.next();
        assert result;
        int x = reader.x();
        int y = reader.y();
        int z = reader.z();

        int value;
        if (splitDim == 0) {
          value = x;
        } else if (splitDim == 1) {
          value = y;
        } else {
          value = z;
        }

        // Our input source is supposed to be sorted on the incoming dimension:
        assert value >= lastValue;
        lastValue = value;

        assert value <= splitValue: "i=" + i + " value=" + value + " vs splitValue=" + splitValue;
        long ord = reader.ord();
        int docID = reader.docID();
        assert docID >= 0: "docID=" + docID + " reader=" + reader;

        // We should never see dup ords:
        assert bitSet.get(ord) == false;
        bitSet.set(ord);
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(reader);
      } else {
        IOUtils.closeWhileHandlingException(reader);
      }
    }

    assert leftCount == bitSet.cardinality(): "leftCount=" + leftCount + " cardinality=" + bitSet.cardinality();

    return splitValue;
  }

  // Split on the dim with the largest range:
  static int getSplitDim(int minX, int maxX, int minY, int maxY, int minZ, int maxZ) {
    long xRange = (long) maxX - (long) minX;
    long yRange = (long) maxY - (long) minY;
    long zRange = (long) maxZ - (long) minZ;

    if (xRange > yRange) {
      if (xRange > zRange) {
        return 0;
      } else {
        return 2;
      }
    } else if (yRange > zRange) {
      return 1;
    } else {
      return 2;
    }
  }

  /** The incoming PathSlice for the dim we will split is already partitioned/sorted. */
  private void build(int nodeID, int leafNodeOffset,
                     PathSlice lastXSorted,
                     PathSlice lastYSorted,
                     PathSlice lastZSorted,
                     LongBitSet bitSet,
                     IndexOutput out,
                     int minX, int maxX,
                     int minY, int maxY,
                     int minZ, int maxZ,
                     int[] splitValues,
                     long[] leafBlockFPs) throws IOException {

    long count = lastXSorted.count;
    assert count > 0;
    assert count <= ArrayUtil.MAX_ARRAY_LENGTH;

    assert count == lastYSorted.count;
    assert count == lastZSorted.count;

    //if (DEBUG) System.out.println("\nBUILD: nodeID=" + nodeID + " leafNodeOffset=" + leafNodeOffset + "\n  lastXSorted=" + lastXSorted + "\n  lastYSorted=" + lastYSorted + "\n  lastZSorted=" + lastZSorted + "\n  count=" + lastXSorted.count + " x=" + minX + " TO " + maxX + " y=" + minY + " TO " + maxY + " z=" + minZ + " TO " + maxZ);

    if (nodeID >= leafNodeOffset) {
      // Leaf node: write block
      //if (DEBUG) System.out.println("  leaf");
      assert maxX >= minX;
      assert maxY >= minY;
      assert maxZ >= minZ;

      //System.out.println("\nleaf:\n  lat range: " + ((long) maxLatEnc-minLatEnc));
      //System.out.println("  lon range: " + ((long) maxLonEnc-minLonEnc));

      // Sort by docID in the leaf so we get sequentiality at search time (may not matter?):
      Reader reader = lastXSorted.writer.getReader(lastXSorted.start);

      assert count <= scratchDocIDs.length: "count=" + count + " scratchDocIDs.length=" + scratchDocIDs.length;

      boolean success = false;
      try {
        for (int i=0;i<count;i++) {

          // NOTE: we discard ord at this point; we only needed it temporarily
          // during building to uniquely identify each point to properly handle
          // the multi-valued case (one docID having multiple values):

          // We also discard lat/lon, since at search time, we reside on the
          // wrapped doc values for this:

          boolean result = reader.next();
          assert result;
          scratchDocIDs[i] = reader.docID();
        }
        success = true;
      } finally {
        if (success) {
          IOUtils.close(reader);
        } else {
          IOUtils.closeWhileHandlingException(reader);
        }
      }

      Arrays.sort(scratchDocIDs, 0, (int) count);

      // Dedup docIDs: for the multi-valued case where more than one value for the doc
      // wound up in this leaf cell, we only need to store the docID once:
      int lastDocID = -1;
      int uniqueCount = 0;
      for(int i=0;i<count;i++) {
        int docID = scratchDocIDs[i];
        if (docID != lastDocID) {
          uniqueCount++;
          lastDocID = docID;
        }
      }
      assert uniqueCount <= count;

      long startFP = out.getFilePointer();
      out.writeVInt(uniqueCount);

      // Save the block file pointer:
      leafBlockFPs[nodeID - leafNodeOffset] = startFP;
      //System.out.println("    leafFP=" + startFP);

      lastDocID = -1;
      for (int i=0;i<count;i++) {
        // Absolute int encode; with "vInt of deltas" encoding, the .kdd size dropped from
        // 697 MB -> 539 MB, but query time for 225 queries went from 1.65 sec -> 2.64 sec.
        // I think if we also indexed prefix terms here we could do less costly compression
        // on those lists:
        int docID = scratchDocIDs[i];
        if (docID != lastDocID) {
          out.writeInt(docID);
          //System.out.println("  write docID=" + docID);
          lastDocID = docID;
        }
      }
      //long endFP = out.getFilePointer();
      //System.out.println("  bytes/doc: " + ((endFP - startFP) / count));
    } else {

      int splitDim = getSplitDim(minX, maxX, minY, maxY, minZ, maxZ);
      //System.out.println("  splitDim=" + splitDim);

      PathSlice source;

      if (splitDim == 0) {
        source = lastXSorted;
      } else if (splitDim == 1) {
        source = lastYSorted;
      } else {
        source = lastZSorted;
      }

      // We let ties go to either side, so we should never get down to count == 0, even
      // in adversarial case (all values are the same):
      assert count > 0;

      // Inner node: partition/recurse
      //if (DEBUG) System.out.println("  non-leaf");

      assert nodeID < splitValues.length: "nodeID=" + nodeID + " splitValues.length=" + splitValues.length;

      int splitValue = markLeftTree(splitDim, source, bitSet,
                                    minX, maxX,
                                    minY, maxY,
                                    minZ, maxZ);
      long leftCount = count/2;

      // TODO: we could save split value in here so we don't have to re-open file later:

      // Partition the other (not split) dims into sorted left and right sets, so we can recurse.
      // This is somewhat hairy: we partition the next X, Y set according to how we had just
      // partitioned the Z set, etc.

      Writer[] leftWriters = new Writer[3];
      Writer[] rightWriters = new Writer[3];

      for(int dim=0;dim<3;dim++) {
        if (dim == splitDim) {
          continue;
        }

        Writer leftWriter = null;
        Writer rightWriter = null;
        Reader reader = null;

        boolean success = false;

        int nextLeftCount = 0;

        PathSlice nextSource;
        if (dim == 0) {
          nextSource = lastXSorted;
        } else if (dim == 1) {
          nextSource = lastYSorted;
        } else {
          nextSource = lastZSorted;
        }

        try {
          leftWriter = getWriter(leftCount);
          rightWriter = getWriter(nextSource.count - leftCount);

          assert nextSource.count == count;
          reader = nextSource.writer.getReader(nextSource.start);

          // TODO: we could compute the split value here for each sub-tree and save an O(N) pass on recursion, but makes code hairier and only
          // changes the constant factor of building, not the big-oh:
          for (int i=0;i<count;i++) {
            boolean result = reader.next();
            assert result;
            int x = reader.x();
            int y = reader.y();
            int z = reader.z();
            long ord = reader.ord();
            int docID = reader.docID();
            assert docID >= 0: "docID=" + docID + " reader=" + reader;
            //System.out.println("  i=" + i + " x=" + x + " ord=" + ord + " docID=" + docID);
            if (bitSet.get(ord)) {
              if (splitDim == 0) {
                assert x <= splitValue: "x=" + x + " splitValue=" + splitValue;
              } else if (splitDim == 1) {
                assert y <= splitValue: "y=" + y + " splitValue=" + splitValue;
              } else {
                assert z <= splitValue: "z=" + z + " splitValue=" + splitValue;
              }
              leftWriter.append(x, y, z, ord, docID);
              nextLeftCount++;
            } else {
              if (splitDim == 0) {
                assert x >= splitValue: "x=" + x + " splitValue=" + splitValue;
              } else if (splitDim == 1) {
                assert y >= splitValue: "y=" + y + " splitValue=" + splitValue;
              } else {
                assert z >= splitValue: "z=" + z + " splitValue=" + splitValue;
              }
              rightWriter.append(x, y, z, ord, docID);
            }
          }
          success = true;
        } finally {
          if (success) {
            IOUtils.close(reader, leftWriter, rightWriter);
          } else {
            IOUtils.closeWhileHandlingException(reader, leftWriter, rightWriter);
          }
        }

        assert leftCount == nextLeftCount: "leftCount=" + leftCount + " nextLeftCount=" + nextLeftCount;
        leftWriters[dim] = leftWriter;
        rightWriters[dim] = rightWriter;
      }
      bitSet.clear(0, pointCount);

      long rightCount = count - leftCount;

      boolean success = false;
      try {
        if (splitDim == 0) {
          build(2*nodeID, leafNodeOffset,
                new PathSlice(source.writer, source.start, leftCount),
                new PathSlice(leftWriters[1], 0, leftCount),
                new PathSlice(leftWriters[2], 0, leftCount),
                bitSet,
                out,
                minX, splitValue,
                minY, maxY,
                minZ, maxZ,
                splitValues, leafBlockFPs);
          leftWriters[1].destroy();
          leftWriters[2].destroy();

          build(2*nodeID+1, leafNodeOffset,
                new PathSlice(source.writer, source.start+leftCount, rightCount),
                new PathSlice(rightWriters[1], 0, rightCount),
                new PathSlice(rightWriters[2], 0, rightCount),
                bitSet,
                out,
                splitValue, maxX,
                minY, maxY,
                minZ, maxZ,
                splitValues, leafBlockFPs);
          rightWriters[1].destroy();
          rightWriters[2].destroy();
        } else if (splitDim == 1) {
          build(2*nodeID, leafNodeOffset,
                new PathSlice(leftWriters[0], 0, leftCount),
                new PathSlice(source.writer, source.start, leftCount),
                new PathSlice(leftWriters[2], 0, leftCount),
                bitSet,
                out,
                minX, maxX,
                minY, splitValue,
                minZ, maxZ,
                splitValues, leafBlockFPs);
          leftWriters[0].destroy();
          leftWriters[2].destroy();

          build(2*nodeID+1, leafNodeOffset,
                new PathSlice(rightWriters[0], 0, rightCount),
                new PathSlice(source.writer, source.start+leftCount, rightCount),    
                new PathSlice(rightWriters[2], 0, rightCount),
                bitSet,
                out,
                minX, maxX,
                splitValue, maxY,
                minZ, maxZ,
                splitValues, leafBlockFPs);
          rightWriters[0].destroy();
          rightWriters[2].destroy();
        } else {
          build(2*nodeID, leafNodeOffset,
                new PathSlice(leftWriters[0], 0, leftCount),
                new PathSlice(leftWriters[1], 0, leftCount),
                new PathSlice(source.writer, source.start, leftCount),
                bitSet,
                out,
                minX, maxX,
                minY, maxY,
                minZ, splitValue,
                splitValues, leafBlockFPs);
          leftWriters[0].destroy();
          leftWriters[1].destroy();

          build(2*nodeID+1, leafNodeOffset,
                new PathSlice(rightWriters[0], 0, rightCount),
                new PathSlice(rightWriters[1], 0, rightCount),
                new PathSlice(source.writer, source.start+leftCount, rightCount),    
                bitSet,
                out,
                minX, maxX,
                minY, maxY,
                splitValue, maxZ,
                splitValues, leafBlockFPs);
          rightWriters[0].destroy();
          rightWriters[1].destroy();
        }
        success = true;
      } finally {
        if (success == false) {
          for(Writer writer : leftWriters) {
            if (writer != null) {
              try {
                writer.destroy();
              } catch (Throwable t) {
                // Suppress to keep throwing original exc
              }
            }
          }
          for(Writer writer : rightWriters) {
            if (writer != null) {
              try {
                writer.destroy();
              } catch (Throwable t) {
                // Suppress to keep throwing original exc
              }
            }
          }
        }
      }

      splitValues[nodeID] = splitValue;
    }
  }

  Writer getWriter(long count) throws IOException {
    if (count < maxPointsSortInHeap) {
      return new HeapWriter((int) count);
    } else {
      return new OfflineWriter(count);
    }
  }
}
