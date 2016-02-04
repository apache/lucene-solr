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
package org.apache.lucene.bkdtree;

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
//   - could we just "use postings" to map leaf -> docIDs?
//   - the polygon query really should be 2-phase
//   - if we could merge trees, we could drop delegating to wrapped DV?
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
//   - geo3d integration should be straightforward?  better accuracy, faster performance for small-poly-with-bbox cases?  right now the poly
//     check is very costly...

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
class BKDTreeWriter {

  // latEnc (int) + lonEnc (int) + ord (long) + docID (int)
  static final int BYTES_PER_DOC = RamUsageEstimator.NUM_BYTES_LONG + 3 * RamUsageEstimator.NUM_BYTES_INT;

  //static final boolean DEBUG = false;

  public static final int DEFAULT_MAX_POINTS_IN_LEAF_NODE = 1024;

  /** This works out to max of ~10 MB peak heap tied up during writing: */
  public static final int DEFAULT_MAX_POINTS_SORT_IN_HEAP = 128*1024;;

  private final byte[] scratchBytes = new byte[BYTES_PER_DOC];
  private final ByteArrayDataOutput scratchBytesOutput = new ByteArrayDataOutput(scratchBytes);

  private OfflineSorter.ByteSequencesWriter writer;
  private GrowingHeapLatLonWriter heapWriter;

  private Path tempInput;
  private final int maxPointsInLeafNode;
  private final int maxPointsSortInHeap;

  private long pointCount;

  public BKDTreeWriter() throws IOException {
    this(DEFAULT_MAX_POINTS_IN_LEAF_NODE, DEFAULT_MAX_POINTS_SORT_IN_HEAP);
  }

  // TODO: instead of maxPointsSortInHeap, change to maxMBHeap ... the mapping is non-obvious:
  public BKDTreeWriter(int maxPointsInLeafNode, int maxPointsSortInHeap) throws IOException {
    verifyParams(maxPointsInLeafNode, maxPointsSortInHeap);
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.maxPointsSortInHeap = maxPointsSortInHeap;

    // We write first maxPointsSortInHeap in heap, then cutover to offline for additional points:
    heapWriter = new GrowingHeapLatLonWriter(maxPointsSortInHeap);
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

  public void add(double lat, double lon, int docID) throws IOException {

    if (validLat(lat) == false) {
      throw new IllegalArgumentException("invalid lat: " + lat);
    }
    if (validLon(lon) == false) {
      throw new IllegalArgumentException("invalid lon: " + lon);
    }

    // Quantize to 32 bit precision, which is plenty: ~.0093 meter precision (longitude) at the equator
    add(encodeLat(lat), encodeLon(lon), docID);
  }

  /** If the current segment has too many points then we switchover to temp files / offline sort. */
  private void switchToOffline() throws IOException {

    // For each .add we just append to this input file, then in .finish we sort this input and resursively build the tree:
    tempInput = Files.createTempFile(OfflineSorter.getDefaultTempDir(), "in", "");
    writer = new OfflineSorter.ByteSequencesWriter(tempInput);
    for(int i=0;i<pointCount;i++) {
      scratchBytesOutput.reset(scratchBytes);
      scratchBytesOutput.writeInt(heapWriter.latEncs[i]);
      scratchBytesOutput.writeInt(heapWriter.lonEncs[i]);
      scratchBytesOutput.writeVInt(heapWriter.docIDs[i]);
      scratchBytesOutput.writeVLong(i);
      // TODO: can/should OfflineSorter optimize the fixed-width case?
      writer.write(scratchBytes, 0, scratchBytes.length);
    }

    heapWriter = null;
  }

  void add(int latEnc, int lonEnc, int docID) throws IOException {
    assert latEnc > Integer.MIN_VALUE;
    assert latEnc < Integer.MAX_VALUE;
    assert lonEnc > Integer.MIN_VALUE;
    assert lonEnc < Integer.MAX_VALUE;

    if (pointCount >= maxPointsSortInHeap) {
      if (writer == null) {
        switchToOffline();
      }
      scratchBytesOutput.reset(scratchBytes);
      scratchBytesOutput.writeInt(latEnc);
      scratchBytesOutput.writeInt(lonEnc);
      scratchBytesOutput.writeVInt(docID);
      scratchBytesOutput.writeVLong(pointCount);
      writer.write(scratchBytes, 0, scratchBytes.length);
    } else {
      // Not too many points added yet, continue using heap:
      heapWriter.append(latEnc, lonEnc, pointCount, docID);
    }

    pointCount++;
  }

  /** Changes incoming {@link ByteSequencesWriter} file to to fixed-width-per-entry file, because we need to be able to slice
   *  as we recurse in {@link #build}. */
  private LatLonWriter convertToFixedWidth(Path in) throws IOException {
    BytesRefBuilder scratch = new BytesRefBuilder();
    scratch.grow(BYTES_PER_DOC);
    BytesRef bytes = scratch.get();
    ByteArrayDataInput dataReader = new ByteArrayDataInput();

    OfflineSorter.ByteSequencesReader reader = null;
    LatLonWriter sortedWriter = null;
    boolean success = false;
    try {
      reader = new OfflineSorter.ByteSequencesReader(in);
      sortedWriter = getWriter(pointCount);
      for (long i=0;i<pointCount;i++) {
        boolean result = reader.read(scratch);
        assert result;
        dataReader.reset(bytes.bytes, bytes.offset, bytes.length);
        int latEnc = dataReader.readInt();
        int lonEnc = dataReader.readInt();
        int docID = dataReader.readVInt();
        long ord = dataReader.readVLong();
        assert docID >= 0: "docID=" + docID;
        assert latEnc > Integer.MIN_VALUE;
        assert latEnc < Integer.MAX_VALUE;
        assert lonEnc > Integer.MIN_VALUE;
        assert lonEnc < Integer.MAX_VALUE;
        sortedWriter.append(latEnc, lonEnc, ord, docID);
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

  private LatLonWriter sort(final boolean lon) throws IOException {
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

          int latEnc = heapWriter.latEncs[i];
          heapWriter.latEncs[i] = heapWriter.latEncs[j];
          heapWriter.latEncs[j] = latEnc;

          int lonEnc = heapWriter.lonEncs[i];
          heapWriter.lonEncs[i] = heapWriter.lonEncs[j];
          heapWriter.lonEncs[j] = lonEnc;
        }

        @Override
        protected int compare(int i, int j) {
          int cmp;
          if (lon) {
            cmp = Integer.compare(heapWriter.lonEncs[i], heapWriter.lonEncs[j]);
          } else {
            cmp = Integer.compare(heapWriter.latEncs[i], heapWriter.latEncs[j]);
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

      HeapLatLonWriter sorted = new HeapLatLonWriter((int) pointCount);
      for(int i=0;i<pointCount;i++) {
        sorted.append(heapWriter.latEncs[i],
                      heapWriter.lonEncs[i],
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
          final int latAEnc = reader.readInt();
          final int lonAEnc = reader.readInt();
          final int docIDA = reader.readVInt();
          final long ordA = reader.readVLong();

          reader.reset(b.bytes, b.offset, b.length);
          final int latBEnc = reader.readInt();
          final int lonBEnc = reader.readInt();
          final int docIDB = reader.readVInt();
          final long ordB = reader.readVLong();

          int cmp;
          if (lon) {
            cmp = Integer.compare(lonAEnc, lonBEnc);
          } else {
            cmp = Integer.compare(latAEnc, latBEnc);
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
        OfflineSorter latSorter = new OfflineSorter(cmp);
        latSorter.sort(tempInput, sorted);
        LatLonWriter writer = convertToFixedWidth(sorted);
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

    //System.out.println("innerNodeCount=" + innerNodeCount);

    if (1+2*innerNodeCount >= Integer.MAX_VALUE) {
      throw new IllegalStateException("too many nodes; increase maxPointsInLeafNode (currently " + maxPointsInLeafNode + ") and reindex");
    }

    innerNodeCount--;

    int numLeaves = (int) (innerNodeCount+1);

    // Indexed by nodeID, but first (root) nodeID is 1
    int[] splitValues = new int[numLeaves];

    // +1 because leaf count is power of 2 (e.g. 8), and innerNodeCount is power of 2 minus 1 (e.g. 7)
    long[] leafBlockFPs = new long[numLeaves];

    // Make sure the math above "worked":
    assert pointCount / splitValues.length <= maxPointsInLeafNode: "pointCount=" + pointCount + " splitValues.length=" + splitValues.length + " maxPointsInLeafNode=" + maxPointsInLeafNode;
    //System.out.println("  avg pointsPerLeaf=" + (pointCount/splitValues.length));

    // Sort all docs once by lat, once by lon:
    LatLonWriter latSortedWriter = null;
    LatLonWriter lonSortedWriter = null;

    boolean success = false;
    try {
      lonSortedWriter = sort(true);
      latSortedWriter = sort(false);
      heapWriter = null;

      build(1, numLeaves, new PathSlice(latSortedWriter, 0, pointCount),
            new PathSlice(lonSortedWriter, 0, pointCount),
            bitSet, out,
            Integer.MIN_VALUE, Integer.MAX_VALUE,
            Integer.MIN_VALUE, Integer.MAX_VALUE,
            //encodeLat(-90.0), encodeLat(Math.nextAfter(90.0, Double.POSITIVE_INFINITY)),
            //encodeLon(-180.0), encodeLon(Math.nextAfter(180.0, Double.POSITIVE_INFINITY)),
            splitValues,
            leafBlockFPs);
      success = true;
    } finally {
      if (success) {
        latSortedWriter.destroy();
        lonSortedWriter.destroy();
        IOUtils.rm(tempInput);
      } else {
        try {
          latSortedWriter.destroy();
        } catch (Throwable t) {
          // Suppress to keep throwing original exc
        }
        try {
          lonSortedWriter.destroy();
        } catch (Throwable t) {
          // Suppress to keep throwing original exc
        }
        IOUtils.deleteFilesIgnoringExceptions(tempInput);
      }
    }

    //System.out.println("Total nodes: " + innerNodeCount);

    // Write index:
    long indexFP = out.getFilePointer();
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
    final LatLonWriter writer;
    final long start;
    final long count;

    public PathSlice(LatLonWriter writer, long start, long count) {
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
  private long markLeftTree(int splitDim, PathSlice source, LongBitSet bitSet, int[] splitValueRet,
                            int minLatEnc, int maxLatEnc, int minLonEnc, int maxLonEnc) throws IOException {

    // This is the initital size of our left tree, but we may lower it below for == case:
    long leftCount = source.count / 2;

    // Read the split value:
    //if (DEBUG) System.out.println("  leftCount=" + leftCount + " vs " + source.count);
    LatLonReader reader = source.writer.getReader(source.start + leftCount);
    boolean success = false;
    int splitValue;
    try {
      boolean result = reader.next();
      assert result;

      int latSplitEnc = reader.latEnc();
      assert latSplitEnc >= minLatEnc && latSplitEnc < maxLatEnc: "latSplitEnc=" + latSplitEnc + " minLatEnc=" + minLatEnc + " maxLatEnc=" + maxLatEnc;

      int lonSplitEnc = reader.lonEnc();
      assert lonSplitEnc >= minLonEnc && lonSplitEnc < maxLonEnc: "lonSplitEnc=" + lonSplitEnc + " minLonEnc=" + minLonEnc + " maxLonEnc=" + maxLonEnc;

      if (splitDim == 0) {
        splitValue = latSplitEnc;
        //if (DEBUG) System.out.println("  splitValue=" + decodeLat(splitValue));
      } else {
        splitValue = lonSplitEnc;
        //if (DEBUG) System.out.println("  splitValue=" + decodeLon(splitValue));
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(reader);
      } else {
        IOUtils.closeWhileHandlingException(reader);
      }
    }

    splitValueRet[0] = splitValue;

    // Mark ords that fall into the left half, and also handle the == boundary case:
    assert bitSet.cardinality() == 0: "cardinality=" + bitSet.cardinality();

    success = false;
    reader = source.writer.getReader(source.start);
    try {
      int lastValue = Integer.MIN_VALUE;
      for (int i=0;i<leftCount;i++) {
        boolean result = reader.next();
        assert result;
        int latEnc = reader.latEnc();
        int lonEnc = reader.lonEnc();

        int value;
        if (splitDim == 0) {
          value = latEnc;
        } else {
          value = lonEnc;
        }

        // Our input source is supposed to be sorted on the incoming dimension:
        assert value >= lastValue;
        lastValue = value;

        if (value == splitValue) {
          // TODO: we could simplify this, by allowing splitValue to be on either side?
          // If we have identical points at the split, we move the count back to before the identical points:
          leftCount = i;
          break;
        }
        assert value < splitValue: "i=" + i + " value=" + value + " vs splitValue=" + splitValue;
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

    return leftCount;
  }

  /** The incoming PathSlice for the dim we will split is already partitioned/sorted. */
  private void build(int nodeID, int leafNodeOffset,
                     PathSlice lastLatSorted,
                     PathSlice lastLonSorted,
                     LongBitSet bitSet,
                     IndexOutput out,
                     int minLatEnc, int maxLatEnc, int minLonEnc, int maxLonEnc,
                     int[] splitValues,
                     long[] leafBlockFPs) throws IOException {

    PathSlice source;
    PathSlice nextSource;

    long latRange = (long) maxLatEnc - (long) minLatEnc;
    long lonRange = (long) maxLonEnc - (long) minLonEnc;

    assert lastLatSorted.count == lastLonSorted.count;

    // Compute which dim we should split on at this level:
    int splitDim;
    if (latRange >= lonRange) {
      // Split by lat:
      splitDim = 0;
      source = lastLatSorted;
      nextSource = lastLonSorted;
    } else {
      // Split by lon:
      splitDim = 1;
      source = lastLonSorted;
      nextSource = lastLatSorted;
    }

    long count = source.count;

    //if (DEBUG) System.out.println("\nBUILD: nodeID=" + nodeID + " leafNodeOffset=" + leafNodeOffset + " splitDim=" + splitDim + "\n  lastLatSorted=" + lastLatSorted + "\n  lastLonSorted=" + lastLonSorted + "\n  count=" + count + " lat=" + decodeLat(minLatEnc) + " TO " + decodeLat(maxLatEnc) + " lon=" + decodeLon(minLonEnc) + " TO " + decodeLon(maxLonEnc));

    if (count == 0) {
      // Dead end in the tree, due to adversary cases, e.g. many identical points:
      if (nodeID < splitValues.length) {
        // Sentinel used to mark that the tree is dead under here:
        splitValues[nodeID] = Integer.MAX_VALUE;
      }
      //if (DEBUG) System.out.println("  dead-end sub-tree");
      return;
    }

    if (nodeID >= leafNodeOffset) {
      // Leaf node: write block
      //if (DEBUG) System.out.println("  leaf");
      assert maxLatEnc > minLatEnc;
      assert maxLonEnc > minLonEnc;

      //System.out.println("\nleaf:\n  lat range: " + ((long) maxLatEnc-minLatEnc));
      //System.out.println("  lon range: " + ((long) maxLonEnc-minLonEnc));

      // Sort by docID in the leaf so we get sequentiality at search time (may not matter?):
      LatLonReader reader = source.writer.getReader(source.start);

      // TODO: we can reuse this
      int[] docIDs = new int[(int) count];

      boolean success = false;
      try {
        for (int i=0;i<source.count;i++) {

          // NOTE: we discard ord at this point; we only needed it temporarily
          // during building to uniquely identify each point to properly handle
          // the multi-valued case (one docID having multiple values):

          // We also discard lat/lon, since at search time, we reside on the
          // wrapped doc values for this:

          boolean result = reader.next();
          assert result;
          docIDs[i] = reader.docID();
        }
        success = true;
      } finally {
        if (success) {
          IOUtils.close(reader);
        } else {
          IOUtils.closeWhileHandlingException(reader);
        }
      }

      Arrays.sort(docIDs);

      // Dedup docIDs: for the multi-valued case where more than one value for the doc
      // wound up in this leaf cell, we only need to store the docID once:
      int lastDocID = -1;
      int uniqueCount = 0;
      for(int i=0;i<docIDs.length;i++) {
        int docID = docIDs[i];
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
      for (int i=0;i<docIDs.length;i++) {
        // Absolute int encode; with "vInt of deltas" encoding, the .kdd size dropped from
        // 697 MB -> 539 MB, but query time for 225 queries went from 1.65 sec -> 2.64 sec.
        // I think if we also indexed prefix terms here we could do less costly compression
        // on those lists:
        int docID = docIDs[i];
        if (docID != lastDocID) {
          out.writeInt(docID);
          lastDocID = docID;
        }
      }
      //long endFP = out.getFilePointer();
      //System.out.println("  bytes/doc: " + ((endFP - startFP) / count));
    } else {
      // Inner node: partition/recurse

      assert nodeID < splitValues.length: "nodeID=" + nodeID + " splitValues.length=" + splitValues.length;

      int[] splitValueArray = new int[1];

      long leftCount = markLeftTree(splitDim, source, bitSet, splitValueArray,
                                    minLatEnc, maxLatEnc, minLonEnc, maxLonEnc);
      int splitValue = splitValueArray[0];

      // TODO: we could save split value in here so we don't have to re-open file later:

      // Partition nextSource into sorted left and right sets, so we can recurse.  This is somewhat hairy: we partition the next lon set
      // according to how we had just partitioned the lat set, and vice/versa:

      LatLonWriter leftWriter = null;
      LatLonWriter rightWriter = null;
      LatLonReader reader = null;

      boolean success = false;

      int nextLeftCount = 0;

      try {
        leftWriter = getWriter(leftCount);
        rightWriter = getWriter(count - leftCount);

        //if (DEBUG) System.out.println("  partition:\n    splitValueEnc=" + splitValue + "\n    " + nextSource + "\n      --> leftSorted=" + leftWriter + "\n      --> rightSorted=" + rightWriter + ")");
        reader = nextSource.writer.getReader(nextSource.start);

        // TODO: we could compute the split value here for each sub-tree and save an O(N) pass on recursion, but makes code hairier and only
        // changes the constant factor of building, not the big-oh:
        for (int i=0;i<count;i++) {
          boolean result = reader.next();
          assert result;
          int latEnc = reader.latEnc();
          int lonEnc = reader.lonEnc();
          long ord = reader.ord();
          int docID = reader.docID();
          assert docID >= 0: "docID=" + docID + " reader=" + reader;
          if (bitSet.get(ord)) {
            if (splitDim == 0) {
              assert latEnc < splitValue: "latEnc=" + latEnc + " splitValue=" + splitValue;
            } else {
              assert lonEnc < splitValue: "lonEnc=" + lonEnc + " splitValue=" + splitValue;
            }
            leftWriter.append(latEnc, lonEnc, ord, docID);
            nextLeftCount++;
          } else {
            if (splitDim == 0) {
              assert latEnc >= splitValue: "latEnc=" + latEnc + " splitValue=" + splitValue;
            } else {
              assert lonEnc >= splitValue: "lonEnc=" + lonEnc + " splitValue=" + splitValue;
            }
            rightWriter.append(latEnc, lonEnc, ord, docID);
          }
        }
        bitSet.clear(0, pointCount);
        success = true;
      } finally {
        if (success) {
          IOUtils.close(reader, leftWriter, rightWriter);
        } else {
          IOUtils.closeWhileHandlingException(reader, leftWriter, rightWriter);
        }
      }

      assert leftCount == nextLeftCount: "leftCount=" + leftCount + " nextLeftCount=" + nextLeftCount;

      success = false;
      try {
        if (splitDim == 0) {
          //if (DEBUG) System.out.println("  recurse left");
          build(2*nodeID, leafNodeOffset,
                new PathSlice(source.writer, source.start, leftCount),
                new PathSlice(leftWriter, 0, leftCount),
                bitSet,
                out,
                minLatEnc, splitValue, minLonEnc, maxLonEnc,
                splitValues, leafBlockFPs);
          leftWriter.destroy();

          //if (DEBUG) System.out.println("  recurse right");
          build(2*nodeID+1, leafNodeOffset,
                new PathSlice(source.writer, source.start+leftCount, count-leftCount),
                new PathSlice(rightWriter, 0, count - leftCount),
                bitSet,
                out,
                splitValue, maxLatEnc, minLonEnc, maxLonEnc,
                splitValues, leafBlockFPs);
          rightWriter.destroy();
        } else {
          //if (DEBUG) System.out.println("  recurse left");
          build(2*nodeID, leafNodeOffset,
                new PathSlice(leftWriter, 0, leftCount),
                new PathSlice(source.writer, source.start, leftCount),
                bitSet,
                out,
                minLatEnc, maxLatEnc, minLonEnc, splitValue,
                splitValues, leafBlockFPs);

          leftWriter.destroy();

          //if (DEBUG) System.out.println("  recurse right");
          build(2*nodeID+1, leafNodeOffset,
                new PathSlice(rightWriter, 0, count-leftCount),
                new PathSlice(source.writer, source.start+leftCount, count-leftCount),    
                bitSet,
                out,
                minLatEnc, maxLatEnc, splitValue, maxLonEnc,
                splitValues, leafBlockFPs);
          rightWriter.destroy();
        }
        success = true;
      } finally {
        if (success == false) {
          try {
            leftWriter.destroy();
          } catch (Throwable t) {
            // Suppress to keep throwing original exc
          }
          try {
            rightWriter.destroy();
          } catch (Throwable t) {
            // Suppress to keep throwing original exc
          }
        }
      }

      splitValues[nodeID] = splitValue;
    }
  }

  LatLonWriter getWriter(long count) throws IOException {
    if (count < maxPointsSortInHeap) {
      return new HeapLatLonWriter((int) count);
    } else {
      return new OfflineLatLonWriter(count);
    }
  }

  // TODO: move/share all this into GeoUtils

  // We allow one iota over the true max:
  static final double MAX_LAT_INCL = Math.nextAfter(90.0D, Double.POSITIVE_INFINITY);
  static final double MAX_LON_INCL = Math.nextAfter(180.0D, Double.POSITIVE_INFINITY);
  static final double MIN_LAT_INCL = -90.0D;
  static final double MIN_LON_INCL = -180.0D;

  static boolean validLat(double lat) {
    return Double.isNaN(lat) == false && lat >= MIN_LAT_INCL && lat <= MAX_LAT_INCL;
  }

  static boolean validLon(double lon) {
    return Double.isNaN(lon) == false && lon >= MIN_LON_INCL && lon <= MAX_LON_INCL;
  }

  private static final int BITS = 32;

  // -3 so valid lat/lon never hit the Integer.MIN_VALUE nor Integer.MAX_VALUE:
  private static final double LON_SCALE = ((0x1L<<BITS)-3)/360.0D;
  private static final double LAT_SCALE = ((0x1L<<BITS)-3)/180.0D;

  /** Max quantization error for both lat and lon when encoding/decoding into 32 bits */
  public static final double TOLERANCE = 1E-7;

  /** Quantizes double (64 bit) latitude into 32 bits */
  static int encodeLat(double lat) {
    assert validLat(lat): "lat=" + lat;
    long x = (long) (lat * LAT_SCALE);
    // We use Integer.MAX_VALUE as a sentinel:
    assert x < Integer.MAX_VALUE: "lat=" + lat + " mapped to Integer.MAX_VALUE + " + (x - Integer.MAX_VALUE);
    assert x > Integer.MIN_VALUE: "lat=" + lat + " mapped to Integer.MIN_VALUE";
    return (int) x;
  }

  /** Quantizes double (64 bit) longitude into 32 bits */
  static int encodeLon(double lon) {
    assert validLon(lon): "lon=" + lon;
    long x = (long) (lon * LON_SCALE);
    // We use Integer.MAX_VALUE as a sentinel:
    assert x < Integer.MAX_VALUE;
    assert x > Integer.MIN_VALUE;
    return (int) x;
  }

  /** Turns quantized value from {@link #encodeLat} back into a double. */
  static double decodeLat(int x) {
    return x / LAT_SCALE;
  }

  /** Turns quantized value from {@link #encodeLon} back into a double. */
  static double decodeLon(int x) {
    return x / LON_SCALE;
  }
}
