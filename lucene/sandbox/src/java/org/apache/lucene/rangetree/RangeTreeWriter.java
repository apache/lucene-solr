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
package org.apache.lucene.rangetree;

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
import org.apache.lucene.util.OfflineSorter.ByteSequencesWriter;
import org.apache.lucene.util.OfflineSorter;
import org.apache.lucene.util.RamUsageEstimator;

// TODO
//   - could we just "use postings" to map leaf -> docIDs?
//   - we could also index "auto-prefix terms" here, and use better compression
//   - the index could be efficiently encoded as an FST, so we don't have wasteful
//     (monotonic) long[] leafBlockFPs; or we could use MonotonicLongValues ... but then
//     the index is already plenty small: 60M OSM points --> 1.1 MB with 128 points
//     per leaf, and you can reduce that by putting more points per leaf
//   - we can quantize the split values to 2 bytes (short): http://people.csail.mit.edu/tmertens/papers/qkdtree.pdf

/** Recursively builds a 1d BKD tree to assign all incoming {@code long} values to smaller
 *  and smaller ranges until the number of points in a given
 *  range is &lt= the <code>maxPointsInLeafNode</code>.  The tree is
 *  fully balanced, which means the leaf nodes will have between 50% and 100% of
 *  the requested <code>maxPointsInLeafNode</code>, except for the adversarial case
 *  of indexing exactly the same value many times.
 *
 *  <p>
 *  See <a href="https://www.cs.duke.edu/~pankaj/publications/papers/bkd-sstd.pdf">this paper</a> for details.
 *
 *  <p>This consumes heap during writing: for any nodes with fewer than <code>maxPointsSortInHeap</code>, it holds
 *  the points in memory as simple java arrays.
 *
 *  <p>
 *  <b>NOTE</b>: This can write at most Integer.MAX_VALUE * <code>maxPointsInLeafNode</code> total values,
 *  which should be plenty since a Lucene index can have at most Integer.MAX_VALUE-1 documents.
 *
 * @lucene.experimental */

class RangeTreeWriter {

  // value (long) + ord (long) + docID (int)
  static final int BYTES_PER_DOC = 2 * RamUsageEstimator.NUM_BYTES_LONG + RamUsageEstimator.NUM_BYTES_INT;

  public static final int DEFAULT_MAX_VALUES_IN_LEAF_NODE = 1024;

  /** This works out to max of ~10 MB peak heap tied up during writing: */
  public static final int DEFAULT_MAX_VALUES_SORT_IN_HEAP = 128*1024;;

  private final byte[] scratchBytes = new byte[BYTES_PER_DOC];
  private final ByteArrayDataOutput scratchBytesOutput = new ByteArrayDataOutput(scratchBytes);

  private OfflineSorter.ByteSequencesWriter writer;
  private GrowingHeapSliceWriter heapWriter;

  private Path tempInput;
  private final int maxValuesInLeafNode;
  private final int maxValuesSortInHeap;

  private long valueCount;
  private long globalMinValue = Long.MAX_VALUE;
  private long globalMaxValue = Long.MIN_VALUE;

  public RangeTreeWriter() throws IOException {
    this(DEFAULT_MAX_VALUES_IN_LEAF_NODE, DEFAULT_MAX_VALUES_SORT_IN_HEAP);
  }

  // TODO: instead of maxValuesSortInHeap, change to maxMBHeap ... the mapping is non-obvious:
  public RangeTreeWriter(int maxValuesInLeafNode, int maxValuesSortInHeap) throws IOException {
    verifyParams(maxValuesInLeafNode, maxValuesSortInHeap);
    this.maxValuesInLeafNode = maxValuesInLeafNode;
    this.maxValuesSortInHeap = maxValuesSortInHeap;

    // We write first maxValuesSortInHeap in heap, then cutover to offline for additional points:
    heapWriter = new GrowingHeapSliceWriter(maxValuesSortInHeap);
  }

  public static void verifyParams(int maxValuesInLeafNode, int maxValuesSortInHeap) {
    if (maxValuesInLeafNode <= 0) {
      throw new IllegalArgumentException("maxValuesInLeafNode must be > 0; got " + maxValuesInLeafNode);
    }
    if (maxValuesInLeafNode > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalArgumentException("maxValuesInLeafNode must be <= ArrayUtil.MAX_ARRAY_LENGTH (= " + ArrayUtil.MAX_ARRAY_LENGTH + "); got " + maxValuesInLeafNode);
    }
    if (maxValuesSortInHeap < maxValuesInLeafNode) {
      throw new IllegalArgumentException("maxValuesSortInHeap must be >= maxValuesInLeafNode; got " + maxValuesSortInHeap + " vs maxValuesInLeafNode="+ maxValuesInLeafNode);
    }
    if (maxValuesSortInHeap > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalArgumentException("maxValuesSortInHeap must be <= ArrayUtil.MAX_ARRAY_LENGTH (= " + ArrayUtil.MAX_ARRAY_LENGTH + "); got " + maxValuesSortInHeap);
    }
  }

  /** If the current segment has too many points then we switchover to temp files / offline sort. */
  private void switchToOffline() throws IOException {

    // For each .add we just append to this input file, then in .finish we sort this input and resursively build the tree:
    tempInput = Files.createTempFile(OfflineSorter.getDefaultTempDir(), "in", "");
    writer = new OfflineSorter.ByteSequencesWriter(tempInput);
    for(int i=0;i<valueCount;i++) {
      scratchBytesOutput.reset(scratchBytes);
      scratchBytesOutput.writeLong(heapWriter.values[i]);
      scratchBytesOutput.writeVInt(heapWriter.docIDs[i]);
      scratchBytesOutput.writeVLong(i);
      // TODO: can/should OfflineSorter optimize the fixed-width case?
      writer.write(scratchBytes, 0, scratchBytes.length);
    }

    heapWriter = null;
  }

  void add(long value, int docID) throws IOException {
    if (valueCount >= maxValuesSortInHeap) {
      if (writer == null) {
        switchToOffline();
      }
      scratchBytesOutput.reset(scratchBytes);
      scratchBytesOutput.writeLong(value);
      scratchBytesOutput.writeVInt(docID);
      scratchBytesOutput.writeVLong(valueCount);
      writer.write(scratchBytes, 0, scratchBytes.length);
    } else {
      // Not too many points added yet, continue using heap:
      heapWriter.append(value, valueCount, docID);
    }

    valueCount++;
    globalMaxValue = Math.max(value, globalMaxValue);
    globalMinValue = Math.min(value, globalMinValue);
  }

  /** Changes incoming {@link ByteSequencesWriter} file to to fixed-width-per-entry file, because we need to be able to slice
   *  as we recurse in {@link #build}. */
  private SliceWriter convertToFixedWidth(Path in) throws IOException {
    BytesRefBuilder scratch = new BytesRefBuilder();
    scratch.grow(BYTES_PER_DOC);
    BytesRef bytes = scratch.get();
    ByteArrayDataInput dataReader = new ByteArrayDataInput();

    OfflineSorter.ByteSequencesReader reader = null;
    SliceWriter sortedWriter = null;
    boolean success = false;
    try {
      reader = new OfflineSorter.ByteSequencesReader(in);
      sortedWriter = getWriter(valueCount);
      for (long i=0;i<valueCount;i++) {
        boolean result = reader.read(scratch);
        assert result;
        dataReader.reset(bytes.bytes, bytes.offset, bytes.length);
        long value = dataReader.readLong();
        int docID = dataReader.readVInt();
        assert docID >= 0: "docID=" + docID;
        long ord = dataReader.readVLong();
        sortedWriter.append(value, ord, docID);
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

  private SliceWriter sort() throws IOException {
    if (heapWriter != null) {

      assert valueCount < Integer.MAX_VALUE;

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

          long value = heapWriter.values[i];
          heapWriter.values[i] = heapWriter.values[j];
          heapWriter.values[j] = value;
        }

        @Override
        protected int compare(int i, int j) {
          int cmp = Long.compare(heapWriter.values[i], heapWriter.values[j]);
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
      }.sort(0, (int) valueCount);

      HeapSliceWriter sorted = new HeapSliceWriter((int) valueCount);
      for(int i=0;i<valueCount;i++) {
        sorted.append(heapWriter.values[i],
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
          final long valueA = reader.readLong();
          final int docIDA = reader.readVInt();
          final long ordA = reader.readVLong();

          reader.reset(b.bytes, b.offset, b.length);
          final long valueB = reader.readLong();
          final int docIDB = reader.readVInt();
          final long ordB = reader.readVLong();

          int cmp = Long.compare(valueA, valueB);
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
        SliceWriter writer = convertToFixedWidth(sorted);
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

  /** Writes the 1d BKD tree to the provided {@link IndexOutput} and returns the file offset where index was written. */
  public long finish(IndexOutput out) throws IOException {

    if (writer != null) {
      writer.close();
    }

    if (valueCount == 0) {
      throw new IllegalStateException("at least one value must be indexed");
    }

    // TODO: we should use in-memory sort here, if number of points is small enough:

    long countPerLeaf = valueCount;
    long innerNodeCount = 1;

    while (countPerLeaf > maxValuesInLeafNode) {
      countPerLeaf = (countPerLeaf+1)/2;
      innerNodeCount *= 2;
    }

    //System.out.println("innerNodeCount=" + innerNodeCount);

    if (1+2*innerNodeCount >= Integer.MAX_VALUE) {
      throw new IllegalStateException("too many nodes; increase maxValuesInLeafNode (currently " + maxValuesInLeafNode + ") and reindex");
    }

    innerNodeCount--;

    int numLeaves = (int) (innerNodeCount+1);

    // Indexed by nodeID, but first (root) nodeID is 1
    long[] blockMinValues = new long[numLeaves];

    // +1 because leaf count is power of 2 (e.g. 8), and innerNodeCount is power of 2 minus 1 (e.g. 7)
    long[] leafBlockFPs = new long[numLeaves];

    // Make sure the math above "worked":
    assert valueCount / blockMinValues.length <= maxValuesInLeafNode: "valueCount=" + valueCount + " blockMinValues.length=" + blockMinValues.length + " maxValuesInLeafNode=" + maxValuesInLeafNode;
    //System.out.println("  avg pointsPerLeaf=" + (valueCount/blockMinValues.length));

    // Sort all docs by value:
    SliceWriter sortedWriter = null;

    boolean success = false;
    try {
      sortedWriter = sort();
      heapWriter = null;

      build(1, numLeaves,
            new PathSlice(sortedWriter, 0, valueCount),
            out,
            globalMinValue, globalMaxValue,
            blockMinValues,
            leafBlockFPs);
      success = true;
    } finally {
      if (success) {
        sortedWriter.destroy();
        IOUtils.rm(tempInput);
      } else {
        try {
          sortedWriter.destroy();
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
    out.writeVInt((int) (valueCount / numLeaves));

    for (int i=0;i<blockMinValues.length;i++) {
      out.writeLong(blockMinValues[i]);
    }
    for (int i=0;i<leafBlockFPs.length;i++) {
      out.writeVLong(leafBlockFPs[i]);
    }
    out.writeLong(globalMaxValue);

    return indexFP;
  }

  /** Sliced reference to points in an OfflineSorter.ByteSequencesWriter file. */
  private static final class PathSlice {
    final SliceWriter writer;
    final long start;
    final long count;

    public PathSlice(SliceWriter writer, long start, long count) {
      this.writer = writer;
      this.start = start;
      this.count = count;
    }

    @Override
    public String toString() {
      return "PathSlice(start=" + start + " count=" + count + " writer=" + writer + ")";
    }
  }

  private long getSplitValue(PathSlice source, long leftCount, long minValue, long maxValue) throws IOException {

    // Read the split value:
    SliceReader reader = source.writer.getReader(source.start + leftCount);
    boolean success = false;
    long splitValue;
    try {
      boolean result = reader.next();
      assert result;
      splitValue = reader.value();
      assert splitValue >= minValue && splitValue <= maxValue: "splitValue=" + splitValue + " minValue=" + minValue + " maxValue=" + maxValue + " reader=" + reader;
      success = true;
    } finally {
      if (success) {
        IOUtils.close(reader);
      } else {
        IOUtils.closeWhileHandlingException(reader);
      }
    }

    return splitValue;
  }

  /** The incoming PathSlice for the dim we will split is already partitioned/sorted. */
  private void build(int nodeID, int leafNodeOffset,
                     PathSlice source,
                     IndexOutput out,
                     long minValue, long maxValue,
                     long[] blockMinValues,
                     long[] leafBlockFPs) throws IOException {

    long count = source.count;

    if (source.writer instanceof OfflineSliceWriter && count <= maxValuesSortInHeap) {
      // Cutover to heap:
      SliceWriter writer = new HeapSliceWriter((int) count);
      SliceReader reader = source.writer.getReader(source.start);
      try {
        for(int i=0;i<count;i++) {
          boolean hasNext = reader.next();
          assert hasNext;
          writer.append(reader.value(), reader.ord(), reader.docID());
        }
      } finally {
        IOUtils.close(reader, writer);
      }
      source = new PathSlice(writer, 0, count);
    }

    // We should never hit dead-end nodes on recursion even in the adversarial cases:
    assert count > 0;

    if (nodeID >= leafNodeOffset) {
      // Leaf node: write block
      assert maxValue >= minValue;

      //System.out.println("\nleaf:\n  lat range: " + ((long) maxLatEnc-minLatEnc));
      //System.out.println("  lon range: " + ((long) maxLonEnc-minLonEnc));

      // Sort by docID in the leaf so we can .or(DISI) at search time:
      SliceReader reader = source.writer.getReader(source.start);

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

      // TODO: not clear we need to do this anymore (we used to make a DISI over
      // the block at search time), but maybe it buys some memory
      // locality/sequentiality at search time?
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

      // TODO: in theory we could compute exactly what this fp will be, since we fixed-width (writeInt) encode docID, and up-front we know
      // how many docIDs are in every leaf since we don't do anything special about multiple splitValue boundary case?
      long startFP = out.getFilePointer();
      out.writeVInt(uniqueCount);

      // Save the block file pointer:
      int blockID = nodeID - leafNodeOffset;
      leafBlockFPs[blockID] = startFP;
      //System.out.println("    leafFP=" + startFP);

      blockMinValues[blockID] = minValue;

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
      // Inner node: sort, partition/recurse

      assert nodeID < blockMinValues.length: "nodeID=" + nodeID + " blockMinValues.length=" + blockMinValues.length;

      assert source.count == count;

      long leftCount = source.count / 2;

      // NOTE: we don't tweak leftCount for the boundary cases, which means at search time if we are looking for exactly splitValue then we
      // must search both left and right trees:
      long splitValue = getSplitValue(source, leftCount, minValue, maxValue);

      build(2*nodeID, leafNodeOffset,
            new PathSlice(source.writer, source.start, leftCount),
            out,
            minValue, splitValue,
            blockMinValues, leafBlockFPs);

      build(2*nodeID+1, leafNodeOffset,
            new PathSlice(source.writer, source.start+leftCount, count-leftCount),
            out,
            splitValue, maxValue,
            blockMinValues, leafBlockFPs);
    }
  }

  SliceWriter getWriter(long count) throws IOException {
    if (count < maxValuesSortInHeap) {
      return new HeapSliceWriter((int) count);
    } else {
      return new OfflineSliceWriter(count);
    }
  }
}
