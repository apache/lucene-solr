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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;

/** Handles intersection of an multi-dimensional shape in byte[] space with a block KD-tree previously written with {@link BKDWriter}.
 *
 * @lucene.experimental */

public class BKDReader extends PointValues implements Accountable {
  // Packed array of byte[] holding all split values in the full binary tree:
  final private byte[] splitPackedValues; 
  final long[] leafBlockFPs;
  final private int leafNodeOffset;
  final int numDims;
  final int bytesPerDim;
  final int bytesPerIndexEntry;
  final IndexInput in;
  final int maxPointsInLeafNode;
  final byte[] minPackedValue;
  final byte[] maxPackedValue;
  final long pointCount;
  final int docCount;
  final int version;
  protected final int packedBytesLength;

  /** Caller must pre-seek the provided {@link IndexInput} to the index location that {@link BKDWriter#finish} returned */
  public BKDReader(IndexInput in) throws IOException {
    version = CodecUtil.checkHeader(in, BKDWriter.CODEC_NAME, BKDWriter.VERSION_START, BKDWriter.VERSION_CURRENT);
    numDims = in.readVInt();
    maxPointsInLeafNode = in.readVInt();
    bytesPerDim = in.readVInt();
    bytesPerIndexEntry = numDims == 1 && version >= BKDWriter.VERSION_IMPLICIT_SPLIT_DIM_1D ? bytesPerDim : bytesPerDim + 1;
    packedBytesLength = numDims * bytesPerDim;

    // Read index:
    int numLeaves = in.readVInt();
    assert numLeaves > 0;
    leafNodeOffset = numLeaves;

    minPackedValue = new byte[packedBytesLength];
    maxPackedValue = new byte[packedBytesLength];

    in.readBytes(minPackedValue, 0, packedBytesLength);
    in.readBytes(maxPackedValue, 0, packedBytesLength);

    pointCount = in.readVLong();
    docCount = in.readVInt();

    splitPackedValues = new byte[bytesPerIndexEntry*numLeaves];

    // TODO: don't write split packed values[0]!
    in.readBytes(splitPackedValues, 0, splitPackedValues.length);

    // Read the file pointers to the start of each leaf block:
    long[] leafBlockFPs = new long[numLeaves];
    long lastFP = 0;
    for(int i=0;i<numLeaves;i++) {
      long delta = in.readVLong();
      leafBlockFPs[i] = lastFP + delta;
      lastFP += delta;
    }

    // Possibly rotate the leaf block FPs, if the index not fully balanced binary tree (only happens
    // if it was created by BKDWriter.merge).  In this case the leaf nodes may straddle the two bottom
    // levels of the binary tree:
    if (numDims == 1 && numLeaves > 1) {
      //System.out.println("BKDR: numLeaves=" + numLeaves);
      int levelCount = 2;
      while (true) {
        //System.out.println("  cycle levelCount=" + levelCount);
        if (numLeaves >= levelCount && numLeaves <= 2*levelCount) {
          int lastLevel = 2*(numLeaves - levelCount);
          assert lastLevel >= 0;
          /*
          System.out.println("BKDR: lastLevel=" + lastLevel + " vs " + levelCount);
          System.out.println("FPs before:");
          for(int i=0;i<leafBlockFPs.length;i++) {
            System.out.println("  " + i + " " + leafBlockFPs[i]);
          }
          */
          if (lastLevel != 0) {
            // Last level is partially filled, so we must rotate the leaf FPs to match.  We do this here, after loading
            // at read-time, so that we can still delta code them on disk at write:
            //System.out.println("BKDR: now rotate index");
            long[] newLeafBlockFPs = new long[numLeaves];
            System.arraycopy(leafBlockFPs, lastLevel, newLeafBlockFPs, 0, leafBlockFPs.length - lastLevel);
            System.arraycopy(leafBlockFPs, 0, newLeafBlockFPs, leafBlockFPs.length - lastLevel, lastLevel);
            leafBlockFPs = newLeafBlockFPs;
          }
          /*
          System.out.println("FPs:");
          for(int i=0;i<leafBlockFPs.length;i++) {
            System.out.println("  " + i + " " + leafBlockFPs[i]);
          }
          */
          break;
        }

        levelCount *= 2;
      }
    }

    this.leafBlockFPs = leafBlockFPs;
    this.in = in;
  }

  /** Called by consumers that have their own on-disk format for the index (e.g. SimpleText) */
  protected BKDReader(IndexInput in, int numDims, int maxPointsInLeafNode, int bytesPerDim, long[] leafBlockFPs, byte[] splitPackedValues,
                      byte[] minPackedValue, byte[] maxPackedValue, long pointCount, int docCount) throws IOException {
    this.in = in;
    this.numDims = numDims;
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.bytesPerDim = bytesPerDim;
    bytesPerIndexEntry = numDims == 1 ? bytesPerDim : bytesPerDim + 1;
    packedBytesLength = numDims * bytesPerDim;
    this.leafNodeOffset = leafBlockFPs.length;
    this.leafBlockFPs = leafBlockFPs;
    this.splitPackedValues = splitPackedValues;
    this.minPackedValue = minPackedValue;
    this.maxPackedValue = maxPackedValue;
    this.pointCount = pointCount;
    this.docCount = docCount;
    this.version = BKDWriter.VERSION_CURRENT;
    assert minPackedValue.length == packedBytesLength;
    assert maxPackedValue.length == packedBytesLength;
  }

  private static class VerifyVisitor implements IntersectVisitor {
    byte[] cellMinPacked;
    byte[] cellMaxPacked;
    byte[] lastPackedValue;
    final int numDims;
    final int bytesPerDim;
    final int maxDoc;

    public VerifyVisitor(int numDims, int bytesPerDim, int maxDoc) {
      this.numDims = numDims;
      this.bytesPerDim = bytesPerDim;
      this.maxDoc = maxDoc;
    }

    @Override
    public void visit(int docID) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void visit(int docID, byte[] packedValue) {
      if (docID < 0 || docID >= maxDoc) {
        throw new RuntimeException("docID=" + docID + " is out of bounds of 0.." + maxDoc);
      }
      for(int dim=0;dim<numDims;dim++) {
        if (StringHelper.compare(bytesPerDim, cellMinPacked, dim*bytesPerDim, packedValue, dim*bytesPerDim) > 0) {
          throw new RuntimeException("value=" + new BytesRef(packedValue, dim*bytesPerDim, bytesPerDim) + " for docID=" + docID + " dim=" + dim + " is less than this leaf block's minimum=" + new BytesRef(cellMinPacked, dim*bytesPerDim, bytesPerDim));
        }
        if (StringHelper.compare(bytesPerDim, cellMaxPacked, dim*bytesPerDim, packedValue, dim*bytesPerDim) < 0) {
          throw new RuntimeException("value=" + new BytesRef(packedValue, dim*bytesPerDim, bytesPerDim) + " for docID=" + docID + " dim=" + dim + " is greater than this leaf block's maximum=" + new BytesRef(cellMaxPacked, dim*bytesPerDim, bytesPerDim));
        }
      }

      if (numDims == 1) {
        // With only 1D, all values should always be in sorted order
        if (lastPackedValue == null) {
          lastPackedValue = Arrays.copyOf(packedValue, packedValue.length);
        } else if (StringHelper.compare(bytesPerDim, lastPackedValue, 0, packedValue, 0) > 0) {
          throw new RuntimeException("value=" + new BytesRef(packedValue) + " for docID=" + docID + " dim=0" + " sorts before last value=" + new BytesRef(lastPackedValue));
        } else {
          System.arraycopy(packedValue, 0, lastPackedValue, 0, bytesPerDim);
        }
      }
    }

    @Override
    public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
      throw new UnsupportedOperationException();
    }
  }

  /** Only used for debugging, to make sure all values in each leaf block fall within the range expected by the index */
  // TODO: maybe we can get this into CheckIndex?
  public void verify(int maxDoc) throws IOException {
    //System.out.println("BKDR.verify this=" + this);
    // Visits every doc in every leaf block and confirms that
    // their values agree with the index:
    byte[] rootMinPacked = new byte[packedBytesLength];
    byte[] rootMaxPacked = new byte[packedBytesLength];
    Arrays.fill(rootMaxPacked, (byte) 0xff);
    verify(getIntersectState(new VerifyVisitor(numDims, bytesPerDim, maxDoc)), 1, rootMinPacked, rootMaxPacked);
  }

  private void verify(IntersectState state, int nodeID, byte[] cellMinPacked, byte[] cellMaxPacked) throws IOException {

    if (nodeID >= leafNodeOffset) {
      int leafID = nodeID - leafNodeOffset;

      // In the unbalanced case it's possible the left most node only has one child:
      if (leafID < leafBlockFPs.length) {
        //System.out.println("CHECK nodeID=" + nodeID + " leaf=" + (nodeID-leafNodeOffset) + " offset=" + leafNodeOffset + " fp=" + leafBlockFPs[leafID]);
        //System.out.println("BKDR.verify leafID=" + leafID + " nodeID=" + nodeID + " fp=" + leafBlockFPs[leafID] + " min=" + new BytesRef(cellMinPacked) + " max=" + new BytesRef(cellMaxPacked));

        // Leaf node: check that all values are in fact in bounds:
        VerifyVisitor visitor = (VerifyVisitor) state.visitor;
        visitor.cellMinPacked = cellMinPacked;
        visitor.cellMaxPacked = cellMaxPacked;

        int count = readDocIDs(state.in, leafBlockFPs[leafID], state.scratchDocIDs);
        visitDocValues(state.commonPrefixLengths, state.scratchPackedValue, state.in, state.scratchDocIDs, count, state.visitor);
      } else {
        //System.out.println("BKDR.verify skip leafID=" + leafID);
      }
    } else {
      // Non-leaf node:

      int address = nodeID * bytesPerIndexEntry;
      int splitDim = numDims == 1 ? 0 : splitPackedValues[address++] & 0xff;
      assert splitDim < numDims;

      byte[] splitPackedValue = new byte[packedBytesLength];

      // Recurse on left sub-tree:
      System.arraycopy(cellMaxPacked, 0, splitPackedValue, 0, packedBytesLength);
      System.arraycopy(splitPackedValues, address, splitPackedValue, splitDim*bytesPerDim, bytesPerDim);
      verify(state,
             2*nodeID,
             cellMinPacked, splitPackedValue);

      // Recurse on right sub-tree:
      System.arraycopy(cellMinPacked, 0, splitPackedValue, 0, packedBytesLength);
      System.arraycopy(splitPackedValues, address, splitPackedValue, splitDim*bytesPerDim, bytesPerDim);
      verify(state,
             2*nodeID+1,
             splitPackedValue, cellMaxPacked);
    }
  }

  /** Used to track all state for a single call to {@link #intersect}. */
  public static final class IntersectState {
    final IndexInput in;
    final int[] scratchDocIDs;
    final byte[] scratchPackedValue;
    final int[] commonPrefixLengths;

    final IntersectVisitor visitor;

    public IntersectState(IndexInput in, int numDims,
                          int packedBytesLength,
                          int maxPointsInLeafNode,
                          IntersectVisitor visitor) {
      this.in = in;
      this.visitor = visitor;
      this.commonPrefixLengths = new int[numDims];
      this.scratchDocIDs = new int[maxPointsInLeafNode];
      this.scratchPackedValue = new byte[packedBytesLength];
    }
  }

  public void intersect(IntersectVisitor visitor) throws IOException {
    intersect(getIntersectState(visitor), 1, minPackedValue, maxPackedValue);
  }

  /** Fast path: this is called when the query box fully encompasses all cells under this node. */
  private void addAll(IntersectState state, int nodeID) throws IOException {
    //System.out.println("R: addAll nodeID=" + nodeID);

    if (nodeID >= leafNodeOffset) {
      //System.out.println("ADDALL");
      visitDocIDs(state.in, leafBlockFPs[nodeID-leafNodeOffset], state.visitor);
      // TODO: we can assert that the first value here in fact matches what the index claimed?
    } else {
      addAll(state, 2*nodeID);
      addAll(state, 2*nodeID+1);
    }
  }

  /** Create a new {@link IntersectState} */
  public IntersectState getIntersectState(IntersectVisitor visitor) {
    return new IntersectState(in.clone(), numDims,
                              packedBytesLength,
                              maxPointsInLeafNode,
                              visitor);
  }

  /** Visits all docIDs and packed values in a single leaf block */
  public void visitLeafBlockValues(int nodeID, IntersectState state) throws IOException {
    int leafID = nodeID - leafNodeOffset;

    // Leaf node; scan and filter all points in this block:
    int count = readDocIDs(state.in, leafBlockFPs[leafID], state.scratchDocIDs);

    // Again, this time reading values and checking with the visitor
    visitDocValues(state.commonPrefixLengths, state.scratchPackedValue, state.in, state.scratchDocIDs, count, state.visitor);
  }

  protected void visitDocIDs(IndexInput in, long blockFP, IntersectVisitor visitor) throws IOException {
    // Leaf node
    in.seek(blockFP);

    // How many points are stored in this leaf cell:
    int count = in.readVInt();
    visitor.grow(count);

    if (version < BKDWriter.VERSION_COMPRESSED_DOC_IDS) {
      DocIdsWriter.readInts32(in, count, visitor);
    } else {
      DocIdsWriter.readInts(in, count, visitor);
    }
  }

  protected int readDocIDs(IndexInput in, long blockFP, int[] docIDs) throws IOException {
    in.seek(blockFP);

    // How many points are stored in this leaf cell:
    int count = in.readVInt();

    if (version < BKDWriter.VERSION_COMPRESSED_DOC_IDS) {
      DocIdsWriter.readInts32(in, count, docIDs);
    } else {
      DocIdsWriter.readInts(in, count, docIDs);
    }

    return count;
  }

  protected void visitDocValues(int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in, int[] docIDs, int count, IntersectVisitor visitor) throws IOException {
    visitor.grow(count);

    readCommonPrefixes(commonPrefixLengths, scratchPackedValue, in);

    int compressedDim = version < BKDWriter.VERSION_COMPRESSED_VALUES
        ? -1
        : readCompressedDim(in);

    if (compressedDim == -1) {
      visitRawDocValues(commonPrefixLengths, scratchPackedValue, in, docIDs, count, visitor);
    } else {
      visitCompressedDocValues(commonPrefixLengths, scratchPackedValue, in, docIDs, count, visitor, compressedDim);
    }
  }

  // Just read suffixes for every dimension
  private void visitRawDocValues(int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in, int[] docIDs, int count, IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < count; ++i) {
      for(int dim=0;dim<numDims;dim++) {
        int prefix = commonPrefixLengths[dim];
        in.readBytes(scratchPackedValue, dim*bytesPerDim + prefix, bytesPerDim - prefix);
      }
      visitor.visit(docIDs[i], scratchPackedValue);
    }
  }

  private void visitCompressedDocValues(int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in, int[] docIDs, int count, IntersectVisitor visitor, int compressedDim) throws IOException {
    // the byte at `compressedByteOffset` is compressed using run-length compression,
    // other suffix bytes are stored verbatim
    final int compressedByteOffset = compressedDim * bytesPerDim + commonPrefixLengths[compressedDim];
    commonPrefixLengths[compressedDim]++;
    int i;
    for (i = 0; i < count; ) {
      scratchPackedValue[compressedByteOffset] = in.readByte();
      final int runLen = Byte.toUnsignedInt(in.readByte());
      for (int j = 0; j < runLen; ++j) {
        for(int dim=0;dim<numDims;dim++) {
          int prefix = commonPrefixLengths[dim];
          in.readBytes(scratchPackedValue, dim*bytesPerDim + prefix, bytesPerDim - prefix);
        }
        visitor.visit(docIDs[i+j], scratchPackedValue);
      }
      i += runLen;
    }
    if (i != count) {
      throw new CorruptIndexException("Sub blocks do not add up to the expected count: " + count + " != " + i, in);
    }
  }

  private int readCompressedDim(IndexInput in) throws IOException {
    int compressedDim = in.readByte();
    if (compressedDim < -1 || compressedDim >= numDims) {
      throw new CorruptIndexException("Got compressedDim="+compressedDim, in);
    }
    return compressedDim;
  }

  private void readCommonPrefixes(int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in) throws IOException {
    for(int dim=0;dim<numDims;dim++) {
      int prefix = in.readVInt();
      commonPrefixLengths[dim] = prefix;
      if (prefix > 0) {
        in.readBytes(scratchPackedValue, dim*bytesPerDim, prefix);
      }
      //System.out.println("R: " + dim + " of " + numDims + " prefix=" + prefix);
    }
  }

  private void intersect(IntersectState state,
                         int nodeID,
                         byte[] cellMinPacked, byte[] cellMaxPacked)
    throws IOException {

    /*
    System.out.println("\nR: intersect nodeID=" + nodeID);
    for(int dim=0;dim<numDims;dim++) {
      System.out.println("  dim=" + dim + "\n    cellMin=" + new BytesRef(cellMinPacked, dim*bytesPerDim, bytesPerDim) + "\n    cellMax=" + new BytesRef(cellMaxPacked, dim*bytesPerDim, bytesPerDim));
    }
    */

    Relation r = state.visitor.compare(cellMinPacked, cellMaxPacked);

    if (r == Relation.CELL_OUTSIDE_QUERY) {
      // This cell is fully outside of the query shape: stop recursing
      return;
    } else if (r == Relation.CELL_INSIDE_QUERY) {
      // This cell is fully inside of the query shape: recursively add all points in this cell without filtering
      addAll(state, nodeID);
      return;
    } else {
      // The cell crosses the shape boundary, or the cell fully contains the query, so we fall through and do full filtering
    }

    if (nodeID >= leafNodeOffset) {
      // TODO: we can assert that the first value here in fact matches what the index claimed?

      int leafID = nodeID - leafNodeOffset;
      
      // In the unbalanced case it's possible the left most node only has one child:
      if (leafID < leafBlockFPs.length) {
        // Leaf node; scan and filter all points in this block:
        int count = readDocIDs(state.in, leafBlockFPs[leafID], state.scratchDocIDs);

        // Again, this time reading values and checking with the visitor
        visitDocValues(state.commonPrefixLengths, state.scratchPackedValue, state.in, state.scratchDocIDs, count, state.visitor);
      }

    } else {
      
      // Non-leaf node: recurse on the split left and right nodes

      // TODO: save the unused 1 byte prefix (it's always 0) in the 1d case here:
      int address = nodeID * bytesPerIndexEntry;
      int splitDim = numDims == 1 ? 0 : splitPackedValues[address++] & 0xff;
      assert splitDim < numDims;

      // TODO: can we alloc & reuse this up front?

      // TODO: can we alloc & reuse this up front?
      byte[] splitPackedValue = new byte[packedBytesLength];

      // Recurse on left sub-tree:
      System.arraycopy(cellMaxPacked, 0, splitPackedValue, 0, packedBytesLength);
      System.arraycopy(splitPackedValues, address, splitPackedValue, splitDim*bytesPerDim, bytesPerDim);
      intersect(state,
                2*nodeID,
                cellMinPacked, splitPackedValue);

      // Recurse on right sub-tree:
      System.arraycopy(cellMinPacked, 0, splitPackedValue, 0, packedBytesLength);
      System.arraycopy(splitPackedValues, address, splitPackedValue, splitDim*bytesPerDim, bytesPerDim);
      intersect(state,
                2*nodeID+1,
                splitPackedValue, cellMaxPacked);
    }
  }

  /** Copies the split value for this node into the provided byte array */
  public void copySplitValue(int nodeID, byte[] splitPackedValue) {
    int address = nodeID * bytesPerIndexEntry;
    int splitDim = numDims == 1 ? 0 : splitPackedValues[address++] & 0xff;
    assert splitDim < numDims;
    System.arraycopy(splitPackedValues, address, splitPackedValue, splitDim*bytesPerDim, bytesPerDim);
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.sizeOf(splitPackedValues) +
        RamUsageEstimator.sizeOf(leafBlockFPs);
  }

  @Override
  public byte[] getMinPackedValue() {
    return minPackedValue.clone();
  }

  @Override
  public byte[] getMaxPackedValue() {
    return maxPackedValue.clone();
  }

  @Override
  public int getNumDimensions() {
    return numDims;
  }

  @Override
  public int getBytesPerDimension() {
    return bytesPerDim;
  }

  @Override
  public long size() {
    return pointCount;
  }

  @Override
  public int getDocCount() {
    return docCount;
  }

  public boolean isLeafNode(int nodeID) {
    return nodeID >= leafNodeOffset;
  }
}
