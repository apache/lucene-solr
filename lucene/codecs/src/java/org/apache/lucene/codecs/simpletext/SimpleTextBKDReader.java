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

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.bkd.BKDReader;

import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.BLOCK_COUNT;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.BLOCK_DOC_ID;
import static org.apache.lucene.codecs.simpletext.SimpleTextPointsWriter.BLOCK_VALUE;

/** Forked from {@link BKDReader} and simplified/specialized for SimpleText's usage */

final class SimpleTextBKDReader extends PointValues implements Accountable {
  // Packed array of byte[] holding all split values in the full binary tree:
  final private byte[] splitPackedValues; 
  final long[] leafBlockFPs;
  final private int leafNodeOffset;
  final int numDims;
  final int numIndexDims;
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
  protected final int packedIndexBytesLength;

  public SimpleTextBKDReader(IndexInput in, int numDims, int numIndexDims, int maxPointsInLeafNode, int bytesPerDim, long[] leafBlockFPs, byte[] splitPackedValues,
                             byte[] minPackedValue, byte[] maxPackedValue, long pointCount, int docCount) throws IOException {
    this.in = in;
    this.numDims = numDims;
    this.numIndexDims = numIndexDims;
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.bytesPerDim = bytesPerDim;
    // no version check here because callers of this API (SimpleText) have no back compat:
    bytesPerIndexEntry = numIndexDims == 1 ? bytesPerDim : bytesPerDim + 1;
    packedBytesLength = numDims * bytesPerDim;
    packedIndexBytesLength = numIndexDims * bytesPerDim;
    this.leafNodeOffset = leafBlockFPs.length;
    this.leafBlockFPs = leafBlockFPs;
    this.splitPackedValues = splitPackedValues;
    this.minPackedValue = minPackedValue;
    this.maxPackedValue = maxPackedValue;
    this.pointCount = pointCount;
    this.docCount = docCount;
    this.version = SimpleTextBKDWriter.VERSION_CURRENT;
    assert minPackedValue.length == packedIndexBytesLength;
    assert maxPackedValue.length == packedIndexBytesLength;
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

  void visitDocIDs(IndexInput in, long blockFP, IntersectVisitor visitor) throws IOException {
    BytesRefBuilder scratch = new BytesRefBuilder();
    in.seek(blockFP);
    readLine(in, scratch);
    int count = parseInt(scratch, BLOCK_COUNT);
    visitor.grow(count);
    for(int i=0;i<count;i++) {
      readLine(in, scratch);
      visitor.visit(parseInt(scratch, BLOCK_DOC_ID));
    }
  }

  int readDocIDs(IndexInput in, long blockFP, int[] docIDs) throws IOException {
    BytesRefBuilder scratch = new BytesRefBuilder();
    in.seek(blockFP);
    readLine(in, scratch);
    int count = parseInt(scratch, BLOCK_COUNT);
    for(int i=0;i<count;i++) {
      readLine(in, scratch);
      docIDs[i] = parseInt(scratch, BLOCK_DOC_ID);
    }
    return count;
  }

  void visitDocValues(int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in, int[] docIDs, int count, IntersectVisitor visitor) throws IOException {
    visitor.grow(count);
    // NOTE: we don't do prefix coding, so we ignore commonPrefixLengths
    assert scratchPackedValue.length == packedBytesLength;
    BytesRefBuilder scratch = new BytesRefBuilder();
    for(int i=0;i<count;i++) {
      readLine(in, scratch);
      assert startsWith(scratch, BLOCK_VALUE);
      BytesRef br = SimpleTextUtil.fromBytesRefString(stripPrefix(scratch, BLOCK_VALUE));
      assert br.length == packedBytesLength;
      System.arraycopy(br.bytes, br.offset, scratchPackedValue, 0, packedBytesLength);
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
        for(int dim = 0; dim< numDims; dim++) {
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
    if (compressedDim < -1 || compressedDim >= numIndexDims) {
      throw new CorruptIndexException("Got compressedDim="+compressedDim, in);
    }
    return compressedDim;
  }

  private void readCommonPrefixes(int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in) throws IOException {
    for(int dim = 0; dim< numDims; dim++) {
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

      int address = nodeID * bytesPerIndexEntry;
      int splitDim;
      if (numIndexDims == 1) {
        splitDim = 0;
      } else {
        splitDim = splitPackedValues[address++] & 0xff;
      }
      
      assert splitDim < numIndexDims;

      // TODO: can we alloc & reuse this up front?

      byte[] splitPackedValue = new byte[packedIndexBytesLength];

      // Recurse on left sub-tree:
      System.arraycopy(cellMaxPacked, 0, splitPackedValue, 0, packedIndexBytesLength);
      System.arraycopy(splitPackedValues, address, splitPackedValue, splitDim*bytesPerDim, bytesPerDim);
      intersect(state,
                2*nodeID,
                cellMinPacked, splitPackedValue);

      // Recurse on right sub-tree:
      System.arraycopy(cellMinPacked, 0, splitPackedValue, 0, packedIndexBytesLength);
      System.arraycopy(splitPackedValues, address, splitPackedValue, splitDim*bytesPerDim, bytesPerDim);
      intersect(state,
                2*nodeID+1,
                splitPackedValue, cellMaxPacked);
    }
  }

  @Override
  public long estimatePointCount(IntersectVisitor visitor) {
    return estimatePointCount(getIntersectState(visitor), 1, minPackedValue, maxPackedValue);
  }

  private long estimatePointCount(IntersectState state,
      int nodeID, byte[] cellMinPacked, byte[] cellMaxPacked) {
    Relation r = state.visitor.compare(cellMinPacked, cellMaxPacked);

    if (r == Relation.CELL_OUTSIDE_QUERY) {
      // This cell is fully outside of the query shape: stop recursing
      return 0L;
    } else if (nodeID >= leafNodeOffset) {
      // Assume all points match and there are no dups
      return maxPointsInLeafNode;
    } else {
      
      // Non-leaf node: recurse on the split left and right nodes

      int address = nodeID * bytesPerIndexEntry;
      int splitDim;
      if (numIndexDims == 1) {
        splitDim = 0;
      } else {
        splitDim = splitPackedValues[address++] & 0xff;
      }
      
      assert splitDim < numIndexDims;

      // TODO: can we alloc & reuse this up front?

      byte[] splitPackedValue = new byte[packedIndexBytesLength];

      // Recurse on left sub-tree:
      System.arraycopy(cellMaxPacked, 0, splitPackedValue, 0, packedIndexBytesLength);
      System.arraycopy(splitPackedValues, address, splitPackedValue, splitDim*bytesPerDim, bytesPerDim);
      final long leftCost = estimatePointCount(state,
                2*nodeID,
                cellMinPacked, splitPackedValue);

      // Recurse on right sub-tree:
      System.arraycopy(cellMinPacked, 0, splitPackedValue, 0, packedIndexBytesLength);
      System.arraycopy(splitPackedValues, address, splitPackedValue, splitDim*bytesPerDim, bytesPerDim);
      final long rightCost = estimatePointCount(state,
                2*nodeID+1,
                splitPackedValue, cellMaxPacked);
      return leftCost + rightCost;
    }
  }

  /** Copies the split value for this node into the provided byte array */
  public void copySplitValue(int nodeID, byte[] splitPackedValue) {
    int address = nodeID * bytesPerIndexEntry;
    int splitDim;
    if (numIndexDims == 1) {
      splitDim = 0;
    } else {
      splitDim = splitPackedValues[address++] & 0xff;
    }
    
    assert splitDim < numIndexDims;
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
  public int getNumIndexDimensions() {
    return numIndexDims;
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

  private int parseInt(BytesRefBuilder scratch, BytesRef prefix) {
    assert startsWith(scratch, prefix);
    return Integer.parseInt(stripPrefix(scratch, prefix));
  }

  private String stripPrefix(BytesRefBuilder scratch, BytesRef prefix) {
    return new String(scratch.bytes(), prefix.length, scratch.length() - prefix.length, StandardCharsets.UTF_8);
  }

  private boolean startsWith(BytesRefBuilder scratch, BytesRef prefix) {
    return StringHelper.startsWith(scratch.get(), prefix);
  }

  private void readLine(IndexInput in, BytesRefBuilder scratch) throws IOException {
    SimpleTextUtil.readLine(in, scratch);
  }
}
