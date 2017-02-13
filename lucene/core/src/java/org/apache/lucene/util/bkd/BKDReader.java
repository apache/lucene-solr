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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;

/** Handles intersection of an multi-dimensional shape in byte[] space with a block KD-tree previously written with {@link BKDWriter}.
 *
 * @lucene.experimental */

public final class BKDReader implements Accountable {
  // Packed array of byte[] holding all split values in the full binary tree:
  final int leafNodeOffset;
  final int numDims;
  final int bytesPerDim;
  final int numLeaves;
  final IndexInput in;
  final int maxPointsInLeafNode;
  final byte[] minPackedValue;
  final byte[] maxPackedValue;
  final long pointCount;
  final int docCount;
  final int version;
  protected final int packedBytesLength;

  // Used for 6.4.0+ index format:
  final byte[] packedIndex;

  // Used for Legacy (pre-6.4.0) index format, to hold a compact form of the index:
  final private byte[] splitPackedValues; 
  final int bytesPerIndexEntry;
  final long[] leafBlockFPs;

  /** Caller must pre-seek the provided {@link IndexInput} to the index location that {@link BKDWriter#finish} returned */
  public BKDReader(IndexInput in) throws IOException {
    version = CodecUtil.checkHeader(in, BKDWriter.CODEC_NAME, BKDWriter.VERSION_START, BKDWriter.VERSION_CURRENT);
    numDims = in.readVInt();
    maxPointsInLeafNode = in.readVInt();
    bytesPerDim = in.readVInt();
    bytesPerIndexEntry = numDims == 1 && version >= BKDWriter.VERSION_IMPLICIT_SPLIT_DIM_1D ? bytesPerDim : bytesPerDim + 1;
    packedBytesLength = numDims * bytesPerDim;

    // Read index:
    numLeaves = in.readVInt();
    assert numLeaves > 0;
    leafNodeOffset = numLeaves;

    minPackedValue = new byte[packedBytesLength];
    maxPackedValue = new byte[packedBytesLength];

    in.readBytes(minPackedValue, 0, packedBytesLength);
    in.readBytes(maxPackedValue, 0, packedBytesLength);

    for(int dim=0;dim<numDims;dim++) {
      if (StringHelper.compare(bytesPerDim, minPackedValue, dim*bytesPerDim, maxPackedValue, dim*bytesPerDim) > 0) {
        throw new CorruptIndexException("minPackedValue " + new BytesRef(minPackedValue) + " is > maxPackedValue " + new BytesRef(maxPackedValue) + " for dim=" + dim, in);
      }
    }
    
    pointCount = in.readVLong();
    docCount = in.readVInt();

    if (version >= BKDWriter.VERSION_PACKED_INDEX) {
      int numBytes = in.readVInt();
      packedIndex = new byte[numBytes];
      in.readBytes(packedIndex, 0, numBytes);
      leafBlockFPs = null;
      splitPackedValues = null;
    } else {
      // legacy un-packed index

      splitPackedValues = new byte[bytesPerIndexEntry*numLeaves];

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
      // if it was created by BKDWriter.merge or OneDimWriter).  In this case the leaf nodes may straddle the two bottom
      // levels of the binary tree:
      if (numDims == 1 && numLeaves > 1) {
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
      
      this.leafBlockFPs = leafBlockFPs;
      packedIndex = null;
    }

    this.in = in;
  }

  long getMinLeafBlockFP() {
    if (packedIndex != null) {
      return new ByteArrayDataInput(packedIndex).readVLong();
    } else {
      long minFP = Long.MAX_VALUE;
      for(long fp : leafBlockFPs) {
        minFP = Math.min(minFP, fp);
      }
      return minFP;
    }
  }

  /** Used to walk the in-heap index
   *
   * @lucene.internal */
  public abstract class IndexTree implements Cloneable {
    protected int nodeID;
    // level is 1-based so that we can do level-1 w/o checking each time:
    protected int level;
    protected int splitDim;
    protected final byte[][] splitPackedValueStack;

    protected IndexTree() {
      int treeDepth = getTreeDepth();
      splitPackedValueStack = new byte[treeDepth+1][];
      nodeID = 1;
      level = 1;
      splitPackedValueStack[level] = new byte[packedBytesLength];
    }      

    public void pushLeft() {
      nodeID *= 2;
      level++;
      if (splitPackedValueStack[level] == null) {
        splitPackedValueStack[level] = new byte[packedBytesLength];
      }
    }

    /** Clone, but you are not allowed to pop up past the point where the clone happened. */
    public abstract IndexTree clone();
    
    public void pushRight() {
      nodeID = nodeID * 2 + 1;
      level++;
      if (splitPackedValueStack[level] == null) {
        splitPackedValueStack[level] = new byte[packedBytesLength];
      }
    }

    public void pop() {
      nodeID /= 2;
      level--;
      splitDim = -1;
      //System.out.println("  pop nodeID=" + nodeID);
    }

    public boolean isLeafNode() {
      return nodeID >= leafNodeOffset;
    }

    public boolean nodeExists() {
      return nodeID - leafNodeOffset < leafNodeOffset;
    }

    public int getNodeID() {
      return nodeID;
    }

    public byte[] getSplitPackedValue() {
      assert isLeafNode() == false;
      assert splitPackedValueStack[level] != null: "level=" + level;
      return splitPackedValueStack[level];
    }
                                                       
    /** Only valid after pushLeft or pushRight, not pop! */
    public int getSplitDim() {
      assert isLeafNode() == false;
      return splitDim;
    }

    /** Only valid after pushLeft or pushRight, not pop! */
    public abstract BytesRef getSplitDimValue();
    
    /** Only valid after pushLeft or pushRight, not pop! */
    public abstract long getLeafBlockFP();

    /** Return the number of leaves below the current node. */
    public int getNumLeaves() {
      int leftMostLeafNode = nodeID;
      while (leftMostLeafNode < leafNodeOffset) {
        leftMostLeafNode = leftMostLeafNode * 2;
      }
      int rightMostLeafNode = nodeID;
      while (rightMostLeafNode < leafNodeOffset) {
        rightMostLeafNode = rightMostLeafNode * 2 + 1;
      }
      final int numLeaves;
      if (rightMostLeafNode >= leftMostLeafNode) {
        // both are on the same level
        numLeaves = rightMostLeafNode - leftMostLeafNode + 1;
      } else {
        // left is one level deeper than right
        numLeaves = rightMostLeafNode - leftMostLeafNode + 1 + leafNodeOffset;
      }
      assert numLeaves == getNumLeavesSlow(nodeID) : numLeaves + " " + getNumLeavesSlow(nodeID);
      return numLeaves;
    }

    // for assertions
    private int getNumLeavesSlow(int node) {
      if (node >= 2 * leafNodeOffset) {
        return 0;
      } else if (node >= leafNodeOffset) {
        return 1;
      } else {
        final int leftCount = getNumLeavesSlow(node * 2);
        final int rightCount = getNumLeavesSlow(node * 2 + 1);
        return leftCount + rightCount;
      }
    }
  }

  /** Reads the original simple yet heap-heavy index format */
  private final class LegacyIndexTree extends IndexTree {

    private long leafBlockFP;
    private final byte[] splitDimValue = new byte[bytesPerDim];
    private final BytesRef scratch = new BytesRef();

    public LegacyIndexTree() {
      setNodeData();
      scratch.bytes = splitDimValue;
      scratch.length = bytesPerDim;
    }

    @Override
    public LegacyIndexTree clone() {
      LegacyIndexTree index = new LegacyIndexTree();
      index.nodeID = nodeID;
      index.level = level;
      index.splitDim = splitDim;
      index.leafBlockFP = leafBlockFP;
      index.splitPackedValueStack[index.level] = splitPackedValueStack[index.level].clone();

      return index;
    }
    
    @Override
    public void pushLeft() {
      super.pushLeft();
      setNodeData();
    }
    
    @Override
    public void pushRight() {
      super.pushRight();
      setNodeData();
    }

    private void setNodeData() {
      if (isLeafNode()) {
        leafBlockFP = leafBlockFPs[nodeID - leafNodeOffset];
        splitDim = -1;
      } else {
        leafBlockFP = -1;
        int address = nodeID * bytesPerIndexEntry;
        if (numDims == 1) {
          splitDim = 0;
          if (version < BKDWriter.VERSION_IMPLICIT_SPLIT_DIM_1D) {
            // skip over wastefully encoded 0 splitDim:
            assert splitPackedValues[address] == 0;
            address++;
          }
        } else {
          splitDim = splitPackedValues[address++] & 0xff;
        }
        System.arraycopy(splitPackedValues, address, splitDimValue, 0, bytesPerDim);
      }
    }

    @Override
    public long getLeafBlockFP() {
      assert isLeafNode();
      return leafBlockFP;
    }

    @Override
    public BytesRef getSplitDimValue() {
      assert isLeafNode() == false;
      return scratch;
    }

    @Override
    public void pop() {
      super.pop();
      leafBlockFP = -1;
    }
  }

  /** Reads the new packed byte[] index format which can be up to ~63% smaller than the legacy index format on 20M NYC taxis tests.  This
   *  format takes advantage of the limited access pattern to the BKD tree at search time, i.e. starting at the root node and recursing
   *  downwards one child at a time. */
  private final class PackedIndexTree extends IndexTree {
    // used to read the packed byte[]
    private final ByteArrayDataInput in;
    // holds the minimum (left most) leaf block file pointer for each level we've recursed to:
    private final long[] leafBlockFPStack;
    // holds the address, in the packed byte[] index, of the left-node of each level:
    private final int[] leftNodePositions;
    // holds the address, in the packed byte[] index, of the right-node of each level:
    private final int[] rightNodePositions;
    // holds the splitDim for each level:
    private final int[] splitDims;
    // true if the per-dim delta we read for the node at this level is a negative offset vs. the last split on this dim; this is a packed
    // 2D array, i.e. to access array[level][dim] you read from negativeDeltas[level*numDims+dim].  this will be true if the last time we
    // split on this dimension, we next pushed to the left sub-tree:
    private final boolean[] negativeDeltas;
    // holds the packed per-level split values; the intersect method uses this to save the cell min/max as it recurses:
    private final byte[][] splitValuesStack;
    // scratch value to return from getPackedValue:
    private final BytesRef scratch;

    public PackedIndexTree() {
      int treeDepth = getTreeDepth();
      leafBlockFPStack = new long[treeDepth+1];
      leftNodePositions = new int[treeDepth+1];
      rightNodePositions = new int[treeDepth+1];
      splitValuesStack = new byte[treeDepth+1][];
      splitDims = new int[treeDepth+1];
      negativeDeltas = new boolean[numDims*(treeDepth+1)];

      in = new ByteArrayDataInput(packedIndex);
      splitValuesStack[0] = new byte[packedBytesLength];
      readNodeData(false);
      scratch = new BytesRef();
      scratch.length = bytesPerDim;
    }

    @Override
    public PackedIndexTree clone() {
      PackedIndexTree index = new PackedIndexTree();
      index.nodeID = nodeID;
      index.level = level;
      index.splitDim = splitDim;
      index.leafBlockFPStack[level] = leafBlockFPStack[level];
      index.leftNodePositions[level] = leftNodePositions[level];
      index.rightNodePositions[level] = rightNodePositions[level];
      index.splitValuesStack[index.level] = splitValuesStack[index.level].clone();
      System.arraycopy(negativeDeltas, level*numDims, index.negativeDeltas, level*numDims, numDims);
      index.splitDims[level] = splitDims[level];
      return index;
    }

    @Override
    public void pushLeft() {
      int nodePosition = leftNodePositions[level];
      super.pushLeft();
      System.arraycopy(negativeDeltas, (level-1)*numDims, negativeDeltas, level*numDims, numDims);
      assert splitDim != -1;
      negativeDeltas[level*numDims+splitDim] = true;
      in.setPosition(nodePosition);
      readNodeData(true);
    }
    
    @Override
    public void pushRight() {
      int nodePosition = rightNodePositions[level];
      super.pushRight();
      System.arraycopy(negativeDeltas, (level-1)*numDims, negativeDeltas, level*numDims, numDims);
      assert splitDim != -1;
      negativeDeltas[level*numDims+splitDim] = false;
      in.setPosition(nodePosition);
      readNodeData(false);
    }

    @Override
    public void pop() {
      super.pop();
      splitDim = splitDims[level];
    }

    @Override
    public long getLeafBlockFP() {
      assert isLeafNode(): "nodeID=" + nodeID + " is not a leaf";
      return leafBlockFPStack[level];
    }

    @Override
    public BytesRef getSplitDimValue() {
      assert isLeafNode() == false;
      scratch.bytes = splitValuesStack[level];
      scratch.offset = splitDim * bytesPerDim;
      return scratch;
    }

    private void readNodeData(boolean isLeft) {

      leafBlockFPStack[level] = leafBlockFPStack[level-1];

      // read leaf block FP delta
      if (isLeft == false) {
        leafBlockFPStack[level] += in.readVLong();
      }

      if (isLeafNode()) {
        splitDim = -1;
      } else {

        // read split dim, prefix, firstDiffByteDelta encoded as int:
        int code = in.readVInt();
        splitDim = code % numDims;
        splitDims[level] = splitDim;
        code /= numDims;
        int prefix = code % (1+bytesPerDim);
        int suffix = bytesPerDim - prefix;

        if (splitValuesStack[level] == null) {
          splitValuesStack[level] = new byte[packedBytesLength];
        }
        System.arraycopy(splitValuesStack[level-1], 0, splitValuesStack[level], 0, packedBytesLength);
        if (suffix > 0) {
          int firstDiffByteDelta = code / (1+bytesPerDim);
          if (negativeDeltas[level*numDims + splitDim]) {
            firstDiffByteDelta = -firstDiffByteDelta;
          }
          int oldByte = splitValuesStack[level][splitDim*bytesPerDim+prefix] & 0xFF;
          splitValuesStack[level][splitDim*bytesPerDim+prefix] = (byte) (oldByte + firstDiffByteDelta);
          in.readBytes(splitValuesStack[level], splitDim*bytesPerDim+prefix+1, suffix-1);
        } else {
          // our split value is == last split value in this dim, which can happen when there are many duplicate values
        }

        int leftNumBytes;
        if (nodeID * 2 < leafNodeOffset) {
          leftNumBytes = in.readVInt();
        } else {
          leftNumBytes = 0;
        }

        leftNodePositions[level] = in.getPosition();
        rightNodePositions[level] = leftNodePositions[level] + leftNumBytes;
      }
    }
  }

  private int getTreeDepth() {
    // First +1 because all the non-leave nodes makes another power
    // of 2; e.g. to have a fully balanced tree with 4 leaves you
    // need a depth=3 tree:

    // Second +1 because MathUtil.log computes floor of the logarithm; e.g.
    // with 5 leaves you need a depth=4 tree:
    return MathUtil.log(numLeaves, 2) + 2;
  }

  /** Used to track all state for a single call to {@link #intersect}. */
  public static final class IntersectState {
    final IndexInput in;
    final int[] scratchDocIDs;
    final byte[] scratchPackedValue;
    final int[] commonPrefixLengths;

    final IntersectVisitor visitor;
    public final IndexTree index;

    public IntersectState(IndexInput in, int numDims,
                          int packedBytesLength,
                          int maxPointsInLeafNode,
                          IntersectVisitor visitor,
                          IndexTree indexVisitor) {
      this.in = in;
      this.visitor = visitor;
      this.commonPrefixLengths = new int[numDims];
      this.scratchDocIDs = new int[maxPointsInLeafNode];
      this.scratchPackedValue = new byte[packedBytesLength];
      this.index = indexVisitor;
    }
  }

  public void intersect(IntersectVisitor visitor) throws IOException {
    intersect(getIntersectState(visitor), minPackedValue, maxPackedValue);
  }

  public long estimatePointCount(IntersectVisitor visitor) {
    return estimatePointCount(getIntersectState(visitor), minPackedValue, maxPackedValue);
  }

  /** Fast path: this is called when the query box fully encompasses all cells under this node. */
  private void addAll(IntersectState state, boolean grown) throws IOException {
    //System.out.println("R: addAll nodeID=" + nodeID);

    if (grown == false) {
      final long maxPointCount = (long) maxPointsInLeafNode * state.index.getNumLeaves();
      if (maxPointCount <= Integer.MAX_VALUE) { // could be >MAX_VALUE if there are more than 2B points in total
        state.visitor.grow((int) maxPointCount);
        grown = true;
      }
    }

    if (state.index.isLeafNode()) {
      assert grown;
      //System.out.println("ADDALL");
      if (state.index.nodeExists()) {
        visitDocIDs(state.in, state.index.getLeafBlockFP(), state.visitor);
      }
      // TODO: we can assert that the first value here in fact matches what the index claimed?
    } else {
      state.index.pushLeft();
      addAll(state, grown);
      state.index.pop();

      state.index.pushRight();
      addAll(state, grown);
      state.index.pop();
    }
  }

  /** Create a new {@link IntersectState} */
  public IntersectState getIntersectState(IntersectVisitor visitor) {
    IndexTree index;
    if (packedIndex != null) {
      index = new PackedIndexTree();
    } else {
      index = new LegacyIndexTree();
    }
    return new IntersectState(in.clone(), numDims,
                              packedBytesLength,
                              maxPointsInLeafNode,
                              visitor,
                              index);
  }

  /** Visits all docIDs and packed values in a single leaf block */
  public void visitLeafBlockValues(IndexTree index, IntersectState state) throws IOException {

    // Leaf node; scan and filter all points in this block:
    int count = readDocIDs(state.in, index.getLeafBlockFP(), state.scratchDocIDs);

    // Again, this time reading values and checking with the visitor
    visitDocValues(state.commonPrefixLengths, state.scratchPackedValue, state.in, state.scratchDocIDs, count, state.visitor);
  }

  private void visitDocIDs(IndexInput in, long blockFP, IntersectVisitor visitor) throws IOException {
    // Leaf node
    in.seek(blockFP);

    // How many points are stored in this leaf cell:
    int count = in.readVInt();
    // No need to call grow(), it has been called up-front

    if (version < BKDWriter.VERSION_COMPRESSED_DOC_IDS) {
      DocIdsWriter.readInts32(in, count, visitor);
    } else {
      DocIdsWriter.readInts(in, count, visitor);
    }
  }

  int readDocIDs(IndexInput in, long blockFP, int[] docIDs) throws IOException {
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

  void visitDocValues(int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in, int[] docIDs, int count, IntersectVisitor visitor) throws IOException {
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

  private void intersect(IntersectState state, byte[] cellMinPacked, byte[] cellMaxPacked) throws IOException {

    /*
    System.out.println("\nR: intersect nodeID=" + state.index.getNodeID());
    for(int dim=0;dim<numDims;dim++) {
      System.out.println("  dim=" + dim + "\n    cellMin=" + new BytesRef(cellMinPacked, dim*bytesPerDim, bytesPerDim) + "\n    cellMax=" + new BytesRef(cellMaxPacked, dim*bytesPerDim, bytesPerDim));
    }
    */

    Relation r = state.visitor.compare(cellMinPacked, cellMaxPacked);

    if (r == Relation.CELL_OUTSIDE_QUERY) {
      // This cell is fully outside of the query shape: stop recursing
    } else if (r == Relation.CELL_INSIDE_QUERY) {
      // This cell is fully inside of the query shape: recursively add all points in this cell without filtering
      addAll(state, false);
      // The cell crosses the shape boundary, or the cell fully contains the query, so we fall through and do full filtering:
    } else if (state.index.isLeafNode()) {
      
      // TODO: we can assert that the first value here in fact matches what the index claimed?
      
      // In the unbalanced case it's possible the left most node only has one child:
      if (state.index.nodeExists()) {
        // Leaf node; scan and filter all points in this block:
        int count = readDocIDs(state.in, state.index.getLeafBlockFP(), state.scratchDocIDs);

        // Again, this time reading values and checking with the visitor
        visitDocValues(state.commonPrefixLengths, state.scratchPackedValue, state.in, state.scratchDocIDs, count, state.visitor);
      }

    } else {
      
      // Non-leaf node: recurse on the split left and right nodes
      int splitDim = state.index.getSplitDim();
      assert splitDim >= 0: "splitDim=" + splitDim;
      assert splitDim < numDims;

      byte[] splitPackedValue = state.index.getSplitPackedValue();
      BytesRef splitDimValue = state.index.getSplitDimValue();
      assert splitDimValue.length == bytesPerDim;
      //System.out.println("  splitDimValue=" + splitDimValue + " splitDim=" + splitDim);

      // make sure cellMin <= splitValue <= cellMax:
      assert StringHelper.compare(bytesPerDim, cellMinPacked, splitDim*bytesPerDim, splitDimValue.bytes, splitDimValue.offset) <= 0: "bytesPerDim=" + bytesPerDim + " splitDim=" + splitDim + " numDims=" + numDims;
      assert StringHelper.compare(bytesPerDim, cellMaxPacked, splitDim*bytesPerDim, splitDimValue.bytes, splitDimValue.offset) >= 0: "bytesPerDim=" + bytesPerDim + " splitDim=" + splitDim + " numDims=" + numDims;

      // Recurse on left sub-tree:
      System.arraycopy(cellMaxPacked, 0, splitPackedValue, 0, packedBytesLength);
      System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, splitDim*bytesPerDim, bytesPerDim);
      state.index.pushLeft();
      intersect(state, cellMinPacked, splitPackedValue);
      state.index.pop();

      // Restore the split dim value since it may have been overwritten while recursing:
      System.arraycopy(splitPackedValue, splitDim*bytesPerDim, splitDimValue.bytes, splitDimValue.offset, bytesPerDim);

      // Recurse on right sub-tree:
      System.arraycopy(cellMinPacked, 0, splitPackedValue, 0, packedBytesLength);
      System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, splitDim*bytesPerDim, bytesPerDim);
      state.index.pushRight();
      intersect(state, splitPackedValue, cellMaxPacked);
      state.index.pop();
    }
  }

  private long estimatePointCount(IntersectState state, byte[] cellMinPacked, byte[] cellMaxPacked) {

    /*
    System.out.println("\nR: intersect nodeID=" + state.index.getNodeID());
    for(int dim=0;dim<numDims;dim++) {
      System.out.println("  dim=" + dim + "\n    cellMin=" + new BytesRef(cellMinPacked, dim*bytesPerDim, bytesPerDim) + "\n    cellMax=" + new BytesRef(cellMaxPacked, dim*bytesPerDim, bytesPerDim));
    }
    */

    Relation r = state.visitor.compare(cellMinPacked, cellMaxPacked);

    if (r == Relation.CELL_OUTSIDE_QUERY) {
      // This cell is fully outside of the query shape: stop recursing
      return 0L;
    } else if (r == Relation.CELL_INSIDE_QUERY) {
      return (long) maxPointsInLeafNode * state.index.getNumLeaves();
    } else if (state.index.isLeafNode()) {
      // Assume half the points matched
      return (maxPointsInLeafNode + 1) / 2;
    } else {
      
      // Non-leaf node: recurse on the split left and right nodes
      int splitDim = state.index.getSplitDim();
      assert splitDim >= 0: "splitDim=" + splitDim;
      assert splitDim < numDims;

      byte[] splitPackedValue = state.index.getSplitPackedValue();
      BytesRef splitDimValue = state.index.getSplitDimValue();
      assert splitDimValue.length == bytesPerDim;
      //System.out.println("  splitDimValue=" + splitDimValue + " splitDim=" + splitDim);

      // make sure cellMin <= splitValue <= cellMax:
      assert StringHelper.compare(bytesPerDim, cellMinPacked, splitDim*bytesPerDim, splitDimValue.bytes, splitDimValue.offset) <= 0: "bytesPerDim=" + bytesPerDim + " splitDim=" + splitDim + " numDims=" + numDims;
      assert StringHelper.compare(bytesPerDim, cellMaxPacked, splitDim*bytesPerDim, splitDimValue.bytes, splitDimValue.offset) >= 0: "bytesPerDim=" + bytesPerDim + " splitDim=" + splitDim + " numDims=" + numDims;

      // Recurse on left sub-tree:
      System.arraycopy(cellMaxPacked, 0, splitPackedValue, 0, packedBytesLength);
      System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, splitDim*bytesPerDim, bytesPerDim);
      state.index.pushLeft();
      final long leftCost = estimatePointCount(state, cellMinPacked, splitPackedValue);
      state.index.pop();

      // Restore the split dim value since it may have been overwritten while recursing:
      System.arraycopy(splitPackedValue, splitDim*bytesPerDim, splitDimValue.bytes, splitDimValue.offset, bytesPerDim);

      // Recurse on right sub-tree:
      System.arraycopy(cellMinPacked, 0, splitPackedValue, 0, packedBytesLength);
      System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, splitDim*bytesPerDim, bytesPerDim);
      state.index.pushRight();
      final long rightCost = estimatePointCount(state, splitPackedValue, cellMaxPacked);
      state.index.pop();
      return leftCost + rightCost;
    }
  }

  @Override
  public long ramBytesUsed() {
    if (packedIndex != null) {
      return packedIndex.length;
    } else {
      return RamUsageEstimator.sizeOf(splitPackedValues) + RamUsageEstimator.sizeOf(leafBlockFPs);
    }
  }

  public byte[] getMinPackedValue() {
    return minPackedValue.clone();
  }

  public byte[] getMaxPackedValue() {
    return maxPackedValue.clone();
  }

  public int getNumDimensions() {
    return numDims;
  }

  public int getBytesPerDimension() {
    return bytesPerDim;
  }

  public long getPointCount() {
    return pointCount;
  }

  public int getDocCount() {
    return docCount;
  }

  public boolean isLeafNode(int nodeID) {
    return nodeID >= leafNodeOffset;
  }
}
