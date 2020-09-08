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
import java.io.UncheckedIOException;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.MathUtil;

/** Handles intersection of an multi-dimensional shape in byte[] space with a block KD-tree previously written with {@link BKDWriter}.
 *
 * @lucene.experimental */

public final class BKDReader extends PointValues {

  // Packed array of byte[] holding all split values in the full binary tree:
  final int leafNodeOffset;
  final BKDConfig config;
  final int numLeaves;
  final IndexInput in;
  final byte[] minPackedValue;
  final byte[] maxPackedValue;
  final long pointCount;
  final int docCount;
  final int version;
  final long minLeafBlockFP;

  final IndexInput packedIndex;

  /** Caller must pre-seek the provided {@link IndexInput} to the index location that {@link BKDWriter#finish} returned.
   * BKD tree is always stored off-heap. */
  public BKDReader(IndexInput metaIn, IndexInput indexIn, IndexInput dataIn) throws IOException {
    version = CodecUtil.checkHeader(metaIn, BKDWriter.CODEC_NAME, BKDWriter.VERSION_START, BKDWriter.VERSION_CURRENT);
    final int numDims = metaIn.readVInt();
    final int numIndexDims;
    if (version >= BKDWriter.VERSION_SELECTIVE_INDEXING) {
      numIndexDims = metaIn.readVInt();
    } else {
      numIndexDims = numDims;
    }
    final int maxPointsInLeafNode = metaIn.readVInt();
    final int bytesPerDim = metaIn.readVInt();
    config = new BKDConfig(numDims, numIndexDims, bytesPerDim, maxPointsInLeafNode);

    // Read index:
    numLeaves = metaIn.readVInt();
    assert numLeaves > 0;
    leafNodeOffset = numLeaves;

    minPackedValue = new byte[config.packedIndexBytesLength];
    maxPackedValue = new byte[config.packedIndexBytesLength];

    metaIn.readBytes(minPackedValue, 0, config.packedIndexBytesLength);
    metaIn.readBytes(maxPackedValue, 0, config.packedIndexBytesLength);

    for(int dim=0;dim<config.numIndexDims;dim++) {
      if (FutureArrays.compareUnsigned(minPackedValue, dim * config.bytesPerDim, dim * config.bytesPerDim + config.bytesPerDim, maxPackedValue, dim * config.bytesPerDim, dim * config.bytesPerDim + config.bytesPerDim) > 0) {
        throw new CorruptIndexException("minPackedValue " + new BytesRef(minPackedValue) + " is > maxPackedValue " + new BytesRef(maxPackedValue) + " for dim=" + dim, metaIn);
      }
    }

    pointCount = metaIn.readVLong();
    docCount = metaIn.readVInt();

    int numIndexBytes = metaIn.readVInt();
    long indexStartPointer;
    if (version >= BKDWriter.VERSION_META_FILE) {
      minLeafBlockFP = metaIn.readLong();
      indexStartPointer = metaIn.readLong();
    } else {
      indexStartPointer = indexIn.getFilePointer();
      minLeafBlockFP = indexIn.readVLong();
      indexIn.seek(indexStartPointer);
    }
    this.packedIndex = indexIn.slice("packedIndex", indexStartPointer, numIndexBytes);
    this.in = dataIn;
  }

  long getMinLeafBlockFP() {
    return minLeafBlockFP;
  }

  /** Used to walk the off-heap index. The format takes advantage of the limited
   *  access pattern to the BKD tree at search time, i.e. starting at the root
   *  node and recursing downwards one child at a time.
   *  @lucene.internal */
  public class IndexTree implements Cloneable {
    private int nodeID;
    // level is 1-based so that we can do level-1 w/o checking each time:
    private int level;
    private int splitDim;
    private final byte[][] splitPackedValueStack;
    // used to read the packed tree off-heap
    private final IndexInput in;
    // holds the minimum (left most) leaf block file pointer for each level we've recursed to:
    private final long[] leafBlockFPStack;
    // holds the address, in the off-heap index, of the right-node of each level:
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

    IndexTree() {
      this(packedIndex.clone(), 1, 1);
      // read root node
      readNodeData(false);
    }

    private IndexTree(IndexInput in, int nodeID, int level) {
      int treeDepth = getTreeDepth();
      splitPackedValueStack = new byte[treeDepth+1][];
      this.nodeID = nodeID;
      this.level = level;
      splitPackedValueStack[level] = new byte[config.packedIndexBytesLength];
      leafBlockFPStack = new long[treeDepth+1];
      rightNodePositions = new int[treeDepth+1];
      splitValuesStack = new byte[treeDepth+1][];
      splitDims = new int[treeDepth+1];
      negativeDeltas = new boolean[config.numIndexDims*(treeDepth+1)];
      this.in = in;
      splitValuesStack[0] = new byte[config.packedIndexBytesLength];
      scratch = new BytesRef();
      scratch.length = config.bytesPerDim;
    }

    public void pushLeft() {
      nodeID *= 2;
      level++;
      readNodeData(true);
    }

    /** Clone, but you are not allowed to pop up past the point where the clone happened. */
    @Override
    public IndexTree clone() {
      IndexTree index = new IndexTree(in.clone(), nodeID, level);
      // copy node data
      index.splitDim = splitDim;
      index.leafBlockFPStack[level] = leafBlockFPStack[level];
      index.rightNodePositions[level] = rightNodePositions[level];
      index.splitValuesStack[index.level] = splitValuesStack[index.level].clone();
      System.arraycopy(negativeDeltas, level*config.numIndexDims, index.negativeDeltas, level*config.numIndexDims, config.numIndexDims);
      index.splitDims[level] = splitDims[level];
      return index;
    }

    public void pushRight() {
      final int nodePosition = rightNodePositions[level];
      assert nodePosition >= in.getFilePointer() : "nodePosition = " + nodePosition + " < currentPosition=" + in.getFilePointer();
      nodeID = nodeID * 2 + 1;
      level++;
      try {
        in.seek(nodePosition);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      readNodeData(false);
    }

    public void pop() {
      nodeID /= 2;
      level--;
      splitDim = splitDims[level];
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
    public BytesRef getSplitDimValue() {
      assert isLeafNode() == false;
      scratch.bytes = splitValuesStack[level];
      scratch.offset = splitDim * config.bytesPerDim;
      return scratch;
    }

    /** Only valid after pushLeft or pushRight, not pop! */
    public long getLeafBlockFP() {
      assert isLeafNode(): "nodeID=" + nodeID + " is not a leaf";
      return leafBlockFPStack[level];
    }

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

    private void readNodeData(boolean isLeft) {
      if (splitPackedValueStack[level] == null) {
        splitPackedValueStack[level] = new byte[config.packedIndexBytesLength];
      }
      System.arraycopy(negativeDeltas, (level-1)*config.numIndexDims, negativeDeltas, level*config.numIndexDims, config.numIndexDims);
      assert splitDim != -1;
      negativeDeltas[level*config.numIndexDims+splitDim] = isLeft;

      try {
        leafBlockFPStack[level] = leafBlockFPStack[level - 1];

        // read leaf block FP delta
        if (isLeft == false) {
          leafBlockFPStack[level] += in.readVLong();
        }

        if (isLeafNode()) {
          splitDim = -1;
        } else {

          // read split dim, prefix, firstDiffByteDelta encoded as int:
          int code = in.readVInt();
          splitDim = code % config.numIndexDims;
          splitDims[level] = splitDim;
          code /= config.numIndexDims;
          int prefix = code % (1 + config.bytesPerDim);
          int suffix = config.bytesPerDim - prefix;

          if (splitValuesStack[level] == null) {
            splitValuesStack[level] = new byte[config.packedIndexBytesLength];
          }
          System.arraycopy(splitValuesStack[level - 1], 0, splitValuesStack[level], 0, config.packedIndexBytesLength);
          if (suffix > 0) {
            int firstDiffByteDelta = code / (1 + config.bytesPerDim);
            if (negativeDeltas[level * config.numIndexDims + splitDim]) {
              firstDiffByteDelta = -firstDiffByteDelta;
            }
            int oldByte = splitValuesStack[level][splitDim * config.bytesPerDim + prefix] & 0xFF;
            splitValuesStack[level][splitDim * config.bytesPerDim + prefix] = (byte) (oldByte + firstDiffByteDelta);
            in.readBytes(splitValuesStack[level], splitDim * config.bytesPerDim + prefix + 1, suffix - 1);
          } else {
            // our split value is == last split value in this dim, which can happen when there are many duplicate values
          }

          int leftNumBytes;
          if (nodeID * 2 < leafNodeOffset) {
            leftNumBytes = in.readVInt();
          } else {
            leftNumBytes = 0;
          }
          rightNodePositions[level] = Math.toIntExact(in.getFilePointer()) + leftNumBytes;
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
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
    final BKDReaderDocIDSetIterator scratchIterator;
    final byte[] scratchDataPackedValue, scratchMinIndexPackedValue, scratchMaxIndexPackedValue;
    final int[] commonPrefixLengths;

    final IntersectVisitor visitor;
    public final IndexTree index;

    public  IntersectState(IndexInput in,
                           BKDConfig config,
                           IntersectVisitor visitor,
                           IndexTree indexVisitor) {
      this.in = in;
      this.visitor = visitor;
      this.commonPrefixLengths = new int[config.numDims];
      this.scratchIterator = new BKDReaderDocIDSetIterator(config.maxPointsInLeafNode);
      this.scratchDataPackedValue = new byte[config.packedBytesLength];
      this.scratchMinIndexPackedValue = new byte[config.packedIndexBytesLength];
      this.scratchMaxIndexPackedValue = new byte[config.packedIndexBytesLength];
      this.index = indexVisitor;
    }
  }

  @Override
  public void intersect(IntersectVisitor visitor) throws IOException {
    intersect(getIntersectState(visitor), minPackedValue, maxPackedValue);
  }

  @Override
  public long estimatePointCount(IntersectVisitor visitor) {
    return estimatePointCount(getIntersectState(visitor), minPackedValue, maxPackedValue);
  }

  /** Fast path: this is called when the query box fully encompasses all cells under this node. */
  private void addAll(IntersectState state, boolean grown) throws IOException {
    //System.out.println("R: addAll nodeID=" + nodeID);

    if (grown == false) {
      final long maxPointCount = (long) config.maxPointsInLeafNode * state.index.getNumLeaves();
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
    IndexTree index = new IndexTree();
    return new IntersectState(in.clone(), config, visitor, index);
  }

  /** Visits all docIDs and packed values in a single leaf block */
  public void visitLeafBlockValues(IndexTree index, IntersectState state) throws IOException {

    // Leaf node; scan and filter all points in this block:
    int count = readDocIDs(state.in, index.getLeafBlockFP(), state.scratchIterator);

    // Again, this time reading values and checking with the visitor
    visitDocValues(state.commonPrefixLengths, state.scratchDataPackedValue, state.scratchMinIndexPackedValue, state.scratchMaxIndexPackedValue, state.in, state.scratchIterator, count, state.visitor);
  }

  private void visitDocIDs(IndexInput in, long blockFP, IntersectVisitor visitor) throws IOException {
    // Leaf node
    in.seek(blockFP);

    // How many points are stored in this leaf cell:
    int count = in.readVInt();
    // No need to call grow(), it has been called up-front

    DocIdsWriter.readInts(in, count, visitor);
  }

  int readDocIDs(IndexInput in, long blockFP, BKDReaderDocIDSetIterator iterator) throws IOException {
    in.seek(blockFP);

    // How many points are stored in this leaf cell:
    int count = in.readVInt();

    DocIdsWriter.readInts(in, count, iterator.docIDs);

    return count;
  }

  void visitDocValues(int[] commonPrefixLengths, byte[] scratchDataPackedValue, byte[] scratchMinIndexPackedValue, byte[] scratchMaxIndexPackedValue,
                      IndexInput in, BKDReaderDocIDSetIterator scratchIterator, int count, IntersectVisitor visitor) throws IOException {
    if (version >= BKDWriter.VERSION_LOW_CARDINALITY_LEAVES) {
      visitDocValuesWithCardinality(commonPrefixLengths, scratchDataPackedValue, scratchMinIndexPackedValue, scratchMaxIndexPackedValue, in, scratchIterator, count, visitor);
    } else {
      visitDocValuesNoCardinality(commonPrefixLengths, scratchDataPackedValue, scratchMinIndexPackedValue, scratchMaxIndexPackedValue, in, scratchIterator, count, visitor);
    }
  }

  void visitDocValuesNoCardinality(int[] commonPrefixLengths, byte[] scratchDataPackedValue, byte[] scratchMinIndexPackedValue, byte[] scratchMaxIndexPackedValue,
                                   IndexInput in, BKDReaderDocIDSetIterator scratchIterator, int count, IntersectVisitor visitor) throws IOException {
    readCommonPrefixes(commonPrefixLengths, scratchDataPackedValue, in);

    if (config.numIndexDims != 1 && version >= BKDWriter.VERSION_LEAF_STORES_BOUNDS) {
      byte[] minPackedValue = scratchMinIndexPackedValue;
      System.arraycopy(scratchDataPackedValue, 0, minPackedValue, 0, config.packedIndexBytesLength);
      byte[] maxPackedValue = scratchMaxIndexPackedValue;
      // Copy common prefixes before reading adjusted box
      System.arraycopy(minPackedValue, 0, maxPackedValue, 0, config.packedIndexBytesLength);
      readMinMax(commonPrefixLengths, minPackedValue, maxPackedValue, in);

      // The index gives us range of values for each dimension, but the actual range of values
      // might be much more narrow than what the index told us, so we double check the relation
      // here, which is cheap yet might help figure out that the block either entirely matches
      // or does not match at all. This is especially more likely in the case that there are
      // multiple dimensions that have correlation, ie. splitting on one dimension also
      // significantly changes the range of values in another dimension.
      Relation r = visitor.compare(minPackedValue, maxPackedValue);
      if (r == Relation.CELL_OUTSIDE_QUERY) {
        return;
      }
      visitor.grow(count);

      if (r == Relation.CELL_INSIDE_QUERY) {
        for (int i = 0; i < count; ++i) {
          visitor.visit(scratchIterator.docIDs[i]);
        }
        return;
      }
    } else {
      visitor.grow(count);
    }


    int compressedDim = readCompressedDim(in);

    if (compressedDim == -1) {
      visitUniqueRawDocValues(scratchDataPackedValue, scratchIterator, count, visitor);
    } else {
      visitCompressedDocValues(commonPrefixLengths, scratchDataPackedValue, in, scratchIterator, count, visitor, compressedDim);
    }
  }

  void visitDocValuesWithCardinality(int[] commonPrefixLengths, byte[] scratchDataPackedValue, byte[] scratchMinIndexPackedValue, byte[] scratchMaxIndexPackedValue,
                                     IndexInput in, BKDReaderDocIDSetIterator scratchIterator, int count, IntersectVisitor visitor) throws IOException {

    readCommonPrefixes(commonPrefixLengths, scratchDataPackedValue, in);
    int compressedDim = readCompressedDim(in);
    if (compressedDim == -1) {
      // all values are the same
      visitor.grow(count);
      visitUniqueRawDocValues(scratchDataPackedValue, scratchIterator, count, visitor);
    } else {
      if (config.numIndexDims != 1) {
        byte[] minPackedValue = scratchMinIndexPackedValue;
        System.arraycopy(scratchDataPackedValue, 0, minPackedValue, 0, config.packedIndexBytesLength);
        byte[] maxPackedValue = scratchMaxIndexPackedValue;
        // Copy common prefixes before reading adjusted box
        System.arraycopy(minPackedValue, 0, maxPackedValue, 0, config.packedIndexBytesLength);
        readMinMax(commonPrefixLengths, minPackedValue, maxPackedValue, in);

        // The index gives us range of values for each dimension, but the actual range of values
        // might be much more narrow than what the index told us, so we double check the relation
        // here, which is cheap yet might help figure out that the block either entirely matches
        // or does not match at all. This is especially more likely in the case that there are
        // multiple dimensions that have correlation, ie. splitting on one dimension also
        // significantly changes the range of values in another dimension.
        Relation r = visitor.compare(minPackedValue, maxPackedValue);
        if (r == Relation.CELL_OUTSIDE_QUERY) {
          return;
        }
        visitor.grow(count);

        if (r == Relation.CELL_INSIDE_QUERY) {
          for (int i = 0; i < count; ++i) {
            visitor.visit(scratchIterator.docIDs[i]);
          }
          return;
        }
      } else {
        visitor.grow(count);
      }
      if (compressedDim == -2) {
        // low cardinality values
        visitSparseRawDocValues(commonPrefixLengths, scratchDataPackedValue, in, scratchIterator, count, visitor);
      } else {
        // high cardinality
        visitCompressedDocValues(commonPrefixLengths, scratchDataPackedValue, in, scratchIterator, count, visitor, compressedDim);
      }
    }
  }

  private void readMinMax(int[] commonPrefixLengths, byte[] minPackedValue, byte[] maxPackedValue, IndexInput in) throws IOException {
    for (int dim = 0; dim < config.numIndexDims; dim++) {
      int prefix = commonPrefixLengths[dim];
      in.readBytes(minPackedValue, dim * config.bytesPerDim + prefix, config.bytesPerDim - prefix);
      in.readBytes(maxPackedValue, dim * config.bytesPerDim + prefix, config.bytesPerDim - prefix);
    }
  }

  // read cardinality and point
  private void visitSparseRawDocValues(int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in, BKDReaderDocIDSetIterator scratchIterator, int count, IntersectVisitor visitor) throws IOException {
    int i;
    for (i = 0; i < count;) {
      int length = in.readVInt();
      for(int dim = 0; dim < config.numDims; dim++) {
        int prefix = commonPrefixLengths[dim];
        in.readBytes(scratchPackedValue, dim*config.bytesPerDim + prefix, config.bytesPerDim - prefix);
      }
      scratchIterator.reset(i, length);
      visitor.visit(scratchIterator, scratchPackedValue);
      i += length;
    }
    if (i != count) {
      throw new CorruptIndexException("Sub blocks do not add up to the expected count: " + count + " != " + i, in);
    }
  }

  // point is under commonPrefix
  private void visitUniqueRawDocValues(byte[] scratchPackedValue, BKDReaderDocIDSetIterator scratchIterator, int count, IntersectVisitor visitor) throws IOException {
    scratchIterator.reset(0, count);
    visitor.visit(scratchIterator, scratchPackedValue);
  }

  private void visitCompressedDocValues(int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in, BKDReaderDocIDSetIterator scratchIterator, int count, IntersectVisitor visitor, int compressedDim) throws IOException {
    // the byte at `compressedByteOffset` is compressed using run-length compression,
    // other suffix bytes are stored verbatim
    final int compressedByteOffset = compressedDim * config.bytesPerDim + commonPrefixLengths[compressedDim];
    commonPrefixLengths[compressedDim]++;
    int i;
    for (i = 0; i < count; ) {
      scratchPackedValue[compressedByteOffset] = in.readByte();
      final int runLen = Byte.toUnsignedInt(in.readByte());
      for (int j = 0; j < runLen; ++j) {
        for(int dim = 0; dim < config.numDims; dim++) {
          int prefix = commonPrefixLengths[dim];
          in.readBytes(scratchPackedValue, dim*config.bytesPerDim + prefix, config.bytesPerDim - prefix);
        }
        visitor.visit(scratchIterator.docIDs[i+j], scratchPackedValue);
      }
      i += runLen;
    }
    if (i != count) {
      throw new CorruptIndexException("Sub blocks do not add up to the expected count: " + count + " != " + i, in);
    }
  }

  private int readCompressedDim(IndexInput in) throws IOException {
    int compressedDim = in.readByte();
    if (compressedDim < -2 || compressedDim >= config.numDims || (version < BKDWriter.VERSION_LOW_CARDINALITY_LEAVES && compressedDim == -2)) {
      throw new CorruptIndexException("Got compressedDim="+compressedDim, in);
    }
    return compressedDim;
  }

  private void readCommonPrefixes(int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in) throws IOException {
    for(int dim=0;dim<config.numDims;dim++) {
      int prefix = in.readVInt();
      commonPrefixLengths[dim] = prefix;
      if (prefix > 0) {
        in.readBytes(scratchPackedValue, dim*config.bytesPerDim, prefix);
      }
      //System.out.println("R: " + dim + " of " + numDims + " prefix=" + prefix);
    }
  }

  private void intersect(IntersectState state, byte[] cellMinPacked, byte[] cellMaxPacked) throws IOException {

    /*
    System.out.println("\nR: intersect nodeID=" + state.index.getNodeID());
    for(int dim=0;dim<numDims;dim++) {
      System.out.println("  dim=" + dim + "\n    cellMin=" + new BytesRef(cellMinPacked, dim*config.bytesPerDim, config.bytesPerDim) + "\n    cellMax=" + new BytesRef(cellMaxPacked, dim*config.bytesPerDim, config.bytesPerDim));
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
        int count = readDocIDs(state.in, state.index.getLeafBlockFP(), state.scratchIterator);

        // Again, this time reading values and checking with the visitor
        visitDocValues(state.commonPrefixLengths, state.scratchDataPackedValue, state.scratchMinIndexPackedValue, state.scratchMaxIndexPackedValue, state.in, state.scratchIterator, count, state.visitor);
      }

    } else {

      // Non-leaf node: recurse on the split left and right nodes
      int splitDim = state.index.getSplitDim();
      assert splitDim >= 0: "splitDim=" + splitDim + ", config.numIndexDims=" + config.numIndexDims;
      assert splitDim < config.numIndexDims: "splitDim=" + splitDim + ", config.numIndexDims=" + config.numIndexDims;

      byte[] splitPackedValue = state.index.getSplitPackedValue();
      BytesRef splitDimValue = state.index.getSplitDimValue();
      assert splitDimValue.length == config.bytesPerDim;
      //System.out.println("  splitDimValue=" + splitDimValue + " splitDim=" + splitDim);

      // make sure cellMin <= splitValue <= cellMax:
      assert FutureArrays.compareUnsigned(cellMinPacked, splitDim * config.bytesPerDim, splitDim * config.bytesPerDim + config.bytesPerDim, splitDimValue.bytes, splitDimValue.offset, splitDimValue.offset + config.bytesPerDim) <= 0: "config.bytesPerDim=" + config.bytesPerDim + " splitDim=" + splitDim + " config.numIndexDims=" + config.numIndexDims + " config.numDims=" + config.numDims;
      assert FutureArrays.compareUnsigned(cellMaxPacked, splitDim * config.bytesPerDim, splitDim * config.bytesPerDim + config.bytesPerDim, splitDimValue.bytes, splitDimValue.offset, splitDimValue.offset + config.bytesPerDim) >= 0: "config.bytesPerDim=" + config.bytesPerDim + " splitDim=" + splitDim + " config.numIndexDims=" + config.numIndexDims + " config.numDims=" + config.numDims;

      // Recurse on left sub-tree:
      System.arraycopy(cellMaxPacked, 0, splitPackedValue, 0, config.packedIndexBytesLength);
      System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, splitDim*config.bytesPerDim, config.bytesPerDim);
      state.index.pushLeft();
      intersect(state, cellMinPacked, splitPackedValue);
      state.index.pop();

      // Restore the split dim value since it may have been overwritten while recursing:
      System.arraycopy(splitPackedValue, splitDim*config.bytesPerDim, splitDimValue.bytes, splitDimValue.offset, config.bytesPerDim);

      // Recurse on right sub-tree:
      System.arraycopy(cellMinPacked, 0, splitPackedValue, 0, config.packedIndexBytesLength);
      System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, splitDim*config.bytesPerDim, config.bytesPerDim);
      state.index.pushRight();
      intersect(state, splitPackedValue, cellMaxPacked);
      state.index.pop();
    }
  }

  private long estimatePointCount(IntersectState state, byte[] cellMinPacked, byte[] cellMaxPacked) {

    /*
    System.out.println("\nR: intersect nodeID=" + state.index.getNodeID());
    for(int dim=0;dim<numDims;dim++) {
      System.out.println("  dim=" + dim + "\n    cellMin=" + new BytesRef(cellMinPacked, dim*config.bytesPerDim, config.bytesPerDim) + "\n    cellMax=" + new BytesRef(cellMaxPacked, dim*config.bytesPerDim, config.bytesPerDim));
    }
    */

    Relation r = state.visitor.compare(cellMinPacked, cellMaxPacked);

    if (r == Relation.CELL_OUTSIDE_QUERY) {
      // This cell is fully outside of the query shape: stop recursing
      return 0L;
    } else if (r == Relation.CELL_INSIDE_QUERY) {
      return (long) config.maxPointsInLeafNode * state.index.getNumLeaves();
    } else if (state.index.isLeafNode()) {
      // Assume half the points matched
      return (config.maxPointsInLeafNode + 1) / 2;
    } else {

      // Non-leaf node: recurse on the split left and right nodes
      int splitDim = state.index.getSplitDim();
      assert splitDim >= 0: "splitDim=" + splitDim + ", config.numIndexDims=" + config.numIndexDims;
      assert splitDim < config.numIndexDims: "splitDim=" + splitDim + ", config.numIndexDims=" + config.numIndexDims;

      byte[] splitPackedValue = state.index.getSplitPackedValue();
      BytesRef splitDimValue = state.index.getSplitDimValue();
      assert splitDimValue.length == config.bytesPerDim;
      //System.out.println("  splitDimValue=" + splitDimValue + " splitDim=" + splitDim);

      // make sure cellMin <= splitValue <= cellMax:
      assert FutureArrays.compareUnsigned(cellMinPacked, splitDim * config.bytesPerDim, splitDim * config.bytesPerDim + config.bytesPerDim, splitDimValue.bytes, splitDimValue.offset, splitDimValue.offset + config.bytesPerDim) <= 0: "config.bytesPerDim=" + config.bytesPerDim + " splitDim=" + splitDim + " config.numIndexDims=" + config.numIndexDims + " config.numDims=" + config.numDims;
      assert FutureArrays.compareUnsigned(cellMaxPacked, splitDim * config.bytesPerDim, splitDim * config.bytesPerDim + config.bytesPerDim, splitDimValue.bytes, splitDimValue.offset, splitDimValue.offset + config.bytesPerDim) >= 0: "config.bytesPerDim=" + config.bytesPerDim + " splitDim=" + splitDim + " config.numIndexDims=" + config.numIndexDims + " config.numDims=" + config.numDims;

      // Recurse on left sub-tree:
      System.arraycopy(cellMaxPacked, 0, splitPackedValue, 0, config.packedIndexBytesLength);
      System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, splitDim*config.bytesPerDim, config.bytesPerDim);
      state.index.pushLeft();
      final long leftCost = estimatePointCount(state, cellMinPacked, splitPackedValue);
      state.index.pop();

      // Restore the split dim value since it may have been overwritten while recursing:
      System.arraycopy(splitPackedValue, splitDim*config.bytesPerDim, splitDimValue.bytes, splitDimValue.offset, config.bytesPerDim);

      // Recurse on right sub-tree:
      System.arraycopy(cellMinPacked, 0, splitPackedValue, 0, config.packedIndexBytesLength);
      System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, splitDim*config.bytesPerDim, config.bytesPerDim);
      state.index.pushRight();
      final long rightCost = estimatePointCount(state, splitPackedValue, cellMaxPacked);
      state.index.pop();
      return leftCost + rightCost;
    }
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
    return config.numDims;
  }

  @Override
  public int getNumIndexDimensions() {
    return config.numIndexDims;
  }

  @Override
  public int getBytesPerDimension() {
    return config.bytesPerDim;
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

  /**
   * Reusable {@link DocIdSetIterator} to handle low cardinality leaves. */
  protected static class BKDReaderDocIDSetIterator extends DocIdSetIterator {

    private int idx;
    private int length;
    private int offset;
    private int docID;
    final int[] docIDs;

    public BKDReaderDocIDSetIterator(int maxPointsInLeafNode) {
      this.docIDs = new int[maxPointsInLeafNode];
    }

    @Override
    public int docID() {
      return docID;
    }

    private void  reset(int offset, int length) {
      this.offset = offset;
      this.length = length;
      assert offset + length <= docIDs.length;
      this.docID = -1;
      this.idx = 0;
    }

    @Override
    public int nextDoc() throws IOException {
      if (idx == length) {
        docID = DocIdSetIterator.NO_MORE_DOCS;
      } else {
        docID = docIDs[offset + idx];
        idx++;
      }
      return docID;
    }

    @Override
    public int advance(int target) throws IOException {
      return slowAdvance(target);
    }

    @Override
    public long cost() {
      return length;
    }
  }
}
