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
import java.util.Arrays;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.MathUtil;

/**
 * Handles reading a block KD-tree previously written with {@link BKDWriter}.
 *
 * @lucene.experimental
 */
public class BKDDefaultIndexInput implements BKDIndexInput {

  final BKDConfig config;
  final int numLeaves;
  // Packed array of byte[] holding all docs and values:
  final IndexInput in;
  final byte[] minPackedValue;
  final byte[] maxPackedValue;
  final long pointCount;
  final int docCount;
  final int version;
  final long minLeafBlockFP;
  // Packed array of byte[] holding all split values in the full binary tree:
  private final IndexInput packedIndex;

  /**
   * Caller must pre-seek the provided {@link IndexInput} to the index location that {@link
   * BKDWriter#finish} returned. BKD tree is always stored off-heap.
   */
  public BKDDefaultIndexInput(IndexInput metaIn, IndexInput indexIn, IndexInput dataIn)
      throws IOException {
    version =
        CodecUtil.checkHeader(
            metaIn, BKDWriter.CODEC_NAME, BKDWriter.VERSION_START, BKDWriter.VERSION_CURRENT);
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

    minPackedValue = new byte[config.packedIndexBytesLength];
    maxPackedValue = new byte[config.packedIndexBytesLength];

    metaIn.readBytes(minPackedValue, 0, config.packedIndexBytesLength);
    metaIn.readBytes(maxPackedValue, 0, config.packedIndexBytesLength);

    for (int dim = 0; dim < config.numIndexDims; dim++) {
      if (Arrays.compareUnsigned(
              minPackedValue,
              dim * config.bytesPerDim,
              dim * config.bytesPerDim + config.bytesPerDim,
              maxPackedValue,
              dim * config.bytesPerDim,
              dim * config.bytesPerDim + config.bytesPerDim)
          > 0) {
        throw new CorruptIndexException(
            "minPackedValue "
                + new BytesRef(minPackedValue)
                + " is > maxPackedValue "
                + new BytesRef(maxPackedValue)
                + " for dim="
                + dim,
            metaIn);
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

  @Override
  public BKDConfig getConfig() {
    return config;
  }

  @Override
  public byte[] getMinPackedValue() {
    return minPackedValue;
  }

  @Override
  public byte[] getMaxPackedValue() {
    return maxPackedValue;
  }

  @Override
  public long getPointCount() {
    return pointCount;
  }

  @Override
  public int getDocCount() {
    return docCount;
  }

  @Override
  public BKDIndexInput.IndexTree getIndexTree() {
    return new IndexTree(
        packedIndex.clone(),
        this.in.clone(),
        config,
        numLeaves,
        version,
        minPackedValue,
        maxPackedValue);
  }

  @Override
  public LeafIterator getLeafTreeIterator() throws IOException {
    final IndexInput input = in.clone();
    final IndexTree indexTree =
        new IndexTree(
            packedIndex.clone(), input, config, numLeaves, version, minPackedValue, maxPackedValue);
    input.seek(minLeafBlockFP);
    return new LeafIterator() {
      int leaf = 0;

      @Override
      public boolean visitNextLeaf(PointValues.IntersectVisitor visitor) throws IOException {
        if (leaf < numLeaves) {
          indexTree.visitDocValues(visitor, input.getFilePointer());
          leaf++;
          return true;
        }
        return false;
      }
    };
  }

  private static class IndexTree implements BKDIndexInput.IndexTree {
    private int nodeID;
    // level is 1-based so that we can do level-1 w/o checking each time:
    private int level;
    // used to read the packed tree off-heap
    private final IndexInput innerNodes;
    // used to read the packed leaves off-heap
    private final IndexInput leafNodes;
    // holds the minimum (left most) leaf block file pointer for each level we've recursed to:
    private final long[] leafBlockFPStack;
    // holds the address, in the off-heap index, of the right-node of each level:
    private final int[] rightNodePositions;
    // holds the splitDim for each level:
    private final int[] splitDims;
    // true if the per-dim delta we read for the node at this level is a negative offset vs. the
    // last split on this dim; this is a packed
    // 2D array, i.e. to access array[level][dim] you read from negativeDeltas[level*numDims+dim].
    // this will be true if the last time we
    // split on this dimension, we next pushed to the left sub-tree:
    private final boolean[] negativeDeltas;
    // holds the packed per-level split values
    private final byte[][] splitValuesStack;
    // holds the packed per-level min/max values for all dimensions.
    private final byte[][] minPackedValueStack, maxPackedValueStack;
    // tree parameters
    private final BKDConfig config;
    // number of leaves
    private final int leafNodeOffset;
    // version of the index
    private final int version;
    // helper objects for reading doc values
    private final byte[] scratchDataPackedValue,
        scratchMinIndexPackedValue,
        scratchMaxIndexPackedValue;
    private final int[] commonPrefixLengths;
    private final BKDReaderDocIDSetIterator scratchIterator;

    private IndexTree(
        IndexInput innerNodes,
        IndexInput leafNodes,
        BKDConfig config,
        int numLeaves,
        int version,
        byte[] minPackedValue,
        byte[] maxPackedValue) {
      this(
          innerNodes,
          leafNodes,
          config,
          numLeaves,
          version,
          1,
          1,
          minPackedValue,
          maxPackedValue,
          new BKDReaderDocIDSetIterator(config.maxPointsInLeafNode),
          new byte[config.packedBytesLength],
          new byte[config.packedIndexBytesLength],
          new byte[config.packedIndexBytesLength],
          new int[config.numDims]);
      // read root node
      readNodeData(false);
    }

    private IndexTree(
        IndexInput innerNodes,
        IndexInput leafNodes,
        BKDConfig config,
        int numLeaves,
        int version,
        int nodeID,
        int level,
        byte[] minPackedValue,
        byte[] maxPackedValue,
        BKDReaderDocIDSetIterator scratchIterator,
        byte[] scratchDataPackedValue,
        byte[] scratchMinIndexPackedValue,
        byte[] scratchMaxIndexPackedValue,
        int[] commonPrefixLengths) {
      this.config = config;
      this.version = version;
      this.nodeID = nodeID;
      this.level = level;
      leafNodeOffset = numLeaves;
      this.innerNodes = innerNodes;
      this.leafNodes = leafNodes;
      // stack arrays that keep information at different levels
      int treeDepth = getTreeDepth(numLeaves);
      minPackedValueStack = new byte[treeDepth + 1][];
      minPackedValueStack[level] = minPackedValue.clone();
      maxPackedValueStack = new byte[treeDepth + 1][];
      maxPackedValueStack[level] = maxPackedValue.clone();
      splitValuesStack = new byte[treeDepth + 1][];
      splitValuesStack[0] = new byte[config.packedIndexBytesLength];
      leafBlockFPStack = new long[treeDepth + 1];
      rightNodePositions = new int[treeDepth + 1];
      splitDims = new int[treeDepth + 1];
      negativeDeltas = new boolean[config.numIndexDims * (treeDepth + 1)];
      // scratch objects
      this.scratchIterator = scratchIterator;
      this.commonPrefixLengths = commonPrefixLengths;
      this.scratchDataPackedValue = scratchDataPackedValue;
      this.scratchMinIndexPackedValue = scratchMinIndexPackedValue;
      this.scratchMaxIndexPackedValue = scratchMaxIndexPackedValue;
    }

    @Override
    public BKDIndexInput.IndexTree clone() {
      BKDDefaultIndexInput.IndexTree index =
          new BKDDefaultIndexInput.IndexTree(
              innerNodes.clone(),
              leafNodes.clone(),
              config,
              leafNodeOffset,
              version,
              nodeID,
              level,
              minPackedValueStack[level],
              maxPackedValueStack[level],
              scratchIterator,
              scratchDataPackedValue,
              scratchMinIndexPackedValue,
              scratchMaxIndexPackedValue,
              commonPrefixLengths);
      // copy node data
      index.leafBlockFPStack[index.level] = leafBlockFPStack[level];
      index.rightNodePositions[index.level] = rightNodePositions[level];
      if (isLeafNode() == false) {
        index.splitValuesStack[index.level] = splitValuesStack[level].clone();
      }
      System.arraycopy(
          negativeDeltas,
          level * config.numIndexDims,
          index.negativeDeltas,
          level * config.numIndexDims,
          config.numIndexDims);
      index.splitDims[level] = splitDims[level];
      return index;
    }

    @Override
    public byte[] getMinPackedValue() {
      return minPackedValueStack[level];
    }

    @Override
    public byte[] getMaxPackedValue() {
      return maxPackedValueStack[level];
    }

    @Override
    public boolean moveToChild() {
      if (isLeafNode()) {
        return false;
      }
      pushLeft();
      return true;
    }

    private void pushLeft() {
      final int splitDimPos = splitDims[level] * config.bytesPerDim;
      assert Arrays.compareUnsigned(
                  maxPackedValueStack[level],
                  splitDimPos,
                  splitDimPos + config.bytesPerDim,
                  splitValuesStack[level],
                  splitDimPos,
                  splitDimPos + config.bytesPerDim)
              >= 0
          : "config.bytesPerDim="
              + config.bytesPerDim
              + " splitDim="
              + splitDims[level]
              + " config.numIndexDims="
              + config.numIndexDims
              + " config.numDims="
              + config.numDims;
      nodeID *= 2;
      level++;
      if (minPackedValueStack[level] == null) {
        minPackedValueStack[level] = minPackedValueStack[level - 1].clone();
        maxPackedValueStack[level] = maxPackedValueStack[level - 1].clone();
      } else {
        System.arraycopy(
            maxPackedValueStack[level - 1],
            0,
            maxPackedValueStack[level],
            0,
            config.packedIndexBytesLength);
        System.arraycopy(
            minPackedValueStack[level - 1],
            0,
            minPackedValueStack[level],
            0,
            config.packedIndexBytesLength);
      }
      // add the split dim value:
      System.arraycopy(
          splitValuesStack[level - 1],
          splitDimPos,
          maxPackedValueStack[level],
          splitDimPos,
          config.bytesPerDim);
      readNodeData(true);
    }

    private void pushRight() {
      final int splitDimPos = splitDims[level] * config.bytesPerDim;
      assert Arrays.compareUnsigned(
                  minPackedValueStack[level],
                  splitDimPos,
                  splitDimPos + config.bytesPerDim,
                  splitValuesStack[level],
                  splitDimPos,
                  splitDimPos + config.bytesPerDim)
              <= 0
          : "config.bytesPerDim="
              + config.bytesPerDim
              + " splitDim="
              + splitDims[level]
              + " config.numIndexDims="
              + config.numIndexDims
              + " config.numDims="
              + config.numDims;
      final int nodePosition = rightNodePositions[level];
      assert nodePosition >= innerNodes.getFilePointer()
          : "nodePosition = " + nodePosition + " < currentPosition=" + innerNodes.getFilePointer();
      nodeID = nodeID * 2 + 1;
      level++;
      try {
        innerNodes.seek(nodePosition);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      // we should have already called pushLeft,
      assert minPackedValueStack[level] != null;
      assert maxPackedValueStack[level] != null;
      // restore the split dim value:
      System.arraycopy(
          maxPackedValueStack[level - 1],
          splitDimPos,
          maxPackedValueStack[level],
          splitDimPos,
          config.bytesPerDim);
      // add the split dim value:
      System.arraycopy(
          splitValuesStack[level - 1],
          splitDimPos,
          minPackedValueStack[level],
          splitDimPos,
          config.bytesPerDim);
      readNodeData(false);
    }

    @Override
    public boolean moveToSibling() {
      if (nodeID % 2 == 0) {
        pop();
        pushRight();
        assert nodeExists();
        return true;
      }
      return false;
    }

    private void pop() {
      nodeID /= 2;
      level--;
    }

    @Override
    public boolean moveToParent() {
      if (nodeID == 1) {
        return false;
      }
      pop();
      return true;
    }

    private boolean isLeafNode() {
      return nodeID >= leafNodeOffset;
    }

    private boolean nodeExists() {
      return nodeID - leafNodeOffset < leafNodeOffset;
    }

    /** Only valid after pushLeft or pushRight, not pop! */
    private long getLeafBlockFP() {
      assert isLeafNode() : "nodeID=" + nodeID + " is not a leaf";
      return leafBlockFPStack[level];
    }

    @Override
    public long size() {
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
      return (long) numLeaves * config.maxPointsInLeafNode;
    }

    @Override
    public void visitDocIDs(PointValues.IntersectVisitor visitor) throws IOException {
      // Leaf node
      leafNodes.seek(getLeafBlockFP());
      // How many points are stored in this leaf cell:
      int count = leafNodes.readVInt();
      // No need to call grow(), it has been called up-front
      DocIdsWriter.readInts(leafNodes, count, visitor);
    }

    @Override
    public void visitDocValues(PointValues.IntersectVisitor visitor) throws IOException {
      visitDocValues(visitor, getLeafBlockFP());
    }

    private void visitDocValues(PointValues.IntersectVisitor visitor, long fp) throws IOException {
      // Leaf node; scan and filter all points in this block:
      int count = readDocIDs(leafNodes, fp, scratchIterator);
      if (version >= BKDWriter.VERSION_LOW_CARDINALITY_LEAVES) {
        visitDocValuesWithCardinality(
            commonPrefixLengths,
            scratchDataPackedValue,
            scratchMinIndexPackedValue,
            scratchMaxIndexPackedValue,
            leafNodes,
            scratchIterator,
            count,
            visitor);
      } else {
        visitDocValuesNoCardinality(
            commonPrefixLengths,
            scratchDataPackedValue,
            scratchMinIndexPackedValue,
            scratchMaxIndexPackedValue,
            leafNodes,
            scratchIterator,
            count,
            visitor);
      }
    }

    private int readDocIDs(IndexInput in, long blockFP, BKDReaderDocIDSetIterator iterator)
        throws IOException {
      in.seek(blockFP);
      // How many points are stored in this leaf cell:
      int count = in.readVInt();

      DocIdsWriter.readInts(in, count, iterator.docIDs);

      return count;
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
      System.arraycopy(
          negativeDeltas,
          (level - 1) * config.numIndexDims,
          negativeDeltas,
          level * config.numIndexDims,
          config.numIndexDims);
      negativeDeltas[level * config.numIndexDims + splitDims[level - 1]] = isLeft;

      try {
        leafBlockFPStack[level] = leafBlockFPStack[level - 1];

        // read leaf block FP delta
        if (isLeft == false) {
          leafBlockFPStack[level] += innerNodes.readVLong();
        }

        if (isLeafNode() == false) {
          // read split dim, prefix, firstDiffByteDelta encoded as int:
          int code = innerNodes.readVInt();
          splitDims[level] = code % config.numIndexDims;
          ;
          code /= config.numIndexDims;
          int prefix = code % (1 + config.bytesPerDim);
          int suffix = config.bytesPerDim - prefix;

          if (splitValuesStack[level] == null) {
            splitValuesStack[level] = new byte[config.packedIndexBytesLength];
          }
          System.arraycopy(
              splitValuesStack[level - 1],
              0,
              splitValuesStack[level],
              0,
              config.packedIndexBytesLength);
          if (suffix > 0) {
            int firstDiffByteDelta = code / (1 + config.bytesPerDim);
            if (negativeDeltas[level * config.numIndexDims + splitDims[level]]) {
              firstDiffByteDelta = -firstDiffByteDelta;
            }
            int oldByte =
                splitValuesStack[level][splitDims[level] * config.bytesPerDim + prefix] & 0xFF;
            splitValuesStack[level][splitDims[level] * config.bytesPerDim + prefix] =
                (byte) (oldByte + firstDiffByteDelta);
            innerNodes.readBytes(
                splitValuesStack[level],
                splitDims[level] * config.bytesPerDim + prefix + 1,
                suffix - 1);
          } else {
            // our split value is == last split value in this dim, which can happen when there are
            // many duplicate values
          }

          int leftNumBytes;
          if (nodeID * 2 < leafNodeOffset) {
            leftNumBytes = innerNodes.readVInt();
          } else {
            leftNumBytes = 0;
          }
          rightNodePositions[level] = Math.toIntExact(innerNodes.getFilePointer()) + leftNumBytes;
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private int getTreeDepth(int numLeaves) {
      // First +1 because all the non-leave nodes makes another power
      // of 2; e.g. to have a fully balanced tree with 4 leaves you
      // need a depth=3 tree:

      // Second +1 because MathUtil.log computes floor of the logarithm; e.g.
      // with 5 leaves you need a depth=4 tree:
      return MathUtil.log(numLeaves, 2) + 2;
    }

    private void visitDocValuesNoCardinality(
        int[] commonPrefixLengths,
        byte[] scratchDataPackedValue,
        byte[] scratchMinIndexPackedValue,
        byte[] scratchMaxIndexPackedValue,
        IndexInput in,
        BKDReaderDocIDSetIterator scratchIterator,
        int count,
        PointValues.IntersectVisitor visitor)
        throws IOException {
      readCommonPrefixes(commonPrefixLengths, scratchDataPackedValue, in);

      if (config.numIndexDims != 1 && version >= BKDWriter.VERSION_LEAF_STORES_BOUNDS) {
        byte[] minPackedValue = scratchMinIndexPackedValue;
        System.arraycopy(
            scratchDataPackedValue, 0, minPackedValue, 0, config.packedIndexBytesLength);
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
        PointValues.Relation r = visitor.compare(minPackedValue, maxPackedValue);
        if (r == PointValues.Relation.CELL_OUTSIDE_QUERY) {
          return;
        }
        visitor.grow(count);

        if (r == PointValues.Relation.CELL_INSIDE_QUERY) {
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
        visitCompressedDocValues(
            commonPrefixLengths,
            scratchDataPackedValue,
            in,
            scratchIterator,
            count,
            visitor,
            compressedDim);
      }
    }

    private void visitDocValuesWithCardinality(
        int[] commonPrefixLengths,
        byte[] scratchDataPackedValue,
        byte[] scratchMinIndexPackedValue,
        byte[] scratchMaxIndexPackedValue,
        IndexInput in,
        BKDReaderDocIDSetIterator scratchIterator,
        int count,
        PointValues.IntersectVisitor visitor)
        throws IOException {

      readCommonPrefixes(commonPrefixLengths, scratchDataPackedValue, in);
      int compressedDim = readCompressedDim(in);
      if (compressedDim == -1) {
        // all values are the same
        visitor.grow(count);
        visitUniqueRawDocValues(scratchDataPackedValue, scratchIterator, count, visitor);
      } else {
        if (config.numIndexDims != 1) {
          byte[] minPackedValue = scratchMinIndexPackedValue;
          System.arraycopy(
              scratchDataPackedValue, 0, minPackedValue, 0, config.packedIndexBytesLength);
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
          PointValues.Relation r = visitor.compare(minPackedValue, maxPackedValue);
          if (r == PointValues.Relation.CELL_OUTSIDE_QUERY) {
            return;
          }
          visitor.grow(count);

          if (r == PointValues.Relation.CELL_INSIDE_QUERY) {
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
          visitSparseRawDocValues(
              commonPrefixLengths, scratchDataPackedValue, in, scratchIterator, count, visitor);
        } else {
          // high cardinality
          visitCompressedDocValues(
              commonPrefixLengths,
              scratchDataPackedValue,
              in,
              scratchIterator,
              count,
              visitor,
              compressedDim);
        }
      }
    }

    private void readMinMax(
        int[] commonPrefixLengths, byte[] minPackedValue, byte[] maxPackedValue, IndexInput in)
        throws IOException {
      for (int dim = 0; dim < config.numIndexDims; dim++) {
        int prefix = commonPrefixLengths[dim];
        in.readBytes(
            minPackedValue, dim * config.bytesPerDim + prefix, config.bytesPerDim - prefix);
        in.readBytes(
            maxPackedValue, dim * config.bytesPerDim + prefix, config.bytesPerDim - prefix);
      }
    }

    // read cardinality and point
    private void visitSparseRawDocValues(
        int[] commonPrefixLengths,
        byte[] scratchPackedValue,
        IndexInput in,
        BKDReaderDocIDSetIterator scratchIterator,
        int count,
        PointValues.IntersectVisitor visitor)
        throws IOException {
      int i;
      for (i = 0; i < count; ) {
        int length = in.readVInt();
        for (int dim = 0; dim < config.numDims; dim++) {
          int prefix = commonPrefixLengths[dim];
          in.readBytes(
              scratchPackedValue, dim * config.bytesPerDim + prefix, config.bytesPerDim - prefix);
        }
        scratchIterator.reset(i, length);
        visitor.visit(scratchIterator, scratchPackedValue);
        i += length;
      }
      if (i != count) {
        throw new CorruptIndexException(
            "Sub blocks do not add up to the expected count: " + count + " != " + i, in);
      }
    }

    // point is under commonPrefix
    private void visitUniqueRawDocValues(
        byte[] scratchPackedValue,
        BKDReaderDocIDSetIterator scratchIterator,
        int count,
        PointValues.IntersectVisitor visitor)
        throws IOException {
      scratchIterator.reset(0, count);
      visitor.visit(scratchIterator, scratchPackedValue);
    }

    private void visitCompressedDocValues(
        int[] commonPrefixLengths,
        byte[] scratchPackedValue,
        IndexInput in,
        BKDReaderDocIDSetIterator scratchIterator,
        int count,
        PointValues.IntersectVisitor visitor,
        int compressedDim)
        throws IOException {
      // the byte at `compressedByteOffset` is compressed using run-length compression,
      // other suffix bytes are stored verbatim
      final int compressedByteOffset =
          compressedDim * config.bytesPerDim + commonPrefixLengths[compressedDim];
      commonPrefixLengths[compressedDim]++;
      int i;
      for (i = 0; i < count; ) {
        scratchPackedValue[compressedByteOffset] = in.readByte();
        final int runLen = Byte.toUnsignedInt(in.readByte());
        for (int j = 0; j < runLen; ++j) {
          for (int dim = 0; dim < config.numDims; dim++) {
            int prefix = commonPrefixLengths[dim];
            in.readBytes(
                scratchPackedValue, dim * config.bytesPerDim + prefix, config.bytesPerDim - prefix);
          }
          visitor.visit(scratchIterator.docIDs[i + j], scratchPackedValue);
        }
        i += runLen;
      }
      if (i != count) {
        throw new CorruptIndexException(
            "Sub blocks do not add up to the expected count: " + count + " != " + i, in);
      }
    }

    private int readCompressedDim(IndexInput in) throws IOException {
      int compressedDim = in.readByte();
      if (compressedDim < -2
          || compressedDim >= config.numDims
          || (version < BKDWriter.VERSION_LOW_CARDINALITY_LEAVES && compressedDim == -2)) {
        throw new CorruptIndexException("Got compressedDim=" + compressedDim, in);
      }
      return compressedDim;
    }

    private void readCommonPrefixes(
        int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in) throws IOException {
      for (int dim = 0; dim < config.numDims; dim++) {
        int prefix = in.readVInt();
        commonPrefixLengths[dim] = prefix;
        if (prefix > 0) {
          in.readBytes(scratchPackedValue, dim * config.bytesPerDim, prefix);
        }
        // System.out.println("R: " + dim + " of " + numDims + " prefix=" + prefix);
      }
    }

    @Override
    public String toString() {
      return "nodeID=" + nodeID;
    }
  }

  /** Reusable {@link DocIdSetIterator} to handle low cardinality leaves. */
  private static class BKDReaderDocIDSetIterator extends DocIdSetIterator {

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

    private void reset(int offset, int length) {
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
