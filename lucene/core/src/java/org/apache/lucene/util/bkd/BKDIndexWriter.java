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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/** Utility class providing methods to serialize a KD-tree.
 *
 * @lucene.experimental */
public class BKDIndexWriter {

  public static final String CODEC_NAME = "BKD";
  public static final int VERSION_START = 4; // version used by Lucene 7.0
  //public static final int VERSION_CURRENT = VERSION_START;
  public static final int VERSION_LEAF_STORES_BOUNDS = 5;
  public static final int VERSION_SELECTIVE_INDEXING = 6;
  public static final int VERSION_LOW_CARDINALITY_LEAVES = 7;
  public static final int VERSION_CURRENT = VERSION_LOW_CARDINALITY_LEAVES;

  /** BKD configuration */
  private final BKDConfig config;

  private final byte[] scratch;

  // Reused when writing leaf blocks
  private final ByteBuffersDataOutput scratchOut = ByteBuffersDataOutput.newResettableInstance();

  BKDIndexWriter(BKDConfig config) {
    this.config = config;
    this.scratch = new byte[config.packedBytesLength];
  }

  /** writes a leaf block in the provided DataOutput */
  public void writeLeafBlock(DataOutput out, BKDLeafBlock leafBlock,
                             int[] commonPrefixes, int sortedDim, int leafCardinality) throws IOException {
    assert leafBlock.count() > 0 : "count must be bigger than 0";
    assert leafBlock.count() <= config.maxPointsInLeafNode: "maxPointsInLeafNode=" + config.maxPointsInLeafNode + " > count=" + leafBlock.count();
    assert scratchOut.size() == 0;
    // Write docIDs first, as their own chunk, so that at intersect time we can add all docIDs w/o
    // loading the values:
    writeLeafBlockDocs(scratchOut, leafBlock);
    // Write common prefixes:
    writeCommonPrefixes(scratchOut, commonPrefixes, leafBlock.packedValue(0));
    // Write point values:
    writeLeafBlockPackedValues(scratchOut, leafBlock, commonPrefixes, sortedDim, leafCardinality);
    scratchOut.copyTo(out);
    scratchOut.reset();
  }

  /** writes inner nodes in the provided DataOutput */
  public void writeIndex(DataOutput out, int countPerLeaf, BKDWriter.BKDTreeLeafNodes leafNodes,
                         byte[] minPackedValue, byte[] maxPackedValue, long pointCount, int numberDocs) throws IOException {
    final byte[] packedIndex = packIndex(leafNodes);
    final int numLeaves = leafNodes.numLeaves();
    CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT);
    out.writeVInt(config.numDims);
    out.writeVInt(config.numIndexDims);
    out.writeVInt(countPerLeaf);
    out.writeVInt(config.bytesPerDim);

    assert numLeaves > 0;
    out.writeVInt(numLeaves);
    out.writeBytes(minPackedValue, 0, config.packedIndexBytesLength);
    out.writeBytes(maxPackedValue, 0, config.packedIndexBytesLength);

    out.writeVLong(pointCount);
    out.writeVInt(numberDocs);
    out.writeVInt(packedIndex.length);
    out.writeBytes(packedIndex, 0, packedIndex.length);
  }

  /** Packs the two arrays, representing a balanced binary tree, into a compact byte[] structure. */
  private byte[] packIndex(BKDWriter.BKDTreeLeafNodes leafNodes) throws IOException {
    // This is the "file" we append the byte[] to:
    List<byte[]> blocks = new ArrayList<>();
    byte[] lastSplitValues = new byte[config.packedIndexBytesLength];
    //System.out.println("\npack index");
    assert scratchOut.size() == 0;
    int totalSize = recursePackIndex(scratchOut, leafNodes, 0l, blocks, lastSplitValues, new boolean[config.numIndexDims], false,
        0, leafNodes.numLeaves());
    scratchOut.reset();
    // Compact the byte[] blocks into single byte index:
    byte[] index = new byte[totalSize];
    int upto = 0;
    for(byte[] block : blocks) {
      System.arraycopy(block, 0, index, upto, block.length);
      upto += block.length;
    }
    assert upto == totalSize;

    return index;
  }

  /**
   * lastSplitValues is per-dimension split value previously seen; we use this to prefix-code the split byte[] on each inner node
   */
  private int recursePackIndex(ByteBuffersDataOutput writeBuffer, BKDWriter.BKDTreeLeafNodes leafNodes, long minBlockFP, List<byte[]> blocks,
                               byte[] lastSplitValues, boolean[] negativeDeltas, boolean isLeft, int leavesOffset, int numLeaves) throws IOException {
    if (numLeaves == 1) {
      if (isLeft) {
        assert leafNodes.getLeafLP(leavesOffset) - minBlockFP == 0;
        return 0;
      } else {
        long delta = leafNodes.getLeafLP(leavesOffset) - minBlockFP;
        assert leafNodes.numLeaves() == numLeaves || delta > 0 : "expected delta > 0; got numLeaves =" + numLeaves + " and delta=" + delta;
        writeBuffer.writeVLong(delta);
        return appendBlock(writeBuffer, blocks);
      }
    } else {
      long leftBlockFP;
      if (isLeft) {
        // The left tree's left most leaf block FP is always the minimal FP:
        assert leafNodes.getLeafLP(leavesOffset) == minBlockFP;
        leftBlockFP = minBlockFP;
      } else {
        leftBlockFP = leafNodes.getLeafLP(leavesOffset);
        long delta = leftBlockFP - minBlockFP;
        assert leafNodes.numLeaves() == numLeaves || delta > 0 : "expected delta > 0; got numLeaves =" + numLeaves + " and delta=" + delta;
        writeBuffer.writeVLong(delta);
      }

      int numLeftLeafNodes = BKDWriter.getNumLeftLeafNodes(numLeaves);
      final int rightOffset = leavesOffset + numLeftLeafNodes;
      final int splitOffset = rightOffset - 1;

      int splitDim = leafNodes.getSplitDimension(splitOffset);
      BytesRef splitValue = leafNodes.getSplitValue(splitOffset);
      int address = splitValue.offset;

      //System.out.println("recursePack inner nodeID=" + nodeID + " splitDim=" + splitDim + " splitValue=" + new BytesRef(splitPackedValues, address, bytesPerDim));

      // find common prefix with last split value in this dim:
      int prefix = Arrays.mismatch(splitValue.bytes, address, address + config.bytesPerDim, lastSplitValues,
          splitDim * config.bytesPerDim, splitDim * config.bytesPerDim + config.bytesPerDim);
      if (prefix == -1) {
        prefix = config.bytesPerDim;
      }

      //System.out.println("writeNodeData nodeID=" + nodeID + " splitDim=" + splitDim + " numDims=" + numDims + " bytesPerDim=" + bytesPerDim + " prefix=" + prefix);

      int firstDiffByteDelta;
      if (prefix < config.bytesPerDim) {
        //System.out.println("  delta byte cur=" + Integer.toHexString(splitPackedValues[address+prefix]&0xFF) + " prev=" + Integer.toHexString(lastSplitValues[splitDim * bytesPerDim + prefix]&0xFF) + " negated?=" + negativeDeltas[splitDim]);
        firstDiffByteDelta = (splitValue.bytes[address+prefix]&0xFF) - (lastSplitValues[splitDim * config.bytesPerDim + prefix]&0xFF);
        if (negativeDeltas[splitDim]) {
          firstDiffByteDelta = -firstDiffByteDelta;
        }
        //System.out.println("  delta=" + firstDiffByteDelta);
        assert firstDiffByteDelta > 0;
      } else {
        firstDiffByteDelta = 0;
      }

      // pack the prefix, splitDim and delta first diff byte into a single vInt:
      int code = (firstDiffByteDelta * (1+config.bytesPerDim) + prefix) * config.numIndexDims + splitDim;

      //System.out.println("  code=" + code);
      //System.out.println("  splitValue=" + new BytesRef(splitPackedValues, address, bytesPerDim));

      writeBuffer.writeVInt(code);

      // write the split value, prefix coded vs. our parent's split value:
      int suffix = config.bytesPerDim - prefix;
      byte[] savSplitValue = new byte[suffix];
      if (suffix > 1) {
        writeBuffer.writeBytes(splitValue.bytes, address+prefix+1, suffix-1);
      }

      byte[] cmp = lastSplitValues.clone();

      System.arraycopy(lastSplitValues, splitDim * config.bytesPerDim + prefix, savSplitValue, 0, suffix);

      // copy our split value into lastSplitValues for our children to prefix-code against
      System.arraycopy(splitValue.bytes, address+prefix, lastSplitValues, splitDim * config.bytesPerDim + prefix, suffix);

      int numBytes = appendBlock(writeBuffer, blocks);

      // placeholder for left-tree numBytes; we need this so that at search time if we only need to recurse into the right sub-tree we can
      // quickly seek to its starting point
      int idxSav = blocks.size();
      blocks.add(null);

      boolean savNegativeDelta = negativeDeltas[splitDim];
      negativeDeltas[splitDim] = true;


      int leftNumBytes = recursePackIndex(writeBuffer, leafNodes, leftBlockFP, blocks, lastSplitValues, negativeDeltas, true,
          leavesOffset, numLeftLeafNodes);

      if (numLeftLeafNodes != 1) {
        writeBuffer.writeVInt(leftNumBytes);
      } else {
        assert leftNumBytes == 0: "leftNumBytes=" + leftNumBytes;
      }

      byte[] bytes2 = writeBuffer.toArrayCopy();
      writeBuffer.reset();
      // replace our placeholder:
      blocks.set(idxSav, bytes2);

      negativeDeltas[splitDim] = false;
      int rightNumBytes = recursePackIndex(writeBuffer,  leafNodes, leftBlockFP, blocks, lastSplitValues, negativeDeltas, false,
          rightOffset, numLeaves - numLeftLeafNodes);

      negativeDeltas[splitDim] = savNegativeDelta;

      // restore lastSplitValues to what caller originally passed us:
      System.arraycopy(savSplitValue, 0, lastSplitValues, splitDim * config.bytesPerDim + prefix, suffix);

      assert Arrays.equals(lastSplitValues, cmp);

      return numBytes + bytes2.length + leftNumBytes + rightNumBytes;
    }
  }

  /** Appends the current contents of writeBuffer as another block on the growing in-memory file */
  private int appendBlock(ByteBuffersDataOutput writeBuffer, List<byte[]> blocks) throws IOException {
    byte[] block = writeBuffer.toArrayCopy();
    blocks.add(block);
    writeBuffer.reset();
    return block.length;
  }

  private void writeLeafBlockDocs(DataOutput out, BKDLeafBlock leafBlock) throws IOException {
    out.writeVInt(leafBlock.count());
    DocIdsWriter.writeDocIds(leafBlock, out);
  }

  private void writeCommonPrefixes(DataOutput out, int[] commonPrefixes, BytesRef packedValue) throws IOException {
    for(int dim = 0; dim < config.numDims; dim++) {
      out.writeVInt(commonPrefixes[dim]);
      //System.out.println(commonPrefixes[dim] + " of " + bytesPerDim);
      out.writeBytes(packedValue.bytes, packedValue.offset + dim * config.bytesPerDim, commonPrefixes[dim]);
    }
  }

  private void writeLeafBlockPackedValues(DataOutput out, BKDLeafBlock packedValues, int[] commonPrefixLengths, int sortedDim, int leafCardinality) throws IOException {
    int prefixLenSum = Arrays.stream(commonPrefixLengths).sum();
    if (prefixLenSum == config.packedBytesLength) {
      // all values in this block are equal
      out.writeByte((byte) -1);
    } else {
      assert commonPrefixLengths[sortedDim] < config.bytesPerDim;
      final int count = packedValues.count();
      // estimate if storing the values with cardinality is cheaper than storing all values.
      int compressedByteOffset = sortedDim * config.bytesPerDim + commonPrefixLengths[sortedDim];
      int highCardinalityCost;
      int lowCardinalityCost;
      if (count == leafCardinality) {
        // all values in this block are different
        highCardinalityCost = 0;
        lowCardinalityCost = 1;
      } else {
        // compute cost of runLen compression
        int numRunLens = 0;
        for (int i = 0; i < count; ) {
          // do run-length compression on the byte at compressedByteOffset
          int runLen = runLen(packedValues, i, Math.min(i + 0xff, count), compressedByteOffset);
          assert runLen <= 0xff;
          numRunLens++;
          i += runLen;
        }
        // Add cost of runLen compression
        highCardinalityCost = count * (config.packedBytesLength - prefixLenSum - 1) + 2 * numRunLens;
        // +1 is the byte needed for storing the cardinality
        lowCardinalityCost = leafCardinality * (config.packedBytesLength - prefixLenSum + 1);
      }
      if (lowCardinalityCost <= highCardinalityCost) {
        out.writeByte((byte) -2);
        writeLowCardinalityLeafBlockPackedValues(out, packedValues, commonPrefixLengths);
      } else {
        out.writeByte((byte) sortedDim);
        writeHighCardinalityLeafBlockPackedValues(out, packedValues, commonPrefixLengths, sortedDim, compressedByteOffset);
      }
    }
  }

  private static int runLen(BKDLeafBlock packedValues, int start, int end, int byteOffset) {
    BytesRef first = packedValues.packedValue(start);
    byte b = first.bytes[first.offset + byteOffset];
    for (int i = start + 1; i < end; ++i) {
      BytesRef ref = packedValues.packedValue(i);
      byte b2 = ref.bytes[ref.offset + byteOffset];
      assert Byte.toUnsignedInt(b2) >= Byte.toUnsignedInt(b);
      if (b != b2) {
        return i - start;
      }
    }
    return end - start;
  }

  private void writeLowCardinalityLeafBlockPackedValues(DataOutput out, BKDLeafBlock packedValues, int[] commonPrefixLengths) throws IOException {
    if (config.numIndexDims != 1) {
      writeActualBounds(out, packedValues, commonPrefixLengths);
    }
    BytesRef value = packedValues.packedValue(0);
    System.arraycopy(value.bytes, value.offset, scratch, 0, config.packedBytesLength);
    int cardinality = 1;
    for (int i = 1; i < packedValues.count(); i++) {
      value = packedValues.packedValue(i);
      for(int dim = 0; dim < config.numDims; dim++) {
        final int start = dim * config.bytesPerDim + commonPrefixLengths[dim];
        final int end = dim * config.bytesPerDim + config.bytesPerDim;
        if (Arrays.mismatch(value.bytes, value.offset + start, value.offset + end, scratch, start, end) != -1) {
          out.writeVInt(cardinality);
          for (int j = 0; j < config.numDims; j++) {
            out.writeBytes(scratch, j * config.bytesPerDim + commonPrefixLengths[j], config.bytesPerDim - commonPrefixLengths[j]);
          }
          System.arraycopy(value.bytes, value.offset, scratch, 0, config.packedBytesLength);
          cardinality = 1;
          break;
        } else if (dim == config.numDims - 1){
          cardinality++;
        }
      }
    }
    out.writeVInt(cardinality);
    for (int i = 0; i < config.numDims; i++) {
      out.writeBytes(scratch, i * config.bytesPerDim + commonPrefixLengths[i], config.bytesPerDim - commonPrefixLengths[i]);
    }
  }

  private void writeActualBounds(DataOutput out, BKDLeafBlock packedValues, int[] commonPrefixLengths) throws IOException {
    for (int dim = 0; dim < config.numIndexDims; ++dim) {
      int commonPrefixLength = commonPrefixLengths[dim];
      int suffixLength = config.bytesPerDim - commonPrefixLength;
      if (suffixLength > 0) {
        BytesRef[] minMax = computeMinMax(packedValues, dim * config.bytesPerDim + commonPrefixLength, suffixLength);
        BytesRef min = minMax[0];
        BytesRef max = minMax[1];
        out.writeBytes(min.bytes, min.offset, min.length);
        out.writeBytes(max.bytes, max.offset, max.length);
      }
    }
  }

  /** Return an array that contains the min and max values for the [offset, offset+length] interval
   *  of the given {@link BytesRef}s. */
  private static BytesRef[] computeMinMax(BKDLeafBlock packedValues, int offset, int length) {
    assert length > 0;
    BytesRefBuilder min = new BytesRefBuilder();
    BytesRefBuilder max = new BytesRefBuilder();
    final BytesRef first = packedValues.packedValue(0);
    min.copyBytes(first.bytes, first.offset + offset, length);
    max.copyBytes(first.bytes, first.offset + offset, length);
    for (int i = 1; i < packedValues.count(); ++i) {
      BytesRef candidate = packedValues.packedValue(i);
      if (Arrays.compareUnsigned(min.bytes(), 0, length, candidate.bytes, candidate.offset + offset, candidate.offset + offset + length) > 0) {
        min.copyBytes(candidate.bytes, candidate.offset + offset, length);
      } else if (Arrays.compareUnsigned(max.bytes(), 0, length, candidate.bytes, candidate.offset + offset, candidate.offset + offset + length) < 0) {
        max.copyBytes(candidate.bytes, candidate.offset + offset, length);
      }
    }
    return new BytesRef[]{min.get(), max.get()};
  }

  private void writeLeafBlockPackedValuesRange(DataOutput out, BKDLeafBlock packedValues, int[] commonPrefixLengths, int start, int end) throws IOException {
    for (int i = start; i < end; ++i) {
      BytesRef ref = packedValues.packedValue(i);
      assert ref.length == config.packedBytesLength;

      for(int dim = 0; dim < config.numDims; dim++) {
        int prefix = commonPrefixLengths[dim];
        out.writeBytes(ref.bytes, ref.offset + dim * config.bytesPerDim + prefix, config.bytesPerDim - prefix);
      }
    }
  }

  private void writeHighCardinalityLeafBlockPackedValues(DataOutput out, BKDLeafBlock packedValues, int[] commonPrefixLengths, int sortedDim, int compressedByteOffset) throws IOException {
    if (config.numIndexDims != 1) {
      writeActualBounds(out, packedValues, commonPrefixLengths);
    }
    final int count  = packedValues.count();
    commonPrefixLengths[sortedDim]++;
    for (int i = 0; i < count; ) {
      // do run-length compression on the byte at compressedByteOffset
      int runLen = runLen(packedValues, i, Math.min(i + 0xff, count), compressedByteOffset);
      assert runLen <= 0xff;
      BytesRef first = packedValues.packedValue(i);
      byte prefixByte = first.bytes[first.offset + compressedByteOffset];
      out.writeByte(prefixByte);
      out.writeByte((byte) runLen);
      writeLeafBlockPackedValuesRange(out, packedValues, commonPrefixLengths, i, i + runLen);
      i += runLen;
      assert i <= count;
    }
  }
}
