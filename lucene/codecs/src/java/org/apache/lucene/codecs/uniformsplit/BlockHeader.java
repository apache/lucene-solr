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

package org.apache.lucene.codecs.uniformsplit;

import java.io.IOException;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Block header containing block metadata.
 * <p>
 * Holds the number of lines in the block.
 * <p>
 * Holds the base file pointers to apply delta base encoding to all the file
 * pointers in the block with {@link DeltaBaseTermStateSerializer}.
 * <p>
 * Holds the offset to the details region of the block (the term states).
 * <p>
 * Holds the offset to the middle term of the block to divide the number
 * of terms to scan by 2.
 *
 * @lucene.experimental
 */
public class BlockHeader implements Accountable {

  private static final long RAM_USAGE = RamUsageEstimator.shallowSizeOfInstance(BlockHeader.class);

  protected int linesCount;

  protected long baseDocsFP;
  protected long basePositionsFP;
  protected long basePayloadsFP;

  protected int termStatesBaseOffset;
  protected int middleLineIndex;
  protected int middleLineOffset;

  /**
   * @param linesCount           Number of lines in the block.
   * @param baseDocsFP           File pointer to the docs of the first term with docs in the block.
   * @param basePositionsFP      File pointer to the positions of the first term with positions in the block.
   * @param basePayloadsFP       File pointer to the payloads of the first term with payloads in the block.
   * @param termStatesBaseOffset Offset to the details region of the block (the term states), relative to the block start.
   * @param middleLineOffset     Offset to the middle term of the block, relative to the block start.
   */
  protected BlockHeader(int linesCount, long baseDocsFP, long basePositionsFP, long basePayloadsFP,
                        int termStatesBaseOffset, int middleLineOffset) {
    reset(linesCount, baseDocsFP, basePositionsFP, basePayloadsFP, termStatesBaseOffset, middleLineOffset);
  }

  /**
   * Empty constructor. {@link #reset} must be called before writing.
   */
  protected BlockHeader() {
  }

  protected BlockHeader reset(int linesCount, long baseDocsFP, long basePositionsFP,
                              long basePayloadsFP, int termStatesBaseOffset, int middleTermOffset) {
    this.baseDocsFP = baseDocsFP;
    this.basePositionsFP = basePositionsFP;
    this.basePayloadsFP = basePayloadsFP;
    this.linesCount = linesCount;
    this.middleLineIndex = linesCount >> 1;
    this.termStatesBaseOffset = termStatesBaseOffset;
    this.middleLineOffset = middleTermOffset;
    return this;
  }

  /**
   * @return The number of lines in the block.
   */
  public int getLinesCount() {
    return linesCount;
  }

  /**
   * @return The index of the middle line of the block.
   */
  public int getMiddleLineIndex() {
    return middleLineIndex;
  }

  /**
   * @return The offset to the middle line of the block, relative to the block start.
   */
  public int getMiddleLineOffset() {
    return middleLineOffset;
  }

  /**
   * @return The offset to the details region of the block (the term states), relative to the block start.
   */
  public int getTermStatesBaseOffset() {
    return termStatesBaseOffset;
  }

  /**
   * @return The file pointer to the docs of the first term with docs in the block.
   */
  public long getBaseDocsFP() {
    return baseDocsFP;
  }

  /**
   * @return The file pointer to the positions of the first term with positions in the block.
   */
  public long getBasePositionsFP() {
    return basePositionsFP;
  }

  /**
   * @return The file pointer to the payloads of the first term with payloads in the block.
   */
  public long getBasePayloadsFP() {
    return basePayloadsFP;
  }

  @Override
  public long ramBytesUsed() {
    return RAM_USAGE;
  }

  /**
   * Reads/writes block header.
   */
  public static class Serializer {

    public void write(DataOutput output, BlockHeader blockHeader) throws IOException {
      assert blockHeader.linesCount > 0 : "Block header is not initialized";
      output.writeVInt(blockHeader.linesCount);

      output.writeVLong(blockHeader.baseDocsFP);
      output.writeVLong(blockHeader.basePositionsFP);
      output.writeVLong(blockHeader.basePayloadsFP);

      output.writeVInt(blockHeader.termStatesBaseOffset);
      output.writeVInt(blockHeader.middleLineOffset);
    }

    public BlockHeader read(DataInput input, BlockHeader reuse) throws IOException {
      int linesCount = input.readVInt();
      if (linesCount <= 0 || linesCount > UniformSplitTermsWriter.MAX_NUM_BLOCK_LINES) {
        throw new CorruptIndexException("Illegal number of lines in a block: " + linesCount, input);
      }

      long baseDocsFP = input.readVLong();
      long basePositionsFP = input.readVLong();
      long basePayloadsFP = input.readVLong();

      int termStatesBaseOffset = input.readVInt();
      if (termStatesBaseOffset < 0) {
        throw new CorruptIndexException("Illegal termStatesBaseOffset= " + termStatesBaseOffset, input);
      }
      int middleTermOffset = input.readVInt();
      if (middleTermOffset < 0) {
        throw new CorruptIndexException("Illegal middleTermOffset= " + middleTermOffset, input);
      }

      BlockHeader blockHeader = reuse == null ? new BlockHeader() : reuse;
      return blockHeader.reset(linesCount, baseDocsFP, basePositionsFP, basePayloadsFP, termStatesBaseOffset, middleTermOffset);
    }
  }
}