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

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * One term block line.
 * <p>
 * Contains a term and its details as a {@link BlockTermState}.
 * <p>
 * The line is written to the {@link UniformSplitPostingsFormat#TERMS_BLOCKS_EXTENSION block file}
 * in two parts. The first part is the term followed by an offset to the details
 * region. The second part is the term {@link BlockTermState}, written in
 * the details region, after all the terms of the block.
 * <p>
 * The separate details region allows fast scan of the terms without having
 * to decode the details for each term. At read time, the {@link BlockLine.Serializer#readLine}
 * only reads the term and its offset to the details. The corresponding {@link BlockTermState}
 * is decoded on demand in the {@link BlockReader} (see {@link BlockReader#readTermStateIfNotRead}).
 *
 * @lucene.experimental
 */
public class BlockLine implements Accountable {

  private static final long BASE_RAM_USAGE = RamUsageEstimator.shallowSizeOfInstance(BlockLine.class);

  protected TermBytes termBytes;
  protected int termStateRelativeOffset;

  /**
   * Only used for writing.
   */
  protected final BlockTermState termState;

  /**
   * Constructor used for writing a {@link BlockLine}.
   */
  protected BlockLine(TermBytes termBytes, BlockTermState termState) {
    this(termBytes, -1, termState);
  }

  /**
   * Constructor used for reading a {@link BlockLine}.
   */
  protected BlockLine(TermBytes termBytes, int termStateRelativeOffset) {
    this(termBytes, termStateRelativeOffset, null);
  }

  private BlockLine(TermBytes termBytes, int termStateRelativeOffset, BlockTermState termState) {
    reset(termBytes, termStateRelativeOffset);
    this.termState = termState;
  }

  /**
   * Resets this {@link BlockLine} to reuse it when reading.
   */
  protected BlockLine reset(TermBytes termBytes, int termStateRelativeOffset) {
    assert termState == null;
    this.termBytes = termBytes;
    this.termStateRelativeOffset = termStateRelativeOffset;
    return this;
  }

  public TermBytes getTermBytes() {
    return termBytes;
  }

  /**
   * @return The offset of the {@link org.apache.lucene.index.TermState}
   * bytes in the block, relatively to the term states base offset.
   */
  public int getTermStateRelativeOffset() {
    return termStateRelativeOffset;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_USAGE
        + termBytes.ramBytesUsed()
        + RamUsageUtil.ramBytesUsed(termState);
  }

  /**
   * Reads/writes block lines with terms encoded incrementally inside a block.
   * This class keeps a state of the previous term read to decode the next term.
   */
  public static class Serializer implements Accountable {

    private static final long BASE_RAM_USAGE = RamUsageEstimator.shallowSizeOfInstance(Serializer.class);

    protected final BytesRef currentTerm;

    public Serializer() {
      currentTerm = new BytesRef(64);
    }

    /**
     * Reads the current line.
     *
     * @param isIncrementalEncodingSeed Whether the term is a seed of the
     *                                  incremental encoding. {@code true} for the first and
     *                                  middle term, {@code false} for other terms.
     * @param reuse                     A {@link BlockLine} instance to reuse; or null if none.
     */
    public BlockLine readLine(DataInput blockInput, boolean isIncrementalEncodingSeed, BlockLine reuse) throws IOException {
      int termStateRelativeOffset = blockInput.readVInt();
      if (termStateRelativeOffset < 0) {
        throw new CorruptIndexException("Illegal termStateRelativeOffset= " + termStateRelativeOffset, blockInput);
      }
      return reuse == null ?
          new BlockLine(readIncrementallyEncodedTerm(blockInput, isIncrementalEncodingSeed, null), termStateRelativeOffset)
          : reuse.reset(readIncrementallyEncodedTerm(blockInput, isIncrementalEncodingSeed, reuse.termBytes), termStateRelativeOffset);
    }

    /**
     * Writes a line and its offset to the corresponding term state details in
     * the details region.
     *
     * @param blockOutput               The output pointing to the block terms region.
     * @param termStateRelativeOffset   The offset to the corresponding term
     *                                  state details in the details region.
     * @param isIncrementalEncodingSeed Whether the term is a seed of
     *                                  the incremental encoding. {@code true} for the first
     *                                  and middle term, {@code false} for other terms.
     */
    public void writeLine(DataOutput blockOutput, BlockLine line, BlockLine previousLine,
                                 int termStateRelativeOffset, boolean isIncrementalEncodingSeed) throws IOException {
      blockOutput.writeVInt(termStateRelativeOffset);
      writeIncrementallyEncodedTerm(line.getTermBytes(), previousLine == null ? null : previousLine.getTermBytes(),
          isIncrementalEncodingSeed, blockOutput);
    }

    /**
     * Writes the term state details of a line in the details region.
     *
     * @param termStatesOutput The output pointing to the details region.
     */
    protected void writeLineTermState(DataOutput termStatesOutput, BlockLine line,
                                   FieldInfo fieldInfo, DeltaBaseTermStateSerializer encoder) throws IOException {
      assert line.termState != null;
      encoder.writeTermState(termStatesOutput, fieldInfo, line.termState);
    }

    protected void writeIncrementallyEncodedTerm(TermBytes termBytes, TermBytes previousTermBytes,
                                                      boolean isIncrementalEncodingSeed, DataOutput blockOutput) throws IOException {
      BytesRef term = termBytes.getTerm();
      assert term.offset == 0;
      if (isIncrementalEncodingSeed) {
        // Mdp length is always 1 for an incremental encoding seed.
        blockOutput.writeVLong(term.length);
        blockOutput.writeBytes(term.bytes, 0, term.length);
        return;
      }
      if (term.length == 0) {
        // Empty term.
        blockOutput.writeVLong(0);
        return;
      }

      // For other lines we store:
      // - Mdp length.
      // - Suffix length.
      // - Suffix bytes.
      // Instead of writing mdp length and suffix length with 2 VInt, we can compress the storage
      // by merging them in a single VLong. The idea is to leverage the information we have about
      // the previous line. We know the previous line term length. And we know that
      // new line mdp length <= (previous line term length + 1)
      // So if numMdpBits = numBitsToEncode(previous line term length),
      // then we know we can encode (new line mdp length - 1) in numMdpBits.
      // Hence we encode (new line mdp length - 1) in the rightmost numMdpBits of the VLong.
      // And we encode new line suffix length in the remaining left bits of the VLong.
      // Most of the time both values will be encoded in a single byte.

      assert previousTermBytes != null;
      assert termBytes.getMdpLength() >= 1;

      int numMdpBits = numBitsToEncode(previousTermBytes.getTerm().length);
      assert numBitsToEncode(termBytes.getMdpLength() - 1) <= numMdpBits;

      long mdpAndSuffixLengths = (((long) termBytes.getSuffixLength()) << numMdpBits) | (termBytes.getMdpLength() - 1);
      assert mdpAndSuffixLengths != 0;
      blockOutput.writeVLong(mdpAndSuffixLengths);
      blockOutput.writeBytes(term.bytes, termBytes.getSuffixOffset(), termBytes.getSuffixLength());
    }

    protected TermBytes readIncrementallyEncodedTerm(DataInput blockInput, boolean isIncrementalEncodingSeed, TermBytes reuse) throws IOException {
      assert currentTerm.offset == 0;
      int mdpLength;
      if (isIncrementalEncodingSeed) {
        int length = (int) blockInput.readVLong();
        mdpLength = length == 0 ? 0 : 1;
        readBytes(blockInput, currentTerm, 0, length);
      } else {
        long mdpAndSuffixLengths = blockInput.readVLong();
        if (mdpAndSuffixLengths == 0) {
          // Empty term.
          mdpLength = 0;
          currentTerm.length = 0;
        } else {
          int numMdpBits = numBitsToEncode(currentTerm.length);
          mdpLength = (int) (mdpAndSuffixLengths & ((1 << numMdpBits) - 1)) + 1; // Get rightmost numMdpBits.
          int suffixLength = (int) (mdpAndSuffixLengths >>> numMdpBits); // Get remaining left bits.
          assert mdpLength >= 1;
          assert suffixLength >= 1;
          readBytes(blockInput, currentTerm, mdpLength - 1, suffixLength);
        }
      }
      return reuse == null ?
          new TermBytes(mdpLength, currentTerm)
          : reuse.reset(mdpLength, currentTerm);
    }

    /**
     * Reads {@code length} bytes from the given {@link DataInput} and stores
     * them at {@code offset} in {@code bytes.bytes}.
     */
    protected void readBytes(DataInput input, BytesRef bytes, int offset, int length) throws IOException {
      assert bytes.offset == 0;
      bytes.length = offset + length;
      bytes.bytes = ArrayUtil.grow(bytes.bytes, bytes.length);
      input.readBytes(bytes.bytes, offset, length);
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_USAGE
          + RamUsageUtil.ramBytesUsed(currentTerm);
    }

    /**
     * Gets the number of bits required to encode the value of the provided int.
     * Returns 0 for int value 0. Equivalent to (log2(i) + 1).
     */
    protected static int numBitsToEncode(int i) {
      return 32 - Integer.numberOfLeadingZeros(i);
    }
  }
}