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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

/**
 * Writes blocks in the block file.
 * <p>
 * According the Uniform Split technique, the writing combines three steps
 * per block, and it is repeated for all the field blocks:
 * <ol>
 * <li>Select the term with the shortest {@link TermBytes minimal distinguishing prefix}
 * (MDP) in the neighborhood of the {@link #targetNumBlockLines target block size}
 * (+- {@link #deltaNumLines delta size})</li>
 * <li>The selected term becomes the first term of the next block, and its
 * MDP is the next block key.</li>
 * <li>The current block is written to the {@link UniformSplitPostingsFormat#TERMS_BLOCKS_EXTENSION block file}.
 * And its block key is {@link IndexDictionary.Builder#add(BytesRef, long) added}
 * to the {@link IndexDictionary index dictionary}.</li>
 * </ol>
 * <p>
 * This stateful {@link BlockWriter} is called repeatedly to
 * {@link #addLine(BytesRef, BlockTermState, IndexDictionary.Builder) add}
 * all the {@link BlockLine} terms of a field. Then {@link #finishLastBlock}
 * is called. And then this {@link BlockWriter} can be reused to add the terms
 * of another field.
 *
 * @lucene.experimental
 */
public class BlockWriter {

  protected final int targetNumBlockLines;
  protected final int deltaNumLines;
  protected final List<BlockLine> blockLines;

  protected final IndexOutput blockOutput;
  protected final ByteBuffersDataOutput blockLinesWriteBuffer;
  protected final ByteBuffersDataOutput termStatesWriteBuffer;

  protected final DeltaBaseTermStateSerializer termStateSerializer;
  protected final BlockEncoder blockEncoder;
  protected final ByteBuffersDataOutput blockWriteBuffer;

  protected FieldMetadata fieldMetadata;
  protected BytesRef lastTerm;

  protected final BlockHeader reusableBlockHeader;
  protected BytesRef scratchBytesRef;

  protected BlockWriter(IndexOutput blockOutput, int targetNumBlockLines, int deltaNumLines, BlockEncoder blockEncoder) {
    assert blockOutput != null;
    assert targetNumBlockLines > 0;
    assert deltaNumLines > 0;
    assert deltaNumLines < targetNumBlockLines;
    this.blockOutput = blockOutput;
    this.targetNumBlockLines = targetNumBlockLines;
    this.deltaNumLines = deltaNumLines;
    this.blockEncoder = blockEncoder;

    this.blockLines = new ArrayList<>(targetNumBlockLines);
    this.termStateSerializer = new DeltaBaseTermStateSerializer();

    this.blockLinesWriteBuffer = ByteBuffersDataOutput.newResettableInstance();
    this.termStatesWriteBuffer = ByteBuffersDataOutput.newResettableInstance();
    this.blockWriteBuffer = ByteBuffersDataOutput.newResettableInstance();

    this.reusableBlockHeader = new BlockHeader();
    this.scratchBytesRef = new BytesRef();
  }

  /**
   * Adds a new {@link BlockLine} term for the current field.
   * <p>
   * This method determines whether the new term is part of the current block,
   * or if it is part of the next block. In the latter case, a new block is started
   * (including one or more of the lastly added lines), the current block is
   * written to the block file, and the current block key is added to the
   * {@link IndexDictionary.Builder}.
   *
   * @param term              The block line term. The {@link BytesRef} instance is used directly,
   *                          the caller is responsible to make a deep copy if needed. This is required
   *                          because we keep a list of block lines until we decide to write the
   *                          current block, and each line must have a different term instance.
   * @param blockTermState    Block line details.
   * @param dictionaryBuilder to which the block keys are added.
   */
  protected void addLine(BytesRef term, BlockTermState blockTermState, IndexDictionary.Builder dictionaryBuilder) throws IOException {
    assert term != null;
    assert blockTermState != null;
    int mdpLength = TermBytes.computeMdpLength(lastTerm, term);
    blockLines.add(new BlockLine(new TermBytes(mdpLength, term), blockTermState));
    lastTerm = term;
    if (blockLines.size() >= targetNumBlockLines + deltaNumLines) {
      splitAndWriteBlock(dictionaryBuilder);
    }
  }

  /**
   * This method is called when there is no more term for the field. It writes
   * the remaining lines added with {@link #addLine} as the last block of the
   * field and resets this {@link BlockWriter} state. Then this {@link BlockWriter}
   * can be used for another field.
   */
  protected void finishLastBlock(IndexDictionary.Builder dictionaryBuilder) throws IOException {
    while (!blockLines.isEmpty()) {
      splitAndWriteBlock(dictionaryBuilder);
    }
    fieldMetadata = null;
    lastTerm = null;
  }

  /**
   * Defines the new block start according to {@link #targetNumBlockLines}
   * and {@link #deltaNumLines}.
   * The new block is started (including one or more of the lastly added lines),
   * the current block is written to the block file, and the current block key
   * is added to the {@link IndexDictionary.Builder}.
   */
  protected void splitAndWriteBlock(IndexDictionary.Builder dictionaryBuilder) throws IOException {
    assert !blockLines.isEmpty();
    int numLines = blockLines.size();

    if (numLines <= targetNumBlockLines - deltaNumLines) {
      writeBlock(blockLines, dictionaryBuilder);
      blockLines.clear();
      return;
    }
    int deltaStart = numLines - deltaNumLines * 2;
    assert deltaStart >= 1 : "blockLines size: " + numLines;
    int minMdpLength = Integer.MAX_VALUE;
    int minMdpEndIndex = 0;

    for (int i = deltaStart; i < numLines; i++) {
      TermBytes term = blockLines.get(i).getTermBytes();
      int mdpLength = term.getMdpLength();
      if (mdpLength <= minMdpLength) {
        minMdpLength = mdpLength;
        minMdpEndIndex = i;
      }
    }

    List<BlockLine> subList = blockLines.subList(0, minMdpEndIndex);
    writeBlock(subList, dictionaryBuilder);
    // Clear the written block lines to keep only the lines composing the next block.
    // ArrayList.subList().clear() is O(N) but still fast since we work on a small list.
    // It is internally an array copy and an iteration to set array refs to null.
    // For clarity we keep that until the day a CircularArrayList is available in the jdk.
    subList.clear();
  }

  /**
   * Writes a block and adds its block key to the dictionary builder.
   */
  protected void writeBlock(List<BlockLine> blockLines, IndexDictionary.Builder dictionaryBuilder) throws IOException {

    long blockStartFP = blockOutput.getFilePointer();

    addBlockKey(blockLines, dictionaryBuilder);

    int middle = blockLines.size() >> 1;
    int middleOffset = -1;
    BlockLine previousLine = null;
    for (int i = 0, size = blockLines.size(); i < size; i++) {
      boolean isIncrementalEncodingSeed = i == 0;
      if (i == middle) {
        middleOffset = Math.toIntExact(blockLinesWriteBuffer.size());
        isIncrementalEncodingSeed = true;
      }
      BlockLine line = blockLines.get(i);
      writeBlockLine(isIncrementalEncodingSeed, line, previousLine);
      previousLine = line;
    }

    reusableBlockHeader.reset(blockLines.size(), termStateSerializer.getBaseDocStartFP(), termStateSerializer.getBasePosStartFP(),
        termStateSerializer.getBasePayStartFP(), Math.toIntExact(blockLinesWriteBuffer.size()), middleOffset);
    reusableBlockHeader.write(blockWriteBuffer);

    blockLinesWriteBuffer.copyTo(blockWriteBuffer);
    termStatesWriteBuffer.copyTo(blockWriteBuffer);

    if (blockEncoder == null) {
      blockOutput.writeVInt(Math.toIntExact(blockWriteBuffer.size()));
      blockWriteBuffer.copyTo(blockOutput);
    } else {
      BlockEncoder.WritableBytes encodedBytes = blockEncoder.encode(blockWriteBuffer.toDataInput(), blockWriteBuffer.size());
      blockOutput.writeVInt(Math.toIntExact(encodedBytes.size()));
      encodedBytes.writeTo(blockOutput);
    }

    blockLinesWriteBuffer.reset();
    termStatesWriteBuffer.reset();
    blockWriteBuffer.reset();

    termStateSerializer.resetBaseStartFP();

    updateFieldMetadata(blockStartFP);
  }

  /**
   * updates the field metadata after all lines were written for the block.
   */
  protected void updateFieldMetadata(long blockStartFP) {
    assert fieldMetadata != null;
    if (fieldMetadata.getFirstBlockStartFP() == -1) {
      fieldMetadata.setFirstBlockStartFP(blockStartFP);
    }
    fieldMetadata.setLastBlockStartFP(blockStartFP);
  }

  void setField(FieldMetadata fieldMetadata) {
    this.fieldMetadata = fieldMetadata;
  }

  protected void writeBlockLine(boolean isIncrementalEncodingSeed, BlockLine line, BlockLine previousLine) throws IOException {
    assert fieldMetadata != null;
    BlockLine.Serializer.writeLine(blockLinesWriteBuffer, line, previousLine, Math.toIntExact(termStatesWriteBuffer.size()), isIncrementalEncodingSeed);
    BlockLine.Serializer.writeLineTermState(termStatesWriteBuffer, line, fieldMetadata.getFieldInfo(), termStateSerializer);
  }

  /**
   * Adds a new block key with its corresponding block file pointer to the
   * {@link IndexDictionary.Builder} .
   * The block key is the MDP (see {@link TermBytes}) of the block first term.
   */
  protected void addBlockKey(List<BlockLine> blockLines, IndexDictionary.Builder dictionaryBuilder) {
    assert !blockLines.isEmpty();
    assert dictionaryBuilder != null;
    TermBytes firstTerm = blockLines.get(0).getTermBytes();
    assert firstTerm.getTerm().offset == 0;
    assert scratchBytesRef.offset == 0;
    scratchBytesRef.bytes = firstTerm.getTerm().bytes;
    scratchBytesRef.length = firstTerm.getMdpLength();
    dictionaryBuilder.add(scratchBytesRef, blockOutput.getFilePointer());
  }
}
