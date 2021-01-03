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
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Seeks the block corresponding to a given term, read the block bytes, and scans the block terms.
 *
 * <p>Reads fully the block in {@link #blockReadBuffer}. Then scans the block terms in memory. The
 * details region is lazily decoded with {@link #termStatesReadBuffer} which shares the same byte
 * array with {@link #blockReadBuffer}. See {@link BlockWriter} and {@link BlockLine} for the block
 * format.
 *
 * @lucene.experimental
 */
public class BlockReader extends BaseTermsEnum implements Accountable {

  private static final long BASE_RAM_USAGE =
      RamUsageEstimator.shallowSizeOfInstance(BlockReader.class)
          + RamUsageEstimator.shallowSizeOfInstance(IndexInput.class)
          + RamUsageEstimator.shallowSizeOfInstance(ByteArrayDataInput.class) * 2;

  /**
   * {@link IndexInput} on the {@link UniformSplitPostingsFormat#TERMS_BLOCKS_EXTENSION block file}.
   */
  protected IndexInput blockInput;

  protected final PostingsReaderBase postingsReader;
  protected final FieldMetadata fieldMetadata;
  protected final BlockDecoder blockDecoder;

  protected BlockHeader.Serializer blockHeaderReader;
  protected BlockLine.Serializer blockLineReader;
  /** In-memory read buffer for the current block. */
  protected ByteArrayDataInput blockReadBuffer;
  /**
   * In-memory read buffer for the details region of the current block. It shares the same byte
   * array as {@link #blockReadBuffer}, with a different position.
   */
  protected ByteArrayDataInput termStatesReadBuffer;

  protected DeltaBaseTermStateSerializer termStateSerializer;

  /** {@link IndexDictionary.Browser} supplier for lazy loading. */
  protected final IndexDictionary.BrowserSupplier dictionaryBrowserSupplier;
  /** Holds the {@link IndexDictionary.Browser} once loaded. */
  protected IndexDictionary.Browser dictionaryBrowser;

  /**
   * Current block start file pointer, absolute in the {@link
   * UniformSplitPostingsFormat#TERMS_BLOCKS_EXTENSION block file}.
   */
  protected long blockStartFP;
  /** Current block header. */
  protected BlockHeader blockHeader;
  /** Current block line. */
  protected BlockLine blockLine;
  /** Current block line details. */
  protected BlockTermState termState;
  /**
   * Offset of the start of the first line of the current block (just after the header), relative to
   * the block start.
   */
  protected int blockFirstLineStart;
  /** Current line index in the block. */
  protected int lineIndexInBlock;
  /**
   * Whether the current {@link TermState} has been forced with a call to {@link
   * #seekExact(BytesRef, TermState)}.
   *
   * @see #forcedTerm
   */
  protected boolean termStateForced;
  /**
   * Set when {@link #seekExact(BytesRef, TermState)} is called.
   *
   * <p>This optimizes the use-case when the caller calls first {@link #seekExact(BytesRef,
   * TermState)} and then {@link #postings(PostingsEnum, int)}. In this case we don't access the
   * terms block file (we don't seek) but directly the postings file because we already have the
   * {@link TermState} with the file pointers to the postings file.
   */
  protected BytesRefBuilder forcedTerm;

  // Scratch objects to avoid object reallocation.
  protected BytesRef scratchBlockBytes;
  protected final BlockTermState scratchTermState;
  protected BlockLine scratchBlockLine;

  /**
   * @param dictionaryBrowserSupplier to load the {@link IndexDictionary.Browser} lazily in {@link
   *     #seekCeil(BytesRef)}.
   * @param blockDecoder Optional block decoder, may be null if none. It can be used for
   *     decompression or decryption.
   */
  protected BlockReader(
      IndexDictionary.BrowserSupplier dictionaryBrowserSupplier,
      IndexInput blockInput,
      PostingsReaderBase postingsReader,
      FieldMetadata fieldMetadata,
      BlockDecoder blockDecoder)
      throws IOException {
    this.dictionaryBrowserSupplier = dictionaryBrowserSupplier;
    this.blockInput = blockInput;
    this.postingsReader = postingsReader;
    this.fieldMetadata = fieldMetadata;
    this.blockDecoder = blockDecoder;
    this.blockStartFP = -1;
    scratchTermState = postingsReader.newTermState();
  }

  @Override
  public SeekStatus seekCeil(BytesRef searchedTerm) throws IOException {
    if (isCurrentTerm(searchedTerm)) {
      return SeekStatus.FOUND;
    }
    clearTermState();

    long blockStartFP = getOrCreateDictionaryBrowser().seekBlock(searchedTerm);
    blockStartFP = Math.max(blockStartFP, fieldMetadata.getFirstBlockStartFP());
    if (isBeyondLastTerm(searchedTerm, blockStartFP)) {
      return SeekStatus.END;
    }
    SeekStatus seekStatus = seekInBlock(searchedTerm, blockStartFP);
    if (seekStatus != SeekStatus.END) {
      return seekStatus;
    }
    // Go to next block.
    return nextTerm() == null ? SeekStatus.END : SeekStatus.NOT_FOUND;
  }

  @Override
  public boolean seekExact(BytesRef searchedTerm) throws IOException {
    if (isCurrentTerm(searchedTerm)) {
      return true;
    }
    clearTermState();

    long blockStartFP = getOrCreateDictionaryBrowser().seekBlock(searchedTerm);
    if (blockStartFP < fieldMetadata.getFirstBlockStartFP()
        || isBeyondLastTerm(searchedTerm, blockStartFP)) {
      return false;
    }
    return seekInBlock(searchedTerm, blockStartFP) == SeekStatus.FOUND;
  }

  protected boolean isCurrentTerm(BytesRef searchedTerm) {
    // Optimization and also required to not search with the same BytesRef
    // instance as the BytesRef used to read the block line (BlockLine.Serializer).
    // Indeed getCurrentTerm() is allowed to return the same BytesRef instance.
    return searchedTerm.equals(term());
  }

  /**
   * Indicates whether the searched term is beyond the last term of the field.
   *
   * @param blockStartFP The current block start file pointer.
   */
  protected boolean isBeyondLastTerm(BytesRef searchedTerm, long blockStartFP) {
    return blockStartFP == fieldMetadata.getLastBlockStartFP()
        && searchedTerm.compareTo(fieldMetadata.getLastTerm()) > 0;
  }

  /**
   * Seeks to the provided term in the block starting at the provided file pointer. Does not exceed
   * the block.
   */
  protected SeekStatus seekInBlock(BytesRef searchedTerm, long blockStartFP) throws IOException {
    initializeHeader(searchedTerm, blockStartFP);
    if (blockHeader == null) {
      throw newCorruptIndexException("Illegal absence of block", blockStartFP);
    }
    return seekInBlock(searchedTerm);
  }

  /**
   * Seeks to the provided term in this block.
   *
   * <p>Does not exceed this block; {@link org.apache.lucene.index.TermsEnum.SeekStatus#END} is
   * returned if it follows the block.
   *
   * <p>Compares the line terms with the <code>searchedTerm</code>, taking advantage of the
   * incremental encoding properties.
   *
   * <p>Scans linearly the terms. Updates the current block line with the current term.
   */
  protected SeekStatus seekInBlock(BytesRef searchedTerm) throws IOException {
    if (compareToMiddleAndJump(searchedTerm) == 0) {
      return SeekStatus.FOUND;
    }
    int comparisonOffset = 0;
    while (true) {
      if (readLineInBlock() == null) {
        // No more terms for the block.
        return SeekStatus.END;
      }
      TermBytes lineTermBytes = blockLine.getTermBytes();
      BytesRef lineTerm = lineTermBytes.getTerm();
      assert lineTerm.offset == 0;

      // Equivalent to comparing with BytesRef.compareTo(),
      // but faster since we start comparing from min(comparisonOffset, suffixOffset).
      int suffixOffset = lineTermBytes.getSuffixOffset();
      int start = Math.min(comparisonOffset, suffixOffset);
      int end = Math.min(searchedTerm.length, lineTerm.length);
      int comparison = searchedTerm.length - lineTerm.length;
      for (int i = start; i < end; i++) {
        // Compare unsigned bytes.
        int byteDiff =
            (searchedTerm.bytes[i + searchedTerm.offset] & 0xFF) - (lineTerm.bytes[i] & 0xFF);
        if (byteDiff != 0) {
          comparison = byteDiff;
          break;
        }
        comparisonOffset = i + 1;
      }
      if (comparison == 0) {
        return SeekStatus.FOUND;
      } else if (comparison < 0) {
        return SeekStatus.NOT_FOUND;
      }
    }
  }

  /**
   * Compares the searched term to the middle term of the block. If the searched term is
   * lexicographically equal or after the middle term then jumps to the second half of the block
   * directly.
   *
   * @return The comparison between the searched term and the middle term.
   */
  protected int compareToMiddleAndJump(BytesRef searchedTerm) throws IOException {
    if (lineIndexInBlock != 0) {
      // Don't try to compare and jump if we are not positioned at the first line.
      // This can happen if we seek in the same current block and we continue
      // scanning from the current line (see initializeHeader()).
      return -1;
    }
    blockReadBuffer.skipBytes(blockHeader.getMiddleLineOffset());
    lineIndexInBlock = blockHeader.getMiddleLineIndex();
    readLineInBlock();
    if (blockLine == null) {
      throw newCorruptIndexException("Illegal absence of line at the middle of the block", null);
    }
    int compare = searchedTerm.compareTo(term());
    if (compare < 0) {
      blockReadBuffer.setPosition(blockFirstLineStart);
      lineIndexInBlock = 0;
    }
    return compare;
  }

  /**
   * Reads the current block line. Sets {@link #blockLine} and increments {@link #lineIndexInBlock}.
   *
   * @return The {@link BlockLine}; or null if there no more line in the block.
   */
  protected BlockLine readLineInBlock() throws IOException {
    if (lineIndexInBlock >= blockHeader.getLinesCount()) {
      return blockLine = null;
    }
    boolean isIncrementalEncodingSeed =
        lineIndexInBlock == 0 || lineIndexInBlock == blockHeader.getMiddleLineIndex();
    lineIndexInBlock++;
    return blockLine =
        blockLineReader.readLine(blockReadBuffer, isIncrementalEncodingSeed, scratchBlockLine);
  }

  /**
   * Positions this {@link BlockReader} without re-seeking the term dictionary.
   *
   * <p>The block containing the term is not read by this method. It will be read lazily only if
   * needed, for example if {@link #next()} is called. Calling {@link #postings} after this method
   * does require the block to be read.
   */
  @Override
  public void seekExact(BytesRef term, TermState state) {
    termStateForced = true;
    termState = scratchTermState;
    termState.copyFrom(state);
    if (forcedTerm == null) {
      forcedTerm = new BytesRefBuilder();
    }
    forcedTerm.copyBytes(term);
  }

  /** Not supported. */
  @Override
  public void seekExact(long ord) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BytesRef next() throws IOException {
    if (termStateForced) {
      initializeHeader(forcedTerm.get(), termState.blockFilePointer);
      if (blockHeader == null) {
        throw newCorruptIndexException(
            "Illegal absence of block for TermState", termState.blockFilePointer);
      }
      for (int i = lineIndexInBlock; i < termState.termBlockOrd; i++) {
        readLineInBlock();
      }
      assert blockLine.getTermBytes().getTerm().equals(forcedTerm.get());
    }
    clearTermState();
    return nextTerm();
  }

  /**
   * Moves to the next term line and reads it, it may be in the next block. The term details are not
   * read yet. They will be read only when needed with {@link #readTermStateIfNotRead()}.
   *
   * @return The read term bytes; or null if there is no more term for the field.
   */
  protected BytesRef nextTerm() throws IOException {
    if (blockHeader == null) {
      // Read the first block for the field.
      initializeHeader(null, fieldMetadata.getFirstBlockStartFP());
      if (blockHeader == null) {
        throw newCorruptIndexException(
            "Illegal absence of first block", fieldMetadata.getFirstBlockStartFP());
      }
    }
    if (readLineInBlock() == null) {
      // No more line in the current block.
      // Read the next block starting at the current file pointer in the block file.
      initializeHeader(null, blockInput.getFilePointer());
      if (blockHeader == null) {
        // No more block for the field.
        return null;
      }
      readLineInBlock();
    }
    return term();
  }

  /**
   * Reads and sets {@link #blockHeader}. Sets null if there is no block for the field anymore.
   *
   * @param searchedTerm The searched term; or null if none.
   * @param targetBlockStartFP The file pointer of the block to read.
   */
  protected void initializeHeader(BytesRef searchedTerm, long targetBlockStartFP)
      throws IOException {
    initializeBlockReadLazily();
    if (blockStartFP == targetBlockStartFP) {
      // Optimization: If the block to read is already the current block, then
      // reuse it directly without reading nor decoding the block bytes.
      if (blockHeader == null) {
        throw newCorruptIndexException("Illegal absence of block", blockStartFP);
      }
      if (searchedTerm == null
          || blockLine == null
          || searchedTerm.compareTo(blockLine.getTermBytes().getTerm()) <= 0) {
        // If the searched term precedes lexicographically the current term,
        // then reset the position to the first term line of the block.
        // If the searched term equals the current term, we also need to reset
        // to scan again the current line.
        blockReadBuffer.setPosition(blockFirstLineStart);
        lineIndexInBlock = 0;
      }
    } else {
      blockInput.seek(targetBlockStartFP);
      blockStartFP = targetBlockStartFP;
      readHeader();
      blockFirstLineStart = blockReadBuffer.getPosition();
      lineIndexInBlock = 0;
    }
  }

  protected void initializeBlockReadLazily() throws IOException {
    if (blockStartFP == -1) {
      blockInput = blockInput.clone();
      blockHeaderReader = createBlockHeaderSerializer();
      blockLineReader = createBlockLineSerializer();
      blockReadBuffer = new ByteArrayDataInput();
      termStatesReadBuffer = new ByteArrayDataInput();
      termStateSerializer = createDeltaBaseTermStateSerializer();
      scratchBlockBytes = new BytesRef();
      scratchBlockLine = new BlockLine(new TermBytes(0, scratchBlockBytes), 0);
    }
  }

  protected BlockHeader.Serializer createBlockHeaderSerializer() {
    return new BlockHeader.Serializer();
  }

  protected BlockLine.Serializer createBlockLineSerializer() {
    return new BlockLine.Serializer();
  }

  protected DeltaBaseTermStateSerializer createDeltaBaseTermStateSerializer() {
    return new DeltaBaseTermStateSerializer();
  }

  /**
   * Reads the block header. Sets {@link #blockHeader}.
   *
   * @return The block header; or null if there is no block for the field anymore.
   */
  protected BlockHeader readHeader() throws IOException {
    if (blockInput.getFilePointer() > fieldMetadata.getLastBlockStartFP()) {
      return blockHeader = null;
    }
    int numBlockBytes = blockInput.readVInt();
    BytesRef blockBytesRef = decodeBlockBytesIfNeeded(numBlockBytes);
    blockReadBuffer.reset(blockBytesRef.bytes, blockBytesRef.offset, blockBytesRef.length);
    termStatesReadBuffer.reset(blockBytesRef.bytes, blockBytesRef.offset, blockBytesRef.length);
    return blockHeader = blockHeaderReader.read(blockReadBuffer, blockHeader);
  }

  protected BytesRef decodeBlockBytesIfNeeded(int numBlockBytes) throws IOException {
    scratchBlockBytes.bytes = ArrayUtil.grow(scratchBlockBytes.bytes, numBlockBytes);
    blockInput.readBytes(scratchBlockBytes.bytes, 0, numBlockBytes);
    scratchBlockBytes.length = numBlockBytes;
    if (blockDecoder == null) {
      return scratchBlockBytes;
    }
    blockReadBuffer.reset(scratchBlockBytes.bytes, 0, numBlockBytes);
    return blockDecoder.decode(blockReadBuffer, numBlockBytes);
  }

  /** Reads the {@link BlockTermState} if it is not already set. Sets {@link #termState}. */
  protected BlockTermState readTermStateIfNotRead() throws IOException {
    if (termState == null) {
      termState = readTermState();
      if (termState != null) {
        termState.termBlockOrd = lineIndexInBlock;
        termState.blockFilePointer = blockStartFP;
      }
    }
    return termState;
  }

  /**
   * Reads the {@link BlockTermState} on the current line. Sets {@link #termState}.
   *
   * <p>Overriding method may return null if there is no {@link BlockTermState} (in this case the
   * extending class must support a null {@link #termState}).
   *
   * @return The {@link BlockTermState}; or null if none.
   */
  protected BlockTermState readTermState() throws IOException {
    // We reuse scratchTermState safely as the read TermState is cloned in the termState() method.
    termStatesReadBuffer.setPosition(
        blockFirstLineStart
            + blockHeader.getTermStatesBaseOffset()
            + blockLine.getTermStateRelativeOffset());
    return termState =
        termStateSerializer.readTermState(
            blockHeader.getBaseDocsFP(),
            blockHeader.getBasePositionsFP(),
            blockHeader.getBasePayloadsFP(),
            termStatesReadBuffer,
            fieldMetadata.getFieldInfo(),
            scratchTermState);
  }

  @Override
  public BytesRef term() {
    if (termStateForced) {
      return forcedTerm.get();
    }
    return blockLine == null ? null : blockLine.getTermBytes().getTerm();
  }

  @Override
  public long ord() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int docFreq() throws IOException {
    readTermStateIfNotRead();
    return termState.docFreq;
  }

  @Override
  public long totalTermFreq() throws IOException {
    readTermStateIfNotRead();
    return termState.totalTermFreq;
  }

  @Override
  public TermState termState() throws IOException {
    readTermStateIfNotRead();
    return termState.clone();
  }

  @Override
  public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
    readTermStateIfNotRead();
    return postingsReader.postings(fieldMetadata.getFieldInfo(), termState, reuse, flags);
  }

  @Override
  public ImpactsEnum impacts(int flags) throws IOException {
    readTermStateIfNotRead();
    return postingsReader.impacts(fieldMetadata.getFieldInfo(), termState, flags);
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_USAGE
        + (blockLineReader == null ? 0 : blockLineReader.ramBytesUsed())
        + (blockReadBuffer == null
            ? 0
            : RamUsageUtil.ramBytesUsedByByteArrayOfLength(blockReadBuffer.length()))
        + (termStateSerializer == null ? 0 : termStateSerializer.ramBytesUsed())
        + (forcedTerm == null ? 0 : RamUsageUtil.ramBytesUsed(forcedTerm))
        + (blockHeader == null ? 0 : blockHeader.ramBytesUsed())
        + (blockLine == null ? 0 : blockLine.ramBytesUsed())
        + (termState == null ? 0 : RamUsageUtil.ramBytesUsed(termState));
  }

  protected IndexDictionary.Browser getOrCreateDictionaryBrowser() throws IOException {
    if (dictionaryBrowser == null) {
      dictionaryBrowser = dictionaryBrowserSupplier.get();
    }
    return dictionaryBrowser;
  }

  /** Called by the primary {@link TermsEnum} methods to clear the previous {@link TermState}. */
  protected void clearTermState() {
    termState = null;
    termStateForced = false;
  }

  protected CorruptIndexException newCorruptIndexException(String msg, Long fp) {
    return new CorruptIndexException(
        msg
            + (fp == null ? "" : " at FP " + fp)
            + " for field \""
            + fieldMetadata.getFieldInfo().name
            + "\"",
        blockInput);
  }
}
