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

package org.apache.lucene.codecs.uniformsplit.sharedterms;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.codecs.uniformsplit.BlockEncoder;
import org.apache.lucene.codecs.uniformsplit.BlockLine;
import org.apache.lucene.codecs.uniformsplit.BlockWriter;
import org.apache.lucene.codecs.uniformsplit.FieldMetadata;
import org.apache.lucene.codecs.uniformsplit.IndexDictionary;
import org.apache.lucene.codecs.uniformsplit.TermBytes;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

/**
 * Writes terms blocks with the Shared Terms format.
 *
 * <p>As defined in {@link STUniformSplitTermsWriter}, all the fields terms are shared in the same
 * dictionary. Each block line contains a term and all the fields {@link
 * org.apache.lucene.index.TermState}s for this term.
 *
 * @lucene.experimental
 */
public class STBlockWriter extends BlockWriter {

  protected final Set<FieldMetadata> fieldsInBlock;

  public STBlockWriter(
      IndexOutput blockOutput,
      int targetNumBlockLines,
      int deltaNumLines,
      BlockEncoder blockEncoder) {
    super(blockOutput, targetNumBlockLines, deltaNumLines, blockEncoder);
    fieldsInBlock = new HashSet<>();
  }

  /**
   * Adds a new {@link BlockLine} term for the current field.
   *
   * <p>This method determines whether the new term is part of the current block, or if it is part
   * of the next block. In the latter case, a new block is started (including one or more of the
   * lastly added lines), the current block is written to the block file, and the current block key
   * is added to the {@link org.apache.lucene.codecs.uniformsplit.IndexDictionary.Builder}.
   *
   * @param term The block line term. The {@link BytesRef} instance is used directly, the caller is
   *     responsible to make a deep copy if needed. This is required because we keep a list of block
   *     lines until we decide to write the current block, and each line must have a different term
   *     instance.
   * @param termStates Block line details for all fields in the line.
   * @param dictionaryBuilder to which the block keys are added.
   */
  public void addLine(
      BytesRef term,
      List<FieldMetadataTermState> termStates,
      IndexDictionary.Builder dictionaryBuilder)
      throws IOException {
    if (termStates.isEmpty()) {
      return;
    }
    int mdpLength = TermBytes.computeMdpLength(lastTerm, term);
    blockLines.add(new STBlockLine(new TermBytes(mdpLength, term), termStates));
    lastTerm = term;
    if (blockLines.size() >= targetNumBlockLines + deltaNumLines) {
      splitAndWriteBlock(dictionaryBuilder);
    }
  }

  @Override
  protected void finishLastBlock(IndexDictionary.Builder dictionaryBuilder) throws IOException {
    // Make this method accessible to package.
    super.finishLastBlock(dictionaryBuilder);
  }

  @Override
  protected BlockLine.Serializer createBlockLineSerializer() {
    return new STBlockLine.Serializer();
  }

  @Override
  protected void writeBlockLine(
      boolean isIncrementalEncodingSeed, BlockLine line, BlockLine previousLine)
      throws IOException {
    blockLineWriter.writeLine(
        blockLinesWriteBuffer,
        line,
        previousLine,
        Math.toIntExact(termStatesWriteBuffer.size()),
        isIncrementalEncodingSeed);
    ((STBlockLine.Serializer) blockLineWriter)
        .writeLineTermStates(termStatesWriteBuffer, (STBlockLine) line, termStateSerializer);
    ((STBlockLine) line).collectFields(fieldsInBlock);
  }

  @Override
  protected void updateFieldMetadata(long blockStartFP) {
    assert !fieldsInBlock.isEmpty();
    for (FieldMetadata fieldMetadata : fieldsInBlock) {
      if (fieldMetadata.getFirstBlockStartFP() == -1) {
        fieldMetadata.setFirstBlockStartFP(blockStartFP);
      }
      fieldMetadata.setLastBlockStartFP(blockStartFP);
    }
    fieldsInBlock.clear();
  }
}
