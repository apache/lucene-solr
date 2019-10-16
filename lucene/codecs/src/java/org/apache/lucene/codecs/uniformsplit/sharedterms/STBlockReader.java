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
import java.util.function.Supplier;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.uniformsplit.BlockDecoder;
import org.apache.lucene.codecs.uniformsplit.BlockReader;
import org.apache.lucene.codecs.uniformsplit.FieldMetadata;
import org.apache.lucene.codecs.uniformsplit.IndexDictionary;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

/**
 * Reads terms blocks with the Shared Terms format.
 *
 * @see STBlockWriter
 * @lucene.experimental
 */
public class STBlockReader extends BlockReader {

  protected final FieldInfos fieldInfos;

  public STBlockReader(Supplier<IndexDictionary.Browser> dictionaryBrowserSupplier,
                       IndexInput blockInput, PostingsReaderBase postingsReader,
                       FieldMetadata fieldMetadata, BlockDecoder blockDecoder, FieldInfos fieldInfos) throws IOException {
    super(dictionaryBrowserSupplier, blockInput, postingsReader, fieldMetadata, blockDecoder);
    this.fieldInfos = fieldInfos;
  }

  @Override
  public BytesRef next() throws IOException {
    BytesRef next = super.next();
    if (next == null) {
      return null;
    }
    // Check if the term occurs for the searched field.
    while (!termOccursInField()) {
      next = super.next();
      if (next == null) {
        // No more term for any field.
        return null;
      }
    }
    // The term occurs for the searched field.
    return next;
  }

  private boolean termOccursInField() throws IOException {
    readTermStateIfNotRead();
    return termState != null;
  }

  /**
   * Moves to the next term line and reads it, whichever are the corresponding fields.
   * The term details are not read yet. They will be read only when needed
   * with {@link #readTermStateIfNotRead()}.
   *
   * @return The read term bytes.
   */
  @Override
  protected BytesRef nextTerm() throws IOException {
    BytesRef nextTerm = super.nextTerm();
    if (nextTerm != null && super.isBeyondLastTerm(nextTerm, blockStartFP)) {
      return null;
    }
    return nextTerm;
  }

  @Override
  public SeekStatus seekCeil(BytesRef searchedTerm) throws IOException {
    SeekStatus seekStatus = seekCeilIgnoreField(searchedTerm);
    if (seekStatus != SeekStatus.END) {
      if (!termOccursInField()) {
        // The term does not occur for the field.
        // We have to move the iterator to the next valid term for the field.
        BytesRef nextTerm = next();
        seekStatus = nextTerm == null ? SeekStatus.END : SeekStatus.NOT_FOUND;
      }
    }
    return seekStatus;
  }

  // Visible for testing.
  SeekStatus seekCeilIgnoreField(BytesRef searchedTerm) throws IOException {
    return super.seekCeil(searchedTerm);
  }

  @Override
  public boolean seekExact(BytesRef searchedTerm) throws IOException {
    if (super.seekExact(searchedTerm)) {
      return termOccursInField();
    }
    return false;
  }

  @Override
  protected boolean isBeyondLastTerm(BytesRef searchedTerm, long blockStartFP) {
    return blockStartFP > fieldMetadata.getLastBlockStartFP() || super.isBeyondLastTerm(searchedTerm, blockStartFP);
  }

  /**
   * Reads the {@link BlockTermState} on the current line for this reader's field.
   *
   * @return The {@link BlockTermState}; or null if the term does not occur for the field.
   */
  @Override
  protected BlockTermState readTermState() throws IOException {
    termStatesReadBuffer.setPosition(blockFirstLineStart + blockHeader.getTermStatesBaseOffset() + blockLine.getTermStateRelativeOffset());
    return termState = STBlockLine.Serializer.readTermStateForField(
        fieldMetadata.getFieldInfo().number,
        termStatesReadBuffer,
        termStateSerializer,
        blockHeader,
        fieldInfos,
        scratchTermState
    );
  }
}

