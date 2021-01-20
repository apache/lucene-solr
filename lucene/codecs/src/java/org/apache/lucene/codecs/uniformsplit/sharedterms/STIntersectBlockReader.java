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
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.uniformsplit.BlockDecoder;
import org.apache.lucene.codecs.uniformsplit.FieldMetadata;
import org.apache.lucene.codecs.uniformsplit.IndexDictionary;
import org.apache.lucene.codecs.uniformsplit.IntersectBlockReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * The "intersect" {@link org.apache.lucene.index.TermsEnum} response to {@link
 * STUniformSplitTerms#intersect(CompiledAutomaton, BytesRef)}, intersecting the terms with an
 * automaton.
 *
 * @lucene.experimental
 */
public class STIntersectBlockReader extends IntersectBlockReader {

  protected final FieldInfos fieldInfos;

  public STIntersectBlockReader(
      CompiledAutomaton compiled,
      BytesRef startTerm,
      IndexDictionary.BrowserSupplier dictionaryBrowserSupplier,
      IndexInput blockInput,
      PostingsReaderBase postingsReader,
      FieldMetadata fieldMetadata,
      BlockDecoder blockDecoder,
      FieldInfos fieldInfos)
      throws IOException {
    super(
        compiled,
        startTerm,
        dictionaryBrowserSupplier,
        blockInput,
        postingsReader,
        fieldMetadata,
        blockDecoder);
    this.fieldInfos = fieldInfos;
  }

  // ---------------------------------------------
  // The methods below are duplicate from STBlockReader.
  //
  // This class inherits code from both IntersectBlockReader and STBlockReader.
  // We choose to extend IntersectBlockReader because this is the one that
  // runs the next(), reads the block lines and keeps the reader state.
  // But we still need the STBlockReader logic to skip terms that do not occur
  // in this TermsEnum field.
  // So we end up having a couple of methods directly duplicate from STBlockReader.
  // We tried various different approaches to avoid duplicating the code, but
  // actually this becomes difficult to read and to understand. This is simpler
  // to duplicate and explain it here.
  // ---------------------------------------------

  @Override
  public BytesRef next() throws IOException {
    BytesRef next;
    do {
      next = super.next();
      if (next == null) {
        // No more terms.
        return null;
      }
      // Check if the term occurs for the searched field.
    } while (!termOccursInField());
    // The term occurs for the searched field.
    return next;
  }

  private boolean termOccursInField() throws IOException {
    readTermStateIfNotRead();
    return termState != null;
  }

  @Override
  protected STBlockLine.Serializer createBlockLineSerializer() {
    return new STBlockLine.Serializer();
  }

  /**
   * Reads the {@link BlockTermState} on the current line for the specific field corresponding to
   * this reader. Returns null if the term does not occur for the field.
   */
  @Override
  protected BlockTermState readTermState() throws IOException {
    termStatesReadBuffer.setPosition(
        blockFirstLineStart
            + blockHeader.getTermStatesBaseOffset()
            + blockLine.getTermStateRelativeOffset());
    return ((STBlockLine.Serializer) blockLineReader)
        .readTermStateForField(
            fieldMetadata.getFieldInfo().number,
            termStatesReadBuffer,
            termStateSerializer,
            blockHeader,
            fieldInfos,
            scratchTermState);
  }
}
