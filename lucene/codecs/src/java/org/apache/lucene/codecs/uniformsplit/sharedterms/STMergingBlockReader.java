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
import java.util.Map;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.uniformsplit.BlockDecoder;
import org.apache.lucene.codecs.uniformsplit.FieldMetadata;
import org.apache.lucene.codecs.uniformsplit.IndexDictionary;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

/**
 * {@link org.apache.lucene.index.TermsEnum} used when merging segments,
 * to enumerate the terms of an input segment and get all the fields {@link TermState}s
 * of each term.
 * <p>
 * It only supports calls to {@link #next()} and no seek method.
 *
 * @lucene.experimental
 */
public class STMergingBlockReader extends STBlockReader {

  public STMergingBlockReader(
      IndexDictionary.BrowserSupplier dictionaryBrowserSupplier,
      IndexInput blockInput,
      PostingsReaderBase postingsReader,
      FieldMetadata fieldMetadata,
      BlockDecoder blockDecoder,
      FieldInfos fieldInfos) throws IOException {
    super(dictionaryBrowserSupplier, blockInput, postingsReader, fieldMetadata, blockDecoder, fieldInfos);
  }

  @Override
  public SeekStatus seekCeil(BytesRef searchedTerm) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean seekExact(BytesRef searchedTerm) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void seekExact(BytesRef term, TermState state) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void seekExact(long ord) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected BlockTermState readTermStateIfNotRead() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BytesRef next() throws IOException {
    return nextTerm();
  }

  /**
   * Creates a new {@link PostingsEnum} for the provided field and {@link BlockTermState}.
   * @param reuse Previous {@link PostingsEnum} to reuse; or null to create a new one.
   * @param flags Postings flags.
   */
  public PostingsEnum postings(String fieldName, BlockTermState termState, PostingsEnum reuse, int flags) throws IOException {
    return postingsReader.postings(fieldInfos.fieldInfo(fieldName), termState, reuse, flags);
  }

  /**
   * Reads all the fields {@link TermState}s of the current term and put them
   * in the provided map. Clears the map first, before putting {@link TermState}s.
   */
  public void readFieldTermStatesMap(Map<String, BlockTermState> fieldTermStatesMap) throws IOException {
    if (term() != null) {
      termStatesReadBuffer.setPosition(blockFirstLineStart + blockHeader.getTermStatesBaseOffset() + blockLine.getTermStateRelativeOffset());
      ((STBlockLine.Serializer) blockLineReader).readFieldTermStatesMap(
          termStatesReadBuffer,
          termStateSerializer,
          blockHeader,
          fieldInfos,
          fieldTermStatesMap
      );
    }
  }
}
