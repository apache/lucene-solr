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

import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * {@link Terms} based on the Uniform Split technique.
 * <p>
 * The {@link IndexDictionary index dictionary} is lazy loaded only when
 * {@link TermsEnum#seekCeil} or {@link TermsEnum#seekExact} are called
 * (it is not loaded for a direct terms enumeration).
 *
 * @see UniformSplitTermsWriter
 * @lucene.experimental
 */
public class UniformSplitTerms extends Terms implements Accountable {

  private static final long BASE_RAM_USAGE = RamUsageEstimator.shallowSizeOfInstance(UniformSplitTerms.class);

  protected final IndexInput blockInput;
  protected final FieldMetadata fieldMetadata;
  protected final PostingsReaderBase postingsReader;
  protected final BlockDecoder blockDecoder;
  protected final DictionaryBrowserSupplier dictionaryBrowserSupplier;

  /**
   * @param blockDecoder Optional block decoder, may be null if none. It can be used for decompression or decryption.
   */
  protected UniformSplitTerms(IndexInput dictionaryInput, IndexInput blockInput, FieldMetadata fieldMetadata,
                    PostingsReaderBase postingsReader, BlockDecoder blockDecoder) throws IOException {
    this(blockInput, fieldMetadata, postingsReader, blockDecoder,
        new DictionaryBrowserSupplier(dictionaryInput, fieldMetadata.getDictionaryStartFP(), blockDecoder));
  }

  /**
   * @param blockDecoder Optional block decoder, may be null if none. It can be used for decompression or decryption.
   */
  protected UniformSplitTerms(IndexInput blockInput, FieldMetadata fieldMetadata,
                              PostingsReaderBase postingsReader, BlockDecoder blockDecoder,
                              DictionaryBrowserSupplier dictionaryBrowserSupplier) {
    assert fieldMetadata != null;
    assert fieldMetadata.getFieldInfo() != null;
    assert fieldMetadata.getLastTerm() != null;
    assert dictionaryBrowserSupplier != null;
    this.blockInput = blockInput;
    this.fieldMetadata = fieldMetadata;
    this.postingsReader = postingsReader;
    this.blockDecoder = blockDecoder;
    this.dictionaryBrowserSupplier = dictionaryBrowserSupplier;
  }

  @Override
  public TermsEnum iterator() throws IOException {
    return new BlockReader(dictionaryBrowserSupplier, blockInput, postingsReader, fieldMetadata, blockDecoder);
  }

  @Override
  public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
    checkIntersectAutomatonType(compiled);
    return new IntersectBlockReader(compiled, startTerm, dictionaryBrowserSupplier, blockInput, postingsReader, fieldMetadata, blockDecoder);
  }

  protected void checkIntersectAutomatonType(CompiledAutomaton automaton) {
    // This check is consistent with other impls and precondition stated in javadoc.
    if (automaton.type != CompiledAutomaton.AUTOMATON_TYPE.NORMAL) {
      throw new IllegalArgumentException("please use CompiledAutomaton.getTermsEnum instead");
    }
  }

  @Override
  public BytesRef getMax() {
    return fieldMetadata.getLastTerm();
  }

  @Override
  public long size() {
    return fieldMetadata.getNumTerms();
  }

  @Override
  public long getSumTotalTermFreq() {
    return fieldMetadata.getSumTotalTermFreq();
  }

  @Override
  public long getSumDocFreq() {
    return fieldMetadata.getSumDocFreq();
  }

  @Override
  public int getDocCount() {
    return fieldMetadata.getDocCount();
  }

  @Override
  public boolean hasFreqs() {
    return fieldMetadata.getFieldInfo().getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
  }

  @Override
  public boolean hasOffsets() {
    return fieldMetadata.getFieldInfo().getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
  }

  @Override
  public boolean hasPositions() {
    return fieldMetadata.getFieldInfo().getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
  }

  @Override
  public boolean hasPayloads() {
    return fieldMetadata.getFieldInfo().hasPayloads();
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsedWithoutDictionary() + getDictionaryRamBytesUsed();
  }

  public long ramBytesUsedWithoutDictionary() {
    return BASE_RAM_USAGE + fieldMetadata.ramBytesUsed();
  }

  public long getDictionaryRamBytesUsed() {
    return dictionaryBrowserSupplier.ramBytesUsed();
  }
}