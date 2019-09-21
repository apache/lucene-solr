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

import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.uniformsplit.BlockDecoder;
import org.apache.lucene.codecs.uniformsplit.DictionaryBrowserSupplier;
import org.apache.lucene.codecs.uniformsplit.FieldMetadata;
import org.apache.lucene.codecs.uniformsplit.UniformSplitTerms;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * Extends {@link UniformSplitTerms} for a shared-terms dictionary, with
 * all the fields of a term in the same block line.
 *
 * @lucene.experimental
 */
public class STUniformSplitTerms extends UniformSplitTerms {

  protected final FieldMetadata unionFieldMetadata;
  protected final FieldInfos fieldInfos;

  protected STUniformSplitTerms(IndexInput blockInput, FieldMetadata fieldMetadata,
                                FieldMetadata unionFieldMetadata, PostingsReaderBase postingsReader,
                                BlockDecoder blockDecoder, FieldInfos fieldInfos, DictionaryBrowserSupplier dictionaryBrowserSupplier) {
    super(blockInput, fieldMetadata, postingsReader, blockDecoder, dictionaryBrowserSupplier);
    this.unionFieldMetadata = unionFieldMetadata;
    this.fieldInfos = fieldInfos;
  }

  @Override
  public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
    return new STIntersectBlockReader(compiled, startTerm, dictionaryBrowserSupplier, blockInput, postingsReader, fieldMetadata, blockDecoder, fieldInfos);
  }

  @Override
  public TermsEnum iterator() throws IOException {
    return new STBlockReader(dictionaryBrowserSupplier, blockInput, postingsReader, fieldMetadata, blockDecoder, fieldInfos);
  }

  STMergingBlockReader createMergingBlockReader() throws IOException {
    return new STMergingBlockReader(dictionaryBrowserSupplier, blockInput, postingsReader, unionFieldMetadata, blockDecoder, fieldInfos);
  }
}