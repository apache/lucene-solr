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
import java.util.Collection;

import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.uniformsplit.BlockDecoder;
import org.apache.lucene.codecs.uniformsplit.FieldMetadata;
import org.apache.lucene.codecs.uniformsplit.IndexDictionary;
import org.apache.lucene.codecs.uniformsplit.UniformSplitTerms;
import org.apache.lucene.codecs.uniformsplit.UniformSplitTermsReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.IndexInput;

import static org.apache.lucene.codecs.uniformsplit.sharedterms.STUniformSplitPostingsFormat.*;

/**
 * A block-based terms index and dictionary based on the Uniform Split technique,
 * and sharing all the fields terms in the same dictionary, with all the fields
 * of a term in the same block line.
 *
 * @see STUniformSplitTermsWriter
 * @lucene.experimental
 */
public class STUniformSplitTermsReader extends UniformSplitTermsReader {

  /**
   * @see UniformSplitTermsReader#UniformSplitTermsReader(PostingsReaderBase, SegmentReadState, BlockDecoder, boolean)
   */
  public STUniformSplitTermsReader(PostingsReaderBase postingsReader, SegmentReadState state, BlockDecoder blockDecoder,
                                   boolean dictionaryOnHeap) throws IOException {
    this(postingsReader, state, blockDecoder, dictionaryOnHeap, FieldMetadata.Serializer.INSTANCE,
        NAME, VERSION_START, VERSION_CURRENT, TERMS_BLOCKS_EXTENSION, TERMS_DICTIONARY_EXTENSION);
  }

  /**
   * @see UniformSplitTermsReader#UniformSplitTermsReader(PostingsReaderBase, SegmentReadState, BlockDecoder, boolean)
   */
  protected STUniformSplitTermsReader(PostingsReaderBase postingsReader, SegmentReadState state, BlockDecoder blockDecoder,
                                      boolean dictionaryOnHeap, FieldMetadata.Serializer fieldMetadataReader,
                                      String codecName, int versionStart, int versionCurrent,
                                      String termsBlocksExtension, String dictionaryExtension) throws IOException {
    super(postingsReader, state, blockDecoder, dictionaryOnHeap, fieldMetadataReader, codecName, versionStart, versionCurrent, termsBlocksExtension, dictionaryExtension);
  }

  @Override
  protected void fillFieldMap(PostingsReaderBase postingsReader, SegmentReadState state, BlockDecoder blockDecoder,
                              boolean dictionaryOnHeap, IndexInput dictionaryInput, IndexInput blockInput,
                              Collection<FieldMetadata> fieldMetadataCollection, FieldInfos fieldInfos) throws IOException {
    if (!fieldMetadataCollection.isEmpty()) {
      FieldMetadata unionFieldMetadata = createUnionFieldMetadata(fieldMetadataCollection);
      // Share the same immutable dictionary between all fields.
      IndexDictionary.BrowserSupplier dictionaryBrowserSupplier = createDictionaryBrowserSupplier(state, dictionaryInput, unionFieldMetadata, blockDecoder, dictionaryOnHeap);
      for (FieldMetadata fieldMetadata : fieldMetadataCollection) {
        fieldToTermsMap.put(fieldMetadata.getFieldInfo().name,
            new STUniformSplitTerms(blockInput, fieldMetadata, unionFieldMetadata, postingsReader, blockDecoder, fieldInfos, dictionaryBrowserSupplier));
      }
    }
  }

  @Override
  protected long getTermsRamBytesUsed() {
    long termsRamUsage = 0L;
    long dictionaryRamUsage = 0L;
    for (UniformSplitTerms terms : fieldToTermsMap.values()) {
      termsRamUsage += terms.ramBytesUsedWithoutDictionary();
      dictionaryRamUsage = terms.getDictionaryRamBytesUsed();
    }
    termsRamUsage += dictionaryRamUsage;
    return termsRamUsage;
  }

  /**
   * Creates a virtual {@link FieldMetadata} that is the union of the given {@link FieldMetadata}s.
   * Its {@link FieldMetadata#getFirstBlockStartFP}, {@link FieldMetadata#getLastBlockStartFP}
   * and {@link FieldMetadata#getLastTerm()} are respectively the min and
   * max among the {@link FieldMetadata}s provided as parameter.
   */
  protected FieldMetadata createUnionFieldMetadata(Iterable<FieldMetadata> fieldMetadataIterable) {
    UnionFieldMetadataBuilder builder = new UnionFieldMetadataBuilder();
    for (FieldMetadata fieldMetadata : fieldMetadataIterable) {
      builder.addFieldMetadata(fieldMetadata);
    }
    return builder.build();
  }
}
