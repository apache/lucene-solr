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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

import static org.apache.lucene.codecs.uniformsplit.UniformSplitPostingsFormat.NAME;
import static org.apache.lucene.codecs.uniformsplit.UniformSplitPostingsFormat.TERMS_BLOCKS_EXTENSION;
import static org.apache.lucene.codecs.uniformsplit.UniformSplitPostingsFormat.TERMS_DICTIONARY_EXTENSION;
import static org.apache.lucene.codecs.uniformsplit.UniformSplitPostingsFormat.VERSION_CURRENT;

/**
 * A block-based terms index and dictionary based on the Uniform Split technique.
 *
 * @see UniformSplitTermsWriter
 * @lucene.experimental
 */
public class UniformSplitTermsReader extends FieldsProducer {

  protected static final int VERSION_START = 0;

  private static final long BASE_RAM_USAGE = RamUsageEstimator.shallowSizeOfInstance(UniformSplitTermsReader.class)
      + RamUsageEstimator.shallowSizeOfInstance(IndexInput.class) * 2;

  protected final PostingsReaderBase postingsReader;
  protected final IndexInput blockInput;
  protected final IndexInput dictionaryInput;

  protected final Map<String, UniformSplitTerms> fieldToTermsMap;
  // Keeps the order of the field names; much more efficient than having a TreeMap for the fieldToTermsMap.
  protected final Collection<String> sortedFieldNames;

  /**
   * @param blockDecoder Optional block decoder, may be null if none.
   *                     It can be used for decompression or decryption.
   */
  public UniformSplitTermsReader(PostingsReaderBase postingsReader, SegmentReadState state, BlockDecoder blockDecoder) throws IOException {
    this(postingsReader, state, blockDecoder, NAME, VERSION_START, VERSION_CURRENT,
        TERMS_BLOCKS_EXTENSION, TERMS_DICTIONARY_EXTENSION);
   }
   
  /**
   * @param blockDecoder Optional block decoder, may be null if none.
   *                     It can be used for decompression or decryption.
   */
  protected UniformSplitTermsReader(PostingsReaderBase postingsReader, SegmentReadState state, BlockDecoder blockDecoder,
                                     String codecName, int versionStart, int versionCurrent, String termsBlocksExtension, String dictionaryExtension) throws IOException {
     IndexInput dictionaryInput = null;
     IndexInput blockInput = null;
     boolean success = false;
     try {
       this.postingsReader = postingsReader;
       String segmentName = state.segmentInfo.name;
       String termsName = IndexFileNames.segmentFileName(segmentName, state.segmentSuffix, termsBlocksExtension);
       blockInput = state.directory.openInput(termsName, state.context);

       int version = CodecUtil.checkIndexHeader(blockInput, codecName, versionStart,
           versionCurrent, state.segmentInfo.getId(), state.segmentSuffix);
       String indexName = IndexFileNames.segmentFileName(segmentName, state.segmentSuffix, dictionaryExtension);
       dictionaryInput = state.directory.openInput(indexName, state.context);

       CodecUtil.checkIndexHeader(dictionaryInput, codecName, version, version, state.segmentInfo.getId(), state.segmentSuffix);
       CodecUtil.checksumEntireFile(dictionaryInput);

       postingsReader.init(blockInput, state);
       CodecUtil.retrieveChecksum(blockInput);

       seekFieldsMetadata(blockInput);
       Collection<FieldMetadata> fieldMetadataCollection = parseFieldsMetadata(blockInput, state.fieldInfos);

       fieldToTermsMap = new HashMap<>();
       this.blockInput = blockInput;
       this.dictionaryInput = dictionaryInput;

       fillFieldMap(postingsReader, blockDecoder, dictionaryInput, blockInput, fieldMetadataCollection, state.fieldInfos);

       List<String> fieldNames = new ArrayList<>(fieldToTermsMap.keySet());
       Collections.sort(fieldNames);
       sortedFieldNames = Collections.unmodifiableList(fieldNames);

       success = true;
     } finally {
       if (!success) {
         IOUtils.closeWhileHandlingException(blockInput, dictionaryInput);
       }
     }
   }

  protected void fillFieldMap(PostingsReaderBase postingsReader, BlockDecoder blockDecoder,
                    IndexInput dictionaryInput, IndexInput blockInput,
                    Collection<FieldMetadata> fieldMetadataCollection, FieldInfos fieldInfos) throws IOException {
    for (FieldMetadata fieldMetadata : fieldMetadataCollection) {
      fieldToTermsMap.put(fieldMetadata.getFieldInfo().name,
          new UniformSplitTerms(dictionaryInput, blockInput, fieldMetadata, postingsReader, blockDecoder));
    }
  }

  /**
   * @param indexInput {@link IndexInput} must be positioned to the fields metadata
   *                   details by calling {@link #seekFieldsMetadata(IndexInput)} before this call.
   */
  protected static Collection<FieldMetadata> parseFieldsMetadata(IndexInput indexInput, FieldInfos fieldInfos) throws IOException {
    Collection<FieldMetadata> fieldMetadataCollection = new ArrayList<>();
    int fieldsNumber = indexInput.readVInt();
    for (int i = 0; i < fieldsNumber; i++) {
      fieldMetadataCollection.add(FieldMetadata.read(indexInput, fieldInfos));
    }
    return fieldMetadataCollection;
  }


  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(blockInput, dictionaryInput, postingsReader);
    } finally {
      // Clear so refs to terms index is GCable even if app hangs onto us.
      fieldToTermsMap.clear();
    }
  }

  @Override
  public void checkIntegrity() throws IOException {
    // term dictionary
    CodecUtil.checksumEntireFile(blockInput);

    // postings
    postingsReader.checkIntegrity();
  }

  @Override
  public Iterator<String> iterator() {
    return sortedFieldNames.iterator();
  }

  @Override
  public Terms terms(String field) {
    return fieldToTermsMap.get(field);
  }

  @Override
  public int size() {
    return fieldToTermsMap.size();
  }

  @Override
  public long ramBytesUsed() {
    long ramUsage = BASE_RAM_USAGE;
    ramUsage += postingsReader.ramBytesUsed();
    ramUsage += RamUsageUtil.ramBytesUsedByHashMapOfSize(fieldToTermsMap.size());
    ramUsage += getTermsRamBytesUsed();
    ramUsage += RamUsageUtil.ramBytesUsedByUnmodifiableArrayListOfSize(sortedFieldNames.size());
    return ramUsage;
  }

  protected long getTermsRamBytesUsed() {
    long ramUsage = 0L;
    for (UniformSplitTerms terms : fieldToTermsMap.values()) {
      ramUsage += terms.ramBytesUsed();
    }
    return ramUsage;
  }

  /**
   * Positions the given {@link IndexInput} at the beginning of the fields metadata.
   */
  protected static void seekFieldsMetadata(IndexInput indexInput) throws IOException {
    indexInput.seek(indexInput.length() - CodecUtil.footerLength() - 8);
    indexInput.seek(indexInput.readLong());
  }
}
