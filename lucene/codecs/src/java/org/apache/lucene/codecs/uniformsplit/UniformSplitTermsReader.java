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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

import static org.apache.lucene.codecs.uniformsplit.UniformSplitPostingsFormat.*;

/**
 * A block-based terms index and dictionary based on the Uniform Split technique.
 *
 * @see UniformSplitTermsWriter
 * @lucene.experimental
 */
public class UniformSplitTermsReader extends FieldsProducer {

  private static final long BASE_RAM_USAGE = RamUsageEstimator.shallowSizeOfInstance(UniformSplitTermsReader.class)
      + RamUsageEstimator.shallowSizeOfInstance(IndexInput.class) * 2;

  protected final PostingsReaderBase postingsReader;
  protected final int version;
  protected final IndexInput blockInput;
  protected final IndexInput dictionaryInput;

  protected final Map<String, UniformSplitTerms> fieldToTermsMap;
  // Keeps the order of the field names; much more efficient than having a TreeMap for the fieldToTermsMap.
  protected final Collection<String> sortedFieldNames;

  /**
   * @param blockDecoder     Optional block decoder, may be null if none.
   *                         It can be used for decompression or decryption.
   * @param dictionaryOnHeap Whether to force loading the terms dictionary on-heap. By default it is kept off-heap without
   *                         impact on performance. If block encoding/decoding is used, then the dictionary is always
   *                         loaded on-heap whatever this parameter value is.
   */
  public UniformSplitTermsReader(PostingsReaderBase postingsReader, SegmentReadState state, BlockDecoder blockDecoder,
                                 boolean dictionaryOnHeap) throws IOException {
    this(postingsReader, state, blockDecoder, dictionaryOnHeap, FieldMetadata.Serializer.INSTANCE, NAME, VERSION_START, VERSION_CURRENT,
        TERMS_BLOCKS_EXTENSION, TERMS_DICTIONARY_EXTENSION);
   }
   
  /**
   * @see #UniformSplitTermsReader(PostingsReaderBase, SegmentReadState, BlockDecoder, boolean)
   */
  protected UniformSplitTermsReader(PostingsReaderBase postingsReader, SegmentReadState state, BlockDecoder blockDecoder,
                                    boolean dictionaryOnHeap, FieldMetadata.Serializer fieldMetadataReader,
                                    String codecName, int versionStart, int versionCurrent,
                                    String termsBlocksExtension, String dictionaryExtension) throws IOException {
     IndexInput dictionaryInput = null;
     IndexInput blockInput = null;
     boolean success = false;
     try {
       this.postingsReader = postingsReader;
       String segmentName = state.segmentInfo.name;
       String termsName = IndexFileNames.segmentFileName(segmentName, state.segmentSuffix, termsBlocksExtension);
       blockInput = state.directory.openInput(termsName, state.context);

       version = CodecUtil.checkIndexHeader(blockInput, codecName, versionStart,
           versionCurrent, state.segmentInfo.getId(), state.segmentSuffix);
       String indexName = IndexFileNames.segmentFileName(segmentName, state.segmentSuffix, dictionaryExtension);
       dictionaryInput = state.directory.openInput(indexName, state.context);

       CodecUtil.checkIndexHeader(dictionaryInput, codecName, version, version, state.segmentInfo.getId(), state.segmentSuffix);
       CodecUtil.checksumEntireFile(dictionaryInput);

       postingsReader.init(blockInput, state);
       CodecUtil.retrieveChecksum(blockInput);

       seekFieldsMetadata(blockInput);
       Collection<FieldMetadata> fieldMetadataCollection =
           readFieldsMetadata(blockInput, blockDecoder, state.fieldInfos, fieldMetadataReader, state.segmentInfo.maxDoc());

       fieldToTermsMap = new HashMap<>();
       this.blockInput = blockInput;
       this.dictionaryInput = dictionaryInput;

       fillFieldMap(postingsReader, state, blockDecoder, dictionaryOnHeap, dictionaryInput, blockInput, fieldMetadataCollection, state.fieldInfos);

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

  protected void fillFieldMap(PostingsReaderBase postingsReader, SegmentReadState state, BlockDecoder blockDecoder,
                              boolean dictionaryOnHeap, IndexInput dictionaryInput, IndexInput blockInput,
                              Collection<FieldMetadata> fieldMetadataCollection, FieldInfos fieldInfos) throws IOException {
    for (FieldMetadata fieldMetadata : fieldMetadataCollection) {
      IndexDictionary.BrowserSupplier dictionaryBrowserSupplier = createDictionaryBrowserSupplier(state, dictionaryInput, fieldMetadata, blockDecoder, dictionaryOnHeap);
      fieldToTermsMap.put(fieldMetadata.getFieldInfo().name,
          new UniformSplitTerms(blockInput, fieldMetadata, postingsReader, blockDecoder, dictionaryBrowserSupplier));
    }
  }

  protected IndexDictionary.BrowserSupplier createDictionaryBrowserSupplier(SegmentReadState state, IndexInput dictionaryInput, FieldMetadata fieldMetadata,
                                                                         BlockDecoder blockDecoder, boolean dictionaryOnHeap) throws IOException {
    return new FSTDictionary.BrowserSupplier(dictionaryInput, fieldMetadata.getDictionaryStartFP(), blockDecoder, dictionaryOnHeap);
  }

  /**
   * @param indexInput {@link IndexInput} must be positioned to the fields metadata
   *                   details by calling {@link #seekFieldsMetadata(IndexInput)} before this call.
   * @param blockDecoder Optional block decoder, may be null if none.
   */
  protected Collection<FieldMetadata> readFieldsMetadata(IndexInput indexInput, BlockDecoder blockDecoder, FieldInfos fieldInfos,
                                                                FieldMetadata.Serializer fieldMetadataReader, int maxNumDocs) throws IOException {
    int numFields = indexInput.readVInt();
    if (numFields < 0) {
      throw new CorruptIndexException("Illegal number of fields= " + numFields, indexInput);
    }
    return (blockDecoder != null && version >= VERSION_ENCODABLE_FIELDS_METADATA) ?
        readEncodedFieldsMetadata(numFields, indexInput, blockDecoder, fieldInfos, fieldMetadataReader, maxNumDocs)
        : readUnencodedFieldsMetadata(numFields, indexInput, fieldInfos, fieldMetadataReader, maxNumDocs);
  }

  protected Collection<FieldMetadata> readEncodedFieldsMetadata(int numFields, DataInput metadataInput, BlockDecoder blockDecoder,
                                                                FieldInfos fieldInfos, FieldMetadata.Serializer fieldMetadataReader,
                                                                int maxNumDocs) throws IOException {
    long encodedLength = metadataInput.readVLong();
    if (encodedLength < 0) {
      throw new CorruptIndexException("Illegal encoded length: " + encodedLength, metadataInput);
    }
    BytesRef decodedBytes = blockDecoder.decode(metadataInput, encodedLength);
    DataInput decodedMetadataInput = new ByteArrayDataInput(decodedBytes.bytes, 0, decodedBytes.length);
    return readUnencodedFieldsMetadata(numFields, decodedMetadataInput, fieldInfos, fieldMetadataReader, maxNumDocs);
  }

  protected Collection<FieldMetadata> readUnencodedFieldsMetadata(int numFields, DataInput metadataInput, FieldInfos fieldInfos,
                                                                  FieldMetadata.Serializer fieldMetadataReader, int maxNumDocs) throws IOException {
    Collection<FieldMetadata> fieldMetadataCollection = new ArrayList<>(numFields);
    for (int i = 0; i < numFields; i++) {
      fieldMetadataCollection.add(fieldMetadataReader.read(metadataInput, fieldInfos, maxNumDocs));
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
  protected void seekFieldsMetadata(IndexInput indexInput) throws IOException {
    indexInput.seek(indexInput.length() - CodecUtil.footerLength() - 8);
    indexInput.seek(indexInput.readLong());
  }
}
