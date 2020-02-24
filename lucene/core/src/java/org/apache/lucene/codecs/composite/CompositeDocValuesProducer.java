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

package org.apache.lucene.codecs.composite;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;

/**
 * {@link DocValuesProducer} with pluggable implementation of {@link DocValuesProducerSupplier}.
 */
public class CompositeDocValuesProducer extends DocValuesProducer {

  protected final IndexInput indexInput;
  private final Map<Integer, CompositeFieldMetadata> fieldMetadataMap;

  private BinaryProducer binaryProducer;
  private NumericProducer numericProducer;
  private SortedNumericProducer sortedNumericProducer;
  private SortedProducer sortedProducer;
  private SortedSetProducer sortedSetProducer;

  public CompositeDocValuesProducer(SegmentReadState state, DocValuesProducerSupplier producerSupplier,
                                    String codecName, String extension, int versionStart, int versionCurrent) throws IOException {
    IndexInput indexInput = null;
    boolean success = false;
    try {
      String segmentName = state.segmentInfo.name;
      String termsName = IndexFileNames.segmentFileName(segmentName, state.segmentSuffix, extension);
      indexInput = state.directory.openInput(termsName, state.context);

      CodecUtil.checkIndexHeader(indexInput, codecName, versionStart, versionCurrent, state.segmentInfo.getId(), state.segmentSuffix);
      CodecUtil.retrieveChecksum(indexInput);

      seekFieldsMetadata(indexInput);
      fieldMetadataMap = parseFieldsMetadata(indexInput);
      this.indexInput = indexInput;

      sortedSetProducer = producerSupplier.createSortedSetProducer(state);
      sortedProducer = producerSupplier.createSortedProducer(state);
      sortedNumericProducer = producerSupplier.createSortedNumericProducer(state);
      binaryProducer = producerSupplier.createBinaryProducer(state);
      numericProducer = producerSupplier.createNumericProducer(state);

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(indexInput);
      }
    }
  }

  @Override
  public NumericDocValues getNumeric(FieldInfo field) throws IOException {
    CompositeFieldMetadata compositeFieldMetadata = fieldMetadataMap.get(field.number);
    if (compositeFieldMetadata == null) {
      return DocValues.emptyNumeric();
    }
    return numericProducer.getNumeric(field, compositeFieldMetadata, indexInput);
  }

  @Override
  public BinaryDocValues getBinary(FieldInfo field) throws IOException {
    CompositeFieldMetadata compositeFieldMetadata = fieldMetadataMap.get(field.number);
    if (compositeFieldMetadata == null) {
      return DocValues.emptyBinary();
    }
    return binaryProducer.getBinary(field, compositeFieldMetadata, indexInput);
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    CompositeFieldMetadata compositeFieldMetadata = fieldMetadataMap.get(field.number);
    if (compositeFieldMetadata == null) {
      return DocValues.emptySorted();
    }
    return sortedProducer.getSorted(field, compositeFieldMetadata, indexInput);
  }

  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    CompositeFieldMetadata compositeFieldMetadata = fieldMetadataMap.get(field.number);
    if (compositeFieldMetadata == null) {
      return DocValues.emptySortedNumeric(0);
    }
    return sortedNumericProducer.getSortedNumeric(field, compositeFieldMetadata, indexInput);
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    CompositeFieldMetadata compositeFieldMetadata = fieldMetadataMap.get(field.number);
    if (compositeFieldMetadata == null) {
      return DocValues.emptySortedSet();
    }
    return sortedSetProducer.getSortedSet(field, compositeFieldMetadata, indexInput);
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(indexInput);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(indexInput);
  }

  @Override
  public long ramBytesUsed() {
    long baseRamBytesUsed = numericProducer.ramBytesUsed();
    baseRamBytesUsed += sortedNumericProducer.ramBytesUsed();
    baseRamBytesUsed += binaryProducer.ramBytesUsed();
    baseRamBytesUsed += sortedProducer.ramBytesUsed();
    baseRamBytesUsed += sortedSetProducer.ramBytesUsed();
    return baseRamBytesUsed;
  }

  /**
   * Positions the given {@link IndexInput} at the beginning of the fields metadata.
   */
  protected static void seekFieldsMetadata(IndexInput indexInput) throws IOException {
    indexInput.seek(indexInput.length() - CodecUtil.footerLength() - Long.BYTES);
    indexInput.seek(indexInput.readLong());
  }

  /**
   * @param indexInput {@link IndexInput} must be positioned to the fields metadata
   *                   details by calling {@link #seekFieldsMetadata(IndexInput)} before this call.
   */
  protected Map<Integer, CompositeFieldMetadata> parseFieldsMetadata(IndexInput indexInput) throws IOException {
    Map<Integer, CompositeFieldMetadata> fieldMetadataMap = new HashMap<>();
    int fieldsNumber = indexInput.readVInt();
    CompositeFieldMetadata.Serializer metaFieldSerializer = new CompositeFieldMetadata.Serializer();
    for (int i = 0; i < fieldsNumber; i++) {
      CompositeFieldMetadata fieldMetadata = metaFieldSerializer.read(indexInput);
      fieldMetadataMap.put(fieldMetadata.getFieldId(), fieldMetadata);
    }
    return fieldMetadataMap;
  }

  public interface DocValuesProducerSupplier {

    NumericProducer createNumericProducer(SegmentReadState state) throws IOException;

    BinaryProducer createBinaryProducer(SegmentReadState state) throws IOException;

    SortedNumericProducer createSortedNumericProducer(SegmentReadState state) throws IOException;

    SortedProducer createSortedProducer(SegmentReadState state) throws IOException;

    SortedSetProducer createSortedSetProducer(SegmentReadState state) throws IOException;
  }

  public interface BinaryProducer extends Accountable {
    /**
     * Gets binary docvalues for a field.
     */
    BinaryDocValues getBinary(FieldInfo field, CompositeFieldMetadata compositeFieldMetadata, IndexInput indexInput) throws IOException;
  }

  public interface NumericProducer extends Accountable {
    /**
     * Gets numeric docvalues for a field.
     */
    NumericDocValues getNumeric(FieldInfo field, CompositeFieldMetadata compositeFieldMetadata, IndexInput indexInput) throws IOException;
  }

  public interface SortedNumericProducer extends Accountable {
    /**
     * Gets pre-sorted numeric docvalues for a field.
     */
    SortedNumericDocValues getSortedNumeric(FieldInfo field, CompositeFieldMetadata compositeFieldMetadata, IndexInput indexInput) throws IOException;
  }

  public interface SortedProducer extends Accountable {
    /**
     * Gets pre-sorted byte[] docvalues for a field.
     */
    SortedDocValues getSorted(FieldInfo field, CompositeFieldMetadata fieldMetadata, IndexInput indexInput) throws IOException;
  }

  public interface SortedSetProducer extends Accountable {
    /**
     * Gets multi-valued pre-sorted byte[] docvalues for a field.
     */
    SortedSetDocValues getSortedSet(FieldInfo field, CompositeFieldMetadata compositeFieldMetadata, IndexInput indexInput) throws IOException;
  }
}