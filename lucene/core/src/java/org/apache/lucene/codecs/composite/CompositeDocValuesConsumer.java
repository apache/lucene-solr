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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

/**
 * {@link DocValuesConsumer} with pluggable implementation of {@link DocValuesConsumerSupplier}.
 */
public class CompositeDocValuesConsumer extends DocValuesConsumer {

  protected final IndexOutput indexOutput;
  protected final SegmentWriteState state;

  protected final BinaryConsumer binaryConsumer;
  protected final NumericConsumer numericConsumer;
  protected final SortedNumericConsumer sortedNumericConsumer;
  protected final SortedConsumer sortedConsumer;
  protected final SortedSetConsumer sortedSetConsumer;

  protected final ByteBuffersDataOutput fieldsMetadataOutput;
  protected final CompositeFieldMetadata.Serializer fieldMetadataSerializer;
  protected int fieldCount;
  protected boolean isClosed;

  public CompositeDocValuesConsumer(SegmentWriteState state, DocValuesConsumerSupplier consumerSupplier,
                                    String codecName, String extension, int versionCurrent) throws IOException {
    this.state = state;
    boolean success = false;
    IndexOutput indexOutput = null;
    try {
      String termsName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, extension);
      indexOutput = state.directory.createOutput(termsName, state.context);
      CodecUtil.writeIndexHeader(indexOutput, codecName, versionCurrent, state.segmentInfo.getId(), state.segmentSuffix);

      binaryConsumer = consumerSupplier.createBinaryConsumer(state);
      numericConsumer = consumerSupplier.createNumericConsumer(state);
      sortedNumericConsumer = consumerSupplier.createSortedNumericConsumer(state);
      sortedSetConsumer = consumerSupplier.createSortedSetConsumer(state);
      sortedConsumer = consumerSupplier.createSortedConsumer(state);

      fieldsMetadataOutput = ByteBuffersDataOutput.newResettableInstance();
      fieldMetadataSerializer = new CompositeFieldMetadata.Serializer();
      this.indexOutput = indexOutput;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(indexOutput);
        isClosed = true;
      }
    }
  }

  @Override
  public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    CompositeFieldMetadata fieldMetadata = numericConsumer.addNumeric(field, valuesProducer, indexOutput);
    if (fieldMetadata != null) {
      fieldMetadataSerializer.write(fieldsMetadataOutput, fieldMetadata);
      fieldCount++;
    }
  }

  @Override
  public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    CompositeFieldMetadata fieldMetadata = binaryConsumer.addBinary(field, valuesProducer, indexOutput);
    if (fieldMetadata != null) {
      fieldMetadataSerializer.write(fieldsMetadataOutput, fieldMetadata);
      fieldCount++;
    }
  }

  @Override
  public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    CompositeFieldMetadata fieldMetadata = sortedConsumer.addSorted(field, valuesProducer, indexOutput);
    if (fieldMetadata != null) {
      fieldMetadataSerializer.write(fieldsMetadataOutput, fieldMetadata);
      fieldCount++;
    }
  }

  @Override
  public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    CompositeFieldMetadata fieldMetadata = sortedNumericConsumer.addSortedNumeric(field, valuesProducer, indexOutput);
    if (fieldMetadata != null) {
      fieldMetadataSerializer.write(fieldsMetadataOutput, fieldMetadata);
      fieldCount++;
    }
  }

  @Override
  public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    CompositeFieldMetadata fieldMetadata = sortedSetConsumer.addSortedSet(field, valuesProducer, indexOutput);
    if (fieldMetadata != null) {
      fieldMetadataSerializer.write(fieldsMetadataOutput, fieldMetadata);
      fieldCount++;
    }
  }

  @Override
  public void close() throws IOException {
    boolean success = false;
    try {
      if (!isClosed) {
        writeFieldsMetadata(fieldCount, fieldsMetadataOutput);
      }
      success = true;
      isClosed = true;
    } finally {
      if (success) {
        IOUtils.close(indexOutput);
      } else {
        IOUtils.closeWhileHandlingException(indexOutput);
      }
    }
  }

  protected void writeFieldsMetadata(int fieldsNumber, ByteBuffersDataOutput fieldsOutput) throws IOException {
    long fieldsStartPosition = indexOutput.getFilePointer();
    indexOutput.writeVInt(fieldsNumber);
    fieldsOutput.copyTo(indexOutput);
    indexOutput.writeLong(fieldsStartPosition);
    CodecUtil.writeFooter(indexOutput);
  }

  public interface DocValuesConsumerSupplier {

    BinaryConsumer createBinaryConsumer(SegmentWriteState state) throws IOException;

    NumericConsumer createNumericConsumer(SegmentWriteState state) throws IOException;

    SortedNumericConsumer createSortedNumericConsumer(SegmentWriteState state) throws IOException;

    SortedConsumer createSortedConsumer(SegmentWriteState state) throws IOException;

    SortedSetConsumer createSortedSetConsumer(SegmentWriteState state) throws IOException;
  }

  public interface BinaryConsumer {
    /**
     * Adds binary docvalues for a field.
     *
     * @return null if there is nothing to write.
     */
    CompositeFieldMetadata addBinary(FieldInfo field, DocValuesProducer valuesProducer, IndexOutput indexOutput) throws IOException;
  }

  public interface NumericConsumer {
    /**
     * Writes numeric docvalues for a field.
     *
     * @return null if there is nothing to write.
     */
    CompositeFieldMetadata addNumeric(FieldInfo field, DocValuesProducer valuesProducer, IndexOutput indexOutput) throws IOException;
  }

  public interface SortedNumericConsumer {
    /**
     * Writes pre-sorted numeric docvalues for a field.
     *
     * @return null if there is nothing to write.
     */
    CompositeFieldMetadata addSortedNumeric(FieldInfo field, DocValuesProducer valuesProducer, IndexOutput indexOutput) throws IOException;
  }

  public interface SortedConsumer {
    /**
     * Writes pre-sorted byte[] docvalues for a field.
     *
     * @return null if there is nothing to write.
     */
    CompositeFieldMetadata addSorted(FieldInfo field, DocValuesProducer valuesProducer, IndexOutput indexOutput) throws IOException;
  }

  public interface SortedSetConsumer {
    /**
     * Writes multi-valued pre-sorted byte[] docvalues for a field.
     *
     * @return null if there is nothing to write.
     */
    CompositeFieldMetadata addSortedSet(FieldInfo field, DocValuesProducer valuesProducer, IndexOutput indexOutput) throws IOException;
  }
}