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

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.lucene84.Lucene84PostingsReader;
import org.apache.lucene.codecs.lucene84.Lucene84PostingsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 *  {@link UniformSplitPostingsFormat} with block encoding using ROT13 cypher.
 */
public class UniformSplitRot13PostingsFormat extends PostingsFormat {

  public static volatile boolean encoderCalled;
  public static volatile boolean decoderCalled;
  public static volatile boolean blocksEncoded;
  public static volatile boolean fieldsMetadataEncoded;
  public static volatile boolean dictionaryEncoded;
  protected final boolean dictionaryOnHeap;

  public UniformSplitRot13PostingsFormat() {
    this("UniformSplitRot13", false);
  }

  protected UniformSplitRot13PostingsFormat(String name, boolean dictionaryOnHeap) {
    super(name);
    this.dictionaryOnHeap = dictionaryOnHeap;
  }

  public static void resetEncodingFlags() {
    encoderCalled = false;
    decoderCalled = false;
    blocksEncoded = false;
    fieldsMetadataEncoded = false;
    dictionaryEncoded = false;
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState segmentWriteState) throws IOException {
    PostingsWriterBase postingsWriter = new Lucene84PostingsWriter(segmentWriteState);
    boolean success = false;
    try {
      FieldsConsumer fieldsConsumer = createFieldsConsumer(segmentWriteState, postingsWriter);
      success = true;
      return fieldsConsumer;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsWriter);
      }
    }
  }

  protected FieldsConsumer createFieldsConsumer(SegmentWriteState segmentWriteState, PostingsWriterBase postingsWriter) throws IOException {
    return new UniformSplitTermsWriter(postingsWriter, segmentWriteState,
        UniformSplitTermsWriter.DEFAULT_TARGET_NUM_BLOCK_LINES,
        UniformSplitTermsWriter.DEFAULT_DELTA_NUM_LINES,
        getBlockEncoder()
    ) {
      @Override
      protected void writeDictionary(IndexDictionary.Builder dictionaryBuilder) throws IOException {
        recordBlockEncodingCall();
        super.writeDictionary(dictionaryBuilder);
        recordDictionaryEncodingCall();
      }
      @Override
      protected void writeEncodedFieldsMetadata(ByteBuffersDataOutput fieldsOutput) throws IOException {
        super.writeEncodedFieldsMetadata(fieldsOutput);
        recordFieldsMetadataEncodingCall();
      }
    };
  }

  protected void recordBlockEncodingCall() {
    if (encoderCalled) {
      blocksEncoded = true;
      encoderCalled = false;
    }
  }

  protected void recordFieldsMetadataEncodingCall() {
    if (encoderCalled) {
      fieldsMetadataEncoded = true;
      encoderCalled = false;
    }
  }

  protected void recordDictionaryEncodingCall() {
    if (encoderCalled) {
      dictionaryEncoded = true;
      encoderCalled = false;
    }
  }

  protected BlockEncoder getBlockEncoder() {
    return (blockBytes, length) -> {
      byte[] encodedBytes = Rot13CypherTestUtil.encode(blockBytes, Math.toIntExact(length));
      return new BlockEncoder.WritableBytes() {
        @Override
        public long size() {
          return encodedBytes.length;
        }

        @Override
        public void writeTo(DataOutput dataOutput) throws IOException {
          encoderCalled = true;
          dataOutput.writeBytes(encodedBytes, 0, encodedBytes.length);
        }
      };
    };
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState segmentReadState) throws IOException {
    PostingsReaderBase postingsReader = new Lucene84PostingsReader(segmentReadState);
    boolean success = false;
    try {
      FieldsProducer fieldsProducer = createFieldsProducer(segmentReadState, postingsReader);
      success = true;
      return fieldsProducer;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsReader);
      }
    }
  }

  protected FieldsProducer createFieldsProducer(SegmentReadState segmentReadState, PostingsReaderBase postingsReader) throws IOException {
    return new UniformSplitTermsReader(postingsReader, segmentReadState, getBlockDecoder(), dictionaryOnHeap);
  }

  protected BlockDecoder getBlockDecoder() {
    return (blockBytes, length) -> {
      decoderCalled = true;
      return new BytesRef(Rot13CypherTestUtil.decode(blockBytes, length));
    };
  }
}
