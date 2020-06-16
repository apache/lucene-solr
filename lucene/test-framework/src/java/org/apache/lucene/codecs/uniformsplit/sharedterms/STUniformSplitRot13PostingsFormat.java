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

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.uniformsplit.IndexDictionary;
import org.apache.lucene.codecs.uniformsplit.UniformSplitRot13PostingsFormat;
import org.apache.lucene.codecs.uniformsplit.UniformSplitTermsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.ByteBuffersDataOutput;

/**
 *  {@link STUniformSplitPostingsFormat} with block encoding using ROT13 cypher.
 */
public class STUniformSplitRot13PostingsFormat extends UniformSplitRot13PostingsFormat {

  public STUniformSplitRot13PostingsFormat() {
    super("STUniformSplitRot13", false);
  }

  protected FieldsConsumer createFieldsConsumer(SegmentWriteState segmentWriteState, PostingsWriterBase postingsWriter) throws IOException {
    return new STUniformSplitTermsWriter(postingsWriter, segmentWriteState,
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
        recordBlockEncodingCall();
        super.writeEncodedFieldsMetadata(fieldsOutput);
        recordFieldsMetadataEncodingCall();
      }
    };
  }

  protected FieldsProducer createFieldsProducer(SegmentReadState segmentReadState, PostingsReaderBase postingsReader) throws IOException {
    return new STUniformSplitTermsReader(postingsReader, segmentReadState, getBlockDecoder(), dictionaryOnHeap);
  }
}
