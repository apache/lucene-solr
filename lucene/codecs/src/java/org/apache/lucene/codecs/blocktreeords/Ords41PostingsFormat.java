package org.apache.lucene.codecs.blocktreeords;

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

import java.io.IOException;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsReader;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.IOUtils;

/** Uses {@link OrdsBlockTreeTermsWriter} with {@link Lucene41PostingsWriter}. */
public class Ords41PostingsFormat extends PostingsFormat {

  private final int minTermBlockSize;
  private final int maxTermBlockSize;

  /**
   * Fixed packed block size, number of integers encoded in 
   * a single packed block.
   */
  // NOTE: must be multiple of 64 because of PackedInts long-aligned encoding/decoding
  public final static int BLOCK_SIZE = 128;

  /** Creates {@code Lucene41PostingsFormat} with default
   *  settings. */
  public Ords41PostingsFormat() {
    this(OrdsBlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE, OrdsBlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
  }

  /** Creates {@code Lucene41PostingsFormat} with custom
   *  values for {@code minBlockSize} and {@code
   *  maxBlockSize} passed to block terms dictionary.
   *  @see OrdsBlockTreeTermsWriter#OrdsBlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int) */
  public Ords41PostingsFormat(int minTermBlockSize, int maxTermBlockSize) {
    super("OrdsLucene41");
    this.minTermBlockSize = minTermBlockSize;
    assert minTermBlockSize > 1;
    this.maxTermBlockSize = maxTermBlockSize;
    assert minTermBlockSize <= maxTermBlockSize;
  }

  @Override
  public String toString() {
    return getName() + "(blocksize=" + BLOCK_SIZE + ")";
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase postingsWriter = new Lucene41PostingsWriter(state);

    boolean success = false;
    try {
      FieldsConsumer ret = new OrdsBlockTreeTermsWriter(state, 
                                                        postingsWriter,
                                                        minTermBlockSize, 
                                                        maxTermBlockSize);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsWriter);
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    PostingsReaderBase postingsReader = new Lucene41PostingsReader(state.directory,
                                                                   state.fieldInfos,
                                                                   state.segmentInfo,
                                                                   state.context,
                                                                   state.segmentSuffix);
    boolean success = false;
    try {
      FieldsProducer ret = new OrdsBlockTreeTermsReader(state.directory,
                                                        state.fieldInfos,
                                                        state.segmentInfo,
                                                        postingsReader,
                                                        state.context,
                                                        state.segmentSuffix);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsReader);
      }
    }
  }
}
