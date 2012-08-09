package org.apache.lucene.codecs.block;

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

import org.apache.lucene.codecs.BlockTreeTermsReader;
import org.apache.lucene.codecs.BlockTreeTermsWriter;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.IOUtils;

/**
 * Encodes/decode postings in packed int blocks for faster
 * decode.
 */
public final class BlockPostingsFormat extends PostingsFormat {
  public static final String DOC_EXTENSION = "doc";
  public static final String POS_EXTENSION = "pos";
  public static final String PAY_EXTENSION = "pay";

  private final int minTermBlockSize;
  private final int maxTermBlockSize;

  // nocommit is this right?:
  // NOTE: should be at least 64 because of PackedInts long-aligned encoding/decoding
  // NOTE: must be factor of ... 64?
  public final static int BLOCK_SIZE = 128;

  public BlockPostingsFormat() {
    this(BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE, BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
  }

  public BlockPostingsFormat(int minTermBlockSize, int maxTermBlockSize) {
    super("Block");
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
    PostingsWriterBase postingsWriter = new BlockPostingsWriter(state);

    boolean success = false;
    try {
      FieldsConsumer ret = new BlockTreeTermsWriter(state, 
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
    PostingsReaderBase postingsReader = new BlockPostingsReader(state.dir,
                                                                state.fieldInfos,
                                                                state.segmentInfo,
                                                                state.context,
                                                                state.segmentSuffix);
    boolean success = false;
    try {
      FieldsProducer ret = new BlockTreeTermsReader(state.dir,
                                                    state.fieldInfos,
                                                    state.segmentInfo.name,
                                                    postingsReader,
                                                    state.context,
                                                    state.segmentSuffix,
                                                    state.termsIndexDivisor);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsReader);
      }
    }
  }
}
