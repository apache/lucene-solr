package org.apache.lucene.codecs.pfor;

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

import java.util.Set;
import java.io.IOException;

import org.apache.lucene.util.IOUtils;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.BlockTreeTermsWriter;
import org.apache.lucene.codecs.BlockTreeTermsReader;
import org.apache.lucene.codecs.TermsIndexReaderBase;
import org.apache.lucene.codecs.TermsIndexWriterBase;
import org.apache.lucene.codecs.FixedGapTermsIndexReader;
import org.apache.lucene.codecs.FixedGapTermsIndexWriter;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.sep.SepPostingsReader;
import org.apache.lucene.codecs.sep.SepPostingsWriter;

/**
 * Pass ForFactory to a PostingsWriter/ReaderBase, and get 
 * customized postings format plugged.
 */
public final class ForPostingsFormat extends PostingsFormat {
  private final int blockSize;
  private final int minBlockSize;
  private final int maxBlockSize;
  public final static int DEFAULT_BLOCK_SIZE = 128;

  public ForPostingsFormat() {
    super("For");
    this.blockSize = DEFAULT_BLOCK_SIZE;
    this.minBlockSize = BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE;
    this.maxBlockSize = BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE;
  }

  public ForPostingsFormat(int minBlockSize, int maxBlockSize) {
    super("For");
    this.blockSize = DEFAULT_BLOCK_SIZE;
    this.minBlockSize = minBlockSize;
    assert minBlockSize > 1;
    this.maxBlockSize = maxBlockSize;
    assert minBlockSize <= maxBlockSize;
  }

  @Override
  public String toString() {
    return getName() + "(blocksize=" + blockSize + ")";
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    // TODO: implement a new PostingsWriterBase to improve skip-settings
    PostingsWriterBase postingsWriter = new SepPostingsWriter(state, new ForFactory()); 
    boolean success = false;
    try {
      FieldsConsumer ret = new BlockTreeTermsWriter(state, 
                                                    postingsWriter,
                                                    minBlockSize, 
                                                    maxBlockSize);
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
    PostingsReaderBase postingsReader = new SepPostingsReader(state.dir,
                                                              state.fieldInfos,
                                                              state.segmentInfo,
                                                              state.context,
                                                              new ForFactory(),
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
