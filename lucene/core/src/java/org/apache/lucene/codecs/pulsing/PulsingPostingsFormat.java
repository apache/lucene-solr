package org.apache.lucene.codecs.pulsing;

/**
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
import java.util.Set;

import org.apache.lucene.codecs.BlockTreeTermsReader;
import org.apache.lucene.codecs.BlockTreeTermsWriter;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsBaseFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/** This postings format "inlines" the postings for terms that have
 *  low docFreq.  It wraps another postings format, which is used for
 *  writing the non-inlined terms.
 *
 *  @lucene.experimental */

public abstract class PulsingPostingsFormat extends PostingsFormat {

  private final int freqCutoff;
  private final int minBlockSize;
  private final int maxBlockSize;
  private final PostingsBaseFormat wrappedPostingsBaseFormat;
  
  public PulsingPostingsFormat(String name, PostingsBaseFormat wrappedPostingsBaseFormat, int freqCutoff) {
    this(name, wrappedPostingsBaseFormat, freqCutoff, BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE, BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
  }

  /** Terms with freq <= freqCutoff are inlined into terms
   *  dict. */
  public PulsingPostingsFormat(String name, PostingsBaseFormat wrappedPostingsBaseFormat, int freqCutoff, int minBlockSize, int maxBlockSize) {
    super(name);
    this.freqCutoff = freqCutoff;
    this.minBlockSize = minBlockSize;
    assert minBlockSize > 1;
    this.maxBlockSize = maxBlockSize;
    this.wrappedPostingsBaseFormat = wrappedPostingsBaseFormat;
  }

  @Override
  public String toString() {
    return getName() + "(freqCutoff=" + freqCutoff + " minBlockSize=" + minBlockSize + " maxBlockSize=" + maxBlockSize + ")";
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase docsWriter = wrappedPostingsBaseFormat.postingsWriterBase(state);

    // Terms that have <= freqCutoff number of docs are
    // "pulsed" (inlined):
    PostingsWriterBase pulsingWriter = new PulsingPostingsWriter(freqCutoff, docsWriter);

    // Terms dict
    boolean success = false;
    try {
      FieldsConsumer ret = new BlockTreeTermsWriter(state, pulsingWriter, minBlockSize, maxBlockSize);
      success = true;
      return ret;
    } finally {
      if (!success) {
        pulsingWriter.close();
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {

    PostingsReaderBase docsReader = wrappedPostingsBaseFormat.postingsReaderBase(state);
    PostingsReaderBase pulsingReader = new PulsingPostingsReader(docsReader);

    boolean success = false;
    try {
      FieldsProducer ret = new BlockTreeTermsReader(
                                                    state.dir, state.fieldInfos, state.segmentInfo.name,
                                                    pulsingReader,
                                                    state.context,
                                                    state.segmentSuffix,
                                                    state.termsIndexDivisor);
      success = true;
      return ret;
    } finally {
      if (!success) {
        pulsingReader.close();
      }
    }
  }

  public int getFreqCutoff() {
    return freqCutoff;
  }

  @Override
  public void files(SegmentInfo segmentInfo, String segmentSuffix, Set<String> files) throws IOException {
    wrappedPostingsBaseFormat.files(segmentInfo, segmentSuffix, files);
    BlockTreeTermsReader.files(segmentInfo, segmentSuffix, files);
  }
}
