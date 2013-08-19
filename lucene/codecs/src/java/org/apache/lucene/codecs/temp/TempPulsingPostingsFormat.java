package org.apache.lucene.codecs.temp;

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

import org.apache.lucene.codecs.temp.TempBlockTreeTermsReader;
import org.apache.lucene.codecs.temp.TempBlockTreeTermsWriter;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.TempPostingsBaseFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.TempPostingsReaderBase;
import org.apache.lucene.codecs.TempPostingsWriterBase;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.IOUtils;

/** This postings format "inlines" the postings for terms that have
 *  low docFreq.  It wraps another postings format, which is used for
 *  writing the non-inlined terms.
 *
 *  @lucene.experimental */

public abstract class TempPulsingPostingsFormat extends PostingsFormat {

  private final int freqCutoff;
  private final int minBlockSize;
  private final int maxBlockSize;
  private final TempPostingsBaseFormat wrappedPostingsBaseFormat;
  
  public TempPulsingPostingsFormat(String name, TempPostingsBaseFormat wrappedPostingsBaseFormat, int freqCutoff) {
    this(name, wrappedPostingsBaseFormat, freqCutoff, TempBlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE, TempBlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
  }

  /** Terms with freq <= freqCutoff are inlined into terms
   *  dict. */
  public TempPulsingPostingsFormat(String name, TempPostingsBaseFormat wrappedPostingsBaseFormat, int freqCutoff, int minBlockSize, int maxBlockSize) {
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
    TempPostingsWriterBase docsWriter = null;

    // Terms that have <= freqCutoff number of docs are
    // "pulsed" (inlined):
    TempPostingsWriterBase pulsingWriter = null;

    // Terms dict
    boolean success = false;
    try {
      docsWriter = wrappedPostingsBaseFormat.postingsWriterBase(state);

      // Terms that have <= freqCutoff number of docs are
      // "pulsed" (inlined):
      pulsingWriter = new TempPulsingPostingsWriter(state, freqCutoff, docsWriter);
      FieldsConsumer ret = new TempBlockTreeTermsWriter(state, pulsingWriter, minBlockSize, maxBlockSize);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docsWriter, pulsingWriter);
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    TempPostingsReaderBase docsReader = null;
    TempPostingsReaderBase pulsingReader = null;

    boolean success = false;
    try {
      docsReader = wrappedPostingsBaseFormat.postingsReaderBase(state);
      pulsingReader = new TempPulsingPostingsReader(state, docsReader);
      FieldsProducer ret = new TempBlockTreeTermsReader(
                                                    state.directory, state.fieldInfos, state.segmentInfo,
                                                    pulsingReader,
                                                    state.context,
                                                    state.segmentSuffix);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docsReader, pulsingReader);
      }
    }
  }

  public int getFreqCutoff() {
    return freqCutoff;
  }
}
