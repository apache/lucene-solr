package org.apache.lucene.index.codecs.pulsing;

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

import org.apache.lucene.index.PerDocWriteState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.codecs.PostingsReaderBase;
import org.apache.lucene.index.codecs.PostingsWriterBase;
import org.apache.lucene.index.codecs.BlockTreeTermsReader;
import org.apache.lucene.index.codecs.BlockTreeTermsWriter;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.DefaultDocValuesConsumer;
import org.apache.lucene.index.codecs.DefaultDocValuesProducer;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.codecs.PerDocConsumer;
import org.apache.lucene.index.codecs.PerDocValues;
import org.apache.lucene.index.codecs.standard.StandardCodec;
import org.apache.lucene.index.codecs.standard.StandardPostingsReader;
import org.apache.lucene.index.codecs.standard.StandardPostingsWriter;
import org.apache.lucene.store.Directory;

/** This codec "inlines" the postings for terms that have
 *  low docFreq.  It wraps another codec, which is used for
 *  writing the non-inlined terms.
 *
 *  Currently in only inlines docFreq=1 terms, and
 *  otherwise uses the normal "standard" codec. 
 *  @lucene.experimental */

public class PulsingCodec extends Codec {

  private final int freqCutoff;
  private final int minBlockSize;
  private final int maxBlockSize;

  public PulsingCodec() {
    this(1);
  }
  
  public PulsingCodec(int freqCutoff) {
    this(freqCutoff, BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE, BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
  }

  /** Terms with freq <= freqCutoff are inlined into terms
   *  dict. */
  public PulsingCodec(int freqCutoff, int minBlockSize, int maxBlockSize) {
    super("Pulsing");
    this.freqCutoff = freqCutoff;
    this.minBlockSize = minBlockSize;
    assert minBlockSize > 1;
    this.maxBlockSize = maxBlockSize;
  }

  @Override
  public String toString() {
    return name + "(freqCutoff=" + freqCutoff + " minBlockSize=" + minBlockSize + " maxBlockSize=" + maxBlockSize + ")";
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    // We wrap StandardPostingsWriter, but any PostingsWriterBase
    // will work:

    PostingsWriterBase docsWriter = new StandardPostingsWriter(state);

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

    // We wrap StandardPostingsReader, but any StandardPostingsReader
    // will work:
    PostingsReaderBase docsReader = new StandardPostingsReader(state.dir, state.segmentInfo, state.context, state.codecId);
    PostingsReaderBase pulsingReader = new PulsingPostingsReader(docsReader);

    boolean success = false;
    try {
      FieldsProducer ret = new BlockTreeTermsReader(
                                                    state.dir, state.fieldInfos, state.segmentInfo.name,
                                                    pulsingReader,
                                                    state.context,
                                                    state.codecId,
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
  public void files(Directory dir, SegmentInfo segmentInfo, int codecID, Set<String> files) throws IOException {
    StandardPostingsReader.files(dir, segmentInfo, codecID, files);
    BlockTreeTermsReader.files(dir, segmentInfo, codecID, files);
    DefaultDocValuesConsumer.files(dir, segmentInfo, codecID, files, getDocValuesUseCFS());
  }

  @Override
  public void getExtensions(Set<String> extensions) {
    StandardCodec.getStandardExtensions(extensions);
    DefaultDocValuesConsumer.getDocValuesExtensions(extensions, getDocValuesUseCFS());
  }

  @Override
  public PerDocConsumer docsConsumer(PerDocWriteState state) throws IOException {
    return new DefaultDocValuesConsumer(state, getDocValuesSortComparator(), getDocValuesUseCFS());
  }

  @Override
  public PerDocValues docsProducer(SegmentReadState state) throws IOException {
    return new DefaultDocValuesProducer(state.segmentInfo, state.dir, state.fieldInfos, state.codecId, getDocValuesUseCFS(), getDocValuesSortComparator(), state.context);
  }
}
