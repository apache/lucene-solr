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
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.PostingsWriterBase;
import org.apache.lucene.index.codecs.standard.StandardPostingsWriter;
import org.apache.lucene.index.codecs.PostingsReaderBase;
import org.apache.lucene.index.codecs.standard.StandardPostingsReader;
import org.apache.lucene.index.codecs.DefaultDocValuesProducer;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.codecs.PerDocConsumer;
import org.apache.lucene.index.codecs.DefaultDocValuesConsumer;
import org.apache.lucene.index.codecs.PerDocValues;
import org.apache.lucene.index.codecs.VariableGapTermsIndexReader;
import org.apache.lucene.index.codecs.VariableGapTermsIndexWriter;
import org.apache.lucene.index.codecs.BlockTermsReader;
import org.apache.lucene.index.codecs.BlockTermsWriter;
import org.apache.lucene.index.codecs.TermsIndexReaderBase;
import org.apache.lucene.index.codecs.TermsIndexWriterBase;
import org.apache.lucene.index.codecs.standard.StandardCodec;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

/** This codec "inlines" the postings for terms that have
 *  low docFreq.  It wraps another codec, which is used for
 *  writing the non-inlined terms.
 *
 *  Currently in only inlines docFreq=1 terms, and
 *  otherwise uses the normal "standard" codec. 
 *  @lucene.experimental */

public class PulsingCodec extends Codec {

  private final int freqCutoff;

  /**
   * Creates a {@link PulsingCodec} with <tt>freqCutoff = 1</tt>
   * 
   * @see PulsingCodec#PulsingCodec(int)
   */
  public PulsingCodec() {
    this(1);
  }
  
  /** Terms with freq <= freqCutoff are inlined into terms
   *  dict. */
  public PulsingCodec(int freqCutoff) {
    super("Pulsing");
    this.freqCutoff = freqCutoff;
  }

  @Override
  public String toString() {
    return name + "(freqCutoff=" + freqCutoff + ")";
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    // We wrap StandardPostingsWriter, but any StandardPostingsWriter
    // will work:
    PostingsWriterBase docsWriter = new StandardPostingsWriter(state);

    // Terms that have <= freqCutoff number of docs are
    // "pulsed" (inlined):
    PostingsWriterBase pulsingWriter = new PulsingPostingsWriterImpl(freqCutoff, docsWriter);

    // Terms dict index
    TermsIndexWriterBase indexWriter;
    boolean success = false;
    try {
      indexWriter = new VariableGapTermsIndexWriter(state, new VariableGapTermsIndexWriter.EveryNTermSelector(state.termIndexInterval));
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeSafely(true, pulsingWriter);
      }
    }

    // Terms dict
    success = false;
    try {
      FieldsConsumer ret = new BlockTermsWriter(indexWriter, state, pulsingWriter);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeSafely(true, pulsingWriter, indexWriter);
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {

    // We wrap StandardPostingsReader, but any StandardPostingsReader
    // will work:
    PostingsReaderBase docsReader = new StandardPostingsReader(state.dir, state.segmentInfo, state.context, state.codecId);
    PostingsReaderBase pulsingReader = new PulsingPostingsReaderImpl(docsReader);

    // Terms dict index reader
    TermsIndexReaderBase indexReader;

    boolean success = false;
    try {
      indexReader = new VariableGapTermsIndexReader(state.dir,
                                                    state.fieldInfos,
                                                    state.segmentInfo.name,
                                                    state.termsIndexDivisor,
                                                    state.codecId, state.context);
      success = true;
    } finally {
      if (!success) {
        pulsingReader.close();
      }
    }

    // Terms dict reader
    success = false;
    try {
      FieldsProducer ret = new BlockTermsReader(indexReader,
                                                state.dir, state.fieldInfos, state.segmentInfo.name,
                                                pulsingReader,
                                                state.context,
                                                StandardCodec.TERMS_CACHE_SIZE,
                                                state.codecId);
      success = true;
      return ret;
    } finally {
      if (!success) {
        try {
          pulsingReader.close();
        } finally {
          indexReader.close();
        }
      }
    }
  }

  @Override
  public void files(Directory dir, SegmentInfo segmentInfo, int id, Set<String> files) throws IOException {
    StandardPostingsReader.files(dir, segmentInfo, id, files);
    BlockTermsReader.files(dir, segmentInfo, id, files);
    VariableGapTermsIndexReader.files(dir, segmentInfo, id, files);
    DefaultDocValuesConsumer.files(dir, segmentInfo, id, files, getDocValuesUseCFS());
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
