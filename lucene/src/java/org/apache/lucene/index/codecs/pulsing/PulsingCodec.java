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

import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.standard.StandardPostingsWriter;
import org.apache.lucene.index.codecs.standard.StandardPostingsWriterImpl;
import org.apache.lucene.index.codecs.standard.StandardPostingsReader;
import org.apache.lucene.index.codecs.standard.StandardPostingsReaderImpl;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.codecs.standard.SimpleStandardTermsIndexReader;
import org.apache.lucene.index.codecs.standard.SimpleStandardTermsIndexWriter;
import org.apache.lucene.index.codecs.standard.StandardTermsDictReader;
import org.apache.lucene.index.codecs.standard.StandardTermsDictWriter;
import org.apache.lucene.index.codecs.standard.StandardTermsIndexReader;
import org.apache.lucene.index.codecs.standard.StandardTermsIndexWriter;
import org.apache.lucene.index.codecs.standard.StandardCodec;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

/** This codec "inlines" the postings for terms that have
 *  low docFreq.  It wraps another codec, which is used for
 *  writing the non-inlined terms.
 *
 *  Currently in only inlines docFreq=1 terms, and
 *  otherwise uses the normal "standard" codec. 
 *  @lucene.experimental */

public class PulsingCodec extends Codec {

  private final int freqCutoff;

  /** Terms with freq <= freqCutoff are inlined into terms
   *  dict. */
  public PulsingCodec(int freqCutoff) {
    name = "Pulsing";
    this.freqCutoff = freqCutoff;
  }

  @Override
  public String toString() {
    return name + "(freqCutoff=" + freqCutoff + ")";
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    // We wrap StandardPostingsWriterImpl, but any StandardPostingsWriter
    // will work:
    StandardPostingsWriter docsWriter = new StandardPostingsWriterImpl(state);

    // Terms that have <= freqCutoff number of docs are
    // "pulsed" (inlined):
    StandardPostingsWriter pulsingWriter = new PulsingPostingsWriterImpl(freqCutoff, docsWriter);

    // Terms dict index
    StandardTermsIndexWriter indexWriter;
    boolean success = false;
    try {
      indexWriter = new SimpleStandardTermsIndexWriter(state);
      success = true;
    } finally {
      if (!success) {
        pulsingWriter.close();
      }
    }

    // Terms dict
    success = false;
    try {
      FieldsConsumer ret = new StandardTermsDictWriter(indexWriter, state, pulsingWriter, BytesRef.getUTF8SortedAsUnicodeComparator());
      success = true;
      return ret;
    } finally {
      if (!success) {
        try {
          pulsingWriter.close();
        } finally {
          indexWriter.close();
        }
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {

    // We wrap StandardPostingsReaderImpl, but any StandardPostingsReader
    // will work:
    StandardPostingsReader docsReader = new StandardPostingsReaderImpl(state.dir, state.segmentInfo, state.readBufferSize);
    StandardPostingsReader pulsingReader = new PulsingPostingsReaderImpl(docsReader);

    // Terms dict index reader
    StandardTermsIndexReader indexReader;

    boolean success = false;
    try {
      indexReader = new SimpleStandardTermsIndexReader(state.dir,
                                                       state.fieldInfos,
                                                       state.segmentInfo.name,
                                                       state.termsIndexDivisor,
                                                       BytesRef.getUTF8SortedAsUnicodeComparator());
      success = true;
    } finally {
      if (!success) {
        pulsingReader.close();
      }
    }

    // Terms dict reader
    success = false;
    try {
      FieldsProducer ret = new StandardTermsDictReader(indexReader,
                                                       state.dir, state.fieldInfos, state.segmentInfo.name,
                                                       pulsingReader,
                                                       state.readBufferSize,
                                                       BytesRef.getUTF8SortedAsUnicodeComparator(),
                                                       StandardCodec.TERMS_CACHE_SIZE);
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
  public void files(Directory dir, SegmentInfo segmentInfo, Set<String> files) throws IOException {
    StandardPostingsReaderImpl.files(dir, segmentInfo, files);
    StandardTermsDictReader.files(dir, segmentInfo, files);
    SimpleStandardTermsIndexReader.files(dir, segmentInfo, files);
  }

  @Override
  public void getExtensions(Set<String> extensions) {
    StandardCodec.getStandardExtensions(extensions);
  }
}
