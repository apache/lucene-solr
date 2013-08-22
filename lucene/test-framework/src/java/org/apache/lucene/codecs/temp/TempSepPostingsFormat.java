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

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.TempPostingsReaderBase;
import org.apache.lucene.codecs.TempPostingsWriterBase;
import org.apache.lucene.codecs.blockterms.FixedGapTermsIndexReader;
import org.apache.lucene.codecs.blockterms.FixedGapTermsIndexWriter;
import org.apache.lucene.codecs.blockterms.TermsIndexReaderBase;
import org.apache.lucene.codecs.blockterms.TermsIndexWriterBase;
import org.apache.lucene.codecs.sep.*;
import org.apache.lucene.codecs.mocksep.*;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.util.BytesRef;

/**
 * A silly codec that simply writes each file separately as
 * single vInts.  Don't use this (performance will be poor)!
 * This is here just to test the core sep codec
 * classes.
 */
public final class TempSepPostingsFormat extends PostingsFormat {

  public TempSepPostingsFormat() {
    super("TempSep");
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {

    TempPostingsWriterBase postingsWriter = new TempSepPostingsWriter(state, new MockSingleIntFactory());

    boolean success = false;
    TermsIndexWriterBase indexWriter;
    try {
      indexWriter = new FixedGapTermsIndexWriter(state);
      success = true;
    } finally {
      if (!success) {
        postingsWriter.close();
      }
    }

    success = false;
    try {
      FieldsConsumer ret = new TempBlockTermsWriter(indexWriter, state, postingsWriter);
      success = true;
      return ret;
    } finally {
      if (!success) {
        try {
          postingsWriter.close();
        } finally {
          indexWriter.close();
        }
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {

    TempPostingsReaderBase postingsReader = new TempSepPostingsReader(state.directory, state.fieldInfos, state.segmentInfo,
        state.context, new MockSingleIntFactory(), state.segmentSuffix);

    TermsIndexReaderBase indexReader;
    boolean success = false;
    try {
      indexReader = new FixedGapTermsIndexReader(state.directory,
                                                       state.fieldInfos,
                                                       state.segmentInfo.name,
                                                       BytesRef.getUTF8SortedAsUnicodeComparator(),
                                                       state.segmentSuffix, state.context);
      success = true;
    } finally {
      if (!success) {
        postingsReader.close();
      }
    }

    success = false;
    try {
      FieldsProducer ret = new TempBlockTermsReader(indexReader,
                                                state.directory,
                                                state.fieldInfos,
                                                state.segmentInfo,
                                                postingsReader,
                                                state.context,
                                                state.segmentSuffix);
      success = true;
      return ret;
    } finally {
      if (!success) {
        try {
          postingsReader.close();
        } finally {
          indexReader.close();
        }
      }
    }
  }
}
