package org.apache.lucene.index.codecs.intblock;

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
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.codecs.sep.SepCodec;
import org.apache.lucene.index.codecs.sep.SepPostingsReaderImpl;
import org.apache.lucene.index.codecs.sep.SepPostingsWriterImpl;
import org.apache.lucene.index.codecs.standard.SimpleStandardTermsIndexReader;
import org.apache.lucene.index.codecs.standard.SimpleStandardTermsIndexWriter;
import org.apache.lucene.index.codecs.standard.StandardPostingsWriter;
import org.apache.lucene.index.codecs.standard.StandardPostingsReader;
import org.apache.lucene.index.codecs.standard.StandardTermsDictReader;
import org.apache.lucene.index.codecs.standard.StandardTermsDictWriter;
import org.apache.lucene.index.codecs.standard.StandardTermsIndexReader;
import org.apache.lucene.index.codecs.standard.StandardTermsIndexWriter;
import org.apache.lucene.index.codecs.standard.StandardCodec;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

/**
 * @lucene.experimental
 */
public class IntBlockCodec extends Codec {

  public IntBlockCodec() {
    name = "IntBlock";
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    StandardPostingsWriter postingsWriter = new SepPostingsWriterImpl(state, new SimpleIntBlockFactory(1024));

    boolean success = false;
    StandardTermsIndexWriter indexWriter;
    try {
      indexWriter = new SimpleStandardTermsIndexWriter(state);
      success = true;
    } finally {
      if (!success) {
        postingsWriter.close();
      }
    }

    success = false;
    try {
      FieldsConsumer ret = new StandardTermsDictWriter(indexWriter, state, postingsWriter, BytesRef.getUTF8SortedAsUTF16Comparator());
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
    StandardPostingsReader postingsReader = new SepPostingsReaderImpl(state.dir,
                                                                      state.segmentInfo,
                                                                      state.readBufferSize,
                                                                      new SimpleIntBlockFactory(1024));

    StandardTermsIndexReader indexReader;
    boolean success = false;
    try {
      indexReader = new SimpleStandardTermsIndexReader(state.dir,
                                                       state.fieldInfos,
                                                       state.segmentInfo.name,
                                                       state.termsIndexDivisor,
                                                       BytesRef.getUTF8SortedAsUTF16Comparator());
      success = true;
    } finally {
      if (!success) {
        postingsReader.close();
      }
    }

    success = false;
    try {
      FieldsProducer ret = new StandardTermsDictReader(indexReader,
                                                       state.dir,
                                                       state.fieldInfos,
                                                       state.segmentInfo.name,
                                                       postingsReader,
                                                       state.readBufferSize,
                                                       BytesRef.getUTF8SortedAsUTF16Comparator(),
                                                       StandardCodec.TERMS_CACHE_SIZE);
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

  @Override
  public void files(Directory dir, SegmentInfo segmentInfo, Set<String> files) {
    SepPostingsReaderImpl.files(segmentInfo, files);
    StandardTermsDictReader.files(dir, segmentInfo, files);
    SimpleStandardTermsIndexReader.files(dir, segmentInfo, files);
  }

  @Override
  public void getExtensions(Set<String> extensions) {
    SepCodec.getSepExtensions(extensions);
  }
}
