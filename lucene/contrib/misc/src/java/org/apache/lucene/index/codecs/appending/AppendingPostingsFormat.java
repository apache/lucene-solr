package org.apache.lucene.index.codecs.appending;

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
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.codecs.PostingsFormat;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.codecs.FixedGapTermsIndexReader;
import org.apache.lucene.index.codecs.lucene40.Lucene40PostingsFormat;
import org.apache.lucene.index.codecs.lucene40.Lucene40PostingsReader;
import org.apache.lucene.index.codecs.lucene40.Lucene40PostingsWriter;
import org.apache.lucene.index.codecs.PostingsReaderBase;
import org.apache.lucene.index.codecs.PostingsWriterBase;
import org.apache.lucene.index.codecs.BlockTermsReader;
import org.apache.lucene.index.codecs.TermsIndexReaderBase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

/**
 * Appending postings impl
 */
class AppendingPostingsFormat extends PostingsFormat {
  public static String CODEC_NAME = "Appending";
  
  public AppendingPostingsFormat() {
    super(CODEC_NAME);
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state)
          throws IOException {
    PostingsWriterBase docsWriter = new Lucene40PostingsWriter(state);
    boolean success = false;
    AppendingTermsIndexWriter indexWriter = null;
    try {
      indexWriter = new AppendingTermsIndexWriter(state);
      success = true;
    } finally {
      if (!success) {
        docsWriter.close();
      }
    }
    success = false;
    try {
      FieldsConsumer ret = new AppendingTermsDictWriter(indexWriter, state, docsWriter);
      success = true;
      return ret;
    } finally {
      if (!success) {
        try {
          docsWriter.close();
        } finally {
          indexWriter.close();
        }
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state)
          throws IOException {
    PostingsReaderBase docsReader = new Lucene40PostingsReader(state.dir, state.segmentInfo, state.context, state.segmentSuffix);
    TermsIndexReaderBase indexReader;

    boolean success = false;
    try {
      indexReader = new AppendingTermsIndexReader(state.dir,
              state.fieldInfos,
              state.segmentInfo.name,
              state.termsIndexDivisor,
              BytesRef.getUTF8SortedAsUnicodeComparator(),
              state.segmentSuffix, state.context);
      success = true;
    } finally {
      if (!success) {
        docsReader.close();
      }
    }
    success = false;
    try {
      FieldsProducer ret = new AppendingTermsDictReader(indexReader,
              state.dir, state.fieldInfos, state.segmentInfo.name,
              docsReader,
              state.context,
              Lucene40PostingsFormat.TERMS_CACHE_SIZE,
              state.segmentSuffix);
      success = true;
      return ret;
    } finally {
      if (!success) {
        try {
          docsReader.close();
        } finally {
          indexReader.close();
        }
      }
    }
  }

  @Override
  public void files(Directory dir, SegmentInfo segmentInfo, String segmentSuffix, Set<String> files)
          throws IOException {
    Lucene40PostingsReader.files(dir, segmentInfo, segmentSuffix, files);
    BlockTermsReader.files(dir, segmentInfo, segmentSuffix, files);
    FixedGapTermsIndexReader.files(dir, segmentInfo, segmentSuffix, files);
  }
}
