package org.apache.lucene.codecs.memory;

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
import org.apache.lucene.codecs.PostingsBaseFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsWriter;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsReader;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsBaseFormat;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsFormat;
import org.apache.lucene.codecs.pulsing.PulsingPostingsWriter;
import org.apache.lucene.codecs.pulsing.PulsingPostingsReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.IOUtils;

/** FSTOrd + Pulsing41
 *  @lucene.experimental */

public class FSTOrdPulsing41PostingsFormat extends PostingsFormat {
  private final PostingsBaseFormat wrappedPostingsBaseFormat;
  private final int freqCutoff;

  public FSTOrdPulsing41PostingsFormat() {
    this(1);
  }
  
  public FSTOrdPulsing41PostingsFormat(int freqCutoff) {
    super("FSTOrdPulsing41");
    this.wrappedPostingsBaseFormat = new Lucene41PostingsBaseFormat();
    this.freqCutoff = freqCutoff;
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase docsWriter = null;
    PostingsWriterBase pulsingWriter = null;

    boolean success = false;
    try {
      docsWriter = wrappedPostingsBaseFormat.postingsWriterBase(state);
      pulsingWriter = new PulsingPostingsWriter(state, freqCutoff, docsWriter);
      FieldsConsumer ret = new FSTOrdTermsWriter(state, pulsingWriter);
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
    PostingsReaderBase docsReader = null;
    PostingsReaderBase pulsingReader = null;
    boolean success = false;
    try {
      docsReader = wrappedPostingsBaseFormat.postingsReaderBase(state);
      pulsingReader = new PulsingPostingsReader(state, docsReader);
      FieldsProducer ret = new FSTOrdTermsReader(state, pulsingReader);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docsReader, pulsingReader);
      }
    }
  }
}
