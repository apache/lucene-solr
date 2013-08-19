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
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.IOUtils;

/** TempFST + Pulsing41, test only, since
 *  FST does no delta encoding here!
 *  @lucene.experimental */

public class TempFSTPulsing41PostingsFormat extends PostingsFormat {
  private final TempPostingsBaseFormat wrappedPostingsBaseFormat;
  
  public TempFSTPulsing41PostingsFormat() {
    super("TempFSTPulsing41");
    this.wrappedPostingsBaseFormat = new TempPostingsBaseFormat();
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    TempPostingsWriterBase docsWriter = null;
    TempPostingsWriterBase pulsingWriter = null;

    boolean success = false;
    try {
      docsWriter = wrappedPostingsBaseFormat.postingsWriterBase(state);
      pulsingWriter = new TempPulsingPostingsWriter(state, 1, docsWriter);
      FieldsConsumer ret = new TempFSTTermsWriter(state, pulsingWriter);
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
      FieldsProducer ret = new TempFSTTermsReader(state, pulsingReader);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docsReader, pulsingReader);
      }
    }
  }
}
