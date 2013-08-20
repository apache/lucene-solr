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

/**
 * Pulsing(1, Pulsing(2, Lucene41))
 * @lucene.experimental
 */
// TODO: if we create PulsingPostingsBaseFormat then we
// can simplify this? note: I don't like the *BaseFormat
// hierarchy, maybe we can clean that up...
public final class TempNestedPulsingPostingsFormat extends PostingsFormat {
  public TempNestedPulsingPostingsFormat() {
    super("TempNestedPulsing");
  }
  
  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    TempPostingsWriterBase docsWriter = null;
    TempPostingsWriterBase pulsingWriterInner = null;
    TempPostingsWriterBase pulsingWriter = null;
    
    // Terms dict
    boolean success = false;
    try {
      docsWriter = new TempPostingsWriter(state);

      pulsingWriterInner = new TempPulsingPostingsWriter(state, 2, docsWriter);
      pulsingWriter = new TempPulsingPostingsWriter(state, 1, pulsingWriterInner);
      FieldsConsumer ret = new TempBlockTreeTermsWriter(state, pulsingWriter, 
          TempBlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE, TempBlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docsWriter, pulsingWriterInner, pulsingWriter);
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    TempPostingsReaderBase docsReader = null;
    TempPostingsReaderBase pulsingReaderInner = null;
    TempPostingsReaderBase pulsingReader = null;
    boolean success = false;
    try {
      docsReader = new TempPostingsReader(state.directory, state.fieldInfos, state.segmentInfo, state.context, state.segmentSuffix);
      pulsingReaderInner = new TempPulsingPostingsReader(state, docsReader);
      pulsingReader = new TempPulsingPostingsReader(state, pulsingReaderInner);
      FieldsProducer ret = new TempBlockTreeTermsReader(
                                                    state.directory, state.fieldInfos, state.segmentInfo,
                                                    pulsingReader,
                                                    state.context,
                                                    state.segmentSuffix);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docsReader, pulsingReaderInner, pulsingReader);
      }
    }
  }
}
