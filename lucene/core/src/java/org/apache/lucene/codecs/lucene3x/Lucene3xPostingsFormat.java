package org.apache.lucene.codecs.lucene3x;

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

import java.util.Set;
import java.io.IOException;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SegmentReadState;

/** Codec that reads the pre-flex-indexing postings
 *  format.  It does not provide a writer because newly
 *  written segments should use StandardCodec.
 *
 * @deprecated (4.0) This is only used to read indexes created
 * before 4.0.
 * @lucene.experimental
 */
@Deprecated
class Lucene3xPostingsFormat extends PostingsFormat {

  /** Extension of terms file */
  public static final String TERMS_EXTENSION = "tis";

  /** Extension of terms index file */
  public static final String TERMS_INDEX_EXTENSION = "tii";

  /** Extension of freq postings file */
  public static final String FREQ_EXTENSION = "frq";

  /** Extension of prox postings file */
  public static final String PROX_EXTENSION = "prx";

  public Lucene3xPostingsFormat() {
    super("Lucene3x");
  }
  
  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    throw new UnsupportedOperationException("this codec can only be used for reading");
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new Lucene3xFields(state.dir, state.fieldInfos, state.segmentInfo, state.context, state.termsIndexDivisor);
  }

  @Override
  public void files(SegmentInfo info, String segmentSuffix, Set<String> files) throws IOException {
    // preflex fields have no segmentSuffix - we ignore it here
    Lucene3xFields.files(info, files);
  }
}
