package org.apache.lucene.codecs.simpletext;

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
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.IndexFileNames;

/** For debugging, curiosity, transparency only!!  Do not
 *  use this codec in production.
 *
 *  <p>This codec stores all postings data in a single
 *  human-readable text file (_N.pst).  You can view this in
 *  any text editor, and even edit it to alter your index.
 *
 *  @lucene.experimental */
public final class SimpleTextPostingsFormat extends PostingsFormat {
  
  public SimpleTextPostingsFormat() {
    super("SimpleText");
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new SimpleTextFieldsWriter(state);
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new SimpleTextFieldsReader(state);
  }

  /** Extension of freq postings file */
  static final String POSTINGS_EXTENSION = "pst";

  static String getPostingsFileName(String segment, String segmentSuffix) {
    return IndexFileNames.segmentFileName(segment, segmentSuffix, POSTINGS_EXTENSION);
  }
}
