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

package org.apache.lucene.codecs.uniformsplit.sharedterms;

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.codecs.lucene80.Lucene80Codec;

/**
 * Codec that chooses {@link STUniformSplitPostingsFormat}. The problem with doing
 * -Dtests.postingsformat=STUniformSplitTesting instead is that it's awkward to disable this on some Lucene tests.
 * By having a full Codec, we can use {@link org.apache.lucene.util.LuceneTestCase.SuppressCodecs}.
 */
public class STUniformSplitTestingCodec extends Lucene80Codec {

  private static final PostingsFormat ST_UNIFORM_SPLIT_POSTINGS_FORMAT = new STUniformSplitPostingsFormat();

  public STUniformSplitTestingCodec() {
    super("STUniformSplitTesting", Lucene50StoredFieldsFormat.Mode.BEST_SPEED);
  }

  @Override
  public PostingsFormat getPostingsFormatForField(String field) {
    return ST_UNIFORM_SPLIT_POSTINGS_FORMAT;
  }
}