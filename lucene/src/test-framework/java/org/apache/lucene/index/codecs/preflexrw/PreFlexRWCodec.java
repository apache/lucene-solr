package org.apache.lucene.index.codecs.preflexrw;

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

import org.apache.lucene.index.codecs.PostingsFormat;
import org.apache.lucene.index.codecs.lucene3x.Lucene3xCodec;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Writes 3.x-like indexes (not perfect emulation yet) for testing only!
 * @lucene.experimental
 */
public class PreFlexRWCodec extends Lucene3xCodec {
  private final PostingsFormat postings = new PreFlexRWPostingsFormat();

  @Override
  public PostingsFormat postingsFormat() {
    if (LuceneTestCase.PREFLEX_IMPERSONATION_IS_ACTIVE) {
      return postings;
    } else {
      return super.postingsFormat();
    }
  }
}
