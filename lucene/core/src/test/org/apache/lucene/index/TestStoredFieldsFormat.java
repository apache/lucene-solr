package org.apache.lucene.index;

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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests with the default randomized codec. Not really redundant with
 * other specific instantiations since we want to test some test-only impls
 * like Asserting, as well as make it easy to write a codec and pass -Dtests.codec
 */
public class TestStoredFieldsFormat extends BaseStoredFieldsFormatTestCase {

  @Override
  protected Codec getCodec() {
    return Codec.getDefault();
  }
  
  @Override
  public void testWriteReadMerge() throws IOException {
    assumeFalse("impersonation isnt good enough", LuceneTestCase.PREFLEX_IMPERSONATION_IS_ACTIVE);
    // this test tries to switch up between the codec and another codec.
    // for 3.x: we currently cannot take an index with existing 4.x segments
    // and merge into newly formed 3.x segments.
    super.testWriteReadMerge();
  }
}
