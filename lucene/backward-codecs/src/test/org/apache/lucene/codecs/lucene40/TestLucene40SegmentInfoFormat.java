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
package org.apache.lucene.codecs.lucene40;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.BaseSegmentInfoFormatTestCase;
import org.apache.lucene.util.Version;

/**
 * Tests Lucene40InfoFormat
 */
public class TestLucene40SegmentInfoFormat extends BaseSegmentInfoFormatTestCase {

  @Override
  protected Version[] getVersions() {
    // NOTE: some of these bugfix releases we never actually "wrote",
    // but staying on the safe side...
    return new Version[] { 
        Version.LUCENE_4_0_0_ALPHA, 
        Version.LUCENE_4_0_0_BETA,
        Version.LUCENE_4_0_0,
        Version.LUCENE_4_1_0,
        Version.LUCENE_4_2_0,
        Version.LUCENE_4_2_1,
        Version.LUCENE_4_3_0,
        Version.LUCENE_4_3_1,
        Version.LUCENE_4_4_0,
        Version.LUCENE_4_5_0,
        Version.LUCENE_4_5_1,
    };
  }

  @Override
  @Deprecated
  protected void assertIDEquals(byte[] expected, byte[] actual) {
    assertNull(actual); // we don't support IDs
  }

  @Override
  protected Codec getCodec() {
    return new Lucene40RWCodec();
  }
}
