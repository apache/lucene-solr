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
package org.apache.lucene.codecs.lucene46;

import java.util.Map;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.BaseSegmentInfoFormatTestCase;
import org.apache.lucene.util.Version;

/**
 * Tests Lucene46InfoFormat
 */
public class TestLucene46SegmentInfoFormat extends BaseSegmentInfoFormatTestCase {

  @Override
  protected Version[] getVersions() {
    // NOTE: some of these bugfix releases we never actually "wrote",
    // but staying on the safe side...
    return new Version[] { 
        Version.LUCENE_4_6_0,
        Version.LUCENE_4_6_1,
        Version.LUCENE_4_7_0,
        Version.LUCENE_4_7_1,
        Version.LUCENE_4_7_2,
        Version.LUCENE_4_8_0,
        Version.LUCENE_4_8_1,
        Version.LUCENE_4_9_0,
        Version.LUCENE_4_10_0,
        Version.LUCENE_4_10_1
    };
  }

  @Override
  @Deprecated
  protected void assertIDEquals(byte[] expected, byte[] actual) {
    assertNull(actual); // we don't support IDs
  }

  @Override
  @Deprecated
  protected void assertAttributesEquals(Map<String,String> expected, Map<String,String> actual) {
    assertEquals(0, actual.size()); // we don't support attributes
  }

  @Override
  protected Codec getCodec() {
    return new Lucene46RWCodec();
  }
}
