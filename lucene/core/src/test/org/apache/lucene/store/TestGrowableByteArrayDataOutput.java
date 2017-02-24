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

package org.apache.lucene.store;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.UnicodeUtil;
import org.junit.Test;

/**
 * Test for {@link GrowableByteArrayDataOutput}
 */
public class TestGrowableByteArrayDataOutput extends LuceneTestCase {

  @Test
  public void testWriteSmallStrings() throws Exception {
    int minSizeForDoublePass = GrowableByteArrayDataOutput.MIN_UTF8_SIZE_TO_ENABLE_DOUBLE_PASS_ENCODING;

    // a simple string encoding test
    int num = atLeast(1000);
    for (int i = 0; i < num; i++) {
      // create a small string such that the single pass approach is used
      int length = TestUtil.nextInt(random(), 1, minSizeForDoublePass - 1);
      String unicode = TestUtil.randomFixedByteLengthUnicodeString(random(), length);
      byte[] utf8 = new byte[UnicodeUtil.maxUTF8Length(unicode.length())];
      int len = UnicodeUtil.UTF16toUTF8(unicode, 0, unicode.length(), utf8);

      GrowableByteArrayDataOutput dataOutput = new GrowableByteArrayDataOutput(1 << 8);
      //explicitly write utf8 len so that we know how many bytes it occupies
      dataOutput.writeVInt(len);
      int vintLen = dataOutput.getPosition();
      // now write the string which will internally write number of bytes as a vint and then utf8 bytes
      dataOutput.writeString(unicode);

      assertEquals("GrowableByteArrayDataOutput wrote the wrong length after encode", len + vintLen * 2, dataOutput.getPosition());
      for (int j = 0, k = vintLen * 2; j < len; j++, k++) {
        assertEquals(utf8[j], dataOutput.getBytes()[k]);
      }
    }
  }

  @Test
  public void testWriteLargeStrings() throws Exception {
    int minSizeForDoublePass = GrowableByteArrayDataOutput.MIN_UTF8_SIZE_TO_ENABLE_DOUBLE_PASS_ENCODING;

    int num = atLeast(100);
    for (int i = 0; i < num; i++) {
      String unicode = TestUtil.randomRealisticUnicodeString(random(), minSizeForDoublePass, 10 * minSizeForDoublePass);
      byte[] utf8 = new byte[UnicodeUtil.maxUTF8Length(unicode.length())];
      int len = UnicodeUtil.UTF16toUTF8(unicode, 0, unicode.length(), utf8);

      GrowableByteArrayDataOutput dataOutput = new GrowableByteArrayDataOutput(1 << 8);
      //explicitly write utf8 len so that we know how many bytes it occupies
      dataOutput.writeVInt(len);
      int vintLen = dataOutput.getPosition();
      // now write the string which will internally write number of bytes as a vint and then utf8 bytes
      dataOutput.writeString(unicode);

      assertEquals("GrowableByteArrayDataOutput wrote the wrong length after encode", len + vintLen * 2, dataOutput.getPosition());
      for (int j = 0, k = vintLen * 2; j < len; j++, k++) {
        assertEquals(utf8[j], dataOutput.getBytes()[k]);
      }
    }
  }
}
