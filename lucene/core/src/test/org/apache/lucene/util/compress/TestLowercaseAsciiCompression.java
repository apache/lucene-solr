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
package org.apache.lucene.util.compress;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestLowercaseAsciiCompression extends LuceneTestCase {

  private boolean doTestCompress(byte[] bytes) throws IOException {
    return doTestCompress(bytes, bytes.length);
  }

  private boolean doTestCompress(byte[] bytes, int len) throws IOException {
    ByteBuffersDataOutput compressed = new ByteBuffersDataOutput();
    byte[] tmp = new byte[len + random().nextInt(10)];
    random().nextBytes(tmp);
    if (LowercaseAsciiCompression.compress(bytes, len, tmp, compressed)) {
      assertTrue(compressed.size() < len);
      byte[] restored = new byte[len + random().nextInt(10)];
      LowercaseAsciiCompression.decompress(compressed.toDataInput(), restored, len);
      assertArrayEquals(ArrayUtil.copyOfSubArray(bytes, 0, len), ArrayUtil.copyOfSubArray(restored, 0, len));
      return true;
    } else {
      return false;
    }
  }

  public void testSimple() throws Exception {
    assertFalse(doTestCompress("".getBytes("UTF-8"))); // too short
    assertFalse(doTestCompress("ab1".getBytes("UTF-8"))); // too short
    assertFalse(doTestCompress("ab1cdef".getBytes("UTF-8"))); // too short
    assertTrue(doTestCompress("ab1cdefg".getBytes("UTF-8")));
    assertFalse(doTestCompress("ab1cdEfg".getBytes("UTF-8"))); // too many exceptions
    assertTrue(doTestCompress("ab1cdefg".getBytes("UTF-8")));
    // 1 exception, but enough chars to be worth encoding an exception
    assertTrue(doTestCompress("ab1.dEfg427hiogchio:'nwm un!94twxz".getBytes("UTF-8")));
  }

  public void testFarAwayExceptions() throws Exception {
    String s = "01W" + IntStream.range(0, 300).mapToObj(i -> "a").collect(Collectors.joining()) + "W.";
    assertTrue(doTestCompress(s.getBytes("UTF-8")));
  }

  public void testRandomAscii() throws IOException {
    for (int iter = 0; iter < 1000; ++iter) {
      int len = random().nextInt(1000);
      byte[] bytes = new byte[len + random().nextInt(10)];
      for (int i = 0; i < bytes.length; ++i) {
        bytes[i] = (byte) TestUtil.nextInt(random(), ' ', '~');
      }
      doTestCompress(bytes, len);
    }
  }

  public void testRandomCompressibleAscii() throws IOException {
    for (int iter = 0; iter < 1000; ++iter) {
      int len = TestUtil.nextInt(random(), 8, 1000);
      byte[] bytes = new byte[len + random().nextInt(10)];
      for (int i = 0; i < bytes.length; ++i) {
        // only use always compressible bytes
        int b = random().nextInt(32);
        b = b | 0x20 | ((b & 0x20) << 1);
        b -= 1;
        bytes[i] = (byte) b;
      }
      assertTrue(doTestCompress(bytes, len));
    }
  }

  public void testRandomCompressibleAsciiWithExceptions() throws IOException {
    for (int iter = 0; iter < 1000; ++iter) {
      int len = TestUtil.nextInt(random(), 8, 1000);
      int exceptions = 0;
      int maxExceptions = len >>> 5;
      byte[] bytes = new byte[len + random().nextInt(10)];
      for (int i = 0; i < bytes.length; ++i) {
        if (exceptions == maxExceptions || random().nextInt(100) != 0) {
          int b = random().nextInt(32);
          b = b | 0x20 | ((b & 0x20) << 1);
          b -= 1;
          bytes[i] = (byte) b;
        } else {
          exceptions++;
          bytes[i] = (byte) random().nextInt(256);
        }
      }
      assertTrue(doTestCompress(bytes, len));
    }
  }

  public void testRandom() throws IOException {
    for (int iter = 0; iter < 1000; ++iter) {
      int len = random().nextInt(1000);
      byte[] bytes = new byte[len + random().nextInt(10)];
      random().nextBytes(bytes);
      doTestCompress(bytes, len);
    }
  }
}
