package org.apache.lucene.util;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.UnsafeByteArrayInputStream;

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

public class UnsafeByteArrayInputStreamTest extends LuceneTestCase {

  @Test
  public void testSimple() throws IOException {
    int length = 256;
    byte[] buffer = new byte[length];
    for (int i = 0; i < length; ++i) {
      buffer[i] = (byte) i;
    }
    byte[] result = new byte[buffer.length];
    UnsafeByteArrayInputStream ubais = new UnsafeByteArrayInputStream(buffer);
    
    int index = 0;
    int by = ubais.read();
    while (by >= 0) {
      result[index++] = (byte) (by);
      by = ubais.read();
    }
    
    assertEquals(length, index);
    assertTrue(Arrays.equals(buffer, result));
  }
  
  @Test
  public void testStartPos() throws IOException {
    int length = 100;
    byte[] buffer = new byte[length];
    for (int i = 0; i < length; ++i) {
      buffer[i] = (byte) i;
    }
    int startPos = 5;
    byte[] result = new byte[buffer.length];
    UnsafeByteArrayInputStream ubais = new UnsafeByteArrayInputStream(buffer, startPos, length);
    
    int index = 0;
    int by = ubais.read();
    while (by >= 0) {
      result[index++] = (byte) (by);
      by = ubais.read();
    }
    
    assertEquals(length - startPos, index);
    for (int i = startPos; i < length; i++) {
      assertEquals(buffer[i], result[i - startPos]);
    }
  }
  
  @Test
  public void testReinit() throws IOException {
    int length = 100;
    byte[] buffer = new byte[length];
    for (int i = 0; i < length; ++i) {
      buffer[i] = (byte) i;
    }
    byte[] result = new byte[buffer.length];
    UnsafeByteArrayInputStream ubais = new UnsafeByteArrayInputStream(buffer);

    int index = 0;
    int by = ubais.read();
    while (by >= 0) {
      result[index++] = (byte) (by);
      by = ubais.read();
    }

    assertEquals(length, index);
    assertTrue(Arrays.equals(buffer, result));

    int length2 = 50;
    byte[] buffer2 = new byte[length2];
    for (int i = 0; i < length2; ++i) {
      buffer2[i] = (byte) (90 + i);
    }
    byte[] result2 = new byte[buffer2.length];
    ubais.reInit(buffer2);

    int index2 = 0;
    int by2 = ubais.read();
    while (by2 >= 0) {
      result2[index2++] = (byte) (by2);
      by2 = ubais.read();
    }

    assertEquals(length2, index2);
    assertTrue(Arrays.equals(buffer2, result2));
  }

  @Test
  public void testDefaultCtor() throws Exception {
    UnsafeByteArrayInputStream ubais = new UnsafeByteArrayInputStream();
    assertEquals(0, ubais.available());
    assertEquals(-1, ubais.read());
  }

  @Test
  public void testMark() throws Exception {
    byte[] bytes = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    UnsafeByteArrayInputStream ubais = new UnsafeByteArrayInputStream(bytes);
    assertTrue(ubais.markSupported());
    int markIndex = 3;
    // Advance the index
    for (int i = 0; i < markIndex; i++) {
      ubais.read();
    }
    ubais.mark(markIndex);
    for (int i = markIndex; i < bytes.length; i++) {
      ubais.read();
    }
    ubais.reset();
    assertEquals(bytes.length - markIndex, ubais.available());
  }
  
}
