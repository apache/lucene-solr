package org.apache.lucene.util;

import java.io.IOException;

import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.UnsafeByteArrayOutputStream;

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

public class UnsafeByteArrayOutputStreamTest extends LuceneTestCase {

  @Test
  public void testSimpleWrite() throws IOException {
    int length = 100;
    byte[] buffer = new byte[length];
    UnsafeByteArrayOutputStream ubaos = new UnsafeByteArrayOutputStream(buffer);

    for (int i = 0; i < 100; i++) {
      ubaos.write((byte) i);
    }

    byte[] result = ubaos.toByteArray();

    assertEquals(length, ubaos.length());

    for (int j = 0; j < length; ++j) {
      assertEquals(result[j], j);
    }
  }

  @Test
  public void testArrayWrite() throws IOException {
    int length = 100;
    byte[] buffer = new byte[length];
    UnsafeByteArrayOutputStream ubaos = new UnsafeByteArrayOutputStream(buffer);

    for (int i = 0; i < 100; i++) {
      ubaos.write((byte) i);
    }

    int length2 = 10;
    byte[] buffer2 = new byte[length2];
    for (int i = 0; i < length2; i++) {
      buffer2[i] = (byte) (8 + i);
    }

    ubaos.write(buffer2);

    byte[] result = ubaos.toByteArray();

    assertEquals(length + length2, ubaos.length());

    for (int j = 0; j < length; ++j) {
      assertEquals(result[j], j);
    }
    for (int j = 0; j < length2; ++j) {
      assertEquals(result[j + length], buffer2[j]);
    }
  }

  @Test
  public void testArrayWriteStartNotZero() throws IOException {
    int length = 100;
    byte[] buffer = new byte[length];
    UnsafeByteArrayOutputStream ubaos = new UnsafeByteArrayOutputStream(buffer);

    for (int i = 0; i < 100; i++) {
      ubaos.write((byte) i);
    }

    int length2 = 1000;
    byte[] buffer2 = new byte[length2];
    for (int i = 0; i < length2; i++) {
      buffer2[i] = (byte) (8 + i);
    }

    int length3 = 5;
    int start = 2;
    ubaos.write(buffer2, start, length3);

    byte[] result = ubaos.toByteArray();

    assertEquals(length + length3, ubaos.length());

    for (int j = 0; j < length; ++j) {
      assertEquals(result[j], j);
    }
    for (int j = 0; j < length3; ++j) {
      assertEquals(result[j + length], buffer2[j + start]);
    }
  }

  @Test
  public void testBufferGrow() throws IOException {
    int length = 100;
    byte[] buffer = new byte[length / 10];
    UnsafeByteArrayOutputStream ubaos = new UnsafeByteArrayOutputStream(buffer);

    for (int i = 0; i < length; i++) {
      ubaos.write((byte) i);
    }

    byte[] result = ubaos.toByteArray();

    assertEquals(length, ubaos.length());

    for (int j = 0; j < length; ++j) {
      assertEquals(result[j], j);
    }

    buffer = ubaos.toByteArray();

    int length2 = 10;
    byte[] buffer2 = new byte[length2];
    for (int i = 0; i < length2; i++) {
      buffer2[i] = (byte) (8 + i);
    }

    ubaos.reInit(buffer2);
    for (int i = 0; i < length2; i++) {
      ubaos.write(7 + i);
    }

    byte[] result2 = ubaos.toByteArray();

    assertEquals(length2, ubaos.length());

    for (int j = 0; j < length2; ++j) {
      assertEquals(result2[j], j + 7);
    }

    for (int i = 0; i < length; i++) {
      assertEquals(buffer[i], i);
    }
  }
  
  @Test
  public void testStartPos() throws Exception {
    byte[] buf = new byte[10];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) i;
    }
    
    int startPos = 3;
    UnsafeByteArrayOutputStream ubaos = new UnsafeByteArrayOutputStream(buf, startPos);
    int numValues = 5;
    for (int i = 0; i < numValues; i++) {
      ubaos.write((i + 1) * 2);
    }

    // the length of the buffer should be whatever was written after startPos
    // and before that.
    assertEquals("invalid buffer length", startPos + numValues, ubaos.length());

    assertEquals("invalid startPos", startPos, ubaos.getStartPos());

    byte[] bytes = ubaos.toByteArray();
    for (int i = 0; i < startPos; i++) {
      assertEquals(i, bytes[i]);
    }
    
    for (int i = startPos, j = 0; j < numValues; i++, j++) {
      assertEquals((j + 1) * 2, bytes[i]);
    }

    for (int i = startPos + numValues; i < buf.length; i++) {
      assertEquals(i, bytes[i]);
    }

  }

  @Test
  public void testDefaultCtor() throws Exception {
    UnsafeByteArrayOutputStream ubaos = new UnsafeByteArrayOutputStream();
    int numValues = 5;
    for (int i = 0; i < numValues; i++) {
      ubaos.write(i);
    }

    assertEquals("invalid buffer length", numValues, ubaos.length());
    
    byte[] bytes = ubaos.toByteArray();
    for (int i = 0; i < numValues; i++) {
      assertEquals(i, bytes[i]);
    }
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testIllegalBufferSize() throws Exception {
    UnsafeByteArrayOutputStream ubaos = new UnsafeByteArrayOutputStream();
    ubaos.reInit(new byte[0]);
  }

}
