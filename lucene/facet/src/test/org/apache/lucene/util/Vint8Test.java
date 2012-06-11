package org.apache.lucene.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Vint8;

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

/**
 * Tests the {@link Vint8} class.
 */
public class Vint8Test extends LuceneTestCase {

  /**
   * Tests the position wrapper.
   * @throws Exception For any reason.
   */
  @Test
  public void testPosition() throws Exception {
    Vint8.Position pos = new Vint8.Position();
    assertEquals(0, pos.pos);
    pos = new Vint8.Position(12345);
    assertEquals(12345, pos.pos);
  }

  private static int[] testValues = {
    -1000000000,
    -1, 0, (1 << 7) - 1, 1 << 7, (1 << 14) - 1, 1 << 14,
    (1 << 21) - 1, 1 << 21, (1 << 28) - 1, 1 << 28
  };
  private static int[] bytesNeededTestValues = {
    5, 5, 1, 1, 2, 2, 3, 3, 4, 4, 5
  };

  /**
   * Tests the {@code bytesNeeded} method.
   */
  @Test
  public void testBytesNeeded() {
    assertEquals(5, Vint8.MAXIMUM_BYTES_NEEDED);
    for (int j = 0; j < testValues.length; j++) {
      assertEquals(bytesNeededTestValues[j], Vint8.bytesNeeded(testValues[j]));
    }
  }

  /**
   * Tests encoding and decoding to and from a stream.
   */
  @Test
  public void testStreamEncodingAndDecoding() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
    int expectedSize = 0;
    for (int j = 0; j < testValues.length; j++) {
      Vint8.encode(testValues[j], baos);
      expectedSize += bytesNeededTestValues[j];
    }
    assertEquals(expectedSize, baos.size());
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    for (int j = 0; j < testValues.length; j++) {
      assertEquals(testValues[j], Vint8.decode(bais));
    }
    assertEquals(0, bais.available());
  }

  /**
   * Tests encoding and decoding to and from an array.
   */
  @Test
  public void testArrayEncodingAndDecoding() throws IOException {
    byte[] byteArray = new byte[256];
    int position = 0, expectedSize = 0;
    for (int j = 0; j < testValues.length; j++) {
      position += Vint8.encode(testValues[j], byteArray, position);
      expectedSize += bytesNeededTestValues[j];
    }
    assertEquals(expectedSize, position);
    Vint8.Position pos = new Vint8.Position();
    for (int j = 0; j < testValues.length; j++) {
      assertEquals(testValues[j], Vint8.decode(byteArray, pos));
    }
    assertEquals(expectedSize, pos.pos);
  }

  /**
   * The result of encoding the test values with the current algorithm. If these
   * values are changed to match an algorithm change, compatibility with legacy
   * data will be broken.
   */
  private static final byte[] encodedTestValues = {
    -4, -93, -108, -20, 0, -1, -1, -1, -1, 127, 0, 127, -127, 0, -1, 127,
    -127, -128, 0, -1, -1, 127, -127, -128, -128, 0, -1, -1, -1, 127, -127,
    -128, -128, -128, 0
  };

  /**
   * Tests algorithm.
   */
  @Test
  public void testLegacyCompatibility() throws IOException {
    /* To generate the encoded test values:
    byte[] byteArray = new byte[256];
    int position = 0, expectedSize = 0;
    for (int j = 0; j < testValues.length; j++) {
      position += Vint8.encode(testValues[j], byteArray, position);
      expectedSize += bytesNeededTestValues[j];
    }
    assertEquals(expectedSize, position);
    Vint8.Position pos = new Vint8.Position();
    for (int j = 0; j < expectedSize; j++) {
      System.out.print(byteArray[j] + ", ");
    }
    System.out.flush();
    pos.pos = 0;
    */
    Vint8.Position pos = new Vint8.Position();
    for (int j = 0; j < testValues.length; j++) {
      assertEquals(testValues[j], Vint8.decode(encodedTestValues, pos));
    }
  }

} // end class Vint8Test
