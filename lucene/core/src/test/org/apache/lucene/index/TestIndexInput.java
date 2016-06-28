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
package org.apache.lucene.index;


import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Random;

public class TestIndexInput extends LuceneTestCase {

  static final byte[] READ_TEST_BYTES = new byte[] { 
    (byte) 0x80, 0x01,
    (byte) 0xFF, 0x7F,
    (byte) 0x80, (byte) 0x80, 0x01,
    (byte) 0x81, (byte) 0x80, 0x01,
    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x07,
    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x0F,
    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x07,
    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x7F,
    0x06, 'L', 'u', 'c', 'e', 'n', 'e',

    // 2-byte UTF-8 (U+00BF "INVERTED QUESTION MARK") 
    0x02, (byte) 0xC2, (byte) 0xBF,
    0x0A, 'L', 'u', (byte) 0xC2, (byte) 0xBF, 
          'c', 'e', (byte) 0xC2, (byte) 0xBF, 
          'n', 'e',

    // 3-byte UTF-8 (U+2620 "SKULL AND CROSSBONES") 
    0x03, (byte) 0xE2, (byte) 0x98, (byte) 0xA0,
    0x0C, 'L', 'u', (byte) 0xE2, (byte) 0x98, (byte) 0xA0,
          'c', 'e', (byte) 0xE2, (byte) 0x98, (byte) 0xA0,
          'n', 'e',

    // surrogate pairs
    // (U+1D11E "MUSICAL SYMBOL G CLEF")
    // (U+1D160 "MUSICAL SYMBOL EIGHTH NOTE")
    0x04, (byte) 0xF0, (byte) 0x9D, (byte) 0x84, (byte) 0x9E,
    0x08, (byte) 0xF0, (byte) 0x9D, (byte) 0x84, (byte) 0x9E, 
          (byte) 0xF0, (byte) 0x9D, (byte) 0x85, (byte) 0xA0, 
    0x0E, 'L', 'u',
          (byte) 0xF0, (byte) 0x9D, (byte) 0x84, (byte) 0x9E,
          'c', 'e', 
          (byte) 0xF0, (byte) 0x9D, (byte) 0x85, (byte) 0xA0, 
          'n', 'e',  

    // null bytes
    0x01, 0x00,
    0x08, 'L', 'u', 0x00, 'c', 'e', 0x00, 'n', 'e',
    
    // tests for Exceptions on invalid values
    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x17,
    (byte) 0x01, // guard value
    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
    (byte) 0x01, // guard value
  };
  
  static final int COUNT = RANDOM_MULTIPLIER * 65536;
  static int[] INTS;
  static long[] LONGS;
  static byte[] RANDOM_TEST_BYTES;
  
  @BeforeClass
  public static void beforeClass() throws IOException {
    Random random = random();
    INTS = new int[COUNT];
    LONGS = new long[COUNT];
    RANDOM_TEST_BYTES = new byte[COUNT * (5 + 4 + 9 + 8)];
    final ByteArrayDataOutput bdo = new ByteArrayDataOutput(RANDOM_TEST_BYTES);
    for (int i = 0; i < COUNT; i++) {
      final int i1 = INTS[i] = random.nextInt();
      bdo.writeVInt(i1);
      bdo.writeInt(i1);

      final long l1;
      if (rarely()) {
        // a long with lots of zeroes at the end
        l1 = LONGS[i] = TestUtil.nextLong(random, 0, Integer.MAX_VALUE) << 32;
      } else {
        l1 = LONGS[i] = TestUtil.nextLong(random, 0, Long.MAX_VALUE);
      }
      bdo.writeVLong(l1);
      bdo.writeLong(l1);
    }
  }

  @AfterClass
  public static void afterClass() {
    INTS = null;
    LONGS = null;
    RANDOM_TEST_BYTES = null;
  }

  private void checkReads(DataInput is, Class<? extends Exception> expectedEx) throws IOException {
    assertEquals(128,is.readVInt());
    assertEquals(16383,is.readVInt());
    assertEquals(16384,is.readVInt());
    assertEquals(16385,is.readVInt());
    assertEquals(Integer.MAX_VALUE, is.readVInt());
    assertEquals(-1, is.readVInt());
    assertEquals((long) Integer.MAX_VALUE, is.readVLong());
    assertEquals(Long.MAX_VALUE, is.readVLong());
    assertEquals("Lucene",is.readString());

    assertEquals("\u00BF",is.readString());
    assertEquals("Lu\u00BFce\u00BFne",is.readString());

    assertEquals("\u2620",is.readString());
    assertEquals("Lu\u2620ce\u2620ne",is.readString());

    assertEquals("\uD834\uDD1E",is.readString());
    assertEquals("\uD834\uDD1E\uD834\uDD60",is.readString());
    assertEquals("Lu\uD834\uDD1Ece\uD834\uDD60ne",is.readString());
    
    assertEquals("\u0000",is.readString());
    assertEquals("Lu\u0000ce\u0000ne",is.readString());
    
    Exception expected = expectThrows(expectedEx, () -> {
      is.readVInt();
    });
    assertTrue(expected.getMessage().startsWith("Invalid vInt"));
    assertEquals(1, is.readVInt()); // guard value
    
    expected = expectThrows(expectedEx, () -> {
      is.readVLong();
    });
    assertTrue(expected.getMessage().startsWith("Invalid vLong"));
    assertEquals(1L, is.readVLong()); // guard value
  }
  
  private void checkRandomReads(DataInput is) throws IOException {
    for (int i = 0; i < COUNT; i++) {
      assertEquals(INTS[i], is.readVInt());
      assertEquals(INTS[i], is.readInt());
      assertEquals(LONGS[i], is.readVLong());
      assertEquals(LONGS[i], is.readLong());
    }
  }

  // this test checks the IndexInput methods of any impl
  public void testRawIndexInputRead() throws IOException {
    for (int i = 0; i < 10; i++) {
      Random random = random();
      final Directory dir = newDirectory();
      IndexOutput os = dir.createOutput("foo", newIOContext(random));
      os.writeBytes(READ_TEST_BYTES, READ_TEST_BYTES.length);
      os.close();
      IndexInput is = dir.openInput("foo", newIOContext(random));
      checkReads(is, IOException.class);
      is.close();
    
      os = dir.createOutput("bar", newIOContext(random));
      os.writeBytes(RANDOM_TEST_BYTES, RANDOM_TEST_BYTES.length);
      os.close();
      is = dir.openInput("bar", newIOContext(random));
      checkRandomReads(is);
      is.close();
      dir.close();
    }
  }

  public void testByteArrayDataInput() throws IOException {
    ByteArrayDataInput is = new ByteArrayDataInput(READ_TEST_BYTES);
    checkReads(is, RuntimeException.class);
    is = new ByteArrayDataInput(RANDOM_TEST_BYTES);
    checkRandomReads(is);
  }

}
