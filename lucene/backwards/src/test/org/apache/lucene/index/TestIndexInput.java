package org.apache.lucene.index;

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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;

public class TestIndexInput extends LuceneTestCase {

  static final byte[] READ_TEST_BYTES = new byte[] { 
    (byte) 0x80, 0x01,
    (byte) 0xFF, 0x7F,
    (byte) 0x80, (byte) 0x80, 0x01,
    (byte) 0x81, (byte) 0x80, 0x01,
    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x07,
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
  };
  
  private void checkReads(IndexInput is) throws IOException {
    assertEquals(128,is.readVInt());
    assertEquals(16383,is.readVInt());
    assertEquals(16384,is.readVInt());
    assertEquals(16385,is.readVInt());
    assertEquals(Integer.MAX_VALUE, is.readVInt());
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
  }

  // this test only checks BufferedIndexInput because MockIndexInput extends BufferedIndexInput
  public void testBufferedIndexInputRead() throws IOException {
    final IndexInput is = new MockIndexInput(READ_TEST_BYTES);
    checkReads(is);
    is.close();
  }

  // this test checks the raw IndexInput methods as it uses RAMIndexInput which extends IndexInput directly
  public void testRawIndexInputRead() throws IOException {
    final RAMDirectory dir = new RAMDirectory();
    final IndexOutput os = dir.createOutput("foo");
    os.writeBytes(READ_TEST_BYTES, READ_TEST_BYTES.length);
    os.close();
    final IndexInput is = dir.openInput("foo");
    checkReads(is);
    is.close();
    dir.close();
  }

  /**
   * Expert
   *
   * @throws IOException
   */
  public void testSkipChars() throws IOException {
    byte[] bytes = new byte[]{(byte) 0x80, 0x01,
            (byte) 0xFF, 0x7F,
            (byte) 0x80, (byte) 0x80, 0x01,
            (byte) 0x81, (byte) 0x80, 0x01,
            0x06, 'L', 'u', 'c', 'e', 'n', 'e',
    };
    String utf8Str = "\u0634\u1ea1";
    byte [] utf8Bytes = utf8Str.getBytes("UTF-8");
    byte [] theBytes = new byte[bytes.length + 1 + utf8Bytes.length];
    System.arraycopy(bytes, 0, theBytes, 0, bytes.length);
    theBytes[bytes.length] = (byte)utf8Str.length();//Add in the number of chars we are storing, which should fit in a byte for this test 
    System.arraycopy(utf8Bytes, 0, theBytes, bytes.length + 1, utf8Bytes.length);
    IndexInput is = new MockIndexInput(theBytes);
    assertEquals(128, is.readVInt());
    assertEquals(16383, is.readVInt());
    assertEquals(16384, is.readVInt());
    assertEquals(16385, is.readVInt());
    int charsToRead = is.readVInt();//number of chars in the Lucene string
    assertTrue(0x06 + " does not equal: " + charsToRead, 0x06 == charsToRead);
    is.skipChars(3);
    char [] chars = new char[3];//there should be 6 chars remaining
    is.readChars(chars, 0, 3);
    String tmpStr = new String(chars);
    assertTrue(tmpStr + " is not equal to " + "ene", tmpStr.equals("ene" ) == true);
    //Now read the UTF8 stuff
    charsToRead = is.readVInt() - 1;//since we are skipping one
    is.skipChars(1);
    assertTrue(utf8Str.length() - 1 + " does not equal: " + charsToRead, utf8Str.length() - 1 == charsToRead);
    chars = new char[charsToRead];
    is.readChars(chars, 0, charsToRead);
    tmpStr = new String(chars);
    assertTrue(tmpStr + " is not equal to " + utf8Str.substring(1), tmpStr.equals(utf8Str.substring(1)) == true);
  }
}
