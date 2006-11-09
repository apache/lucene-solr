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

import junit.framework.TestCase;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

public class TestIndexInput extends TestCase {
  public void testRead() throws IOException {
    IndexInput is = new MockIndexInput(new byte[]{(byte) 0x80, 0x01,
            (byte) 0xFF, 0x7F,
            (byte) 0x80, (byte) 0x80, 0x01,
            (byte) 0x81, (byte) 0x80, 0x01,
            0x06, 'L', 'u', 'c', 'e', 'n', 'e'});
    assertEquals(128, is.readVInt());
    assertEquals(16383, is.readVInt());
    assertEquals(16384, is.readVInt());
    assertEquals(16385, is.readVInt());
    assertEquals("Lucene", is.readString());
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
