package org.apache.lucene.util;

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

import java.util.Random;
import java.nio.CharBuffer;
import java.nio.ByteBuffer;

public class TestIndexableBinaryStringTools extends LuceneTestCase {
  private static final int NUM_RANDOM_TESTS = 20000;
  private static final int MAX_RANDOM_BINARY_LENGTH = 300;
  
  public void testSingleBinaryRoundTrip() {
    byte[] binary = new byte[] 
      { (byte)0x23, (byte)0x98, (byte)0x13, (byte)0xE4, (byte)0x76, (byte)0x41,
        (byte)0xB2, (byte)0xC9, (byte)0x7F, (byte)0x0A, (byte)0xA6, (byte)0xD8 };

    ByteBuffer binaryBuf = ByteBuffer.wrap(binary);
    CharBuffer encoded = IndexableBinaryStringTools.encode(binaryBuf);
    ByteBuffer decoded = IndexableBinaryStringTools.decode(encoded);
    assertEquals("Round trip decode/decode returned different results:"
                 + System.getProperty("line.separator")
                 + "original: " + binaryDump(binaryBuf)
                 + System.getProperty("line.separator")
                 + " encoded: " + charArrayDump(encoded)
                 + System.getProperty("line.separator")
                 + " decoded: " + binaryDump(decoded),
                 binaryBuf, decoded);
  }
  
  public void testEncodedSortability() {
    Random random = newRandom();
    byte[] originalArray1 = new byte[MAX_RANDOM_BINARY_LENGTH];
    ByteBuffer originalBuf1 = ByteBuffer.wrap(originalArray1);
    char[] originalString1 = new char[MAX_RANDOM_BINARY_LENGTH];
    CharBuffer originalStringBuf1 = CharBuffer.wrap(originalString1);
    char[] encoded1 = new char[IndexableBinaryStringTools.getEncodedLength(originalBuf1)];
    CharBuffer encodedBuf1 = CharBuffer.wrap(encoded1);
    byte[] original2 = new byte[MAX_RANDOM_BINARY_LENGTH];
    ByteBuffer originalBuf2 = ByteBuffer.wrap(original2);
    char[] originalString2 = new char[MAX_RANDOM_BINARY_LENGTH];
    CharBuffer originalStringBuf2 = CharBuffer.wrap(originalString2);
    char[] encoded2 = new char[IndexableBinaryStringTools.getEncodedLength(originalBuf2)];
    CharBuffer encodedBuf2 = CharBuffer.wrap(encoded2);
    for (int testNum = 0 ; testNum < NUM_RANDOM_TESTS ; ++testNum) {
      int numBytes1 = random.nextInt(MAX_RANDOM_BINARY_LENGTH - 1) + 1; // Min == 1
      originalBuf1.limit(numBytes1);
      originalStringBuf1.limit(numBytes1);
      
      for (int byteNum = 0 ; byteNum < numBytes1 ; ++byteNum) {
        int randomInt = random.nextInt(0x100);
        originalArray1[byteNum] = (byte) randomInt;
        originalString1[byteNum] = (char)randomInt;
      }
      
      int numBytes2 = random.nextInt(MAX_RANDOM_BINARY_LENGTH - 1) + 1; // Min == 1
      originalBuf2.limit(numBytes2);
      originalStringBuf2.limit(numBytes2);
      for (int byteNum = 0 ; byteNum < numBytes2 ; ++byteNum) {
        int randomInt = random.nextInt(0x100);
        original2[byteNum] = (byte)randomInt;
        originalString2[byteNum] = (char)randomInt;
      }
      int originalComparison = originalStringBuf1.compareTo(originalStringBuf2);
      originalComparison = originalComparison < 0 ? -1 : originalComparison > 0 ? 1 : 0;
      
      IndexableBinaryStringTools.encode(originalBuf1, encodedBuf1);
      IndexableBinaryStringTools.encode(originalBuf2, encodedBuf2);
      
      int encodedComparison = encodedBuf1.compareTo(encodedBuf2);
      encodedComparison = encodedComparison < 0 ? -1 : encodedComparison > 0 ? 1 : 0;
      
      assertEquals("Test #" + (testNum + 1) 
                   + ": Original bytes and encoded chars compare differently:"
                   + System.getProperty("line.separator")
                   + " binary 1: " + binaryDump(originalBuf1)
                   + System.getProperty("line.separator")
                   + " binary 2: " + binaryDump(originalBuf2)
                   + System.getProperty("line.separator")
                   + "encoded 1: " + charArrayDump(encodedBuf1)
                   + System.getProperty("line.separator")
                   + "encoded 2: " + charArrayDump(encodedBuf2)
                   + System.getProperty("line.separator"),
                   originalComparison, encodedComparison);
    }
  }
  
  public void testEmptyInput() {
    byte[] binary = new byte[0];
    CharBuffer encoded = IndexableBinaryStringTools.encode(ByteBuffer.wrap(binary));
    ByteBuffer decoded = IndexableBinaryStringTools.decode(encoded);
    assertNotNull("decode() returned null", decoded);
    assertEquals("decoded empty input was not empty", decoded.limit(), 0);
  }
  
  public void testAllNullInput() {
    byte[] binary = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    ByteBuffer binaryBuf = ByteBuffer.wrap(binary);
    CharBuffer encoded = IndexableBinaryStringTools.encode(binaryBuf);
    assertNotNull("encode() returned null", encoded);
    ByteBuffer decodedBuf = IndexableBinaryStringTools.decode(encoded);
    assertNotNull("decode() returned null", decodedBuf);
    assertEquals("Round trip decode/decode returned different results:"
                 + System.getProperty("line.separator")
                 + "  original: " + binaryDump(binaryBuf)
                 + System.getProperty("line.separator")
                 + "decodedBuf: " + binaryDump(decodedBuf),
                 binaryBuf, decodedBuf);
  }
  
  public void testRandomBinaryRoundTrip() {
    Random random = newRandom();
    byte[] binary = new byte[MAX_RANDOM_BINARY_LENGTH];
    ByteBuffer binaryBuf = ByteBuffer.wrap(binary);
    char[] encoded = new char[IndexableBinaryStringTools.getEncodedLength(binaryBuf)];
    CharBuffer encodedBuf = CharBuffer.wrap(encoded);
    byte[] decoded = new byte[MAX_RANDOM_BINARY_LENGTH];
    ByteBuffer decodedBuf = ByteBuffer.wrap(decoded);
    for (int testNum = 0 ; testNum < NUM_RANDOM_TESTS ; ++testNum) {
      int numBytes = random.nextInt(MAX_RANDOM_BINARY_LENGTH - 1) + 1 ; // Min == 1
      binaryBuf.limit(numBytes);
      for (int byteNum = 0 ; byteNum < numBytes ; ++byteNum) {
        binary[byteNum] = (byte)random.nextInt(0x100);
      }
      IndexableBinaryStringTools.encode(binaryBuf, encodedBuf);
      IndexableBinaryStringTools.decode(encodedBuf, decodedBuf);
      assertEquals("Test #" + (testNum + 1) 
                   + ": Round trip decode/decode returned different results:"
                   + System.getProperty("line.separator")
                   + "  original: " + binaryDump(binaryBuf)
                   + System.getProperty("line.separator")
                   + "encodedBuf: " + charArrayDump(encodedBuf)
                   + System.getProperty("line.separator")
                   + "decodedBuf: " + binaryDump(decodedBuf),
                   binaryBuf, decodedBuf);
    }
  }
  
  public String binaryDump(ByteBuffer binaryBuf) {
    StringBuffer buf = new StringBuffer();
    int numBytes = binaryBuf.limit() - binaryBuf.arrayOffset();
    byte[] binary = binaryBuf.array();
    for (int byteNum = 0 ; byteNum < numBytes ; ++byteNum) {
      String hex = Integer.toHexString((int)binary[byteNum] & 0xFF);
      if (hex.length() == 1) {
        buf.append('0');
      }
      buf.append(hex.toUpperCase());
      if (byteNum < numBytes - 1) {
        buf.append(' ');
      }
    }
    return buf.toString();
  }

  public String charArrayDump(CharBuffer charBuf) {
    StringBuffer buf = new StringBuffer();
    int numBytes = charBuf.limit() - charBuf.arrayOffset();
    char[] charArray = charBuf.array();
    for (int charNum = 0 ; charNum < numBytes ; ++charNum) {
      String hex = Integer.toHexString((int)charArray[charNum]);
      for (int digit = 0 ; digit < 4 - hex.length() ; ++digit) {
        buf.append('0');
      }
      buf.append(hex.toUpperCase());
      if (charNum < numBytes - 1) {
        buf.append(' ');
      }
    }
    return buf.toString();
  }
}
