package org.apache.lucene.codecs.compressing;

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

import java.io.IOException;

import com.carrotsearch.randomizedtesting.generators.RandomInts;

public abstract class AbstractTestLZ4CompressionMode extends AbstractTestCompressionMode {

  @Override
  public byte[] test(byte[] decompressed) throws IOException {
    final byte[] compressed = super.test(decompressed);
    int off = 0;
    int decompressedOff = 0;
    for (;;) {
      final int token = compressed[off++] & 0xFF;
      int literalLen = token >>> 4;
      if (literalLen == 0x0F) {
        while (compressed[off] == (byte) 0xFF) {
          literalLen += 0xFF;
          ++off;
        }
        literalLen += compressed[off++] & 0xFF;
      }
      // skip literals
      off += literalLen;
      decompressedOff += literalLen;

      // check that the stream ends with literals and that there are at least
      // 5 of them
      if (off == compressed.length) {
        assertEquals(decompressed.length, decompressedOff);
        assertTrue("lastLiterals=" + literalLen + ", bytes=" + decompressed.length,
            literalLen >= LZ4.LAST_LITERALS || literalLen == decompressed.length);
        break;
      }

      final int matchDec = (compressed[off++] & 0xFF) | ((compressed[off++] & 0xFF) << 8);
      // check that match dec is not 0
      assertTrue(matchDec + " " + decompressedOff, matchDec > 0 && matchDec <= decompressedOff);

      int matchLen = token & 0x0F;
      if (matchLen == 0x0F) {
        while (compressed[off] == (byte) 0xFF) {
          matchLen += 0xFF;
          ++off;
        }
        matchLen += compressed[off++] & 0xFF;
      }
      matchLen += LZ4.MIN_MATCH;

      // if the match ends prematurely, the next sequence should not have
      // literals or this means we are wasting space
      if (decompressedOff + matchLen < decompressed.length - LZ4.LAST_LITERALS) {
        final boolean moreCommonBytes = decompressed[decompressedOff + matchLen] == decompressed[decompressedOff - matchDec + matchLen];
        final boolean nextSequenceHasLiterals = ((compressed[off] & 0xFF) >>> 4) != 0;
        assertTrue(!moreCommonBytes || !nextSequenceHasLiterals);
      }      

      decompressedOff += matchLen;
    }
    assertEquals(decompressed.length, decompressedOff);
    return compressed;
  }

  public void testShortLiteralsAndMatchs() throws IOException {
    // literals and matchs lengths <= 15
    final byte[] decompressed = "1234562345673456745678910123".getBytes("UTF-8");
    test(decompressed);
  }

  public void testLongMatchs() throws IOException {
    // match length >= 20
    final byte[] decompressed = new byte[RandomInts.randomIntBetween(random(), 300, 1024)];
    for (int i = 0; i < decompressed.length; ++i) {
      decompressed[i] = (byte) i;
    }
    test(decompressed);
  }

  public void testLongLiterals() throws IOException {
    // long literals (length >= 16) which are not the last literals
    final byte[] decompressed = randomArray(RandomInts.randomIntBetween(random(), 400, 1024), 256);
    final int matchRef = random().nextInt(30);
    final int matchOff = RandomInts.randomIntBetween(random(), decompressed.length - 40, decompressed.length - 20);
    final int matchLength = RandomInts.randomIntBetween(random(), 4, 10);
    System.arraycopy(decompressed, matchRef, decompressed, matchOff, matchLength);
    test(decompressed);
  }

}
