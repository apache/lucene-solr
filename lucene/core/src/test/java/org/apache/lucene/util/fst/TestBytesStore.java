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
package org.apache.lucene.util.fst;


import java.util.Arrays;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestBytesStore extends LuceneTestCase {

  public void testRandom() throws Exception {

    final int iters = atLeast(10);
    final int maxBytes = TEST_NIGHTLY ? 200000 : 20000;
    for(int iter=0;iter<iters;iter++) {
      final int numBytes = TestUtil.nextInt(random(), 1, maxBytes);
      final byte[] expected = new byte[numBytes];
      final int blockBits = TestUtil.nextInt(random(), 8, 15);
      final BytesStore bytes = new BytesStore(blockBits);
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter + " numBytes=" + numBytes + " blockBits=" + blockBits);
      }

      int pos = 0;
      while(pos < numBytes) {
        int op = random().nextInt(8);
        if (VERBOSE) {
          System.out.println("  cycle pos=" + pos);
        }
        switch(op) {

        case 0:
          {
            // write random byte
            byte b = (byte) random().nextInt(256);
            if (VERBOSE) {
              System.out.println("    writeByte b=" + b);
            }

            expected[pos++] = b;
            bytes.writeByte(b);
          }
          break;

        case 1:
          {
            // write random byte[]
            int len = random().nextInt(Math.min(numBytes - pos, 100));
            byte[] temp = new byte[len];
            random().nextBytes(temp);
            if (VERBOSE) {
              System.out.println("    writeBytes len=" + len + " bytes=" + Arrays.toString(temp));
            }
            System.arraycopy(temp, 0, expected, pos, temp.length);
            bytes.writeBytes(temp, 0, temp.length);
            pos += len;
          }
          break;

        case 2:
          {
            // write int @ absolute pos
            if (pos > 4) {
              int x = random().nextInt();
              int randomPos = random().nextInt(pos-4);
              if (VERBOSE) {
                System.out.println("    abs writeInt pos=" + randomPos + " x=" + x);
              }
              bytes.writeInt(randomPos, x);
              expected[randomPos++] = (byte) (x >> 24);
              expected[randomPos++] = (byte) (x >> 16);
              expected[randomPos++] = (byte) (x >> 8);
              expected[randomPos++] = (byte) x;
            }
          }
          break;

        case 3:
          {
            // reverse bytes
            if (pos > 1) {
              int len = TestUtil.nextInt(random(), 2, Math.min(100, pos));
              int start;
              if (len == pos) {
                start = 0;
              } else {
                start = random().nextInt(pos - len);
              }
              int end = start + len - 1;
              if (VERBOSE) {
                System.out.println("    reverse start=" + start + " end=" + end + " len=" + len + " pos=" + pos);
              }
              bytes.reverse(start, end);

              while(start <= end) {
                byte b = expected[end];
                expected[end] = expected[start];
                expected[start] = b;
                start++;
                end--;
              }
            }
          }
          break;

        case 4:
          {
            // abs write random byte[]
            if (pos > 2) {
              int randomPos = random().nextInt(pos-1);
              int len = TestUtil.nextInt(random(), 1, Math.min(pos - randomPos - 1, 100));
              byte[] temp = new byte[len];
              random().nextBytes(temp);
              if (VERBOSE) {
                System.out.println("    abs writeBytes pos=" + randomPos + " len=" + len + " bytes=" + Arrays.toString(temp));
              }
              System.arraycopy(temp, 0, expected, randomPos, temp.length);
              bytes.writeBytes(randomPos, temp, 0, temp.length);
            }
          }
          break;

        case 5:
          {
            // copyBytes
            if (pos > 1) {
              int src = random().nextInt(pos-1);
              int dest = TestUtil.nextInt(random(), src + 1, pos - 1);
              int len = TestUtil.nextInt(random(), 1, Math.min(300, pos - dest));
              if (VERBOSE) {
                System.out.println("    copyBytes src=" + src + " dest=" + dest + " len=" + len);
              }
              System.arraycopy(expected, src, expected, dest, len);
              bytes.copyBytes(src, dest, len);
            }
          }
          break;

        case 6:
          {
            // skip
            int len = random().nextInt(Math.min(100, numBytes - pos));

            if (VERBOSE) {
              System.out.println("    skip len=" + len);
            }

            pos += len;
            bytes.skipBytes(len);

            // NOTE: must fill in zeros in case truncate was
            // used, else we get false fails:
            if (len > 0) {
              byte[] zeros = new byte[len];
              bytes.writeBytes(pos-len, zeros, 0, len);
            }
          }
          break;

        case 7:
          {
            // absWriteByte
            if (pos > 0) {
              int dest = random().nextInt(pos);
              byte b = (byte) random().nextInt(256);
              expected[dest] = b;
              bytes.writeByte(dest, b);
            }
            break;
          }
        }

        assertEquals(pos, bytes.getPosition());

        if (pos > 0 && random().nextInt(50) == 17) {
          // truncate
          int len = TestUtil.nextInt(random(), 1, Math.min(pos, 100));
          bytes.truncate(pos - len);
          pos -= len;
          Arrays.fill(expected, pos, pos+len, (byte) 0);
          if (VERBOSE) {
            System.out.println("    truncate len=" + len + " newPos=" + pos);
          }
        }

        if ((pos > 0 && random().nextInt(200) == 17)) {
          verify(bytes, expected, pos);
        }
      }

      BytesStore bytesToVerify;

      if (random().nextBoolean()) {
        if (VERBOSE) {
          System.out.println("TEST: save/load final bytes");
        }
        Directory dir = newDirectory();
        IndexOutput out = dir.createOutput("bytes", IOContext.DEFAULT);
        bytes.writeTo(out);
        out.close();
        IndexInput in = dir.openInput("bytes", IOContext.DEFAULT);
        bytesToVerify = new BytesStore(in, numBytes, TestUtil.nextInt(random(), 256, Integer.MAX_VALUE));
        in.close();
        dir.close();
      } else {
        bytesToVerify = bytes;
      }

      verify(bytesToVerify, expected, numBytes);
    }
  }

  private void verify(BytesStore bytes, byte[] expected, int totalLength) throws Exception {
    assertEquals(totalLength, bytes.getPosition());
    if (totalLength == 0) {
      return;
    }
    if (VERBOSE) {
      System.out.println("  verify...");
    }
    
    // First verify whole thing in one blast:
    byte[] actual = new byte[totalLength];
    if (random().nextBoolean()) {
      if (VERBOSE) {
        System.out.println("    bulk: reversed");
      }
      // reversed
      FST.BytesReader r = bytes.getReverseReader();
      assertTrue(r.reversed());
      r.setPosition(totalLength-1);
      r.readBytes(actual, 0, actual.length);
      int start = 0;
      int end = totalLength - 1;
      while(start < end) {
        byte b = actual[start];
        actual[start] = actual[end];
        actual[end] = b;
        start++;
        end--;
      }
    } else {
      // forward
      if (VERBOSE) {
        System.out.println("    bulk: forward");
      }
      FST.BytesReader r = bytes.getForwardReader();
      assertFalse(r.reversed());
      r.readBytes(actual, 0, actual.length);
    }

    for(int i=0;i<totalLength;i++) {
      assertEquals("byte @ index=" + i, expected[i], actual[i]);
    }

    FST.BytesReader r;

    // Then verify ops:
    boolean reversed = random().nextBoolean();
    if (reversed) {
      if (VERBOSE) {
        System.out.println("    ops: reversed");
      }
      r = bytes.getReverseReader();
    } else {
      if (VERBOSE) {
        System.out.println("    ops: forward");
      }
      r = bytes.getForwardReader();
    }

    if (totalLength > 1) {
      int numOps = TestUtil.nextInt(random(), 100, 200);
      for(int op=0;op<numOps;op++) {

        int numBytes = random().nextInt(Math.min(1000, totalLength-1));
        int pos;
        if (reversed) {
          pos = TestUtil.nextInt(random(), numBytes, totalLength - 1);
        } else {
          pos = random().nextInt(totalLength-numBytes);
        }
        if (VERBOSE) {
          System.out.println("    op iter=" + op + " reversed=" + reversed + " numBytes=" + numBytes + " pos=" + pos);
        }
        byte[] temp = new byte[numBytes];
        r.setPosition(pos);
        assertEquals(pos, r.getPosition());
        r.readBytes(temp, 0, temp.length);
        for(int i=0;i<numBytes;i++) {
          byte expectedByte;
          if (reversed) {
            expectedByte = expected[pos - i];
          } else {
            expectedByte = expected[pos + i];
          }
          assertEquals("byte @ index=" + i, expectedByte, temp[i]);
        }

        int left;
        int expectedPos;

        if (reversed) {
          expectedPos = pos-numBytes;
          left = (int) r.getPosition();
        } else {
          expectedPos = pos+numBytes;
          left = (int) (totalLength - r.getPosition());
        }
        assertEquals(expectedPos, r.getPosition());

        if (left > 4) {
          int skipBytes = random().nextInt(left-4);

          int expectedInt = 0;
          if (reversed) {
            expectedPos -= skipBytes;
            expectedInt |= (expected[expectedPos--]&0xFF)<<24;
            expectedInt |= (expected[expectedPos--]&0xFF)<<16;
            expectedInt |= (expected[expectedPos--]&0xFF)<<8;
            expectedInt |= (expected[expectedPos--]&0xFF);
          } else {
            expectedPos += skipBytes;
            expectedInt |= (expected[expectedPos++]&0xFF)<<24;
            expectedInt |= (expected[expectedPos++]&0xFF)<<16;
            expectedInt |= (expected[expectedPos++]&0xFF)<<8;
            expectedInt |= (expected[expectedPos++]&0xFF);
          }

          if (VERBOSE) {
            System.out.println("    skip numBytes=" + skipBytes);
            System.out.println("    readInt");
          }

          r.skipBytes(skipBytes);
          assertEquals(expectedInt, r.readInt());
        }
      }
    }
  }
}
