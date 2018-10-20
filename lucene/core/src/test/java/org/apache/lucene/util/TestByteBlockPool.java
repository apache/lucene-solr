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
package org.apache.lucene.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestByteBlockPool extends LuceneTestCase {

  public void testReadAndWrite() throws IOException {
    Counter bytesUsed = Counter.newCounter();
    ByteBlockPool pool = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(bytesUsed));
    pool.nextBuffer();
    boolean reuseFirst = random().nextBoolean();
    for (int j = 0; j < 2; j++) {
        
      List<BytesRef> list = new ArrayList<>();
      int maxLength = atLeast(500);
      final int numValues = atLeast(100);
      BytesRefBuilder ref = new BytesRefBuilder();
      for (int i = 0; i < numValues; i++) {
        final String value = TestUtil.randomRealisticUnicodeString(random(), maxLength);
        list.add(new BytesRef(value));
        ref.copyChars(value);
        pool.append(ref.get());
      }
      // verify
      long position = 0;
      for (BytesRef expected : list) {
        ref.grow(expected.length);
        ref.setLength(expected.length);
        switch (random().nextInt(3)) {
          case 0:
            // copy bytes
            pool.readBytes(position, ref.bytes(), 0, ref.length());
            break;
          case 1:
            // copy bytes one by one
            for (int i = 0; i < ref.length(); ++i) {
              ref.setByteAt(i, pool.readByte(position + i));
            }
            break;
          case 2:
            BytesRef scratch = new BytesRef();
            scratch.length = ref.length();
            pool.setRawBytesRef(scratch, position);
            System.arraycopy(scratch.bytes, scratch.offset, ref.bytes(), 0, ref.length());
            break;
          default:
            fail();
        }
        assertEquals(expected, ref.get());
        position += ref.length();
      }
      pool.reset(random().nextBoolean(), reuseFirst);
      if (reuseFirst) {
        assertEquals(ByteBlockPool.BYTE_BLOCK_SIZE, bytesUsed.get());
      } else {
        assertEquals(0, bytesUsed.get());
        pool.nextBuffer(); // prepare for next iter
      }
    }
  }

  public void testLargeRandomBlocks() throws IOException {
    Counter bytesUsed = Counter.newCounter();
    ByteBlockPool pool = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(bytesUsed));
    pool.nextBuffer();

    List<byte[]> items = new ArrayList<>();
    for (int i=0;i<100;i++) {
      int size;
      if (random().nextBoolean()) {
        size = TestUtil.nextInt(random(), 100, 1000);
      } else {
        size = TestUtil.nextInt(random(), 50000, 100000);
      }
      byte[] bytes = new byte[size];
      random().nextBytes(bytes);
      items.add(bytes);
      pool.append(new BytesRef(bytes));
    }

    long position = 0;
    for (byte[] expected : items) {
      byte[] actual = new byte[expected.length];
      pool.readBytes(position, actual, 0, actual.length);
      assertTrue(Arrays.equals(expected, actual));
      position += expected.length;
    }
  }
}
