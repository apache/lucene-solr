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

import java.util.Random;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestByteSliceReader extends LuceneTestCase {
  private static byte[] RANDOM_DATA;
  private static ByteBlockPool BLOCK_POOL;
  private static int BLOCK_POOL_END;

  @BeforeClass
  public static void beforeClass() {
    int len = atLeast(100);
    RANDOM_DATA = new byte[len];
    random().nextBytes(RANDOM_DATA);

    BLOCK_POOL = new ByteBlockPool(new ByteBlockPool.DirectAllocator());
    BLOCK_POOL.nextBuffer();
    byte[] buffer = BLOCK_POOL.buffer;
    int upto = BLOCK_POOL.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
    for (byte randomByte : RANDOM_DATA) {
      if ((buffer[upto] & 16) != 0) {
        upto = BLOCK_POOL.allocSlice(buffer, upto);
        buffer = BLOCK_POOL.buffer;
      }
      buffer[upto++] = randomByte;
    }
    BLOCK_POOL_END = upto;
  }

  @AfterClass
  public static void afterClass() {
    RANDOM_DATA = null;
    BLOCK_POOL = null;
  }

  public void testReadByte() {
    ByteSliceReader sliceReader = new ByteSliceReader();
    sliceReader.init(BLOCK_POOL, 0, BLOCK_POOL_END);
    for (byte expected : RANDOM_DATA) {
      assertEquals(expected, sliceReader.readByte());
    }
  }

  public void testSkipBytes() {
    Random random = random();
    ByteSliceReader sliceReader = new ByteSliceReader();

    int maxSkipTo = RANDOM_DATA.length - 1;
    int iterations = atLeast(random, 10);
    for (int i = 0; i < iterations; i++) {
      sliceReader.init(BLOCK_POOL, 0, BLOCK_POOL_END);
      // skip random chunks of bytes until exhausted
      for (int curr = 0; curr < maxSkipTo; ) {
        int skipTo = TestUtil.nextInt(random, curr, maxSkipTo);
        int step = skipTo - curr;
        sliceReader.skipBytes(step);
        assertEquals(RANDOM_DATA[skipTo], sliceReader.readByte());
        curr = skipTo + 1; // +1 for read byte
      }
    }
  }
}
