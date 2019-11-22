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
package org.apache.lucene.codecs.lucene84;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.packed.PackedInts;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

public class TestPForUtil extends LuceneTestCase {

  public void testEncodeDecode() throws IOException {
    final int iterations = RandomNumbers.randomIntBetween(random(), 50, 1000);
    final int[] values = new int[iterations * ForUtil.BLOCK_SIZE];

    for (int i = 0; i < iterations; ++i) {
      final int bpv = TestUtil.nextInt(random(), 0, 31);
      for (int j = 0; j < ForUtil.BLOCK_SIZE; ++j) {
        values[i * ForUtil.BLOCK_SIZE + j] = RandomNumbers.randomIntBetween(random(),
            0, (int) PackedInts.maxValue(bpv));
        if (random().nextInt(100) == 0) {
          final int exceptionBpv;
          if (random().nextInt(10) == 0) {
            exceptionBpv = Math.min(bpv + TestUtil.nextInt(random(), 9, 16), 31);
          } else {
            exceptionBpv = Math.min(bpv + TestUtil.nextInt(random(), 1, 8), 31);
          }
          values[i * ForUtil.BLOCK_SIZE + j] |= random().nextInt(1 << (exceptionBpv - bpv)) << bpv;
        }
      }
    }

    final Directory d = new ByteBuffersDirectory();
    final long endPointer;

    {
      // encode
      IndexOutput out = d.createOutput("test.bin", IOContext.DEFAULT);
      final PForUtil pforUtil = new PForUtil(new ForUtil());

      for (int i = 0; i < iterations; ++i) {
        long[] source = new long[ForUtil.BLOCK_SIZE];
        for (int j = 0; j < ForUtil.BLOCK_SIZE; ++j) {
          source[j] = values[i*ForUtil.BLOCK_SIZE+j];
        }
        pforUtil.encode(source, out);
      }
      endPointer = out.getFilePointer();
      out.close();
    }

    {
      // decode
      IndexInput in = d.openInput("test.bin", IOContext.READONCE);
      final PForUtil pforUtil = new PForUtil(new ForUtil());
      for (int i = 0; i < iterations; ++i) {
        if (random().nextInt(5) == 0) {
          pforUtil.skip(in);
          continue;
        }
        final long[] restored = new long[ForUtil.BLOCK_SIZE];
        pforUtil.decode(in, restored);
        int[] ints = new int[ForUtil.BLOCK_SIZE];
        for (int j = 0; j < ForUtil.BLOCK_SIZE; ++j) {
          ints[j] = Math.toIntExact(restored[j]);
        }
        assertArrayEquals(Arrays.toString(ints),
            ArrayUtil.copyOfSubArray(values, i*ForUtil.BLOCK_SIZE, (i+1)*ForUtil.BLOCK_SIZE),
            ints);
      }
      assertEquals(endPointer, in.getFilePointer());
      in.close();
    }

    d.close();
  }
}
