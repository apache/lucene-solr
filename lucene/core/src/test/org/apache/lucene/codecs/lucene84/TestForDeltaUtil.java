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

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.packed.PackedInts;

public class TestForDeltaUtil extends LuceneTestCase {

  public void testEncodeDecode() throws IOException {
    final int iterations = RandomNumbers.randomIntBetween(random(), 50, 1000);
    final int[] values = new int[iterations * ForUtil.BLOCK_SIZE];

    for (int i = 0; i < iterations; ++i) {
      final int bpv = TestUtil.nextInt(random(), 1, 31 - 7);
      for (int j = 0; j < ForUtil.BLOCK_SIZE; ++j) {
        values[i * ForUtil.BLOCK_SIZE + j] =
            RandomNumbers.randomIntBetween(random(), 1, (int) PackedInts.maxValue(bpv));
      }
    }

    final Directory d = new ByteBuffersDirectory();
    final long endPointer;

    {
      // encode
      IndexOutput out = d.createOutput("test.bin", IOContext.DEFAULT);
      final ForDeltaUtil forDeltaUtil = new ForDeltaUtil(new ForUtil());

      for (int i = 0; i < iterations; ++i) {
        long[] source = new long[ForUtil.BLOCK_SIZE];
        for (int j = 0; j < ForUtil.BLOCK_SIZE; ++j) {
          source[j] = values[i * ForUtil.BLOCK_SIZE + j];
        }
        forDeltaUtil.encodeDeltas(source, out);
      }
      endPointer = out.getFilePointer();
      out.close();
    }

    {
      // decode
      IndexInput in = d.openInput("test.bin", IOContext.READONCE);
      final ForDeltaUtil forDeltaUtil = new ForDeltaUtil(new ForUtil());
      for (int i = 0; i < iterations; ++i) {
        if (random().nextInt(5) == 0) {
          forDeltaUtil.skip(in);
          continue;
        }
        long base = 0;
        final long[] restored = new long[ForUtil.BLOCK_SIZE];
        forDeltaUtil.decodeAndPrefixSum(in, base, restored);
        final long[] expected = new long[ForUtil.BLOCK_SIZE];
        for (int j = 0; j < ForUtil.BLOCK_SIZE; ++j) {
          expected[j] = values[i * ForUtil.BLOCK_SIZE + j];
          if (j > 0) {
            expected[j] += expected[j - 1];
          } else {
            expected[j] += base;
          }
        }
        assertArrayEquals(Arrays.toString(restored), expected, restored);
      }
      assertEquals(endPointer, in.getFilePointer());
      in.close();
    }

    d.close();
  }
}
