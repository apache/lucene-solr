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
package org.apache.lucene.codecs.lucene50;


import static org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat.BLOCK_SIZE;
import static org.apache.lucene.codecs.lucene50.ForUtil.MAX_DATA_SIZE;
import static org.apache.lucene.codecs.lucene50.ForUtil.MAX_ENCODED_SIZE;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.packed.PackedInts;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

public class TestForUtil extends LuceneTestCase {

  public void testEncodeDecode() throws IOException {
    final int iterations = RandomNumbers.randomIntBetween(random(), 1, 1000);
    final float acceptableOverheadRatio = random().nextFloat();
    final int[] values = new int[(iterations - 1) * BLOCK_SIZE + ForUtil.MAX_DATA_SIZE];
    for (int i = 0; i < iterations; ++i) {
      final int bpv = random().nextInt(32);
      if (bpv == 0) {
        final int value = RandomNumbers.randomIntBetween(random(), 0, Integer.MAX_VALUE);
        for (int j = 0; j < BLOCK_SIZE; ++j) {
          values[i * BLOCK_SIZE + j] = value;
        }
      } else {
        for (int j = 0; j < BLOCK_SIZE; ++j) {
          values[i * BLOCK_SIZE + j] = RandomNumbers.randomIntBetween(random(),
              0, (int) PackedInts.maxValue(bpv));
        }
      }
    }

    final Directory d = new RAMDirectory();
    final long endPointer;

    {
      // encode
      IndexOutput out = d.createOutput("test.bin", IOContext.DEFAULT);
      final ForUtil forUtil = new ForUtil(acceptableOverheadRatio, out);
      
      for (int i = 0; i < iterations; ++i) {
        forUtil.writeBlock(
            Arrays.copyOfRange(values, i * BLOCK_SIZE, values.length),
            new byte[MAX_ENCODED_SIZE], out);
      }
      endPointer = out.getFilePointer();
      out.close();
    }

    {
      // decode
      IndexInput in = d.openInput("test.bin", IOContext.READONCE);
      final ForUtil forUtil = new ForUtil(in);
      for (int i = 0; i < iterations; ++i) {
        if (random().nextBoolean()) {
          forUtil.skipBlock(in);
          continue;
        }
        final int[] restored = new int[MAX_DATA_SIZE];
        forUtil.readBlock(in, new byte[MAX_ENCODED_SIZE], restored);
        assertArrayEquals(Arrays.copyOfRange(values, i * BLOCK_SIZE, (i + 1) * BLOCK_SIZE),
            Arrays.copyOf(restored, BLOCK_SIZE));
      }
      assertEquals(endPointer, in.getFilePointer());
      in.close();
    }
    
    d.close();
  }

}
