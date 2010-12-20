package org.apache.lucene.index.codecs.pfordelta;

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

import org.apache.lucene.index.BulkPostingsEnum;
import org.apache.lucene.index.codecs.sep.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * This class is to test the PForDeltaFixedIntBlockCodec 
 * 
 *
 */

public class TestPForDelta extends LuceneTestCase {

  public void testRandomInts() throws Exception {
    // nocommit mix up block size too once pfor1 supports it
    int blockSize = 128;
    IntStreamFactory f = new PForDeltaFactory(blockSize);
    for(int iter=0;iter<10*RANDOM_MULTIPLIER;iter++) {
      Directory dir = newDirectory();
      int testDataSize = _TestUtil.nextInt(random, 10000, 100000);
      int[] testData = new int[testDataSize];

      for(int i=0; i<testDataSize; ++i) {
        // nocommit -- do a better job here -- pick
        // numFrameBits, numExceptions, hten gen according
        // to that
        testData[i] = random.nextInt() & Integer.MAX_VALUE;
      }
    
      IntIndexOutput out = f.createOutput(dir, "test");
      for(int i=0;i<testDataSize;i++) {
        out.write(testData[i]);
      }
      out.close();

      IntIndexInput in = f.openInput(dir, "test");
      BulkPostingsEnum.BlockReader r = in.reader();
      final int[] buffer = r.getBuffer();
      int pointer = 0;
      int pointerMax = r.fill();
      assertTrue(pointerMax > 0);

      for(int i=0;i<testDataSize;i++) {
        assertEquals(testData[i], buffer[pointer++]);
        if (pointer == pointerMax) {
          pointerMax = r.fill();
          assertTrue(pointerMax > 0);
          pointer = 0;
        }
      }
      in.close();
      dir.close();
    }
  }

  public void testEmpty() throws Exception {
    Directory dir = newDirectory();
    int blockSize = 128;
    IntStreamFactory f = new PForDeltaFactory(blockSize);
    IntIndexOutput out = f.createOutput(dir, "test");

    // write no ints
    out.close();

    IntIndexInput in = f.openInput(dir, "test");
    in.reader();
    // read no ints
    in.close();
    dir.close();
  }
}

