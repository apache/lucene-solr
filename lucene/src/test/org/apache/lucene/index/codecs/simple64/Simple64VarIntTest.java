package org.apache.lucene.index.codecs.simple64;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.BulkPostingsEnum.BlockReader;
import org.apache.lucene.index.codecs.CodecTestCase;
import org.apache.lucene.index.codecs.sep.IntIndexInput;
import org.apache.lucene.index.codecs.sep.IntIndexOutput;
import org.apache.lucene.index.codecs.sep.IntStreamFactory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util._TestUtil;
import org.junit.Test;

public class Simple64VarIntTest extends CodecTestCase {

  @Test
  public void testSimple() throws IOException {
    final int[] values = { 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
                           1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,};
    doTest(values, 0);
  }

  @Test
  public void testSimple16bits() throws IOException {

    // 60 values:
    final int[] values = { 60149,60149,60149,60149,60149,60149,60149,60149,60149,
                           60149,60149,60149,60149,60149,60149,60149,60149,60149,
                           60149,60149,60149,60149,60149,60149,60149,60149,60149,
                           60149,60149,60149,60149,60149,
                           60149,60149,60149,60149,60149,60149,60149,60149,60149,
                           60149,60149,60149,60149,60149,60149,60149,60149,60149,
                           60149,60149,60149,60149,60149,60149,60149,60149,60149,
                           60149,};
    doTest(values, 0);
  }

  // nocommit -- blockSize is unused:
  @Override
  public void doTest(final int[] values, int blockSize)
      throws IOException {
    final RAMDirectory dir = new RAMDirectory();
    final String filename = Simple64.class.toString();
    final IntStreamFactory factory = new Simple64VarIntCodec().getIntFactory();
    final IntIndexOutput output = factory.createOutput(dir, filename);

    if (VERBOSE) {
      System.out.println("TEST: " + values.length + " values");
    }

    final IndexOutput indexOutput = dir.createOutput("index");
    final List<Integer> indexed = new ArrayList<Integer>();

    final IntIndexOutput.Index index = output.index();

    for (int upto=0;upto<values.length;upto++) {
      final int element = values[upto];
      if (VERBOSE) {
        System.out.println("  add " + element);
      }
      if (random.nextInt(20) == 17) {
        index.mark();
        index.write(indexOutput, true);
        indexed.add(upto);
      }
      output.write(element);
    }
    output.close();
    indexOutput.close();

    final IntIndexInput input = factory.openInput(dir, filename);
    final BlockReader reader = input.reader();
    int buffer[] = reader.getBuffer();
    int pointer = 0;
    int pointerMax = reader.fill();
    assertTrue(pointerMax > 0);

    if (VERBOSE) {
      System.out.println("  verify...");
    }
    for(int i=0;i<values.length;i++) {
      if (pointer == pointerMax) {
        pointerMax = reader.fill();
        assertTrue(pointerMax > 0);
        pointer = 0;
      }
      if (VERBOSE) {
        System.out.println("  got " + buffer[pointer]);
      }
      assertEquals(values[i], buffer[pointer++]);
    }

    // Now test seeking:
    if (indexed.size() != 0) {
      final IndexInput indexInput = dir.openInput("index");
      List<IntIndexInput.Index> indexes = new ArrayList<IntIndexInput.Index>();
      for(int spot : indexed) {
        IntIndexInput.Index index2 = input.index();
        index2.read(indexInput, true);
        indexes.add(index2);
      }
      indexInput.close();

      for(int iter=0;iter<100;iter++) {
        final int spot = random.nextInt(indexed.size());
        if (VERBOSE) {
          System.out.println("TEST: seek index=" + indexes.get(spot));
        }
        indexes.get(spot).seek(reader);
        pointerMax = reader.end();
        pointer = reader.offset();
        int upto = indexed.get(spot);
        int limit = Math.min(upto+20, values.length);
        while(upto < limit) {
          if (pointer == pointerMax) {
            pointerMax = reader.fill();
            assertTrue(pointerMax > 0);
            pointer = 0;
          }
          if (VERBOSE) {
            System.out.println("  got " + buffer[pointer]);
          }
          assertEquals(values[upto++], buffer[pointer++]);
        }
      }
    }

    input.close();
    dir.close();
  }

  @Test
  public void testRandom() throws Exception {
    // nocommit mixup size of int[]
    // nocommit more iters:
    for(int iter=0;iter<10*RANDOM_MULTIPLIER;iter++) {
      int size = _TestUtil.nextInt(random, 10, 1000);
      int[] values = new int[size];
      for(int i=0;i<values.length;i++) {
        if (random.nextInt(20) == 17) {
          values[i] = random.nextInt() & Integer.MAX_VALUE;
        } else {
          // duh -- & 3:
          values[i] = random.nextInt() & 4;
        }
      }
      doTest(values, 0);
    }
  }

  @Test
  public void testIntegerRange32() throws IOException {
    this.doTestIntegerRange(1, 32);
  }
}
