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

import org.apache.lucene.index.BulkPostingsEnum.BlockReader;
import org.apache.lucene.index.codecs.CodecTestCase;
import org.apache.lucene.index.codecs.sep.IntIndexInput;
import org.apache.lucene.index.codecs.sep.IntIndexOutput;
import org.apache.lucene.index.codecs.sep.IntStreamFactory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

public class Simple64Test extends CodecTestCase {

  @Test
  public void testSimple() throws IOException {
    final int blockSize = 60;
    final RAMDirectory dir = new RAMDirectory();
    final String filename = Simple64.class.toString();
    final IntStreamFactory factory = new Simple64Codec(blockSize).getIntFactory();
    final IntIndexOutput output = factory.createOutput(dir, filename);
    final int[] values = { 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
                           1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,};

    for (final int element : values) {
      output.write(element);
    }
    output.close();

    final IntIndexInput input = factory.openInput(dir, filename);
    final BlockReader reader = input.reader();
    int buffer[] = reader.getBuffer();
    reader.fill();
    for (int i = 0; i < values.length; i++) {
      assertEquals("Error at record " + i, values[i], buffer[i]);
    }
    input.close();
    dir.close();
  }

  @Test
  public void testSimple16bits() throws IOException {
    final int blockSize = 60;
    final RAMDirectory dir = new RAMDirectory();
    final String filename = Simple64.class.toString();
    final IntStreamFactory factory = new Simple64Codec(blockSize).getIntFactory();
    final IntIndexOutput output = factory.createOutput(dir, filename);
    final int[] values = { 60149,60149,60149,60149,60149,60149,60149,60149,60149,
                           60149,60149,60149,60149,60149,60149,60149,60149,60149,
                           60149,60149,60149,60149,60149,60149,60149,60149,60149,
                           60149,60149,60149,60149,60149,
                           60149,60149,60149,60149,60149,60149,60149,60149,60149,
                           60149,60149,60149,60149,60149,60149,60149,60149,60149,
                           60149,60149,60149,60149,60149,60149,60149,60149,60149,
                           60149,};

    for (final int element : values) {
      output.write(element);
    }
    output.close();

    final IntIndexInput input = factory.openInput(dir, filename);
    final BlockReader reader = input.reader();
    int buffer[] = reader.getBuffer();
    reader.fill();
    for (int i = 0; i < values.length; i++) {
      assertEquals("Error at record " + i, values[i], buffer[i]);
    }
    input.close();
    dir.close();
  }

  @Override
  public void doTest(final int[] values, final int blockSize)
      throws IOException {
    final RAMDirectory dir = new RAMDirectory();
    final String filename = Simple64.class.toString();
    final IntStreamFactory factory = new Simple64Codec(blockSize).getIntFactory();
    final IntIndexOutput output = factory.createOutput(dir, filename);

    for (final int element : values) {
      output.write(element);
    }
    output.close();

    final IntIndexInput input = factory.openInput(dir, filename);
    final BlockReader reader = input.reader();
    int buffer[] = reader.getBuffer();
    int pointer = 0;
    int pointerMax = reader.fill();
    assertTrue(pointerMax > 0);

    for(int i=0;i<values.length;i++) {
      if (pointer == pointerMax) {
        pointerMax = reader.fill();
        assertTrue(pointerMax > 0);
        pointer = 0;
      }
      assertEquals(values[i], buffer[pointer++]);
    }

    input.close();
    dir.close();
  }

  @Test
  public void testIntegerRange32() throws IOException {
    this.doTestIntegerRange(1, 32);
  }
}
