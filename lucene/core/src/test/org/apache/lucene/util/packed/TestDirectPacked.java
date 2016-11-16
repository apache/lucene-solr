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
package org.apache.lucene.util.packed;


import java.util.Random;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;

public class TestDirectPacked extends LuceneTestCase {
  
  /** simple encode/decode */
  public void testSimple() throws Exception {
    Directory dir = newDirectory();
    int bitsPerValue = DirectWriter.bitsRequired(2);
    IndexOutput output = dir.createOutput("foo", IOContext.DEFAULT);
    DirectWriter writer = DirectWriter.getInstance(output, 5, bitsPerValue);
    writer.add(1);
    writer.add(0);
    writer.add(2);
    writer.add(1);
    writer.add(2);
    writer.finish();
    output.close();
    IndexInput input = dir.openInput("foo", IOContext.DEFAULT);
    LongValues reader = DirectReader.getInstance(input.randomAccessSlice(0, input.length()), bitsPerValue, 0);
    assertEquals(1, reader.get(0));
    assertEquals(0, reader.get(1));
    assertEquals(2, reader.get(2));
    assertEquals(1, reader.get(3));
    assertEquals(2, reader.get(4));
    input.close();
    dir.close();
  }
  
  /** test exception is delivered if you add the wrong number of values */
  public void testNotEnoughValues() throws Exception {
    Directory dir = newDirectory();
    int bitsPerValue = DirectWriter.bitsRequired(2);
    IndexOutput output = dir.createOutput("foo", IOContext.DEFAULT);
    DirectWriter writer = DirectWriter.getInstance(output, 5, bitsPerValue);
    writer.add(1);
    writer.add(0);
    writer.add(2);
    writer.add(1);
    IllegalStateException expected = expectThrows(IllegalStateException.class, () -> {
      writer.finish();
    });
    assertTrue(expected.getMessage().startsWith("Wrong number of values added"));

    output.close();
    dir.close();
  }
  
  public void testRandom() throws Exception {
    Directory dir = newDirectory();
    for (int bpv = 1; bpv <= 64; bpv++) {
      doTestBpv(dir, bpv, 0);
    }
    dir.close();
  }

  public void testRandomWithOffset() throws Exception {
    Directory dir = newDirectory();
    final int offset = TestUtil.nextInt(random(), 1, 100);
    for (int bpv = 1; bpv <= 64; bpv++) {
      doTestBpv(dir, bpv, offset);
    }
    dir.close();
  }

  private void doTestBpv(Directory directory, int bpv, long offset) throws Exception {
    MyRandom random = new MyRandom(random().nextLong());
    int numIters = TEST_NIGHTLY ? 100 : 10;
    for (int i = 0; i < numIters; i++) {
      long original[] = randomLongs(random, bpv);
      int bitsRequired = bpv == 64 ? 64 : DirectWriter.bitsRequired(1L<<(bpv-1));
      String name = "bpv" + bpv + "_" + i;
      IndexOutput output = directory.createOutput(name, IOContext.DEFAULT);
      for (long j = 0; j < offset; ++j) {
        output.writeByte((byte) random().nextInt());
      }
      DirectWriter writer = DirectWriter.getInstance(output, original.length, bitsRequired);
      for (int j = 0; j < original.length; j++) {
        writer.add(original[j]);
      }
      writer.finish();
      output.close();
      IndexInput input = directory.openInput(name, IOContext.DEFAULT);
      LongValues reader = DirectReader.getInstance(input.randomAccessSlice(0, input.length()), bitsRequired, offset);
      for (int j = 0; j < original.length; j++) {
        assertEquals("bpv=" + bpv, original[j], reader.get(j));
      }
      input.close();
    }
  }
    
  private long[] randomLongs(MyRandom random, int bpv) {
    int amount = random.nextInt(5000);
    long longs[] = new long[amount];
    for (int i = 0; i < longs.length; i++) {
      longs[i] = random.nextLong(bpv);
    }
    return longs;
  }

  // java.util.Random only returns 48bits of randomness in nextLong...
  static class MyRandom extends Random {
    byte buffer[] = new byte[8];
    ByteArrayDataInput input = new ByteArrayDataInput();
    
    MyRandom(long seed) {
      super(seed);
    }
    
    public synchronized long nextLong(int bpv) {
      nextBytes(buffer);
      input.reset(buffer);
      long bits = input.readLong();
      return bits >>> (64-bpv);
    }
  }
}
