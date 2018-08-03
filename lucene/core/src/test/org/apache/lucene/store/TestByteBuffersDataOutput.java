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
package org.apache.lucene.store;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

public final class TestByteBuffersDataOutput extends BaseDataOutputTestCase<ByteBuffersDataOutput> {
  @Override
  protected ByteBuffersDataOutput newInstance() {
    return new ByteBuffersDataOutput();
  }
  
  @Override
  protected byte[] toBytes(ByteBuffersDataOutput instance) {
    return instance.toArray();
  }

  @Test
  public void testReuse() throws IOException {
    AtomicInteger allocations = new AtomicInteger(0);
    ByteBuffersDataOutput.BufferReuser reuser = new ByteBuffersDataOutput.BufferReuser(
        (size) -> {
          allocations.incrementAndGet();
          return ByteBuffer.allocate(size);
        });
    
    ByteBuffersDataOutput o = new ByteBuffersDataOutput(
        ByteBuffersDataOutput.DEFAULT_MIN_BITS_PER_BLOCK,
        ByteBuffersDataOutput.DEFAULT_MAX_BITS_PER_BLOCK, 
        reuser);

    // Add some random data first.
    long genSeed = randomLong();
    int addCount = randomIntBetween(1000, 5000);
    addRandomData(o, new Random(genSeed), addCount);
    byte[] data = o.toArray();

    // Use the same sequence over reused instance.
    final int expectedAllocationCount = allocations.get();
    o.reset(reuser::reuse);
    addRandomData(o, new Random(genSeed), addCount);

    assertEquals(expectedAllocationCount, allocations.get());
    assertArrayEquals(data, o.toArray());
  }

  @Test
  public void testConstructorWithExpectedSize() {
    {
      ByteBuffersDataOutput o = new ByteBuffersDataOutput(0);
      o.writeByte((byte) 0);
      assertEquals(1 << ByteBuffersDataOutput.DEFAULT_MIN_BITS_PER_BLOCK, o.toBufferList().get(0).capacity());
    }

    {
      long MB = 1024 * 1024;
      long expectedSize = randomLongBetween(MB, MB * 1024);
      ByteBuffersDataOutput o = new ByteBuffersDataOutput(expectedSize);
      o.writeByte((byte) 0);
      int cap = o.toBufferList().get(0).capacity();
      assertTrue((cap >> 1) * ByteBuffersDataOutput.MAX_BLOCKS_BEFORE_BLOCK_EXPANSION < expectedSize);
      assertTrue("cap=" + cap + ", exp=" + expectedSize,
          (cap) * ByteBuffersDataOutput.MAX_BLOCKS_BEFORE_BLOCK_EXPANSION >= expectedSize);
    }
  }

  @Test
  public void testSanity() {
    ByteBuffersDataOutput o = newInstance();
    assertEquals(0, o.size());
    assertEquals(0, o.toArray().length);
    assertEquals(0, o.ramBytesUsed());

    o.writeByte((byte) 1);
    assertEquals(1, o.size());
    assertTrue(o.ramBytesUsed() > 0);
    assertArrayEquals(new byte [] { 1 }, o.toArray());

    o.writeBytes(new byte [] {2, 3, 4}, 3);
    assertEquals(4, o.size());
    assertArrayEquals(new byte [] { 1, 2, 3, 4 }, o.toArray());    
  }

  @Test
  public void testWriteByteBuffer() {
    ByteBuffersDataOutput o = new ByteBuffersDataOutput();
    byte[] bytes = randomBytesOfLength(1024 * 8 + 10);
    ByteBuffer src = ByteBuffer.wrap(bytes);
    int offset = randomIntBetween(0, 100);
    int len = bytes.length - offset;
    src.position(offset);
    src.limit(offset + len);
    o.writeBytes(src);
    assertEquals(len, o.size());
    Assert.assertArrayEquals(Arrays.copyOfRange(bytes, offset, offset + len), o.toArray());
  }

  @Test
  public void testLargeArrayAdd() {
    ByteBuffersDataOutput o = new ByteBuffersDataOutput();
    int MB = 1024 * 1024;
    byte [] bytes = randomBytesOfLength(5 * MB, 15 * MB);
    int offset = randomIntBetween(0, 100);
    int len = bytes.length - offset;
    o.writeBytes(bytes, offset, len);
    assertEquals(len, o.size());
    Assert.assertArrayEquals(Arrays.copyOfRange(bytes, offset, offset + len), o.toArray());
  }
}
