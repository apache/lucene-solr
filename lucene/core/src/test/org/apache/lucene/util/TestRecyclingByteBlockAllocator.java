package org.apache.lucene.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Test;

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

/**
 * Testcase for {@link RecyclingByteBlockAllocator}
 */
public class TestRecyclingByteBlockAllocator extends LuceneTestCase {

  /**
   */
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  private RecyclingByteBlockAllocator newAllocator() {
    return new RecyclingByteBlockAllocator(1 << (2 + random().nextInt(15)),
        random().nextInt(97), new AtomicLong());
  }

  @Test
  public void testAllocate() {
    RecyclingByteBlockAllocator allocator = newAllocator();
    HashSet<byte[]> set = new HashSet<byte[]>();
    byte[] block = allocator.getByteBlock();
    set.add(block);
    assertNotNull(block);
    final int size = block.length;

    int num = atLeast(97);
    for (int i = 0; i < num; i++) {
      block = allocator.getByteBlock();
      assertNotNull(block);
      assertEquals(size, block.length);
      assertTrue("block is returned twice", set.add(block));
      assertEquals(size * (i + 2), allocator.bytesUsed()); // zero based + 1
      assertEquals(0, allocator.numBufferedBlocks());
    }
  }

  @Test
  public void testAllocateAndRecycle() {
    RecyclingByteBlockAllocator allocator = newAllocator();
    HashSet<byte[]> allocated = new HashSet<byte[]>();

    byte[] block = allocator.getByteBlock();
    allocated.add(block);
    assertNotNull(block);
    final int size = block.length;

    int numIters = atLeast(97);
    for (int i = 0; i < numIters; i++) {
      int num = 1 + random().nextInt(39);
      for (int j = 0; j < num; j++) {
        block = allocator.getByteBlock();
        assertNotNull(block);
        assertEquals(size, block.length);
        assertTrue("block is returned twice", allocated.add(block));
        assertEquals(size * (allocated.size() +  allocator.numBufferedBlocks()), allocator
            .bytesUsed());
      }
      byte[][] array = allocated.toArray(new byte[0][]);
      int begin = random().nextInt(array.length);
      int end = begin + random().nextInt(array.length - begin);
      List<byte[]> selected = new ArrayList<byte[]>();
      for (int j = begin; j < end; j++) {
        selected.add(array[j]);
      }
      allocator.recycleByteBlocks(array, begin, end);
      for (int j = begin; j < end; j++) {
        assertNull(array[j]);
        byte[] b = selected.remove(0);
        assertTrue(allocated.remove(b));
      }
    }
  }

  @Test
  public void testAllocateAndFree() {
    RecyclingByteBlockAllocator allocator = newAllocator();
    HashSet<byte[]> allocated = new HashSet<byte[]>();
    int freeButAllocated = 0;
    byte[] block = allocator.getByteBlock();
    allocated.add(block);
    assertNotNull(block);
    final int size = block.length;

    int numIters = atLeast(97);
    for (int i = 0; i < numIters; i++) {
      int num = 1 + random().nextInt(39);
      for (int j = 0; j < num; j++) {
        block = allocator.getByteBlock();
        freeButAllocated = Math.max(0, freeButAllocated - 1);
        assertNotNull(block);
        assertEquals(size, block.length);
        assertTrue("block is returned twice", allocated.add(block));
        assertEquals(size * (allocated.size() + allocator.numBufferedBlocks()),
            allocator.bytesUsed());
      }

      byte[][] array = allocated.toArray(new byte[0][]);
      int begin = random().nextInt(array.length);
      int end = begin + random().nextInt(array.length - begin);
      for (int j = begin; j < end; j++) {
        byte[] b = array[j];
        assertTrue(allocated.remove(b));
      }
      allocator.recycleByteBlocks(array, begin, end);
      for (int j = begin; j < end; j++) {
        assertNull(array[j]);
      }
      // randomly free blocks
      int numFreeBlocks = allocator.numBufferedBlocks();
      int freeBlocks = allocator.freeBlocks(random().nextInt(7 + allocator
          .maxBufferedBlocks()));
      assertEquals(allocator.numBufferedBlocks(), numFreeBlocks - freeBlocks);
    }
  }
}