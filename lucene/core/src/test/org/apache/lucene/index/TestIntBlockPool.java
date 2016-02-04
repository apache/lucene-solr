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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntBlockPool;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * tests basic {@link IntBlockPool} functionality
 */
public class TestIntBlockPool extends LuceneTestCase {
  
  public void testSingleWriterReader() {
    Counter bytesUsed = Counter.newCounter();
    IntBlockPool pool = new IntBlockPool(new ByteTrackingAllocator(bytesUsed));
    
    for (int j = 0; j < 2; j++) {
      IntBlockPool.SliceWriter writer = new IntBlockPool.SliceWriter(pool);
      int start = writer.startNewSlice();
      int num = atLeast(100);
      for (int i = 0; i < num; i++) {
        writer.writeInt(i);
      }
      
      int upto = writer.getCurrentOffset();
      IntBlockPool.SliceReader reader = new IntBlockPool.SliceReader(pool);
      reader.reset(start, upto);
      for (int i = 0; i < num; i++) {
        assertEquals(i, reader.readInt());
      }
      assertTrue(reader.endOfSlice());
      if (random().nextBoolean()) {
        pool.reset(true, false);
        assertEquals(0, bytesUsed.get());
      } else {
        pool.reset(true, true);
        assertEquals(IntBlockPool.INT_BLOCK_SIZE
            * RamUsageEstimator.NUM_BYTES_INT, bytesUsed.get());
      }
    }
  }
  
  public void testMultipleWriterReader() {
    Counter bytesUsed = Counter.newCounter();
    IntBlockPool pool = new IntBlockPool(new ByteTrackingAllocator(bytesUsed));
    for (int j = 0; j < 2; j++) {
      List<StartEndAndValues> holders = new ArrayList<>();
      int num = atLeast(4);
      for (int i = 0; i < num; i++) {
        holders.add(new StartEndAndValues(random().nextInt(1000)));
      }
      IntBlockPool.SliceWriter writer = new IntBlockPool.SliceWriter(pool);
      IntBlockPool.SliceReader reader = new IntBlockPool.SliceReader(pool);
      
      int numValuesToWrite = atLeast(10000);
      for (int i = 0; i < numValuesToWrite; i++) {
        StartEndAndValues values = holders
            .get(random().nextInt(holders.size()));
        if (values.valueCount == 0) {
          values.start = writer.startNewSlice();
        } else {
          writer.reset(values.end);
        }
        writer.writeInt(values.nextValue());
        values.end = writer.getCurrentOffset();
        if (random().nextInt(5) == 0) {
          // pick one and reader the ints
          assertReader(reader, holders.get(random().nextInt(holders.size())));
        }
      }
      
      while (!holders.isEmpty()) {
        StartEndAndValues values = holders.remove(random().nextInt(
            holders.size()));
        assertReader(reader, values);
      }
      if (random().nextBoolean()) {
        pool.reset(true, false);
        assertEquals(0, bytesUsed.get());
      } else {
        pool.reset(true, true);
        assertEquals(IntBlockPool.INT_BLOCK_SIZE
            * RamUsageEstimator.NUM_BYTES_INT, bytesUsed.get());
      }
    }
  }
  
  private static class ByteTrackingAllocator extends IntBlockPool.Allocator {
    private final Counter bytesUsed;
    
    public ByteTrackingAllocator(Counter bytesUsed) {
      this(IntBlockPool.INT_BLOCK_SIZE, bytesUsed);
    }
    
    public ByteTrackingAllocator(int blockSize, Counter bytesUsed) {
      super(blockSize);
      this.bytesUsed = bytesUsed;
    }
    
    @Override
    public int[] getIntBlock() {
      bytesUsed.addAndGet(blockSize * RamUsageEstimator.NUM_BYTES_INT);
      return new int[blockSize];
    }
    
    @Override
    public void recycleIntBlocks(int[][] blocks, int start, int end) {
      bytesUsed
          .addAndGet(-((end - start) * blockSize * RamUsageEstimator.NUM_BYTES_INT));
    }
    
  }
  
  private void assertReader(IntBlockPool.SliceReader reader,
      StartEndAndValues values) {
    reader.reset(values.start, values.end);
    for (int i = 0; i < values.valueCount; i++) {
      assertEquals(values.valueOffset + i, reader.readInt());
    }
    assertTrue(reader.endOfSlice());
  }
  
  private static class StartEndAndValues {
    int valueOffset;
    int valueCount;
    int start;
    int end;
    
    public StartEndAndValues(int valueOffset) {
      this.valueOffset = valueOffset;
    }
    
    public int nextValue() {
      return valueOffset + valueCount++;
    }
    
  }
  
}
