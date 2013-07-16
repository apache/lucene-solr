package org.apache.solr.store.blockcache;

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

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class BlockCacheTest extends LuceneTestCase {
  @Test
  public void testBlockCache() {
    int blocksInTest = 2000000;
    int blockSize = 1024;
    
    int slabSize = blockSize * 4096;
    long totalMemory = 2 * slabSize;
    
    BlockCache blockCache = new BlockCache(new Metrics(new Configuration()), true,totalMemory,slabSize,blockSize);
    byte[] buffer = new byte[1024];
    Random random = random();
    byte[] newData = new byte[blockSize];
    AtomicLong hitsInCache = new AtomicLong();
    AtomicLong missesInCache = new AtomicLong();
    long storeTime = 0;
    long fetchTime = 0;
    int passes = 10000;

    BlockCacheKey blockCacheKey = new BlockCacheKey();

    for (int j = 0; j < passes; j++) {
      long block = random.nextInt(blocksInTest);
      int file = 0;
      blockCacheKey.setBlock(block);
      blockCacheKey.setFile(file);

      if (blockCache.fetch(blockCacheKey, buffer)) {
        hitsInCache.incrementAndGet();
      } else {
        missesInCache.incrementAndGet();
      }

      byte[] testData = testData(random, blockSize, newData);
      long t1 = System.nanoTime();
      blockCache.store(blockCacheKey, 0, testData, 0, blockSize);
      storeTime += (System.nanoTime() - t1);

      long t3 = System.nanoTime();
      if (blockCache.fetch(blockCacheKey, buffer)) {
        fetchTime += (System.nanoTime() - t3);
        assertTrue(Arrays.equals(testData, buffer));
      }
    }
    System.out.println("Cache Hits    = " + hitsInCache.get());
    System.out.println("Cache Misses  = " + missesInCache.get());
    System.out.println("Store         = " + (storeTime / (double) passes) / 1000000.0);
    System.out.println("Fetch         = " + (fetchTime / (double) passes) / 1000000.0);
    System.out.println("# of Elements = " + blockCache.getSize());
  }

  /**
   * Verify checking of buffer size limits against the cached block size.
   */
  @Test
  public void testLongBuffer() {
    Random random = random();
    int blockSize = BlockCache._32K;
    int slabSize = blockSize * 1024;
    long totalMemory = 2 * slabSize;

    BlockCache blockCache = new BlockCache(new Metrics(new Configuration()),
        true, totalMemory, slabSize);
    BlockCacheKey blockCacheKey = new BlockCacheKey();
    blockCacheKey.setBlock(0);
    blockCacheKey.setFile(0);
    byte[] newData = new byte[blockSize*3];
    byte[] testData = testData(random, blockSize, newData);

    assertTrue(blockCache.store(blockCacheKey, 0, testData, 0, blockSize));
    assertTrue(blockCache.store(blockCacheKey, 0, testData, blockSize, blockSize));
    assertTrue(blockCache.store(blockCacheKey, 0, testData, blockSize*2, blockSize));

    assertTrue(blockCache.store(blockCacheKey, 1, testData, 0, blockSize - 1));
    assertTrue(blockCache.store(blockCacheKey, 1, testData, blockSize, blockSize - 1));
    assertTrue(blockCache.store(blockCacheKey, 1, testData, blockSize*2, blockSize - 1));
  }

  private static byte[] testData(Random random, int size, byte[] buf) {
    random.nextBytes(buf);
    return buf;
  }
}
