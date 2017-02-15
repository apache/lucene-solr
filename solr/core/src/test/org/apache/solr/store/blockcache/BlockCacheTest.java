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
package org.apache.solr.store.blockcache;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class BlockCacheTest extends LuceneTestCase {

  @Test
  public void testBlockCache() {
    int blocksInTest = 2000000;
    int blockSize = 1024;
    
    int slabSize = blockSize * 4096;
    long totalMemory = 2 * slabSize;
    
    BlockCache blockCache = new BlockCache(new Metrics(), true, totalMemory, slabSize, blockSize);
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
      blockCacheKey.setPath("/");

      if (blockCache.fetch(blockCacheKey, buffer)) {
        hitsInCache.incrementAndGet();
      } else {
        missesInCache.incrementAndGet();
      }

      byte[] testData = testData(random, blockSize, newData);
      long t1 = System.nanoTime();
      boolean success = blockCache.store(blockCacheKey, 0, testData, 0, blockSize);
      storeTime += (System.nanoTime() - t1);
      if (!success) continue;  // for now, updating existing blocks is not supported... see SOLR-10121

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

  private static byte[] testData(Random random, int size, byte[] buf) {
    random.nextBytes(buf);
    return buf;
  }

  // given a position, return the appropriate byte.
  // always returns the same thing so we don't actually have to store the bytes redundantly to check them.
  private static byte getByte(long pos) {
    // knuth multiplicative hash method, then take top 8 bits
    return (byte) ((((int)pos) * (int)(2654435761L)) >> 24);

    // just the lower bits of the block number, to aid in debugging...
    // return (byte)(pos>>10);
  }

  @Test
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-10121")
  public void testBlockCacheConcurrent() throws Exception {
    Random rnd = random();

    final int blocksInTest = 400;  // pick something bigger than 256, since that would lead to a slab size of 64 blocks and the bitset locks would consist of a single word.
    final int blockSize = 64;
    final int slabSize = blocksInTest * blockSize / 4;
    final long totalMemory = 2 * slabSize;  // 2 slabs of memory, so only half of what is needed for all blocks

    /***
     final int blocksInTest = 16384;  // pick something bigger than 256, since that would lead to a slab size of 64 blocks and the bitset locks would consist of a single word.
     final int blockSize = 1024;
     final int slabSize = blocksInTest * blockSize / 4;
     final long totalMemory = 2 * slabSize;  // 2 slabs of memory, so only half of what is needed for all blocks
     ***/

    final int nThreads=64;
    final int nReads=1000000;
    final int readsPerThread=nReads/nThreads;
    final int readLastBlockOdds=10; // odds (1 in N) of the next block operation being on the same block as the previous operation... helps flush concurrency issues
    final int showErrors=50; // show first 50 validation failures

    final BlockCache blockCache = new BlockCache(new Metrics(), true, totalMemory, slabSize, blockSize);

    final AtomicBoolean failed = new AtomicBoolean(false);
    final AtomicLong hitsInCache = new AtomicLong();
    final AtomicLong missesInCache = new AtomicLong();
    final AtomicLong storeFails = new AtomicLong();
    final AtomicLong lastBlock = new AtomicLong();
    final AtomicLong validateFails = new AtomicLong(0);

    final int file = 0;


    Thread[] threads = new Thread[nThreads];
    for (int i=0; i<threads.length; i++) {
      final int threadnum = i;
      final long seed = rnd.nextLong();

      threads[i] = new Thread() {
        Random r;
        BlockCacheKey blockCacheKey;
        byte[] buffer = new byte[blockSize];

        @Override
        public void run() {
          try {
            r = new Random(seed);
            blockCacheKey = new BlockCacheKey();
            blockCacheKey.setFile(file);
            blockCacheKey.setPath("/foo.txt");

            test(readsPerThread);

          } catch (Throwable e) {
            failed.set(true);
            e.printStackTrace();
          }
        }

        public void test(int iter) {
          for (int i=0; i<iter; i++) {
            test();
          }
        }

        public void test() {
          long block = r.nextInt(blocksInTest);
          if (r.nextInt(readLastBlockOdds) == 0) block = lastBlock.get();  // some percent of the time, try to read the last block another thread was just reading/writing
          lastBlock.set(block);


          int blockOffset = r.nextInt(blockSize);
          long globalOffset = block * blockSize + blockOffset;
          int len = r.nextInt(blockSize - blockOffset) + 1;  // TODO: bias toward smaller reads?

          blockCacheKey.setBlock(block);

          if (blockCache.fetch(blockCacheKey, buffer, blockOffset, 0, len)) {
            hitsInCache.incrementAndGet();
            // validate returned bytes
            for (int i = 0; i < len; i++) {
              long globalPos = globalOffset + i;
              if (buffer[i] != getByte(globalPos)) {
                failed.set(true);
                if (validateFails.incrementAndGet() <= showErrors) System.out.println("ERROR: read was " + "block=" + block + " blockOffset=" + blockOffset + " len=" + len + " globalPos=" + globalPos + " localReadOffset=" + i + " got=" + buffer[i] + " expected=" + getByte(globalPos));
                break;
              }
            }
          } else {
            missesInCache.incrementAndGet();

            // OK, we should "get" the data and then cache the block
            for (int i = 0; i < blockSize; i++) {
              buffer[i] = getByte(block * blockSize + i);
            }
            boolean cached = blockCache.store(blockCacheKey, 0, buffer, 0, blockSize);
            if (!cached) {
              storeFails.incrementAndGet();
            }
          }

        }

      };
    }


    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    System.out.println("# of Elements = " + blockCache.getSize());
    System.out.println("Cache Hits = " + hitsInCache.get());
    System.out.println("Cache Misses = " + missesInCache.get());
    System.out.println("Cache Store Fails = " + storeFails.get());
    System.out.println("Blocks with Errors = " + validateFails.get());

    assertFalse( failed.get() );
  }

}
