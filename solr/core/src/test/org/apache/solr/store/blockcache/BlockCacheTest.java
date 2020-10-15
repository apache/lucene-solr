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

import com.github.benmanes.caffeine.cache.*;
import org.apache.solr.SolrTestCase;

import org.junit.Test;

public class BlockCacheTest extends SolrTestCase {

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
        assertTrue("buffer content differs", Arrays.equals(testData, buffer));
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
    return (byte) ((((int) pos) * (int) (2654435761L)) >> 24);

    // just the lower bits of the block number, to aid in debugging...
    // return (byte)(pos>>10);
  }

  @Test
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

    final int nThreads = 64;
    final int nReads = 1000000;
    final int readsPerThread = nReads / nThreads;
    final int readLastBlockOdds = 10; // odds (1 in N) of the next block operation being on the same block as the previous operation... helps flush concurrency issues
    final int showErrors = 50; // show first 50 validation failures

    final BlockCache blockCache = new BlockCache(new Metrics(), true, totalMemory, slabSize, blockSize);

    final AtomicBoolean failed = new AtomicBoolean(false);
    final AtomicLong hitsInCache = new AtomicLong();
    final AtomicLong missesInCache = new AtomicLong();
    final AtomicLong storeFails = new AtomicLong();
    final AtomicLong lastBlock = new AtomicLong();
    final AtomicLong validateFails = new AtomicLong(0);

    final int file = 0;


    Thread[] threads = new Thread[nThreads];
    for (int i = 0; i < threads.length; i++) {
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
          for (int i = 0; i < iter; i++) {
            test();
          }
        }

        public void test() {
          long block = r.nextInt(blocksInTest);
          if (r.nextInt(readLastBlockOdds) == 0)
            block = lastBlock.get();  // some percent of the time, try to read the last block another thread was just reading/writing
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
                if (validateFails.incrementAndGet() <= showErrors)
                  System.out.println("ERROR: read was " + "block=" + block + " blockOffset=" + blockOffset + " len=" + len + " globalPos=" + globalPos + " localReadOffset=" + i + " got=" + buffer[i] + " expected=" + getByte(globalPos));
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

    assertFalse("cached bytes differ from expected", failed.get());
  }


  static class Val {
    long key;
    AtomicBoolean live = new AtomicBoolean(true);
  }

  // Sanity test the underlying concurrent map that BlockCache is using, in the same way that we use it.
  @Test
  public void testCacheConcurrent() throws Exception {
    Random rnd = random();

    // TODO: introduce more randomness in cache size, hit rate, etc
    final int blocksInTest = 400;
    final int maxEntries = blocksInTest / 2;

    final int nThreads = 64;
    final int nReads = 1000000;
    final int readsPerThread = nReads / nThreads;
    final int readLastBlockOdds = 10; // odds (1 in N) of the next block operation being on the same block as the previous operation... helps flush concurrency issues
    final int updateAnywayOdds = 3; // sometimes insert a new entry for the key even if one was found
    final int invalidateOdds = 20; // sometimes invalidate an entry

    final AtomicLong hits = new AtomicLong();
    final AtomicLong removals = new AtomicLong();
    final AtomicLong inserts = new AtomicLong();

    RemovalListener<Long, Val> listener = (k, v, removalCause) -> {
      removals.incrementAndGet();
      if (v == null) {
        if (removalCause != RemovalCause.COLLECTED) {
          throw new RuntimeException("Null value for key " + k + ", removalCause=" + removalCause);
        } else {
          return;
        }
      }
      assertEquals("cache key differs from value's key", k, (Long) v.key);
      if (!v.live.compareAndSet(true, false)) {
        throw new RuntimeException("listener called more than once! k=" + k + " v=" + v + " removalCause=" + removalCause);
        // return;  // use this variant if listeners may be called more than once
      }
    };


    com.github.benmanes.caffeine.cache.Cache<Long, Val> cache = Caffeine.newBuilder()
        .removalListener(listener)
        .maximumSize(maxEntries)
        .executor(Runnable::run)
        .build();

    final AtomicBoolean failed = new AtomicBoolean(false);
    final AtomicLong lastBlock = new AtomicLong();
    final AtomicLong maxObservedSize = new AtomicLong();

    Thread[] threads = new Thread[nThreads];
    for (int i = 0; i < threads.length; i++) {
      final long seed = rnd.nextLong();

      threads[i] = new Thread() {
        Random r;

        @Override
        public void run() {
          try {
            r = new Random(seed);
            test(readsPerThread);
          } catch (Throwable e) {
            failed.set(true);
            e.printStackTrace();
          }
        }

        public void test(int iter) {
          for (int i = 0; i < iter; i++) {
            test();
          }
        }

        boolean odds(int odds) {
          return odds > 0 && r.nextInt(odds) == 0;
        }

        long getBlock() {
          long block;
          if (odds(readLastBlockOdds)) {
            block = lastBlock.get();  // some percent of the time, try to read the last block another thread was just reading/writing
          } else {
            block = r.nextInt(blocksInTest);
            lastBlock.set(block);
          }
          return block;
        }

        public void test() {
          Long k = getBlock();

          if (odds(invalidateOdds)) {
            // This tests that invalidate always ends up calling the removal listener exactly once
            // even if the entry may be in the process of concurrent removal in a different thread.
            // This also inadvertently tests concurrently inserting, getting, and invalidating the same key, which we don't need in Solr's BlockCache.
            cache.invalidate(k);
          }

          Val v = cache.getIfPresent(k);
          if (v != null) {
            hits.incrementAndGet();
            assertEquals("cache key differs from value's key", k, (Long) v.key);
          }

          if (v == null || odds(updateAnywayOdds)) {
            v = new Val();
            v.key = k;
            cache.put(k, v);
            inserts.incrementAndGet();
          }

          long sz = cache.estimatedSize();
          if (sz > maxObservedSize.get()) maxObservedSize.set(sz);  // race condition here, but an estimate is OK

        }
      };
    }


    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }


    // Thread.sleep(1000); // need to wait if executor is used for listener?
    long cacheSize = cache.estimatedSize();
    System.out.println("Done! # of Elements = " + cacheSize + " inserts=" + inserts.get() + " removals=" + removals.get() + " hits=" + hits.get() + " maxObservedSize=" + maxObservedSize);
    assertEquals("cache size different from (inserts - removal)", cacheSize,  inserts.get() - removals.get());
    assertFalse(failed.get());
  }


}
