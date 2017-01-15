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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;

/**
 * @lucene.experimental
 */
public class BlockCache {
  
  public static final int _128M = 134217728;
  public static final int _32K = 32768;
  private final Cache<BlockCacheKey,BlockCacheLocation> cache;
  private final ByteBuffer[] banks;
  private final BlockLocks[] locks;
  private final AtomicInteger[] lockCounters;
  private final int blockSize;
  private final int numberOfBlocksPerBank;
  private final int maxEntries;
  private final Metrics metrics;
  private volatile OnRelease onRelease;
  
  public static interface OnRelease {
    public void release(BlockCacheKey blockCacheKey);
  }
  
  public BlockCache(Metrics metrics, boolean directAllocation, long totalMemory) {
    this(metrics, directAllocation, totalMemory, _128M);
  }
  
  public BlockCache(Metrics metrics, boolean directAllocation,
      long totalMemory, int slabSize) {
    this(metrics, directAllocation, totalMemory, slabSize, _32K);
  }
  
  public BlockCache(Metrics metrics, boolean directAllocation,
      long totalMemory, int slabSize, int blockSize) {
    this.metrics = metrics;
    numberOfBlocksPerBank = slabSize / blockSize;
    int numberOfBanks = (int) (totalMemory / slabSize);
    
    banks = new ByteBuffer[numberOfBanks];
    locks = new BlockLocks[numberOfBanks];
    lockCounters = new AtomicInteger[numberOfBanks];
    maxEntries = (numberOfBlocksPerBank * numberOfBanks) - 1;
    for (int i = 0; i < numberOfBanks; i++) {
      if (directAllocation) {
        banks[i] = ByteBuffer.allocateDirect(numberOfBlocksPerBank * blockSize);
      } else {
        banks[i] = ByteBuffer.allocate(numberOfBlocksPerBank * blockSize);
      }
      locks[i] = new BlockLocks(numberOfBlocksPerBank);
      lockCounters[i] = new AtomicInteger();
    }

    RemovalListener<BlockCacheKey,BlockCacheLocation> listener = 
        notification -> releaseLocation(notification.getKey(), notification.getValue());
    cache = Caffeine.newBuilder()
        .removalListener(listener)
        .maximumSize(maxEntries)
        .build();
    this.blockSize = blockSize;
  }
  
  public void release(BlockCacheKey key) {
    cache.invalidate(key);
  }
  
  private void releaseLocation(BlockCacheKey blockCacheKey, BlockCacheLocation location) {
    if (location == null) {
      return;
    }
    int bankId = location.getBankId();
    int block = location.getBlock();
    location.setRemoved(true);
    locks[bankId].clear(block);
    lockCounters[bankId].decrementAndGet();
    if (onRelease != null) {
      onRelease.release(blockCacheKey);
    }
    metrics.blockCacheEviction.incrementAndGet();
    metrics.blockCacheSize.decrementAndGet();
  }
  
  public boolean store(BlockCacheKey blockCacheKey, int blockOffset,
      byte[] data, int offset, int length) {
    if (length + blockOffset > blockSize) {
      throw new RuntimeException("Buffer size exceeded, expecting max ["
          + blockSize + "] got length [" + length + "] with blockOffset ["
          + blockOffset + "]");
    }
    BlockCacheLocation location = cache.getIfPresent(blockCacheKey);
    boolean newLocation = false;
    if (location == null) {
      newLocation = true;
      location = new BlockCacheLocation();
      if (!findEmptyLocation(location)) {
        return false;
      }
    }
    if (location.isRemoved()) {
      return false;
    }
    int bankId = location.getBankId();
    int bankOffset = location.getBlock() * blockSize;
    ByteBuffer bank = getBank(bankId);
    bank.position(bankOffset + blockOffset);
    bank.put(data, offset, length);
    if (newLocation) {
      cache.put(blockCacheKey.clone(), location);
      metrics.blockCacheSize.incrementAndGet();
    }
    return true;
  }
  
  public boolean fetch(BlockCacheKey blockCacheKey, byte[] buffer,
      int blockOffset, int off, int length) {
    BlockCacheLocation location = cache.getIfPresent(blockCacheKey);
    if (location == null) {
      return false;
    }
    if (location.isRemoved()) {
      return false;
    }
    int bankId = location.getBankId();
    int offset = location.getBlock() * blockSize;
    location.touch();
    ByteBuffer bank = getBank(bankId);
    bank.position(offset + blockOffset);
    bank.get(buffer, off, length);
    return true;
  }
  
  public boolean fetch(BlockCacheKey blockCacheKey, byte[] buffer) {
    checkLength(buffer);
    return fetch(blockCacheKey, buffer, 0, 0, blockSize);
  }
  
  private boolean findEmptyLocation(BlockCacheLocation location) {
    // This is a tight loop that will try and find a location to
    // place the block before giving up
    for (int j = 0; j < 10; j++) {
      OUTER: for (int bankId = 0; bankId < banks.length; bankId++) {
        AtomicInteger bitSetCounter = lockCounters[bankId];
        BlockLocks bitSet = locks[bankId];
        if (bitSetCounter.get() == numberOfBlocksPerBank) {
          // if bitset is full
          continue OUTER;
        }
        // this check needs to spin, if a lock was attempted but not obtained
        // the rest of the bank should not be skipped
        int bit = bitSet.nextClearBit(0);
        INNER: while (bit != -1) {
          if (bit >= numberOfBlocksPerBank) {
            // bit set is full
            continue OUTER;
          }
          if (!bitSet.set(bit)) {
            // lock was not obtained
            // this restarts at 0 because another block could have been unlocked
            // while this was executing
            bit = bitSet.nextClearBit(0);
            continue INNER;
          } else {
            // lock obtained
            location.setBankId(bankId);
            location.setBlock(bit);
            bitSetCounter.incrementAndGet();
            return true;
          }
        }
      }
    }
    return false;
  }
  
  private void checkLength(byte[] buffer) {
    if (buffer.length != blockSize) {
      throw new RuntimeException("Buffer wrong size, expecting [" + blockSize
          + "] got [" + buffer.length + "]");
    }
  }
  
  private ByteBuffer getBank(int bankId) {
    return banks[bankId].duplicate();
  }
  
  public int getSize() {
    return cache.asMap().size();
  }

  void setOnRelease(OnRelease onRelease) {
    this.onRelease = onRelease;
  }
}
