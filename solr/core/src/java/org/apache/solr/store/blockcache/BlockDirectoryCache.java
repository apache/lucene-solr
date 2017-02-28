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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.store.blockcache.BlockCache.OnRelease;

import com.github.benmanes.caffeine.cache.Caffeine;


/**
 * @lucene.experimental
 */
public class BlockDirectoryCache implements Cache {
  private final BlockCache blockCache;
  private final AtomicInteger counter = new AtomicInteger();
  private final com.github.benmanes.caffeine.cache.Cache<String,Integer> names;
  private Set<BlockCacheKey> keysToRelease;
  private final String path;
  private final Metrics metrics;
  
  public BlockDirectoryCache(BlockCache blockCache, String path, Metrics metrics) {
    this(blockCache, path, metrics, false);
  }
  
  public BlockDirectoryCache(BlockCache blockCache, String path, Metrics metrics, boolean releaseBlocks) {
    this.blockCache = blockCache;
    this.path = path;
    this.metrics = metrics;
        
    names = Caffeine.newBuilder().maximumSize(50000).build();
    
    if (releaseBlocks) {
      keysToRelease = Collections.newSetFromMap(new ConcurrentHashMap<BlockCacheKey,Boolean>(1024, 0.75f, 512));
      blockCache.setOnRelease(new OnRelease() {
        
        @Override
        public void release(BlockCacheKey key) {
          keysToRelease.remove(key);
        }
      });
    }
  }

  /**
   * Expert: mostly for tests
   * 
   * @lucene.experimental
   */
  public BlockCache getBlockCache() {
    return blockCache;
  }
  
  @Override
  public void delete(String name) {
    names.invalidate(name);
  }
  
  @Override
  public void update(String name, long blockId, int blockOffset, byte[] buffer,
      int offset, int length) {
    Integer file = names.getIfPresent(name);
    if (file == null) {
      file = counter.incrementAndGet();
      names.put(name, file);
    }
    BlockCacheKey blockCacheKey = new BlockCacheKey();
    blockCacheKey.setPath(path);
    blockCacheKey.setBlock(blockId);
    blockCacheKey.setFile(file);
    if (blockCache.store(blockCacheKey, blockOffset, buffer, offset, length) && keysToRelease != null) {
      keysToRelease.add(blockCacheKey);
    }
  }
  
  @Override
  public boolean fetch(String name, long blockId, int blockOffset, byte[] b,
      int off, int lengthToReadInBlock) {
    Integer file = names.getIfPresent(name);
    if (file == null) {
      return false;
    }
    BlockCacheKey blockCacheKey = new BlockCacheKey();
    blockCacheKey.setPath(path);
    blockCacheKey.setBlock(blockId);
    blockCacheKey.setFile(file);
    boolean fetch = blockCache.fetch(blockCacheKey, b, blockOffset, off,
        lengthToReadInBlock);
    return fetch;
  }
  
  @Override
  public long size() {
    return blockCache.getSize();
  }
  
  @Override
  public void renameCacheFile(String source, String dest) {
    Integer file = names.getIfPresent(source);
    names.invalidate(source);
    // possible if the file is empty
    if (file != null) {
      names.put(dest, file);
    }
  }

  @Override
  public void releaseResources() {
    if (keysToRelease != null) {
      for (BlockCacheKey key : keysToRelease) {
        blockCache.release(key);
      }
    }
  }
}
