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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockDirectoryCache implements Cache {
  private BlockCache blockCache;
  private AtomicInteger counter = new AtomicInteger();
  private Map<String,Integer> names = new ConcurrentHashMap<String,Integer>();
  private Metrics metrics;
  
  public BlockDirectoryCache(BlockCache blockCache, Metrics metrics) {
    this.blockCache = blockCache;
    this.metrics = metrics;
  }
  
  @Override
  public void delete(String name) {
    names.remove(name);
  }
  
  @Override
  public void update(String name, long blockId, int blockOffset, byte[] buffer,
      int offset, int length) {
    Integer file = names.get(name);
    if (file == null) {
      file = counter.incrementAndGet();
      names.put(name, file);
    }
    BlockCacheKey blockCacheKey = new BlockCacheKey();
    blockCacheKey.setBlock(blockId);
    blockCacheKey.setFile(file);
    blockCache.store(blockCacheKey, blockOffset, buffer, offset, length);
  }
  
  @Override
  public boolean fetch(String name, long blockId, int blockOffset, byte[] b,
      int off, int lengthToReadInBlock) {
    Integer file = names.get(name);
    if (file == null) {
      return false;
    }
    BlockCacheKey blockCacheKey = new BlockCacheKey();
    blockCacheKey.setBlock(blockId);
    blockCacheKey.setFile(file);
    boolean fetch = blockCache.fetch(blockCacheKey, b, blockOffset, off,
        lengthToReadInBlock);
    if (fetch) {
      metrics.blockCacheHit.incrementAndGet();
    } else {
      metrics.blockCacheMiss.incrementAndGet();
    }
    return fetch;
  }
  
  @Override
  public long size() {
    return blockCache.getSize();
  }
  
  @Override
  public void renameCacheFile(String source, String dest) {
    Integer file = names.remove(source);
    // possible if the file is empty
    if (file != null) {
      names.put(dest, file);
    }
  }
}
