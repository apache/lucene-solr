package org.apache.lucene.facet.taxonomy.writercache.cl2o;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.writercache.TaxonomyWriterCache;

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

/**
 * {@link TaxonomyWriterCache} using {@link CompactLabelToOrdinal}. Although
 * called cache, it maintains in memory all the mappings from category to
 * ordinal, relying on that {@link CompactLabelToOrdinal} is an efficient
 * mapping for this purpose.
 * 
 * @lucene.experimental
 */
public class Cl2oTaxonomyWriterCache implements TaxonomyWriterCache {  

  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final int initialCapcity, numHashArrays;
  private final float loadFactor;
  
  private volatile CompactLabelToOrdinal cache;

  public Cl2oTaxonomyWriterCache(int initialCapcity, float loadFactor, int numHashArrays) {
    this.cache = new CompactLabelToOrdinal(initialCapcity, loadFactor, numHashArrays);
    this.initialCapcity = initialCapcity;
    this.numHashArrays = numHashArrays;
    this.loadFactor = loadFactor;
  }

  @Override
  public void clear() {
    lock.writeLock().lock();
    try {
      cache = new CompactLabelToOrdinal(initialCapcity, loadFactor, numHashArrays);
    } finally {
      lock.writeLock().unlock();
    }
  }
  
  @Override
  public synchronized void close() {
    cache = null;
  }

  @Override
  public boolean isFull() {
    // This cache is never full
    return false;
  }

  @Override
  public int get(CategoryPath categoryPath) {
    lock.readLock().lock();
    try {
      return cache.getOrdinal(categoryPath);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean put(CategoryPath categoryPath, int ordinal) {
    lock.writeLock().lock();
    try {
      cache.addLabel(categoryPath, ordinal);
      // Tell the caller we didn't clear part of the cache, so it doesn't
      // have to flush its on-disk index now
      return false;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** Returns the number of bytes in memory used by this object. */
  public int getMemoryUsage() {
    return cache == null ? 0 : cache.getMemoryUsage();
  }

}
