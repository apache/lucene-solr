package org.apache.solr.util;
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.solr.common.util.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.ref.WeakReference;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A LFU cache implementation based upon ConcurrentHashMap.
 * <p>
 * This is not a terribly efficient implementation.  The tricks used in the
 * LRU version were not directly usable, perhaps it might be possible to
 * rewrite them with LFU in mind.
 * <p>
 * <b>This API is experimental and subject to change</b>
 *
 * @since solr 1.6
 */
public class ConcurrentLFUCache<K, V> implements Cache<K,V> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ConcurrentHashMap<Object, CacheEntry<K, V>> map;
  private final int upperWaterMark, lowerWaterMark;
  private final ReentrantLock markAndSweepLock = new ReentrantLock(true);
  private boolean isCleaning = false;  // not volatile... piggybacked on other volatile vars
  private final boolean newThreadForCleanup;
  private volatile boolean islive = true;
  private final Stats stats = new Stats();
  @SuppressWarnings("unused")
  private final int acceptableWaterMark;
  private long lowHitCount = 0;  // not volatile, only accessed in the cleaning method
  private final EvictionListener<K, V> evictionListener;
  private CleanupThread cleanupThread;
  private final boolean timeDecay;

  public ConcurrentLFUCache(int upperWaterMark, final int lowerWaterMark, int acceptableSize,
                            int initialSize, boolean runCleanupThread, boolean runNewThreadForCleanup,
                            EvictionListener<K, V> evictionListener, boolean timeDecay) {
    if (upperWaterMark < 1) throw new IllegalArgumentException("upperWaterMark must be > 0");
    if (lowerWaterMark >= upperWaterMark)
      throw new IllegalArgumentException("lowerWaterMark must be  < upperWaterMark");
    map = new ConcurrentHashMap<>(initialSize);
    newThreadForCleanup = runNewThreadForCleanup;
    this.upperWaterMark = upperWaterMark;
    this.lowerWaterMark = lowerWaterMark;
    this.acceptableWaterMark = acceptableSize;
    this.evictionListener = evictionListener;
    this.timeDecay = timeDecay;
    if (runCleanupThread) {
      cleanupThread = new CleanupThread(this);
      cleanupThread.start();
    }
  }

  public ConcurrentLFUCache(int size, int lowerWatermark) {
    this(size, lowerWatermark, (int) Math.floor((lowerWatermark + size) / 2),
        (int) Math.ceil(0.75 * size), false, false, null, true);
  }

  public void setAlive(boolean live) {
    islive = live;
  }

  @Override
  public V get(K key) {
    CacheEntry<K, V> e = map.get(key);
    if (e == null) {
      if (islive) stats.missCounter.incrementAndGet();
      return null;
    }
    if (islive) {
      e.lastAccessed = stats.accessCounter.incrementAndGet();
      e.hits.incrementAndGet();
    }
    return e.value;
  }

  @Override
  public V remove(K key) {
    CacheEntry<K, V> cacheEntry = map.remove(key);
    if (cacheEntry != null) {
      stats.size.decrementAndGet();
      return cacheEntry.value;
    }
    return null;
  }

  @Override
  public V put(K key, V val) {
    if (val == null) return null;
    CacheEntry<K, V> e = new CacheEntry<>(key, val, stats.accessCounter.incrementAndGet());
    CacheEntry<K, V> oldCacheEntry = map.put(key, e);
    int currentSize;
    if (oldCacheEntry == null) {
      currentSize = stats.size.incrementAndGet();
    } else {
      currentSize = stats.size.get();
    }
    if (islive) {
      stats.putCounter.incrementAndGet();
    } else {
      stats.nonLivePutCounter.incrementAndGet();
    }

    // Check if we need to clear out old entries from the cache.
    // isCleaning variable is checked instead of markAndSweepLock.isLocked()
    // for performance because every put invokation will check until
    // the size is back to an acceptable level.
    //
    // There is a race between the check and the call to markAndSweep, but
    // it's unimportant because markAndSweep actually aquires the lock or returns if it can't.
    //
    // Thread safety note: isCleaning read is piggybacked (comes after) other volatile reads
    // in this method.
    if (currentSize > upperWaterMark && !isCleaning) {
      if (newThreadForCleanup) {
        new Thread() {
          @Override
          public void run() {
            markAndSweep();
          }
        }.start();
      } else if (cleanupThread != null) {
        cleanupThread.wakeThread();
      } else {
        markAndSweep();
      }
    }
    return oldCacheEntry == null ? null : oldCacheEntry.value;
  }

  /**
   * Removes items from the cache to bring the size down to the lowerWaterMark.
   */
  private void markAndSweep() {
    if (!markAndSweepLock.tryLock()) return;
    try {
      long lowHitCount = this.lowHitCount;
      isCleaning = true;
      this.lowHitCount = lowHitCount; // volatile write to make isCleaning visible
      
      int sz = stats.size.get();
      if (sz <= upperWaterMark) {
        /* SOLR-7585: Even though we acquired a lock, multiple threads might detect a need for calling this method.
         * Locking keeps these from executing at the same time, so they run sequentially.  The second and subsequent
         * sequential runs of this method don't need to be done, since there are no elements to remove.
        */
        return;
      }
      
      int wantToRemove = sz - lowerWaterMark;
      
      TreeSet<CacheEntry<K, V>> tree = new TreeSet<>();
      
      for (CacheEntry<K, V> ce : map.values()) {
        // set hitsCopy to avoid later Atomic reads.  Primitive types are faster than the atomic get().
        ce.hitsCopy = ce.hits.get();
        ce.lastAccessedCopy = ce.lastAccessed;
        if (timeDecay) {
          ce.hits.set(ce.hitsCopy >>> 1);
        }
        
        if (tree.size() < wantToRemove) {
          tree.add(ce);
        } else {
          /*
           * SOLR-7585: Before doing this part, make sure the TreeSet actually has an element, since the first() method
           * fails with NoSuchElementException if the set is empty.  If that test passes, check hits. This test may
           * never actually fail due to the upperWaterMark check above, but we'll do it anyway.
           */
          if (tree.size() > 0) {
            /* If hits are not equal, we can remove before adding which is slightly faster. I can no longer remember
             * why removing first is faster, but I vaguely remember being sure about it!
             */
            if (ce.hitsCopy < tree.first().hitsCopy) {
              tree.remove(tree.first());
              tree.add(ce);
            } else if (ce.hitsCopy == tree.first().hitsCopy) {
              tree.add(ce);
              tree.remove(tree.first());
            }
          }
        }
      }
      
      for (CacheEntry<K, V> e : tree) {
        evictEntry(e.key);
      }
    } finally {
      isCleaning = false; // set before markAndSweep.unlock() for visibility
      markAndSweepLock.unlock();
    }
  }

  private void evictEntry(K key) {
    CacheEntry<K, V> o = map.remove(key);
    if (o == null) return;
    stats.size.decrementAndGet();
    stats.evictionCounter.incrementAndGet();
    if (evictionListener != null) evictionListener.evictedEntry(o.key, o.value);
  }

  /**
   * Returns 'n' number of least used entries present in this cache.
   * <p>
   * This uses a TreeSet to collect the 'n' least used items ordered by ascending hitcount
   * and returns a LinkedHashMap containing 'n' or less than 'n' entries.
   *
   * @param n the number of items needed
   * @return a LinkedHashMap containing 'n' or less than 'n' entries
   */
  public Map<K, V> getLeastUsedItems(int n) {
    Map<K, V> result = new LinkedHashMap<>();
    if (n <= 0)
      return result;
    TreeSet<CacheEntry<K, V>> tree = new TreeSet<>();
    // we need to grab the lock since we are changing the copy variables
    markAndSweepLock.lock();
    try {
      for (Map.Entry<Object, CacheEntry<K, V>> entry : map.entrySet()) {
        CacheEntry<K, V> ce = entry.getValue();
        ce.hitsCopy = ce.hits.get();
        ce.lastAccessedCopy = ce.lastAccessed;
        if (tree.size() < n) {
          tree.add(ce);
        } else {
          // If the hits are not equal, we can remove before adding
          // which is slightly faster
          if (ce.hitsCopy < tree.first().hitsCopy) {
            tree.remove(tree.first());
            tree.add(ce);
          } else if (ce.hitsCopy == tree.first().hitsCopy) {
            tree.add(ce);
            tree.remove(tree.first());
          }
        }
      }
    } finally {
      markAndSweepLock.unlock();
    }
    for (CacheEntry<K, V> e : tree) {
      result.put(e.key, e.value);
    }
    return result;
  }

  /**
   * Returns 'n' number of most used entries present in this cache.
   * <p>
   * This uses a TreeSet to collect the 'n' most used items ordered by descending hitcount
   * and returns a LinkedHashMap containing 'n' or less than 'n' entries.
   *
   * @param n the number of items needed
   * @return a LinkedHashMap containing 'n' or less than 'n' entries
   */
  public Map<K, V> getMostUsedItems(int n) {
    Map<K, V> result = new LinkedHashMap<>();
    if (n <= 0)
      return result;
    TreeSet<CacheEntry<K, V>> tree = new TreeSet<>();
    // we need to grab the lock since we are changing the copy variables
    markAndSweepLock.lock();
    try {
      for (Map.Entry<Object, CacheEntry<K, V>> entry : map.entrySet()) {
        CacheEntry<K, V> ce = entry.getValue();
        ce.hitsCopy = ce.hits.get();
        ce.lastAccessedCopy = ce.lastAccessed;
        if (tree.size() < n) {
          tree.add(ce);
        } else {
          // If the hits are not equal, we can remove before adding
          // which is slightly faster
          if (ce.hitsCopy > tree.last().hitsCopy) {
            tree.remove(tree.last());
            tree.add(ce);
          } else if (ce.hitsCopy == tree.last().hitsCopy) {
            tree.add(ce);
            tree.remove(tree.last());
          }
        }
      }
    } finally {
      markAndSweepLock.unlock();
    }
    for (CacheEntry<K, V> e : tree) {
      result.put(e.key, e.value);
    }
    return result;
  }

  public int size() {
    return stats.size.get();
  }

  @Override
  public void clear() {
    map.clear();
  }

  public Map<Object, CacheEntry<K, V>> getMap() {
    return map;
  }

  public static class CacheEntry<K, V> implements Comparable<CacheEntry<K, V>> {
    K key;
    V value;
    volatile AtomicLong hits = new AtomicLong(0);
    long hitsCopy = 0;
    volatile long lastAccessed = 0;
    long lastAccessedCopy = 0;

    public CacheEntry(K key, V value, long lastAccessed) {
      this.key = key;
      this.value = value;
      this.lastAccessed = lastAccessed;
    }

    @Override
    public int compareTo(CacheEntry<K, V> that) {
      if (this.hitsCopy == that.hitsCopy) {
        if (this.lastAccessedCopy == that.lastAccessedCopy) {
          return 0;
        }
        return this.lastAccessedCopy < that.lastAccessedCopy ? 1 : -1;
      }
      return this.hitsCopy < that.hitsCopy ? 1 : -1;
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return value.equals(obj);
    }

    @Override
    public String toString() {
      return "key: " + key + " value: " + value + " hits:" + hits.get();
    }
  }

  private boolean isDestroyed = false;

  public void destroy() {
    try {
      if (cleanupThread != null) {
        cleanupThread.stopThread();
      }
    } finally {
      isDestroyed = true;
    }
  }

  public Stats getStats() {
    return stats;
  }


  public static class Stats {
    private final AtomicLong accessCounter = new AtomicLong(0),
        putCounter = new AtomicLong(0),
        nonLivePutCounter = new AtomicLong(0),
        missCounter = new AtomicLong();
    private final AtomicInteger size = new AtomicInteger();
    private AtomicLong evictionCounter = new AtomicLong();

    public long getCumulativeLookups() {
      return (accessCounter.get() - putCounter.get() - nonLivePutCounter.get()) + missCounter.get();
    }

    public long getCumulativeHits() {
      return accessCounter.get() - putCounter.get() - nonLivePutCounter.get();
    }

    public long getCumulativePuts() {
      return putCounter.get();
    }

    public long getCumulativeEvictions() {
      return evictionCounter.get();
    }

    public int getCurrentSize() {
      return size.get();
    }

    public long getCumulativeNonLivePuts() {
      return nonLivePutCounter.get();
    }

    public long getCumulativeMisses() {
      return missCounter.get();
    }

    public void add(Stats other) {
      accessCounter.addAndGet(other.accessCounter.get());
      putCounter.addAndGet(other.putCounter.get());
      nonLivePutCounter.addAndGet(other.nonLivePutCounter.get());
      missCounter.addAndGet(other.missCounter.get());
      evictionCounter.addAndGet(other.evictionCounter.get());
      size.set(Math.max(size.get(), other.size.get()));
    }
  }

  public static interface EvictionListener<K, V> {
    public void evictedEntry(K key, V value);
  }

  private static class CleanupThread extends Thread {
    private WeakReference<ConcurrentLFUCache> cache;

    private boolean stop = false;

    public CleanupThread(ConcurrentLFUCache c) {
      cache = new WeakReference<>(c);
    }

    @Override
    public void run() {
      while (true) {
        synchronized (this) {
          if (stop) break;
          try {
            this.wait();
          } catch (InterruptedException e) {
          }
        }
        if (stop) break;
        ConcurrentLFUCache c = cache.get();
        if (c == null) break;
        c.markAndSweep();
      }
    }

    void wakeThread() {
      synchronized (this) {
        this.notify();
      }
    }

    void stopThread() {
      synchronized (this) {
        stop = true;
        this.notify();
      }
    }
  }

  @Override
  protected void finalize() throws Throwable {
    try {
      if (!isDestroyed) {
        log.error("ConcurrentLFUCache was not destroyed prior to finalize(), indicates a bug -- POSSIBLE RESOURCE LEAK!!!");
        destroy();
      }
    } finally {
      super.finalize();
    }
  }
}
