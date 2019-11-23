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
package org.apache.solr.util;

import java.lang.invoke.MethodHandles;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.util.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.solr.common.util.TimeSource;

import static org.apache.lucene.util.RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
import static org.apache.lucene.util.RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED;

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
public class ConcurrentLFUCache<K, V> implements Cache<K,V>, Accountable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(ConcurrentLFUCache.class) +
      new Stats().ramBytesUsed() +
      RamUsageEstimator.shallowSizeOfInstance(ConcurrentHashMap.class);

  private final ConcurrentHashMap<Object, CacheEntry<K, V>> map;
  private int upperWaterMark, lowerWaterMark;
  private final ReentrantLock markAndSweepLock = new ReentrantLock(true);
  private boolean isCleaning = false;  // not volatile... piggybacked on other volatile vars
  private boolean newThreadForCleanup;
  private boolean runCleanupThread;
  private volatile boolean islive = true;
  private final Stats stats = new Stats();
  @SuppressWarnings("unused")
  private int acceptableWaterMark;
  private long lowHitCount = 0;  // not volatile, only accessed in the cleaning method
  private final EvictionListener<K, V> evictionListener;
  private CleanupThread cleanupThread;
  private boolean timeDecay;
  private long maxIdleTimeNs;
  private final TimeSource timeSource = TimeSource.NANO_TIME;
  private final AtomicLong oldestEntry = new AtomicLong(0L);
  private final LongAdder ramBytes = new LongAdder();

  public ConcurrentLFUCache(int upperWaterMark, final int lowerWaterMark, int acceptableSize,
                            int initialSize, boolean runCleanupThread, boolean runNewThreadForCleanup,
                            EvictionListener<K, V> evictionListener, boolean timeDecay) {
    this(upperWaterMark, lowerWaterMark, acceptableSize, initialSize, runCleanupThread,
        runNewThreadForCleanup, evictionListener, timeDecay, -1);
  }

  public ConcurrentLFUCache(int upperWaterMark, final int lowerWaterMark, int acceptableSize,
                            int initialSize, boolean runCleanupThread, boolean runNewThreadForCleanup,
                            EvictionListener<K, V> evictionListener, boolean timeDecay, int maxIdleTimeSec) {
    setUpperWaterMark(upperWaterMark);
    setLowerWaterMark(lowerWaterMark);
    setAcceptableWaterMark(acceptableSize);
    map = new ConcurrentHashMap<>(initialSize);
    this.evictionListener = evictionListener;
    setNewThreadForCleanup(runNewThreadForCleanup);
    setTimeDecay(timeDecay);
    setMaxIdleTime(maxIdleTimeSec);
    setRunCleanupThread(runCleanupThread);
  }

  public ConcurrentLFUCache(int size, int lowerWatermark) {
    this(size, lowerWatermark, (int) Math.floor((lowerWatermark + size) / 2),
        (int) Math.ceil(0.75 * size), false, false, null, true, -1);
  }

  public void setAlive(boolean live) {
    islive = live;
  }

  public void setUpperWaterMark(int upperWaterMark) {
    if (upperWaterMark < 1) throw new IllegalArgumentException("upperWaterMark must be > 0");
    this.upperWaterMark = upperWaterMark;
  }

  public void setLowerWaterMark(int lowerWaterMark) {
    if (lowerWaterMark >= upperWaterMark)
      throw new IllegalArgumentException("lowerWaterMark must be  < upperWaterMark");
    this.lowerWaterMark = lowerWaterMark;
  }

  public void setAcceptableWaterMark(int acceptableWaterMark) {
    this.acceptableWaterMark = acceptableWaterMark;
  }

  public void setTimeDecay(boolean timeDecay) {
    this.timeDecay = timeDecay;
  }

  public void setMaxIdleTime(int maxIdleTime) {
    long oldMaxIdleTimeNs = maxIdleTimeNs;
    maxIdleTimeNs = maxIdleTime > 0 ? TimeUnit.NANOSECONDS.convert(maxIdleTime, TimeUnit.SECONDS) : Long.MAX_VALUE;
    if (cleanupThread != null && maxIdleTimeNs < oldMaxIdleTimeNs) {
      cleanupThread.wakeThread();
    }
  }

  public synchronized void setNewThreadForCleanup(boolean newThreadForCleanup) {
    this.newThreadForCleanup = newThreadForCleanup;
    if (newThreadForCleanup) {
      setRunCleanupThread(false);
    }
  }

  public synchronized void setRunCleanupThread(boolean runCleanupThread) {
    this.runCleanupThread = runCleanupThread;
    if (this.runCleanupThread) {
      newThreadForCleanup = false;
      if (cleanupThread == null) {
        cleanupThread = new CleanupThread(this);
        cleanupThread.start();
      }
    } else {
      if (cleanupThread != null) {
        cleanupThread.stopThread();
        cleanupThread = null;
      }
    }
  }

  @Override
  public V get(K key) {
    CacheEntry<K, V> e = map.get(key);
    if (e == null) {
      if (islive) stats.missCounter.increment();
    } else if (islive) {
      e.lastAccessed = timeSource.getEpochTimeNs();
      stats.accessCounter.increment();
      e.hits.increment();
    }
    return e != null ? e.value : null;
  }

  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    // prescreen access first
    V val = get(key);
    if (val != null) {
      return val;
    }
    AtomicBoolean newValue = new AtomicBoolean();
    if (islive) {
      stats.accessCounter.increment();
    }
    CacheEntry<K, V> entry =  map.computeIfAbsent(key, k -> {
      V value = mappingFunction.apply(key);
      // preserve the semantics of computeIfAbsent
      if (value == null) {
        return null;
      }
      CacheEntry<K, V> e = new CacheEntry<>(key, value, timeSource.getEpochTimeNs());
      newValue.set(true);
      oldestEntry.updateAndGet(x -> x > e.lastAccessed  || x == 0 ? e.lastAccessed : x);
      stats.size.increment();
      ramBytes.add(e.ramBytesUsed() + HASHTABLE_RAM_BYTES_PER_ENTRY); // added key + value + entry
      if (islive) {
        stats.putCounter.increment();
      } else {
        stats.nonLivePutCounter.increment();
      }
      return e;
    });
    if (newValue.get()) {
      maybeMarkAndSweep();
    } else {
      if (islive && entry != null) {
        entry.lastAccessed = timeSource.getEpochTimeNs();
        entry.hits.increment();
      }
    }
    return entry != null ? entry.value : null;
  }

  @Override
  public V remove(K key) {
    CacheEntry<K, V> cacheEntry = map.remove(key);
    if (cacheEntry != null) {
      stats.size.decrement();
      ramBytes.add(-cacheEntry.ramBytesUsed() - HASHTABLE_RAM_BYTES_PER_ENTRY);
      return cacheEntry.value;
    }
    return null;
  }

  @Override
  public V put(K key, V val) {
    if (val == null) return null;
    CacheEntry<K, V> e = new CacheEntry<>(key, val, timeSource.getEpochTimeNs());
    return putCacheEntry(e);
  }



  /**
   * Visible for testing to create synthetic cache entries.
   * @lucene.internal
   */
  public V putCacheEntry(CacheEntry<K, V> e) {
    stats.accessCounter.increment();
    // initialize oldestEntry
    oldestEntry.updateAndGet(x -> x > e.lastAccessed  || x == 0 ? e.lastAccessed : x);
    CacheEntry<K, V> oldCacheEntry = map.put(e.key, e);
    if (oldCacheEntry == null) {
      stats.size.increment();
      ramBytes.add(e.ramBytesUsed() + HASHTABLE_RAM_BYTES_PER_ENTRY); // added key + value + entry
    } else {
      ramBytes.add(-oldCacheEntry.ramBytesUsed());
      ramBytes.add(e.ramBytesUsed());
    }
    if (islive) {
      stats.putCounter.increment();
    } else {
      stats.nonLivePutCounter.increment();
    }
    maybeMarkAndSweep();
    return oldCacheEntry == null ? null : oldCacheEntry.value;
  }

  private void maybeMarkAndSweep() {
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
    boolean evictByIdleTime = maxIdleTimeNs != Long.MAX_VALUE;
    int currentSize = stats.size.intValue();
    long idleCutoff = evictByIdleTime ? timeSource.getEpochTimeNs() - maxIdleTimeNs : -1L;
    if ((currentSize > upperWaterMark || (evictByIdleTime && oldestEntry.get() < idleCutoff)) && !isCleaning) {
      if (newThreadForCleanup) {
        new Thread(this::markAndSweep).start();
      } else if (cleanupThread != null) {
        cleanupThread.wakeThread();
      } else {
        markAndSweep();
      }
    }
  }

  /**
   * Removes items from the cache to bring the size down to the lowerWaterMark.
   * <p>Visible for unit testing.</p>
   * @lucene.internal
   */
  public void markAndSweep() {
    if (!markAndSweepLock.tryLock()) return;
    try {
      long lowHitCount = this.lowHitCount;
      isCleaning = true;
      this.lowHitCount = lowHitCount; // volatile write to make isCleaning visible
      
      int sz = stats.size.intValue();
      boolean evictByIdleTime = maxIdleTimeNs != Long.MAX_VALUE;
      long idleCutoff = evictByIdleTime ? timeSource.getEpochTimeNs() - maxIdleTimeNs : -1L;
      if (sz <= upperWaterMark && (evictByIdleTime && oldestEntry.get() > idleCutoff)) {
        /* SOLR-7585: Even though we acquired a lock, multiple threads might detect a need for calling this method.
         * Locking keeps these from executing at the same time, so they run sequentially.  The second and subsequent
         * sequential runs of this method don't need to be done, since there are no elements to remove.
        */
        return;
      }

      // first evict by idleTime - it's less costly to do an additional pass over the
      // map than to manage the outdated entries in a TreeSet
      if (evictByIdleTime) {
        long currentOldestEntry = Long.MAX_VALUE;
        Iterator<Map.Entry<Object, CacheEntry<K, V>>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
          Map.Entry<Object, CacheEntry<K, V>> entry = iterator.next();
          entry.getValue().lastAccessedCopy = entry.getValue().lastAccessed;
          if (entry.getValue().lastAccessedCopy < idleCutoff) {
            iterator.remove();
            postRemoveEntry(entry.getValue());
            stats.evictionIdleCounter.increment();
          } else {
            if (entry.getValue().lastAccessedCopy < currentOldestEntry) {
              currentOldestEntry = entry.getValue().lastAccessedCopy;
            }
          }
        }
        if (currentOldestEntry != Long.MAX_VALUE) {
          oldestEntry.set(currentOldestEntry);
        }
        // refresh size and maybe return
        sz = stats.size.intValue();
        if (sz <= upperWaterMark) {
          return;
        }
      }
      int wantToRemove = sz - lowerWaterMark;

      TreeSet<CacheEntry<K, V>> tree = new TreeSet<>();

      for (CacheEntry<K, V> ce : map.values()) {
        // set hitsCopy to avoid later Atomic reads.  Primitive types are faster than the atomic get().
        ce.hitsCopy = ce.hits.longValue();
        ce.lastAccessedCopy = ce.lastAccessed;
        if (timeDecay) {
          ce.hits.reset();
          ce.hits.add(ce.hitsCopy >>> 1);
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
      if (evictByIdleTime) {
        // do a full pass because we don't what is the max. age of remaining items
        long currentOldestEntry = Long.MAX_VALUE;
        for (CacheEntry<K, V> e : map.values()) {
          if (e.lastAccessedCopy < currentOldestEntry) {
            currentOldestEntry = e.lastAccessedCopy;
          }
        }
        if (currentOldestEntry != Long.MAX_VALUE) {
          oldestEntry.set(currentOldestEntry);
        }
      }
    } finally {
      isCleaning = false; // set before markAndSweep.unlock() for visibility
      markAndSweepLock.unlock();
    }
  }

  private void evictEntry(K key) {
    CacheEntry<K, V> o = map.remove(key);
    postRemoveEntry(o);
  }

  private void postRemoveEntry(CacheEntry<K, V> o) {
    if (o == null) return;
    ramBytes.add(-(o.ramBytesUsed() + HASHTABLE_RAM_BYTES_PER_ENTRY));
    stats.size.decrement();
    stats.evictionCounter.increment();
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
        ce.hitsCopy = ce.hits.longValue();
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
        ce.hitsCopy = ce.hits.longValue();
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
    return stats.size.intValue();
  }

  @Override
  public void clear() {
    map.clear();
    ramBytes.reset();
  }

  public Map<Object, CacheEntry<K, V>> getMap() {
    return map;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + ramBytes.sum();
  }

  public static class CacheEntry<K, V> implements Comparable<CacheEntry<K, V>>, Accountable {
    public static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(CacheEntry.class)
        // AtomicLong
        + RamUsageEstimator.primitiveSizes.get(long.class);

    final K key;
    final V value;
    final long ramBytesUsed;
    final LongAdder hits = new LongAdder();
    long hitsCopy = 0;
    volatile long lastAccessed = 0;
    long lastAccessedCopy = 0;

    public CacheEntry(K key, V value, long lastAccessed) {
      this.key = key;
      this.value = value;
      this.lastAccessed = lastAccessed;
      ramBytesUsed = BASE_RAM_BYTES_USED +
          RamUsageEstimator.sizeOfObject(key, QUERY_DEFAULT_RAM_BYTES_USED) +
          RamUsageEstimator.sizeOfObject(value, QUERY_DEFAULT_RAM_BYTES_USED);
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
      return "key: " + key + " value: " + value + " hits:" + hits.longValue();
    }

    @Override
    public long ramBytesUsed() {
      return ramBytesUsed;
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


  public static class Stats implements Accountable {
    private static final long RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(Stats.class) +
            // LongAdder
            7 * (
                RamUsageEstimator.NUM_BYTES_ARRAY_HEADER +
                    RamUsageEstimator.primitiveSizes.get(long.class) +
                    2 * (RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.primitiveSizes.get(long.class))
            );

    private final LongAdder accessCounter = new LongAdder();
    private final LongAdder putCounter = new LongAdder();
    private final LongAdder nonLivePutCounter = new LongAdder();
    private final LongAdder missCounter = new LongAdder();
    private final LongAdder size = new LongAdder();
    private LongAdder evictionCounter = new LongAdder();
    private LongAdder evictionIdleCounter = new LongAdder();

    public long getCumulativeLookups() {
      return (accessCounter.longValue() - putCounter.longValue() - nonLivePutCounter.longValue()) + missCounter.longValue();
    }

    public long getCumulativeHits() {
      return accessCounter.longValue() - putCounter.longValue() - nonLivePutCounter.longValue();
    }

    public long getCumulativePuts() {
      return putCounter.longValue();
    }

    public long getCumulativeEvictions() {
      return evictionCounter.longValue();
    }

    public long getCumulativeIdleEvictions() {
      return evictionIdleCounter.longValue();
    }

    public int getCurrentSize() {
      return size.intValue();
    }

    public long getCumulativeNonLivePuts() {
      return nonLivePutCounter.longValue();
    }

    public long getCumulativeMisses() {
      return missCounter.longValue();
    }

    public void add(Stats other) {
      accessCounter.add(other.accessCounter.longValue());
      putCounter.add(other.putCounter.longValue());
      nonLivePutCounter.add(other.nonLivePutCounter.longValue());
      missCounter.add(other.missCounter.longValue());
      evictionCounter.add(other.evictionCounter.longValue());
      evictionIdleCounter.add(other.evictionIdleCounter.longValue());
      long maxSize = Math.max(size.longValue(), other.size.longValue());
      size.reset();
      size.add(maxSize);
    }

    @Override
    public long ramBytesUsed() {
      return RAM_BYTES_USED;
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
        ConcurrentLFUCache c = cache.get();
        if(c == null) break;
        synchronized (this) {
          if (stop) break;
          long waitTimeMs =  c.maxIdleTimeNs != Long.MAX_VALUE ? TimeUnit.MILLISECONDS.convert(c.maxIdleTimeNs, TimeUnit.NANOSECONDS) : 0L;
          try {
            this.wait(waitTimeMs);
          } catch (InterruptedException e) {
          }
        }
        if (stop) break;
        c = cache.get();
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
