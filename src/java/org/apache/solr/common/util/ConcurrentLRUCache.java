package org.apache.solr.common.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A LRU cache implementation based upon ConcurrentHashMap and other techniques to reduce
 * contention and synchronization overhead to utilize multiple CPU cores more effectively.
 * <p/>
 * Note that the implementation does not follow a true LRU (least-recently-used) eviction
 * strategy. Instead it strives to remove least recently used items but when the initial
 * cleanup does not remove enough items to reach the 'acceptableWaterMark' limit, it can
 * remove more items forcefully regardless of access order.
 *
 * @version $Id$
 * @since solr 1.4
 */
public class ConcurrentLRUCache {

  private final ConcurrentHashMap<Object, CacheEntry> map;
  private final int upperWaterMark, lowerWaterMark;
  private volatile boolean stop = false;
  private final ReentrantLock markAndSweepLock = new ReentrantLock(true);
  private volatile boolean isCleaning = false;
  private final boolean newThreadForCleanup;
  private volatile boolean islive = true;
  private final Stats stats = new Stats();
  private final int acceptableWaterMark;

  public ConcurrentLRUCache(int upperWaterMark, final int lowerWaterMark, int acceptableWatermark, int initialSize, boolean runCleanupThread, boolean runNewThreadForCleanup, final int delay) {
    if (upperWaterMark < 1) throw new IllegalArgumentException("upperWaterMark must be > 0");
    if (lowerWaterMark >= upperWaterMark)
      throw new IllegalArgumentException("lowerWaterMark must be  < upperWaterMark");
    map = new ConcurrentHashMap<Object, CacheEntry>(initialSize);
    newThreadForCleanup = runNewThreadForCleanup;
    this.upperWaterMark = upperWaterMark;
    this.lowerWaterMark = lowerWaterMark;
    this.acceptableWaterMark = acceptableWatermark;
    if (runCleanupThread) {
      new Thread() {
        public void run() {
          while (true) {
            if (stop) break;
            try {
              Thread.sleep(delay * 1000);
            } catch (InterruptedException e) {/*no op*/ }
            markAndSweep();
          }
        }
      }.start();
    }
  }

  public void setAlive(boolean live) {
    islive = live;
  }

  public Object get(Object key) {
    CacheEntry e = map.get(key);
    if (e == null) {
      if (islive) stats.missCounter.incrementAndGet();
      return null;
    }
    if (islive) e.lastAccessed = stats.accessCounter.incrementAndGet();
    return e.value;
  }

  public Object remove(Object key) {
    CacheEntry cacheEntry = map.remove(key);
    if (cacheEntry != null) {
      stats.size.decrementAndGet();
      return cacheEntry.value;
    }
    return null;
  }

  public Object put(Object key, Object val) {
    if (val == null) return null;
    CacheEntry e = new CacheEntry(key, val, stats.accessCounter.incrementAndGet());
    CacheEntry oldCacheEntry = map.put(key, e);
    if (oldCacheEntry == null) {
      stats.size.incrementAndGet();
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
    if (stats.size.get() > upperWaterMark && !isCleaning) {
      if (newThreadForCleanup) {
        new Thread() {
          public void run() {
            markAndSweep();
          }
        }.start();
      } else {
        markAndSweep();
      }
    }
    return oldCacheEntry == null ? null : oldCacheEntry.value;
  }

  /**
   * Removes items from the cache to bring the size down
   * to an acceptable value ('acceptableWaterMark').
   * <p/>
   * It is done in two stages. In the first stage, least recently used items are evicted.
   * If, after the first stage, the cache size is still greater than 'acceptableSize'
   * config parameter, the second stage takes over.
   * <p/>
   * The second stage is more intensive and tries to bring down the cache size
   * to the 'minSize' config parameter.
   */
  public void markAndSweep() {
    if (!markAndSweepLock.tryLock()) return;
    try {
      isCleaning = true;
      int size = stats.size.get();
      long currentLatestAccessed = stats.accessCounter.get();
      int itemsToBeRemoved = size - lowerWaterMark;
      int itemsRemoved = 0;
      if (itemsToBeRemoved < 1) return;
      // currentLatestAccessed is the counter value of the item accessed most recently
      // therefore remove all items whose last accessed counter is less than (currentLatestAccessed - lowerWaterMark)
      long removeOlderThan = currentLatestAccessed - lowerWaterMark;
      for (Map.Entry<Object, CacheEntry> entry : map.entrySet()) {
        if (entry.getValue().lastAccessed <= removeOlderThan && itemsRemoved < itemsToBeRemoved) {
          evictEntry(entry.getKey());
        }
      }

      // Since the removal of items in the above loop depends on the value of the lastAccessed variable,
      // between the time we recorded the number of items to be removed and the actual removal process,
      // some items may graduate above the removeOlderThan value and escape eviction.
      // Therefore, we again check if the size less than acceptableWaterMark, if not we remove items forcefully
      // using a method which does not depend on the value of lastAccessed but can be more costly to run

      size = stats.size.get();
      // In the first attempt, try to use a simple algorithm to remove old entries
      // If the size of the cache is <= acceptableWatermark then return
      if (size <= acceptableWaterMark) return;
      // Remove items until size becomes lower than acceptableWaterMark
      itemsToBeRemoved = size - acceptableWaterMark;
      TreeSet<CacheEntry> tree = new TreeSet<CacheEntry>();
      // This loop may remove a few newer items because we try to forcefully fill a
      // bucket of fixed size and remove them even if they have become newer in the meantime
      // The caveat is that this may lead to more cache misses because we may have removed
      // an item which was used very recently (against the philosophy of LRU)
      for (Map.Entry<Object, CacheEntry> entry : map.entrySet()) {
        CacheEntry v = entry.getValue();
        v.lastAccessedCopy = v.lastAccessed;
        if (tree.size() < itemsToBeRemoved) {
          tree.add(v);
        } else {
          if (v.lastAccessedCopy < tree.first().lastAccessedCopy) {
            tree.remove(tree.first());
            tree.add(v);
          }
        }
      }
      for (CacheEntry sortCacheEntry : tree)
        evictEntry(sortCacheEntry.key);
    } finally {
      isCleaning = false;
      markAndSweepLock.unlock();
    }
  }


  private void evictEntry(Object key) {
    Object o = map.remove(key);
    if (o == null) return;
    stats.size.decrementAndGet();
    stats.evictionCounter++;
  }


  public Map getLatestAccessedItems(long n) {
    markAndSweepLock.lock();
    Map result = new LinkedHashMap();
    TreeSet<CacheEntry> tree = new TreeSet<CacheEntry>();
    try {
      for (Map.Entry<Object, CacheEntry> entry : map.entrySet()) {
        CacheEntry ce = entry.getValue();
        ce.lastAccessedCopy = ce.lastAccessed;
        if (tree.size() < n) {
          tree.add(ce);
        } else {
          if (ce.lastAccessedCopy > tree.last().lastAccessedCopy) {
            tree.remove(tree.last());
            tree.add(entry.getValue());
          }
        }
      }
    } finally {
      markAndSweepLock.unlock();
    }
    for (CacheEntry e : tree) {
      result.put(e.key, e.value);
    }
    return result;
  }

  public int size() {
    return stats.size.get();
  }

  public void clear() {
    map.clear();
  }

  public Map<Object, CacheEntry> getMap() {
    return map;
  }

  private static class CacheEntry implements Comparable<CacheEntry> {
    Object key, value;
    volatile long lastAccessed = 0;
    long lastAccessedCopy = 0;


    public CacheEntry(Object key, Object value, long lastAccessed) {
      this.key = key;
      this.value = value;
      this.lastAccessed = lastAccessed;
    }

    public void setLastAccessed(long lastAccessed) {
      this.lastAccessed = lastAccessed;
    }

    public int compareTo(CacheEntry that) {
      if (this.lastAccessedCopy == that.lastAccessedCopy) return 0;
      return this.lastAccessedCopy < that.lastAccessedCopy ? 1 : -1;
    }

    public int hashCode() {
      return value.hashCode();
    }

    public boolean equals(Object obj) {
      return value.equals(obj);
    }

    public String toString() {
      return "key: " + key + " value: " + value + " lastAccessed:" + lastAccessed;
    }
  }


  public void destroy() {
    stop = true;
  }

  public Stats getStats() {
    return stats;
  }

  protected void finalize() throws Throwable {
    destroy();
    super.finalize();
  }

  public static class Stats {
    private final AtomicLong accessCounter = new AtomicLong(0),
            putCounter = new AtomicLong(0),
            nonLivePutCounter = new AtomicLong(0),
            missCounter = new AtomicLong();
    private final AtomicInteger size = new AtomicInteger();
    private long evictionCounter = 0;

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
      return evictionCounter;
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
  }
}
