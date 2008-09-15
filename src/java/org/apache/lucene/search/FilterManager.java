package org.apache.lucene.search;

/**
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

import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

/**
 * Filter caching singleton.  It can be used by {@link org.apache.lucene.search.RemoteCachingWrapperFilter}
 * or just to save filters locally for reuse.
 * This class makes it possble to cache Filters even when using RMI, as it
 * keeps the cache on the seaercher side of the RMI connection.
 * 
 * Also could be used as a persistent storage for any filter as long as the
 * filter provides a proper hashCode(), as that is used as the key in the cache.
 * 
 * The cache is periodically cleaned up from a separate thread to ensure the
 * cache doesn't exceed the maximum size.
 */
public class FilterManager {

  protected static FilterManager manager;
  
  /** The default maximum number of Filters in the cache */
  protected static final int  DEFAULT_CACHE_CLEAN_SIZE = 100;
  /** The default frequency of cache clenup */
  protected static final long DEFAULT_CACHE_SLEEP_TIME = 1000 * 60 * 10;

  /** The cache itself */
  protected Map           cache;
  /** Maximum allowed cache size */
  protected int           cacheCleanSize;
  /** Cache cleaning frequency */
  protected long          cleanSleepTime;
  /** Cache cleaner that runs in a separate thread */
  protected FilterCleaner filterCleaner;

  public synchronized static FilterManager getInstance() {
    if (manager == null) {
      manager = new FilterManager();
    }
    return manager;
  }

  /**
   * Sets up the FilterManager singleton.
   */
  protected FilterManager() {
    cache            = new HashMap();
    cacheCleanSize   = DEFAULT_CACHE_CLEAN_SIZE; // Let the cache get to 100 items
    cleanSleepTime   = DEFAULT_CACHE_SLEEP_TIME; // 10 minutes between cleanings

    filterCleaner   = new FilterCleaner();
    Thread fcThread = new Thread(filterCleaner);
    // setto be a Daemon so it doesn't have to be stopped
    fcThread.setDaemon(true);
    fcThread.start();
  }
  
  /**
   * Sets the max size that cache should reach before it is cleaned up
   * @param cacheCleanSize maximum allowed cache size
   */
  public void setCacheSize(int cacheCleanSize) {
    this.cacheCleanSize = cacheCleanSize;
  }

  /**
   * Sets the cache cleaning frequency in milliseconds.
   * @param cleanSleepTime cleaning frequency in millioseconds
   */
  public void setCleanThreadSleepTime(long cleanSleepTime) {
    this.cleanSleepTime  = cleanSleepTime;
  }

  /**
   * Returns the cached version of the filter.  Allows the caller to pass up
   * a small filter but this will keep a persistent version around and allow
   * the caching filter to do its job.
   * 
   * @param filter The input filter
   * @return The cached version of the filter
   */
  public Filter getFilter(Filter filter) {
    synchronized(cache) {
      FilterItem fi = null;
      fi = (FilterItem)cache.get(new Integer(filter.hashCode()));
      if (fi != null) {
        fi.timestamp = new Date().getTime();
        return fi.filter;
      }
      cache.put(new Integer(filter.hashCode()), new FilterItem(filter));
      return filter;
    }
  }

  /**
   * Holds the filter and the last time the filter was used, to make LRU-based
   * cache cleaning possible.
   * TODO: Clean this up when we switch to Java 1.5
   */
  protected class FilterItem {
    public Filter filter;
    public long   timestamp;

    public FilterItem (Filter filter) {        
      this.filter = filter;
      this.timestamp = new Date().getTime();
    }
  }


  /**
   * Keeps the cache from getting too big.
   * If we were using Java 1.5, we could use LinkedHashMap and we would not need this thread
   * to clean out the cache.
   * 
   * The SortedSet sortedFilterItems is used only to sort the items from the cache,
   * so when it's time to clean up we have the TreeSet sort the FilterItems by
   * timestamp.
   * 
   * Removes 1.5 * the numbers of items to make the cache smaller.
   * For example:
   * If cache clean size is 10, and the cache is at 15, we would remove (15 - 10) * 1.5 = 7.5 round up to 8.
   * This way we clean the cache a bit more, and avoid having the cache cleaner having to do it frequently.
   */
  protected class FilterCleaner implements Runnable  {

    private boolean running = true;
    private TreeSet sortedFilterItems;

    public FilterCleaner() {
      sortedFilterItems = new TreeSet(new Comparator() {
        public int compare(Object a, Object b) {
          if( a instanceof Map.Entry && b instanceof Map.Entry) {
            FilterItem fia = (FilterItem) ((Map.Entry)a).getValue();
            FilterItem fib = (FilterItem) ((Map.Entry)b).getValue();
            if ( fia.timestamp == fib.timestamp ) {
              return 0;
            }
            // smaller timestamp first
            if ( fia.timestamp < fib.timestamp ) {
              return -1;
            }
            // larger timestamp last
            return 1;
          } else {
            throw new ClassCastException("Objects are not Map.Entry");
          }
        }
      });
    }

    public void run () {
      while (running) {

        // sort items from oldest to newest 
        // we delete the oldest filters 
        if (cache.size() > cacheCleanSize) {
          // empty the temporary set
          sortedFilterItems.clear();
          synchronized (cache) {
            sortedFilterItems.addAll(cache.entrySet());
            Iterator it = sortedFilterItems.iterator();
            int numToDelete = (int) ((cache.size() - cacheCleanSize) * 1.5);
            int counter = 0;
            // loop over the set and delete all of the cache entries not used in a while
            while (it.hasNext() && counter++ < numToDelete) {
              Map.Entry entry = (Map.Entry)it.next();
              cache.remove(entry.getKey());
            }
          }
          // empty the set so we don't tie up the memory
          sortedFilterItems.clear();
        }
        // take a nap
        try {
          Thread.sleep(cleanSleepTime);
        } catch (InterruptedException e) {
          // just keep going
        }
      }
    }
  }
}
