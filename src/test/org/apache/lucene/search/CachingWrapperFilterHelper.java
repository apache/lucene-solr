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

import java.io.IOException;
import java.util.BitSet;
import java.util.WeakHashMap;

import junit.framework.TestCase;

import org.apache.lucene.index.IndexReader;

/**
 * A unit test helper class to test when the filter is getting cached and when it is not.
 */
public class CachingWrapperFilterHelper extends CachingWrapperFilter {
  
  private boolean shouldHaveCache = false;

  /**
   * @param filter Filter to cache results of
   */
  public CachingWrapperFilterHelper(Filter filter) {
    super(filter);
  }
  
  public void setShouldHaveCache(boolean shouldHaveCache) {
    this.shouldHaveCache = shouldHaveCache;
  }
  
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    if (cache == null) {
      cache = new WeakHashMap();
    }
    
    synchronized (cache) {  // check cache
      DocIdSet cached = (DocIdSet) cache.get(reader);
      if (shouldHaveCache) {
        TestCase.assertNotNull("Cache should have data ", cached);
      } else {
        TestCase.assertNull("Cache should be null " + cached , cached);
      }
      if (cached != null) {
        return cached;
      }
    }

    final DocIdSet bits = filter.getDocIdSet(reader);

    synchronized (cache) {  // update cache
      cache.put(reader, bits);
    }

    return bits;
  }

  public String toString() {
    return "CachingWrapperFilterHelper("+filter+")";
  }

  public boolean equals(Object o) {
    if (!(o instanceof CachingWrapperFilterHelper)) return false;
    return this.filter.equals((CachingWrapperFilterHelper)o);
  }
}
