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

import junit.framework.TestCase;

import org.apache.lucene.index.IndexReader;

/**
 * A unit test helper class to help with RemoteCachingWrapperFilter testing and
 * assert that it is working correctly.
 */
public class RemoteCachingWrapperFilterHelper extends RemoteCachingWrapperFilter {

  private boolean shouldHaveCache;

  public RemoteCachingWrapperFilterHelper(Filter filter, boolean shouldHaveCache) {
    super(filter);
    this.shouldHaveCache = shouldHaveCache;
  }
  
  public void shouldHaveCache(boolean shouldHaveCache) {
    this.shouldHaveCache = shouldHaveCache;
  }

  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    Filter cachedFilter = FilterManager.getInstance().getFilter(filter);
    
    TestCase.assertNotNull("Filter should not be null", cachedFilter);
    if (!shouldHaveCache) {
      TestCase.assertSame("First time filter should be the same ", filter, cachedFilter);
    } else {
      TestCase.assertNotSame("We should have a cached version of the filter", filter, cachedFilter);
    }
    
    if (filter instanceof CachingWrapperFilterHelper) {
      ((CachingWrapperFilterHelper)cachedFilter).setShouldHaveCache(shouldHaveCache);
    }
    return cachedFilter.getDocIdSet(reader);
  }
}
