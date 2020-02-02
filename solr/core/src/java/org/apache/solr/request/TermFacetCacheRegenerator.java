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
package org.apache.solr.request;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.WeakHashMap;
import org.apache.lucene.index.IndexReader.CacheKey;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.request.TermFacetCache.SegmentCacheEntry;
import org.apache.solr.search.CacheRegenerator;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;

/**
 *
 */
public class TermFacetCacheRegenerator implements CacheRegenerator {
  private final Map<CacheKey, Set<CacheKey>> activeSegments;

  public TermFacetCacheRegenerator() {
    activeSegments = new WeakHashMap<>();
  }

  @Override
  public boolean regenerateItem(SolrIndexSearcher newSearcher, SolrCache nc, SolrCache oc, Object oldKey, Object oldVal) throws IOException {
    Set<CacheKey> segmentKeys;
    CacheKey topLevelKey = newSearcher.getIndexReader().getReaderCacheHelper().getKey();
    synchronized (activeSegments) {
      segmentKeys = activeSegments.get(topLevelKey);
      if (segmentKeys == null) {
        List<LeafReaderContext> leaves = newSearcher.getTopReaderContext().leaves();
        segmentKeys = new HashSet<>(leaves.size());
        for (LeafReaderContext leaf : leaves) {
          segmentKeys.add(leaf.reader().getReaderCacheHelper().getKey());
        }
        activeSegments.clear();
        activeSegments.put(topLevelKey, segmentKeys);
      }
    }
    Map<CacheKey, SegmentCacheEntry> oldSegmentCache = (Map<CacheKey, SegmentCacheEntry>)oldVal;
    Map<CacheKey, SegmentCacheEntry> newSegmentCache = new HashMap<>(segmentKeys.size());
    for (Entry<CacheKey, SegmentCacheEntry> e : oldSegmentCache.entrySet()) {
      CacheKey segmentKey = e.getKey();
      if (segmentKeys.contains(segmentKey)) {
        newSegmentCache.put(segmentKey, e.getValue());
      }
    }
    if (!newSegmentCache.isEmpty()) {
      nc.put(oldKey, newSegmentCache);
    }
    return true;
  }
}
