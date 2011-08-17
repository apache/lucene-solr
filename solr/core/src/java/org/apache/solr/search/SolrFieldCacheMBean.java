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

package org.apache.solr.search;

import java.net.URL;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean;

import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldCache.CacheEntry;
import org.apache.lucene.util.FieldCacheSanityChecker;
import org.apache.lucene.util.FieldCacheSanityChecker.Insanity;

/**
 * A SolrInfoMBean that provides introspection of the Lucene FiledCache, this is <b>NOT</b> a cache that is manged by Solr.
 *
 *
 */
public class SolrFieldCacheMBean implements SolrInfoMBean {

  protected FieldCacheSanityChecker checker = new FieldCacheSanityChecker();

  public String getName() { return this.getClass().getName(); }
  public String getVersion() { return SolrCore.version; }
  public String getDescription() {
    return "Provides introspection of the Lucene FieldCache, "
      +    "this is **NOT** a cache that is managed by Solr.";
  }
  public Category getCategory() { return Category.CACHE; } 
  public String getSourceId() { 
    return "$Id$"; 
  }
  public String getSource() { 
    return "$URL$";
  }
  public URL[] getDocs() {
    return null;
  }
  public NamedList getStatistics() {
    NamedList stats = new SimpleOrderedMap();
    CacheEntry[] entries = FieldCache.DEFAULT.getCacheEntries();
    stats.add("entries_count", entries.length);
    for (int i = 0; i < entries.length; i++) {
      CacheEntry e = entries[i];
      stats.add("entry#" + i, e.toString());
    }

    Insanity[] insanity = checker.check(entries);

    stats.add("insanity_count", insanity.length);
    for (int i = 0; i < insanity.length; i++) {

      /** RAM estimation is both CPU and memory intensive... we don't want to do it unless asked.
      // we only estimate the size of insane entries
      for (CacheEntry e : insanity[i].getCacheEntries()) {
        // don't re-estimate if we've already done it.
        if (null == e.getEstimatedSize()) e.estimateSize();
      }
      **/
      
      stats.add("insanity#" + i, insanity[i].toString());
    }
    return stats;
  }

}
