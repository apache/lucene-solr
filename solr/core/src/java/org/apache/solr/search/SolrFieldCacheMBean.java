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
package org.apache.solr.search;

import java.net.URL;

import org.apache.lucene.uninverting.UninvertingReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean;

/**
 * A SolrInfoMBean that provides introspection of the Solr FieldCache
 *
 */
public class SolrFieldCacheMBean implements SolrInfoMBean {

  @Override
  public String getName() { return this.getClass().getName(); }
  @Override
  public String getVersion() { return SolrCore.version; }
  @Override
  public String getDescription() {
    return "Provides introspection of the Solr FieldCache ";
  }
  @Override
  public Category getCategory() { return Category.CACHE; } 
  @Override
  public String getSource() { return null; }
  @Override
  public URL[] getDocs() {
    return null;
  }
  @Override
  public NamedList getStatistics() {
    NamedList stats = new SimpleOrderedMap();
    String[] entries = UninvertingReader.getUninvertedStats();
    stats.add("entries_count", entries.length);
    for (int i = 0; i < entries.length; i++) {
      stats.add("entry#" + i, entries[i]);
    }
    return stats;
  }

}
