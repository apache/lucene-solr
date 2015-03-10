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

package org.apache.solr.client.solrj.io;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class StreamContext {

  private Map entries = new HashMap();
  public int workerID;
  public int numWorkers;
  public SolrClientCache clientCache;

  public SolrClientCache getClientCache() {
    return this.clientCache;
  }

  public void setSolrClientCache(SolrClientCache clientCache) {
    this.clientCache = clientCache;
  }

  public Object get(Object key) {
    return entries.get(key);
  }

  public void put(Object key, Object value) {
    this.entries.put(key, value);
  }
}