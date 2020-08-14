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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.IdentityHashMap;

import org.apache.lucene.search.IndexSearcher;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrRequestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Bridge between old style context and a real class.
 * This is currently slightly more heavy weight than necessary because of the need to inherit from IdentityHashMap rather than
 * instantiate it on demand (and the need to put "searcher" in the map)
 * @lucene.experimental
 */
@SuppressWarnings("rawtypes")
public class QueryContext extends IdentityHashMap implements Closeable {
  // private IdentityHashMap map;  // we are the map for now (for compat w/ ValueSource)
  private final SolrIndexSearcher searcher;
  private final IndexSearcher indexSearcher;
  private IdentityHashMap<Closeable,String> closeHooks;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // migrated from ValueSource
  public static QueryContext newContext(IndexSearcher searcher) {
    QueryContext context = new QueryContext(searcher);
    return context;
  }

  @SuppressWarnings({"unchecked"})
  public QueryContext(IndexSearcher searcher) {
    this.searcher = searcher instanceof SolrIndexSearcher ? (SolrIndexSearcher)searcher : null;
    indexSearcher = searcher;
    this.put("searcher", searcher); // see ValueSource.newContext()  // TODO: move check to "get"?
  }


  public SolrIndexSearcher searcher() {
    return searcher;
  }

  public IndexSearcher indexSearcher() {
    return indexSearcher;
  }

  /***  implementations obtained via inheritance
  public Object get(Object key) {
    return map.get(key);
  }

  public Object put(Object key, Object val) {
    if (map == null) {
      map = new IdentityHashMap();
    }
    return map.put(key, val);
  }
  ***/

  public void addCloseHook(Closeable closeable) {
    if (closeHooks == null) {
      closeHooks = new IdentityHashMap<Closeable, String>();
      // for now, defer closing until the end of the request
      SolrRequestInfo.getRequestInfo().addCloseHook(this);
    }

    closeHooks.put(closeable, "");
  }

  public boolean removeCloseHook(Closeable closeable) {
    return closeHooks.remove(closeable) != null;
  }

  /** Don't call close explicitly!  This will be automatically closed at the end of the request */
  @Override
  public void close() throws IOException {
    if (closeHooks != null) {
      for (Closeable hook : closeHooks.keySet()) {
        try {
          hook.close();
        } catch (Exception e) {
          SolrException.log(log, "Exception during close hook", e);
        }
      }
    }

    closeHooks = null;
    // map = null;
  }

}
