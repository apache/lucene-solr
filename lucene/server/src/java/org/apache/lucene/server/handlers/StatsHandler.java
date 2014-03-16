package org.apache.lucene.server.handlers;

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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherLifetimeManager;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.MyIndexSearcher;
import org.apache.lucene.server.params.Param;
import org.apache.lucene.server.params.Request;
import org.apache.lucene.server.params.StringType;
import org.apache.lucene.server.params.StructType;
import org.apache.lucene.util.RamUsageEstimator;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

/** Handles {@code stats}. */
public class StatsHandler extends Handler {

  StructType TYPE = new StructType(new Param("indexName", "Index name", new StringType()));

  @Override
  public String getTopDoc() {
    return "Retrieve index statistics.";
  }

  @Override
  public StructType getType() {
    return TYPE;
  }

  /** Sole constructor. */
  public StatsHandler(GlobalState state) {
    super(state);
  }

  @Override
  public FinishRequest handle(final IndexState state, final Request r, Map<String,List<String>> params) throws Exception {
    return new FinishRequest() {
      @Override
      public String finish() throws IOException {
        JSONObject result = new JSONObject();
        final JSONObject searchers = new JSONObject();
        result.put("searchers", searchers);

        // TODO: snapshots

        // TODO: go per segment and print more details, and
        // only print segment for a given searcher if it's
        // "new"

        // Doesn't actually prune; just gathers stats
        state.slm.prune(new SearcherLifetimeManager.Pruner() {
            @Override
            public boolean doPrune(double ageSec, IndexSearcher searcher) {
              JSONObject s = new JSONObject();
              searchers.put(Long.toString(((DirectoryReader) searcher.getIndexReader()).getVersion()), s);
              s.put("staleAgeSeconds", ageSec);
              s.put("segments", searcher.getIndexReader().toString());
              return false;
            }
          });

        JSONObject taxo = new JSONObject();
        result.put("taxonomy", taxo);
        
        SearcherAndTaxonomy s = state.manager.acquire();
        try {
          taxo.put("segments", s.taxonomyReader.toString());
          taxo.put("numOrds", s.taxonomyReader.getSize());
        } finally {
          state.manager.release(s);
        }

        JSONArray cachedFilters = new JSONArray();
        result.put("cachedFilters", cachedFilters);
        for(Map.Entry<String,CachingWrapperFilter> ent : state.cachedFilters.entrySet()) {
          JSONObject c = new JSONObject();
          cachedFilters.add(c);
          c.put("id", ent.getKey());
          c.put("bytes", ent.getValue().sizeInBytes());
        }

        return result.toString();
      }
    };
  }
}
