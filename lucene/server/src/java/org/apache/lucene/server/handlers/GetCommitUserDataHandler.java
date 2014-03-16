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
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.params.*;
import net.minidev.json.JSONObject;

/** Handles {@code getCommitUserData}. */
public class GetCommitUserDataHandler extends Handler {

  private static StructType TYPE = new StructType(
                                       new Param("indexName", "Index name", new StringType()),
                                       new Param("searcher", "Specific searcher version to use for retrieving the commit userData; if this is missing, the current IndexWriter's commitData is returned.",
                                                 SearchHandler.SEARCHER_VERSION_TYPE));

  /** Sole constructor. */
  public GetCommitUserDataHandler(GlobalState state) {
    super(state);
  }

  @Override
  public StructType getType() {
    return TYPE;
  }

  @Override
  public String getTopDoc() {
    return "Gets the custom user data in the index, previously set with setCommitUserData.";
  }
  
  @Override
  public FinishRequest handle(final IndexState state, final Request r, Map<String,List<String>> params) throws Exception {

    return new FinishRequest() {
      @Override
      public String finish() throws IOException, InterruptedException {
        long searcherVersion;
        Map<String,String> userData;
        if (r.hasParam("searcher")) {
          // Specific searcher version:
          SearcherAndTaxonomy s = SearchHandler.getSearcherAndTaxonomy(r, state, null);
          try {
            DirectoryReader dr = (DirectoryReader) s.searcher.getIndexReader();
            searcherVersion = dr.getVersion();
            IndexCommit commit = dr.getIndexCommit();
            userData = commit.getUserData();
          } finally {
            state.manager.release(s);
          }

        } else {
          // Just use current IndexWriter:
          searcherVersion = -1;
          state.verifyStarted(r);
          userData = state.writer.getIndexWriter().getCommitData();
        }

        JSONObject result = new JSONObject();
        result.put("searcher", searcherVersion);
        JSONObject data = new JSONObject();
        result.put("userData", data);
        for(Map.Entry<String,String> ent : userData.entrySet()) {
          data.put(ent.getKey(), ent.getValue());
        }
        return data.toString();
      }
    };
  }
}
