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

import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.handlers.BuildSuggestHandler.LookupHighlightFragment;
import org.apache.lucene.server.params.*;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

/** Handles {@code suggestLookup}. */
public class SuggestLookupHandler extends Handler {

  private final static StructType TYPE =
    new StructType(
        new Param("indexName", "Index name", new StringType()),
        new Param("suggestName", "Which suggester to use.", new StringType()),
        new Param("text", "Text to suggest from.", new StringType()),
        new Param("highlight", "True if the suggestions should be highlighted (currently only works with AnalyzingInfixSuggester).", new BooleanType(), true),
        new Param("allTermsRequired", "If true then all terms must be found (this only applies to InfixSuggester currently).",
                  new BooleanType(), false),
        new Param("count", "How many suggestions to return.", new IntType(), 5));

  @Override
  public StructType getType() {
    return TYPE;
  }

  @Override
  public String getTopDoc() {
    return "Perform an auto-suggest lookup.";
  }

  /** Sole constructor. */
  public SuggestLookupHandler(GlobalState state) {
    super(state);
  }

  @SuppressWarnings("unchecked")
  @Override
  public FinishRequest handle(final IndexState state, final Request r, Map<String,List<String>> params) throws Exception {
    String suggestName = r.getString("suggestName");
    final Lookup lookup = state.suggesters.get(suggestName);
    if (lookup == null) {
      r.fail("suggestName", "this suggester (\"" + suggestName + "\") was not yet built; valid suggestNames: " + state.suggesters.keySet());
    }
    final String text = r.getString("text");
    final int count = r.getInt("count");
    final boolean allTermsRequired = r.getBoolean("allTermsRequired");
    final boolean highlight = r.getBoolean("highlight");

    return new FinishRequest() {
      @Override
      public String finish() throws IOException {
        List<LookupResult> results;
        if (lookup instanceof AnalyzingInfixSuggester) {
          results = ((AnalyzingInfixSuggester) lookup).lookup(text, count, allTermsRequired, highlight);
        } else {
          results = lookup.lookup(text, false, count);
        }
        JSONObject ret = new JSONObject();
        JSONArray retArray = new JSONArray();
        ret.put("results", retArray);
        for(LookupResult result : results) {
          JSONObject o = new JSONObject();
          retArray.add(o);
          if (result.highlightKey != null) {
            // Currently only AnalyzingInfixSuggester does highlighting:
            o.put("key", result.highlightKey);
          } else {
            o.put("key", result.key);
          }
          o.put("weight", result.value);
          if (result.payload != null) {
            o.put("payload", result.payload.utf8ToString());
          }
        }
    
        return ret.toString();
      }
    };
  }
}
