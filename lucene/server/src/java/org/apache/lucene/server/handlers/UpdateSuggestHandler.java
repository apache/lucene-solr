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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.FromFileTermFreqIterator;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.params.Param;
import org.apache.lucene.server.params.Request;
import org.apache.lucene.server.params.StringType;
import org.apache.lucene.server.params.StructType;
import org.apache.lucene.util.BytesRef;
import net.minidev.json.JSONObject;

/** Handles {@code updateSuggest}. */

public class UpdateSuggestHandler extends Handler {
  private final static StructType TYPE =
    new StructType(
        new Param("indexName", "Index Name", new StringType()),
        new Param("suggestName", "Which suggester to update.", new StringType()),
        // nocommit option to stream suggestions in over the wire too
        // nocommit also allow pulling from index/expressions, like BuildSuggest:
        new Param("source", "Where to get suggestions from",
            new StructType(
                new Param("localFile", "Local file (to the server) to read suggestions + weights from; format is weight U+001F suggestion U+001F payload, one per line, with suggestion UTF-8 encoded.  If this option is used then searcher, suggestField, weightField/Expression, payloadField should not be specified.", new StringType()))));

  /** Sole constructor. */
  public UpdateSuggestHandler(GlobalState state) {
    super(state);
  }
  
  @Override
  public StructType getType() {
    return TYPE;
  }

  @Override
  public String getTopDoc() {
    return "Updates existing suggestions, if the suggester supports near-real-time changes.";
  }

  @SuppressWarnings("unchecked")
  @Override
  public FinishRequest handle(final IndexState state, final Request r, Map<String,List<String>> params) throws Exception {
    final String suggestName = r.getString("suggestName");
    Lookup lookup = state.suggesters.get(suggestName);
    if (lookup == null) {
      r.fail("suggestName", "this suggester (\"" + suggestName + "\") was not yet built; valid suggestNames: " + state.suggesters.keySet());
    }
    if ((lookup instanceof AnalyzingInfixSuggester) == false) {
      r.fail("suggestName", "can only update AnalyzingInfixSuggester; got " + lookup);
    }

    final AnalyzingInfixSuggester lookup2 = (AnalyzingInfixSuggester) lookup;

    Request r2 = r.getStruct("source");
    final File localFile = new File(r2.getString("localFile"));

    return new FinishRequest() {

      @Override
      public String finish() throws IOException {

        InputIterator iterator = new FromFileTermFreqIterator(localFile);
        boolean hasPayloads = iterator.hasPayloads();
        int count = 0;
        while (true) {
          BytesRef term = iterator.next();
          if (term == null) {
            break;
          }

          lookup2.update(term, iterator.weight(), hasPayloads ? iterator.payload() : null);
          count++;
        }

        lookup2.refresh();

        JSONObject ret = new JSONObject();
        ret.put("count", count);
        return ret.toString();
      }
    };
  }
}

