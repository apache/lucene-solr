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

import java.util.List;
import java.util.Map;

import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.params.*;

/** For changing index settings that can be changed while
 *  the index is running. */
public class LiveSettingsHandler extends Handler {

  /** The parameters this handler accepts. */
  public final static StructType TYPE =
    new StructType(
        new Param("indexName", "The index", new StringType()),
        new Param("maxRefreshSec", "Longest time to wait before reopening IndexSearcher (i.e., periodic background reopen).", new FloatType(), 1.0f),
        new Param("minRefreshSec", "Shortest time to wait before reopening IndexSearcher (i.e., when a search is waiting for a specific indexGen).", new FloatType(), .05f),
        new Param("maxSearcherAgeSec", "Non-current searchers older than this are pruned.", new FloatType(), 60.0f),
        new Param("index.ramBufferSizeMB", "Size (in MB) of IndexWriter's RAM buffer.", new FloatType(), 16.0f)
                   );

  @Override
  public StructType getType() {
    return TYPE;
  }

  @Override
  public String getTopDoc() {
    return "Change global offline or online settings for this index.";
  }

  /** Sole constructor. */
  public LiveSettingsHandler(GlobalState state) {
    super(state);
  }

  @Override
  public FinishRequest handle(final IndexState state, Request r, Map<String,List<String>> params) throws Exception {

    // TODO: should this be done inside finish?  Ie, so it's
    // "all or no change"?
    if (r.hasParam("maxRefreshSec")) {
      state.setMaxRefreshSec(r.getFloat("maxRefreshSec"));
    }
    if (r.hasParam("minRefreshSec")) {
      state.setMinRefreshSec(r.getFloat("minRefreshSec"));
    }
    if (r.hasParam("maxSearcherAgeSec")) {
      state.setMaxSearcherAgeSec(r.getFloat("maxSearcherAgeSec"));
    }
    if (r.hasParam("index.ramBufferSizeMB")) {
      state.setIndexRAMBufferSizeMB(r.getFloat("index.ramBufferSizeMB"));
    }

    return new FinishRequest() {
      @Override
      public String finish() {
        return state.getLiveSettingsJSON();
      }
    };
  }
}
