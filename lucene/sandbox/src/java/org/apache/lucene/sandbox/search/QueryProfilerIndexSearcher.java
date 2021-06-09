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

package org.apache.lucene.sandbox.search;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/**
 * An extension of {@link IndexSearcher} that records profile information for all queries it
 * executes.
 */
public class QueryProfilerIndexSearcher extends IndexSearcher {

  private final QueryProfilerTree profiler;

  public QueryProfilerIndexSearcher(IndexReader reader) {
    super(reader);
    profiler = new QueryProfilerTree();
  }

  @Override
  public Query rewrite(Query original) throws IOException {
    profiler.startRewriteTime();
    try {
      return super.rewrite(original);
    } finally {
      profiler.stopAndAddRewriteTime();
    }
  }

  @Override
  public Weight createWeight(Query query, ScoreMode scoreMode, float boost) throws IOException {
    // createWeight() is called for each query in the tree, so we tell the queryProfiler
    // each invocation so that it can build an internal representation of the query
    // tree
    QueryProfilerBreakdown profile = profiler.getProfileBreakdown(query);
    QueryProfilerTimer timer = profile.getTimer(QueryProfilerTimingType.CREATE_WEIGHT);
    timer.start();
    final Weight weight;
    try {
      weight = query.createWeight(this, scoreMode, boost);
    } finally {
      timer.stop();
      profiler.pollLast();
    }
    return new QueryProfilerWeight(query, weight, profile);
  }

  /** @return total time taken to rewrite all queries in this profile */
  public long getRewriteTime() {
    return profiler.getRewriteTime();
  }

  /** @return a hierarchical representation of the profiled tree */
  public List<QueryProfilerResult> getProfileResult() {
    return profiler.getTree();
  }
}
