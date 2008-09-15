package org.apache.lucene.benchmark.stats;
/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Vector;

import org.apache.lucene.search.Query;
import org.apache.lucene.benchmark.Constants;

/**
 * This class holds parameters for a query benchmark.
 *
 */
public class QueryData {
  /** Benchmark id */
  public String id;
  /** Lucene query */
  public Query q;
  /** If true, re-open index reader before benchmark. */
  public boolean reopen;
  /** If true, warm-up the index reader before searching by sequentially
   * retrieving all documents from index.
   */
  public boolean warmup;
  /**
   * If true, actually retrieve documents returned in Hits.
   */
  public boolean retrieve;

  /**
   * Prepare a list of benchmark data, using all possible combinations of
   * benchmark parameters.
   * @param queries source Lucene queries
   * @return The QueryData
   */
  public static QueryData[] getAll(Query[] queries) {
    Vector vqd = new Vector();
    for (int i = 0; i < queries.length; i++) {
      for (int r = 1; r >= 0; r--) {
        for (int w = 1; w >= 0; w--) {
          for (int t = 0; t < 2; t++) {
            QueryData qd = new QueryData();
            qd.id="qd-" + i + r + w + t;
            qd.reopen = Constants.BOOLEANS[r].booleanValue();
            qd.warmup = Constants.BOOLEANS[w].booleanValue();
            qd.retrieve = Constants.BOOLEANS[t].booleanValue();
            qd.q = queries[i];
            vqd.add(qd);
          }
        }
      }
    }
    return (QueryData[])vqd.toArray(new QueryData[0]);
  }

  /** Short legend for interpreting toString() output. */
  public static String getLabels() {
    return "# Query data: R-reopen, W-warmup, T-retrieve, N-no";
  }

  public String toString() {
    return id + " " + (reopen ? "R" : "NR") + " " + (warmup ? "W" : "NW") +
      " " + (retrieve ? "T" : "NT") + " [" + q.toString() + "]";
  }
}
