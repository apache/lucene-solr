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

package org.apache.lucene.monitor;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.search.Query;

/**
 * Notified of the time it takes to run individual queries against a set of documents
 */
public interface QueryTimeListener {

  /**
   * How long it took to run a particular query
   */
  void logQueryTime(String queryId, long timeInNanos);

  /**
   * A wrapping matcher factory to log query times to a QueryTimeListener
   * @param factory   a matcher factory to use for the actual matching
   * @param listener  the QueryTimeListener
   */
  static <T extends QueryMatch> MatcherFactory<T> timingMatcher(MatcherFactory<T> factory, QueryTimeListener listener) {
    return searcher -> {
      CandidateMatcher<T> matcher = factory.createMatcher(searcher);
      return new CandidateMatcher<T>(searcher) {
        @Override
        protected void matchQuery(String queryId, Query matchQuery, Map<String, String> metadata) throws IOException {
          long t = System.nanoTime();
          matcher.matchQuery(queryId, matchQuery, metadata);
          t = System.nanoTime() - t;
          listener.logQueryTime(queryId, t);
        }

        @Override
        public T resolve(T match1, T match2) {
          return matcher.resolve(match1, match2);
        }

        @Override
        protected void doFinish() {
          copyMatches(matcher);
        }
      };
    };
  }
}
