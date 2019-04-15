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

package org.apache.lucene.luwak.matchers;

import org.apache.lucene.luwak.DocumentBatch;
import org.apache.lucene.luwak.MatcherFactory;
import org.apache.lucene.luwak.QueryMatch;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

/**
 * A Matcher that records whether or not a query matches
 */
public class SimpleMatcher extends CollectingMatcher<QueryMatch> {

  private SimpleMatcher(DocumentBatch docs) {
    super(docs, ScoreMode.COMPLETE_NO_SCORES);
  }

  @Override
  public QueryMatch resolve(QueryMatch match1, QueryMatch match2) {
    return match1;
  }

  @Override
  protected QueryMatch doMatch(String queryId, String docId, Scorable scorer) {
    return new QueryMatch(queryId, docId);
  }

  /**
   * A MatcherFactory to create new SimpleMatcher instances
   */
  public static final MatcherFactory<QueryMatch> FACTORY = SimpleMatcher::new;

}
