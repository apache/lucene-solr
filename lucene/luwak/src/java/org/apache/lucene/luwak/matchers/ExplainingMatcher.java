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

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.luwak.CandidateMatcher;
import org.apache.lucene.luwak.MatcherFactory;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

/**
 * Return {@link Explanation}s for each match
 */
public class ExplainingMatcher extends CandidateMatcher<ExplainingMatch> {

  /**
   * A factory for ExplainingMatchers
   */
  public static final MatcherFactory<ExplainingMatch> FACTORY = ExplainingMatcher::new;

  private ExplainingMatcher(IndexSearcher searcher) {
    super(searcher);
  }

  @Override
  public void doMatchQuery(String queryId, Query matchQuery, Map<String, String> metadata) throws IOException {
    int maxDocs = searcher.getIndexReader().maxDoc();
    for (int i = 0; i < maxDocs; i++) {
      Explanation explanation = searcher.explain(matchQuery, i);
      if (explanation.isMatch())
        addMatch(new ExplainingMatch(queryId, explanation), i);
    }
  }

  @Override
  public ExplainingMatch resolve(ExplainingMatch match1, ExplainingMatch match2) {
    return new ExplainingMatch(match1.getQueryId(),
        Explanation.match(match1.getExplanation().getValue().doubleValue() + match2.getExplanation().getValue().doubleValue(),
            "sum of:", match1.getExplanation(), match2.getExplanation()));
  }
}
