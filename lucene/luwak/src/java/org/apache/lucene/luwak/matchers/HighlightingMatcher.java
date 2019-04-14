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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.luwak.CandidateMatcher;
import org.apache.lucene.luwak.DocumentBatch;
import org.apache.lucene.luwak.MatcherFactory;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/**
 * CandidateMatcher class that will return exact hit positions for all matching queries
 */

public class HighlightingMatcher extends CandidateMatcher<HighlightsMatch> {

  /**
   * Create a new HighlightingMatcher for a provided DocumentBatch, using a SpanRewriter
   *
   * @param docs     the batch to match
   */
  public HighlightingMatcher(DocumentBatch docs) {
    super(docs);
  }

  @Override
  protected void doMatchQuery(String queryId, Query matchQuery, Map<String, String> metadata) throws IOException {
    IndexSearcher searcher = docs.getSearcher();
    Weight w = searcher.createWeight(searcher.rewrite(matchQuery), ScoreMode.COMPLETE_NO_SCORES, 1);
    for (LeafReaderContext ctx : docs.getIndexReader().leaves()) {
      for (int i = 0; i < ctx.reader().maxDoc(); i++) {
        org.apache.lucene.search.Matches matches = w.matches(ctx, i);
        if (matches != null) {
          addMatch(buildMatch(matches, queryId, docs.resolveDocId(i + ctx.docBase)));
        }
      }
    }
  }

  @Override
  public HighlightsMatch resolve(HighlightsMatch match1, HighlightsMatch match2) {
    return HighlightsMatch.merge(match1.getQueryId(), match1.getDocId(), match1, match2);
  }

  private HighlightsMatch buildMatch(org.apache.lucene.search.Matches matches, String queryId, String docId) throws IOException {
    HighlightsMatch m = new HighlightsMatch(queryId, docId);
    for (String field : matches) {
      MatchesIterator mi = matches.getMatches(field);
      while (mi.next()) {
        MatchesIterator sub = mi.getSubMatches();
        if (sub != null) {
          while (sub.next()) {
            m.addHit(field, sub.startPosition(), sub.endPosition(), sub.startOffset(), sub.endOffset());
          }
        }
        else {
          m.addHit(field, mi.startPosition(), mi.endPosition(), mi.startOffset(), mi.endOffset());
        }
      }
    }
    return m;
  }

  public static final MatcherFactory<HighlightsMatch> FACTORY = HighlightingMatcher::new;

}
