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

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.luwak.CandidateMatcher;
import org.apache.lucene.luwak.DocumentBatch;
import org.apache.lucene.luwak.MatcherFactory;
import org.apache.lucene.luwak.util.ForceNoBulkScoringQuery;
import org.apache.lucene.luwak.util.RewriteException;
import org.apache.lucene.luwak.util.SpanExtractor;
import org.apache.lucene.luwak.util.SpanRewriter;

/**
 * CandidateMatcher class that will return exact hit positions for all matching queries
 * <p>
 * If a stored query cannot be rewritten so as to extract Spans, a {@link HighlightsMatch} object
 * with no Hit positions will be returned.
 */

public class HighlightingMatcher extends CandidateMatcher<HighlightsMatch> {

  private final SpanRewriter rewriter;

  /**
   * Create a new HighlightingMatcher for a provided DocumentBatch, using a SpanRewriter
   *
   * @param docs     the batch to match
   * @param rewriter the SpanRewriter to use
   */
  public HighlightingMatcher(DocumentBatch docs, SpanRewriter rewriter) {
    super(docs);
    this.rewriter = rewriter;
  }

  @Override
  protected void doMatchQuery(String queryId, Query matchQuery, Map<String, String> metadata) throws IOException {
    HighlightsMatch match = doMatch(queryId, matchQuery);
    if (match != null)
      this.addMatch(match);
  }

  @Override
  public HighlightsMatch resolve(HighlightsMatch match1, HighlightsMatch match2) {
    return HighlightsMatch.merge(match1.getQueryId(), match1.getDocId(), match1, match2);
  }

  protected class HighlightCollector implements SpanCollector {

    HighlightsMatch match;
    final String queryId;

    public HighlightCollector(String queryId) {
      this.queryId = queryId;
    }

    void setMatch(int doc) {
      if (this.match != null)
        addMatch(this.match);
      this.match = new HighlightsMatch(queryId, docs.resolveDocId(doc));
    }

    @Override
    public void collectLeaf(PostingsEnum postings, int position, Term term) throws IOException {
      match.addHit(term.field(), position, position, postings.startOffset(), postings.endOffset());
    }

    @Override
    public void reset() {

    }
  }

  /**
   * Find the highlights for a specific query
   */
  protected HighlightsMatch findHighlights(String queryId, Query query) throws IOException {

    final HighlightCollector collector = new HighlightCollector(queryId);

    assert query instanceof ForceNoBulkScoringQuery;
    docs.getSearcher().search(query, new SimpleCollector() {

      Scorable scorer;

      @Override
      public void collect(int i) throws IOException {
        try {
          collector.setMatch(i);
          SpanExtractor.collect(scorer, collector, true);
        } catch (Exception e) {
          collector.match.error = e;
        }
      }

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        this.scorer = scorer;
      }

      @Override
      public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE;
      }
    });

    return collector.match;
  }

  protected HighlightsMatch doMatch(String queryId, Query query) throws IOException {
    IndexSearcher searcher = docs.getSearcher();
    if (searcher.count(query) == 0)
      return null;
    try {
      Query rewritten = rewriter.rewrite(query, searcher);
      return findHighlights(queryId, rewritten);
    } catch (RewriteException e) {
      return fallback(queryId, query, e);
    }
  }

  // if we can't extract highlights because of a rewrite exception, just report matches with no hits
  protected HighlightsMatch fallback(String queryId, Query query, RewriteException e) throws IOException {
    final HighlightCollector collector = new HighlightCollector(queryId);
    docs.getSearcher().search(query, new SimpleCollector() {
      @Override
      public void collect(int i) throws IOException {
        collector.setMatch(i);
      }

      @Override
      public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
      }
    });
    collector.match.error = e;
    return collector.match;
  }

  public static final MatcherFactory<HighlightsMatch> FACTORY = docs1 -> new HighlightingMatcher(docs1, new SpanRewriter());

  public static MatcherFactory<HighlightsMatch> factory(final SpanRewriter rewriter) {
    return docs1 -> new HighlightingMatcher(docs1, rewriter);
  }

}
