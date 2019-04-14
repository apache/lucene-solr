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

package org.apache.lucene.luwak.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRef;

public class SpanRewriter {

  public static final SpanRewriter INSTANCE = new SpanRewriter();

  public Query rewrite(Query in, IndexSearcher searcher) throws RewriteException, IOException {
    if (in instanceof SpanOffsetReportingQuery)
      return in;
    if (in instanceof SpanQuery)
      return forceOffsets((SpanQuery) in);
    if (in instanceof ForceNoBulkScoringQuery) {
      return new ForceNoBulkScoringQuery(rewrite(((ForceNoBulkScoringQuery) in).getWrappedQuery(), searcher));
    }
    if (in instanceof TermQuery)
      return rewriteTermQuery((TermQuery) in);
    if (in instanceof BooleanQuery)
      return rewriteBoolean((BooleanQuery) in, searcher);
    if (in instanceof MultiTermQuery)
      return rewriteMultiTermQuery((MultiTermQuery) in);
    if (in instanceof DisjunctionMaxQuery)
      return rewriteDisjunctionMaxQuery((DisjunctionMaxQuery) in, searcher);
    if (in instanceof TermInSetQuery)
      return rewriteTermInSetQuery((TermInSetQuery) in);
    if (in instanceof BoostQuery)
      return rewrite(((BoostQuery) in).getQuery(), searcher);   // we don't care about boosts for rewriting purposes
    if (in instanceof PhraseQuery)
      return rewritePhraseQuery((PhraseQuery) in);
    if (in instanceof ConstantScoreQuery)
      return rewrite(((ConstantScoreQuery) in).getQuery(), searcher);
    if (searcher != null) {
      return rewrite(searcher.rewrite(in), null);
    }

    return rewriteUnknown(in);
  }

  protected final SpanQuery forceOffsets(SpanQuery in) {
    return new SpanOffsetReportingQuery(in);
  }

  protected Query rewriteTermQuery(TermQuery tq) {
    return forceOffsets(new SpanTermQuery(tq.getTerm()));
  }

  protected Query rewriteBoolean(BooleanQuery bq, IndexSearcher searcher) throws RewriteException, IOException {
    BooleanQuery.Builder newbq = new BooleanQuery.Builder();
    newbq.setMinimumNumberShouldMatch(bq.getMinimumNumberShouldMatch());
    for (BooleanClause clause : bq) {
      BooleanClause.Occur occur = clause.getOccur();
      if (occur == BooleanClause.Occur.FILTER)
        occur = BooleanClause.Occur.MUST;   // rewrite FILTER to MUST to ensure scoring
      newbq.add(rewrite(clause.getQuery(), searcher), occur);
    }
    return newbq.build();
  }

  protected Query rewriteMultiTermQuery(MultiTermQuery mtq) {
    return forceOffsets(new SpanMultiTermQueryWrapper<>(mtq));
  }

  protected Query rewriteDisjunctionMaxQuery(DisjunctionMaxQuery disjunctionMaxQuery, IndexSearcher searcher)
      throws RewriteException, IOException {
    ArrayList<Query> subQueries = new ArrayList<>();
    for (Query subQuery : disjunctionMaxQuery) {
      subQueries.add(rewrite(subQuery, searcher));
    }
    return new DisjunctionMaxQuery(subQueries, disjunctionMaxQuery.getTieBreakerMultiplier());
  }

  protected Query rewriteTermInSetQuery(TermInSetQuery query) throws RewriteException {
    Map<String, List<SpanTermQuery>> spanQueries = new HashMap<>();
    try {
      PrefixCodedTerms terms = query.getTermData();
      PrefixCodedTerms.TermIterator it = terms.iterator();
      for (int i = 0; i < terms.size(); i++) {
        BytesRef term = BytesRef.deepCopyOf(it.next());
        List<SpanTermQuery> termQueryList = spanQueries.computeIfAbsent(it.field(), f -> new ArrayList<>());
        termQueryList.add(new SpanTermQuery(new Term(it.field(), term)));
      }
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      for (Map.Entry<String, List<SpanTermQuery>> entry : spanQueries.entrySet()) {
        List<SpanTermQuery> termQueries = entry.getValue();
        builder.add(new SpanOrQuery(termQueries.toArray(new SpanTermQuery[termQueries.size()])),
            BooleanClause.Occur.SHOULD);
      }
      return builder.build();
    } catch (Exception e) {
      throw new RewriteException("Error rewriting query: " + e.getMessage(), query);
    }
  }

  protected Query rewriteUnknown(Query query) throws RewriteException {
    throw new RewriteException("Don't know how to rewrite " + query.getClass(), query);
  }

  /*
   * This method is only able to rewrite standard phrases where each word must follow the previous one
   * with no gaps or overlaps.  This does however cover all common uses (such as "amazing horse").
   */
  protected Query rewritePhraseQuery(PhraseQuery query) throws RewriteException {
    Term[] terms = query.getTerms();
    int[] positions = query.getPositions();
    SpanTermQuery[] spanQueries = new SpanTermQuery[positions.length];

    for (int i = 0; i < positions.length; i++) {
      if (positions[i] - positions[0] != i) {
        // positions must increase by 1 each time (i-1 is safe as the if can't be true for i=0)
        throw new RewriteException("Don't know how to rewrite PhraseQuery with holes or overlaps " +
            "(position must increase by 1 each time but found term " + terms[i - 1] + " at position " +
            positions[i - 1] + " followed by term " + terms[i] + " at position " + positions[i] + ")", query);
      }

      spanQueries[i] = new SpanTermQuery(terms[i]);
    }

    return forceOffsets(new SpanNearQuery(spanQueries, query.getSlop(), true));
  }

}
