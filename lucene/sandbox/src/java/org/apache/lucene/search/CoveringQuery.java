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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

/** A {@link Query} that allows to have a configurable number or required
 *  matches per document. This is typically useful in order to build queries
 *  whose query terms must all appear in documents.
 *  @lucene.experimental
 */
public final class CoveringQuery extends Query implements Accountable {
  private static final long BASE_RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(CoveringQuery.class);

  private final Collection<Query> queries;
  private final LongValuesSource minimumNumberMatch;
  private final int hashCode;
  private final long ramBytesUsed;

  /**
   * Sole constructor.
   * @param queries Sub queries to match.
   * @param minimumNumberMatch Per-document long value that records how many queries
   *                           should match. Values that are less than 1 are treated
   *                           like <tt>1</tt>: only documents that have at least one
   *                           matching clause will be considered matches. Documents
   *                           that do not have a value for <tt>minimumNumberMatch</tt>
   *                           do not match.
   */
  public CoveringQuery(Collection<Query> queries, LongValuesSource minimumNumberMatch) {
    if (queries.size() > IndexSearcher.getMaxClauseCount()) {
      throw new IndexSearcher.TooManyClauses();
    }
    if (minimumNumberMatch.needsScores()) {
      throw new IllegalArgumentException("The minimum number of matches may not depend on the score.");
    }
    this.queries = new Multiset<>();
    this.queries.addAll(queries);
    this.minimumNumberMatch = Objects.requireNonNull(minimumNumberMatch);
    this.hashCode = computeHashCode();

    this.ramBytesUsed = BASE_RAM_BYTES +
        RamUsageEstimator.sizeOfObject(this.queries, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED);
  }

  @Override
  public String toString(String field) {
    String queriesToString = queries.stream()
        .map(q -> q.toString(field))
        .sorted()
        .collect(Collectors.joining(", "));
    return "CoveringQuery(queries=[" + queriesToString + "], minimumNumberMatch=" + minimumNumberMatch + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    CoveringQuery that = (CoveringQuery) obj;
    return hashCode == that.hashCode // not necessary but makes equals faster
        && Objects.equals(queries, that.queries)
        && Objects.equals(minimumNumberMatch, that.minimumNumberMatch);
  }

  private int computeHashCode() {
    int h = classHash();
    h = 31 * h + queries.hashCode();
    h = 31 * h + minimumNumberMatch.hashCode();
    return h;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Multiset<Query> rewritten = new Multiset<>();
    boolean actuallyRewritten = false;
    for (Query query : queries) {
      Query r = query.rewrite(reader);
      rewritten.add(r);
      actuallyRewritten |= query != r;
    }
    if (actuallyRewritten) {
      return new CoveringQuery(rewritten, minimumNumberMatch);
    }
    return super.rewrite(reader);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.SHOULD, this);
    for (Query query : queries) {
      query.visit(v);
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    final List<Weight> weights = new ArrayList<>(queries.size());
    for (Query query : queries) {
      weights.add(searcher.createWeight(query, scoreMode, boost));
    }
    return new CoveringWeight(this, weights, minimumNumberMatch.rewrite(searcher));
  }

  private static class CoveringWeight extends Weight {

    private final Collection<Weight> weights;
    private final LongValuesSource minimumNumberMatch;

    CoveringWeight(Query query, Collection<Weight> weights, LongValuesSource minimumNumberMatch) {
      super(query);
      this.weights = weights;
      this.minimumNumberMatch = minimumNumberMatch;
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      LongValues minMatchValues = minimumNumberMatch.getValues(context, null);
      if (minMatchValues.advanceExact(doc) == false) {
        return null;
      }
      final long minimumNumberMatch = Math.max(1, minMatchValues.longValue());
      long matchCount = 0;
      List<Matches> subMatches = new ArrayList<>();
      for (Weight weight : weights) {
        Matches matches = weight.matches(context, doc);
        if (matches != null) {
          matchCount++;
          subMatches.add(matches);
        }
      }
      if (matchCount < minimumNumberMatch) {
        return null;
      }
      return MatchesUtils.fromSubMatches(subMatches);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      LongValues minMatchValues = minimumNumberMatch.getValues(context, null);
      if (minMatchValues.advanceExact(doc) == false) {
        return Explanation.noMatch("minimumNumberMatch has no value on the current document");
      }
      final long minimumNumberMatch = Math.max(1, minMatchValues.longValue());
      int freq = 0;
      double score = 0;
      List<Explanation> subExpls = new ArrayList<>();
      for (Weight weight : weights) {
        Explanation subExpl = weight.explain(context, doc);
        if (subExpl.isMatch()) {
          freq++;
          score += subExpl.getValue().doubleValue();
        }
        subExpls.add(subExpl);
      }
      if (freq >= minimumNumberMatch) {
        return Explanation.match((float) score, freq + " matches for " + minimumNumberMatch + " required matches, sum of:", subExpls);
      } else {
        return Explanation.noMatch(freq + " matches for " + minimumNumberMatch + " required matches", subExpls);
      }
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      Collection<Scorer> scorers = new ArrayList<>();
      for (Weight w : weights) {
        Scorer s = w.scorer(context);
        if (s != null) {
          scorers.add(s);
        }
      }
      if (scorers.isEmpty()) {
        return null;
      }
      return new CoveringScorer(this, scorers, minimumNumberMatch.getValues(context, null), context.reader().maxDoc());
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return minimumNumberMatch.isCacheable(ctx);
    }

  }

}
