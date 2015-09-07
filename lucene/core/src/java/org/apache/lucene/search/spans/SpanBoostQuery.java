package org.apache.lucene.search.spans;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;

/**
 * Counterpart of {@link BoostQuery} for spans.
 */
public final class SpanBoostQuery extends SpanQuery {

  /** By default we enclose the wrapped query within parenthesis, but this is
   *  not required for all queries, so we use a whitelist of queries that don't
   *  need parenthesis to have a better toString(). */
  private static final Set<Class<? extends SpanQuery>> NO_PARENS_REQUIRED_QUERIES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          SpanTermQuery.class,
          SpanNearQuery.class,
          SpanOrQuery.class,
          SpanFirstQuery.class,
          SpanContainingQuery.class,
          SpanContainQuery.class,
          SpanNotQuery.class,
          SpanWithinQuery.class
      )));

  private final SpanQuery query;
  private final float boost;

  /** Sole constructor: wrap {@code query} in such a way that the produced
   *  scores will be boosted by {@code boost}. */
  public SpanBoostQuery(SpanQuery query, float boost) {
    this.query = Objects.requireNonNull(query);
    this.boost = boost;
  }

  /**
   * Return the wrapped {@link SpanQuery}.
   */
  public SpanQuery getQuery() {
    return query;
  }

  /**
   * Return the applied boost.
   */
  public float getBoost() {
    return boost;
  }

  @Override
  public boolean equals(Object obj) {
    if (super.equals(obj) == false) {
      return false;
    }
    SpanBoostQuery that = (SpanBoostQuery) obj;
    return query.equals(that.query)
        && Float.floatToIntBits(boost) == Float.floatToIntBits(that.boost);
  }

  @Override
  public int hashCode() {
    int h = super.hashCode();
    h = 31 * h + query.hashCode();
    h = 31 * h + Float.floatToIntBits(boost);
    return h;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (boost == 1f) {
      return query;
    }

    final SpanQuery rewritten = (SpanQuery) query.rewrite(reader);
    if (query != rewritten) {
      return new SpanBoostQuery(rewritten, boost);
    }

    if (query.getClass() == SpanBoostQuery.class) {
      SpanBoostQuery in = (SpanBoostQuery) query;
      return new SpanBoostQuery(in.query, boost * in.boost);
    }

    return super.rewrite(reader);
  }

  @Override
  public String toString(String field) {
    boolean needsParens = NO_PARENS_REQUIRED_QUERIES.contains(query.getClass()) == false;
    StringBuilder builder = new StringBuilder();
    if (needsParens) {
      builder.append("(");
    }
    builder.append(query.toString(field));
    if (needsParens) {
      builder.append(")");
    }
    builder.append("^");
    builder.append(boost);
    return builder.toString();
  }

  @Override
  public String getField() {
    return query.getField();
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    final SpanWeight weight = query.createWeight(searcher, needsScores);
    if (needsScores == false) {
      return weight;
    }
    Map<Term, TermContext> terms = new TreeMap<>();
    weight.extractTermContexts(terms);
    weight.normalize(1f, boost);
    return new SpanWeight(this, searcher, terms) {
      
      @Override
      public void extractTerms(Set<Term> terms) {
        weight.extractTerms(terms);
      }

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return weight.explain(context, doc);
      }

      @Override
      public float getValueForNormalization() throws IOException {
        return weight.getValueForNormalization();
      }

      @Override
      public void normalize(float norm, float boost) {
        weight.normalize(norm, SpanBoostQuery.this.boost * boost);
      }
      
      @Override
      public Spans getSpans(LeafReaderContext ctx, Postings requiredPostings) throws IOException {
        return weight.getSpans(ctx, requiredPostings);
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        return weight.scorer(context);
      }

      @Override
      public void extractTermContexts(Map<Term,TermContext> contexts) {
        weight.extractTermContexts(contexts);
      }
    };
  }

}
