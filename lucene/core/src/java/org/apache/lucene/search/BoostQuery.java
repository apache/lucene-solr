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
import java.util.Objects;

import org.apache.lucene.index.IndexReader;

/**
 * A {@link Query} wrapper that allows to give a boost to the wrapped query.
 * Boost values that are less than one will give less importance to this
 * query compared to other ones while values that are greater than one will
 * give more importance to the scores returned by this query.
 *
 * More complex boosts can be applied by using FunctionScoreQuery in the
 * lucene-queries module
 */
public final class BoostQuery extends Query {

  private final Query query;
  private final float boost;

  /** Sole constructor: wrap {@code query} in such a way that the produced
   *  scores will be boosted by {@code boost}. */
  public BoostQuery(Query query, float boost) {
    this.query = Objects.requireNonNull(query);
    this.boost = boost;
  }

  /**
   * Return the wrapped {@link Query}.
   */
  public Query getQuery() {
    return query;
  }

  /**
   * Return the applied boost.
   */
  public float getBoost() {
    return boost;
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }
  
  private boolean equalsTo(BoostQuery other) {
    return query.equals(other.query) && 
           Float.floatToIntBits(boost) == Float.floatToIntBits(other.boost);
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + query.hashCode();
    h = 31 * h + Float.floatToIntBits(boost);
    return h;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query rewritten = query.rewrite(reader);

    if (boost == 1f) {
      return rewritten;
    }

    if (rewritten.getClass() == BoostQuery.class) {
      BoostQuery in = (BoostQuery) rewritten;
      return new BoostQuery(in.query, boost * in.boost);
    }

    if (boost == 0f && rewritten.getClass() != ConstantScoreQuery.class) {
      // so that we pass needScores=false
      return new BoostQuery(new ConstantScoreQuery(rewritten), 0f);
    }

    if (query != rewritten) {
      return new BoostQuery(rewritten, boost);
    }

    return super.rewrite(reader);
  }

  @Override
  public String toString(String field) {
    StringBuilder builder = new StringBuilder();
    builder.append("(");
    builder.append(query.toString(field));
    builder.append(")");
    builder.append("^");
    builder.append(boost);
    return builder.toString();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    return query.createWeight(searcher, needsScores, BoostQuery.this.boost * boost);
  }

}
