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
package org.apache.solr.search;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;

import java.util.Collection;

/**
 *
 */
public class QueryUtils {

  /** return true if this query has no positive components */
  public static boolean isNegative(Query q) {
    if (!(q instanceof BooleanQuery)) return false;
    BooleanQuery bq = (BooleanQuery)q;
    Collection<BooleanClause> clauses = bq.clauses();
    if (clauses.size()==0) return false;
    for (BooleanClause clause : clauses) {
      if (!clause.isProhibited()) return false;
    }
    return true;
  }

  /** Returns the original query if it was already a positive query, otherwise
   * return the negative of the query (i.e., a positive query).
   * <p>
   * Example: both id:10 and id:-10 will return id:10
   * <p>
   * The caller can tell the sign of the original by a reference comparison between
   * the original and returned query.
   * @param q Query to create the absolute version of
   * @return Absolute version of the Query
   */
  public static Query getAbs(Query q) {
    if (q instanceof BoostQuery) {
      BoostQuery bq = (BoostQuery) q;
      Query subQ = bq.getQuery();
      Query absSubQ = getAbs(subQ);
      if (absSubQ == subQ) return q;
      return new BoostQuery(absSubQ, bq.getBoost());
    }

    if (q instanceof WrappedQuery) {
      Query subQ = ((WrappedQuery)q).getWrappedQuery();
      Query absSubQ = getAbs(subQ);
      if (absSubQ == subQ) return q;
      return new WrappedQuery(absSubQ);
    }

    if (!(q instanceof BooleanQuery)) return q;
    BooleanQuery bq = (BooleanQuery)q;

    Collection<BooleanClause> clauses = bq.clauses();
    if (clauses.size()==0) return q;


    for (BooleanClause clause : clauses) {
      if (!clause.isProhibited()) return q;
    }

    if (clauses.size()==1) {
      // if only one clause, dispense with the wrapping BooleanQuery
      Query negClause = clauses.iterator().next().getQuery();
      // we shouldn't need to worry about adjusting the boosts since the negative
      // clause would have never been selected in a positive query, and hence would
      // not contribute to a score.
      return negClause;
    } else {
      BooleanQuery.Builder newBqB = new BooleanQuery.Builder();
      // ignore minNrShouldMatch... it doesn't make sense for a negative query

      // the inverse of -a -b is a OR b
      for (BooleanClause clause : clauses) {
        newBqB.add(clause.getQuery(), BooleanClause.Occur.SHOULD);
      }
      return newBqB.build();
    }
  }

  /** Makes negative queries suitable for querying by
   * lucene.
   */
  public static Query makeQueryable(Query q) {
    if (q instanceof WrappedQuery) {
      return makeQueryable(((WrappedQuery)q).getWrappedQuery());
    }
    return isNegative(q) ? fixNegativeQuery(q) : q;
  }

  /** Fixes a negative query by adding a MatchAllDocs query clause.
   * The query passed in *must* be a negative query.
   */
  public static Query fixNegativeQuery(Query q) {
    float boost = 1f;
    if (q instanceof BoostQuery) {
      BoostQuery bq = (BoostQuery) q;
      boost = bq.getBoost();
      q = bq.getQuery();
    }
    BooleanQuery bq = (BooleanQuery) q;
    BooleanQuery.Builder newBqB = new BooleanQuery.Builder();
    newBqB.setMinimumNumberShouldMatch(bq.getMinimumNumberShouldMatch());
    for (BooleanClause clause : bq) {
      newBqB.add(clause);
    }
    newBqB.add(new MatchAllDocsQuery(), Occur.MUST);
    BooleanQuery newBq = newBqB.build();
    return new BoostQuery(newBq, boost);
  }

  /** @lucene.experimental throw exception if max boolean clauses are exceeded */
  public static BooleanQuery build(BooleanQuery.Builder builder, QParser parser) {
    int configuredMax = parser != null ? parser.getReq().getCore().getSolrConfig().booleanQueryMaxClauseCount : IndexSearcher.getMaxClauseCount();
    BooleanQuery bq = builder.build();
    if (bq.clauses().size() > configuredMax) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Too many clauses in boolean query: encountered=" + bq.clauses().size() + " configured in solrconfig.xml via maxBooleanClauses=" + configuredMax);
    }
    return bq;
  }
}
