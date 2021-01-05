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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;

/**
 * Split a disjunction query into its consituent parts, so that they can be indexed and run
 * separately in the Monitor.
 */
public class QueryDecomposer {

  /**
   * Split a query up into individual parts that can be indexed and run separately
   *
   * @param q the query
   * @return a collection of subqueries
   */
  public Set<Query> decompose(Query q) {

    if (q instanceof BooleanQuery) return decomposeBoolean((BooleanQuery) q);

    if (q instanceof DisjunctionMaxQuery) {
      Set<Query> subqueries = new HashSet<>();
      for (Query subq : ((DisjunctionMaxQuery) q).getDisjuncts()) {
        subqueries.addAll(decompose(subq));
      }
      return subqueries;
    }

    if (q instanceof BoostQuery) {
      return decomposeBoostQuery((BoostQuery) q);
    }

    return Collections.singleton(q);
  }

  public Set<Query> decomposeBoostQuery(BoostQuery q) {
    if (q.getBoost() == 1.0) return decompose(q.getQuery());

    Set<Query> boostedDecomposedQueries = new HashSet<>();
    for (Query subq : decompose(q.getQuery())) {
      boostedDecomposedQueries.add(new BoostQuery(subq, q.getBoost()));
    }
    return boostedDecomposedQueries;
  }

  /**
   * Decompose a {@link org.apache.lucene.search.BooleanQuery}
   *
   * @param q the boolean query
   * @return a collection of subqueries
   */
  public Set<Query> decomposeBoolean(BooleanQuery q) {
    if (q.getMinimumNumberShouldMatch() > 1) return Collections.singleton(q);

    Set<Query> subqueries = new HashSet<>();
    Set<Query> exclusions = new HashSet<>();
    Set<Query> mandatory = new HashSet<>();

    for (BooleanClause clause : q) {
      if (clause.getOccur() == BooleanClause.Occur.MUST
          || clause.getOccur() == BooleanClause.Occur.FILTER) mandatory.add(clause.getQuery());
      else if (clause.getOccur() == BooleanClause.Occur.MUST_NOT) exclusions.add(clause.getQuery());
      else {
        subqueries.addAll(decompose(clause.getQuery()));
      }
    }

    // More than one MUST clause, or a single MUST clause with disjunctions
    if (mandatory.size() > 1 || (mandatory.size() == 1 && subqueries.size() > 0))
      return Collections.singleton(q);

    // If we only have a single MUST clause and no SHOULD clauses, then we can
    // decompose the MUST clause instead
    if (mandatory.size() == 1) {
      subqueries.addAll(decompose(mandatory.iterator().next()));
    }

    if (exclusions.size() == 0) return subqueries;

    // If there are exclusions, then we need to add them to all the decomposed
    // queries
    Set<Query> rewrittenSubqueries = new HashSet<>(subqueries.size());
    for (Query subquery : subqueries) {
      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      bq.add(subquery, BooleanClause.Occur.MUST);
      for (Query ex : exclusions) {
        bq.add(ex, BooleanClause.Occur.MUST_NOT);
      }
      rewrittenSubqueries.add(bq.build());
    }
    return rewrittenSubqueries;
  }
}
