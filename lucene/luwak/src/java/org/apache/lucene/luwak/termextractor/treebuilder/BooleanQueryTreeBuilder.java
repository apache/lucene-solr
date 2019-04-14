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

package org.apache.lucene.luwak.termextractor.treebuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.luwak.termextractor.QueryAnalyzer;
import org.apache.lucene.luwak.termextractor.querytree.AnyNode;
import org.apache.lucene.luwak.termextractor.querytree.ConjunctionNode;
import org.apache.lucene.luwak.termextractor.querytree.DisjunctionNode;
import org.apache.lucene.luwak.termextractor.querytree.QueryTree;
import org.apache.lucene.luwak.termextractor.weights.TermWeightor;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.luwak.termextractor.QueryTreeBuilder;

/**
 * Extract terms from a BooleanQuery, recursing into the BooleanClauses
 * <p>
 * If the query is a pure conjunction, then this extractor will select the best
 * matching term from all the clauses and only extract that.
 */
public class BooleanQueryTreeBuilder extends QueryTreeBuilder<BooleanQuery> {

  public BooleanQueryTreeBuilder() {
    super(BooleanQuery.class);
  }

  protected Clauses analyze(BooleanQuery query) {
    Clauses clauses = new Clauses();
    for (BooleanClause clause : query) {
      if (clause.getOccur() == BooleanClause.Occur.MUST || clause.getOccur() == BooleanClause.Occur.FILTER) {
        clauses.conjunctions.add(clause.getQuery());
      }
      if (clause.getOccur() == BooleanClause.Occur.SHOULD) {
        clauses.disjunctions.add(clause.getQuery());
      }
      if (clause.getOccur() == BooleanClause.Occur.MUST_NOT) {
        clauses.negatives.add(clause.getQuery());
      }
    }
    return clauses;
  }

  @Override
  public QueryTree buildTree(QueryAnalyzer builder, TermWeightor weightor, BooleanQuery query) {

    Clauses clauses = analyze(query);

    if (clauses.isPureNegativeQuery())
      return new AnyNode("PURE NEGATIVE BOOLEAN");

    if (clauses.isDisjunctionQuery()) {
      return DisjunctionNode.build(buildChildTrees(builder, weightor, clauses.getDisjunctions()));
    }

    return ConjunctionNode.build(buildChildTrees(builder, weightor, clauses.getConjunctions()));
  }

  private List<QueryTree> buildChildTrees(QueryAnalyzer builder, TermWeightor weightor, List<Query> children) {
    return children.stream().map(q -> builder.buildTree(q, weightor)).collect(Collectors.toList());
  }

  public static class Clauses {

    final List<Query> disjunctions = new ArrayList<>();
    final List<Query> conjunctions = new ArrayList<>();
    final List<Query> negatives = new ArrayList<>();

    public boolean isConjunctionQuery() {
      return conjunctions.size() > 0;
    }

    public boolean isDisjunctionQuery() {
      return !isConjunctionQuery() && disjunctions.size() > 0;
    }

    public boolean isPureNegativeQuery() {
      return conjunctions.size() == 0 && disjunctions.size() == 0 && negatives.size() > 0;
    }

    public List<Query> getDisjunctions() {
      return disjunctions;
    }

    public List<Query> getConjunctions() {
      return conjunctions;
    }
  }

}
