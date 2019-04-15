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

package org.apache.lucene.luwak.termextractor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.lucene.index.Term;
import org.apache.lucene.luwak.Presearcher;
import org.apache.lucene.luwak.presearcher.PresearcherComponent;
import org.apache.lucene.luwak.termextractor.weights.TermWeightor;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

/**
 * Class to analyze and extract terms from a lucene query, to be used by
 * a {@link Presearcher} in indexing.
 */
public class QueryAnalyzer {

  private final BiFunction<Query, TermWeightor, QueryTree> unknownQueryMapper;

  /**
   * Create a QueryAnalyzer using provided QueryTreeBuilders, in addition to the default set
   *
   * @param queryTreeBuilders QueryTreeBuilders used to analyze queries
   */
  public QueryAnalyzer(List<BiFunction<Query, TermWeightor, QueryTree>> queryTreeBuilders) {
    this.unknownQueryMapper = buildMapper(queryTreeBuilders);
  }

  public QueryAnalyzer() {
    this.unknownQueryMapper = (q, w) -> null;
  }

  private static BiFunction<Query, TermWeightor, QueryTree> buildMapper(List<BiFunction<Query, TermWeightor, QueryTree>> mappers) {
    return (q, w) -> {
      for (BiFunction<Query, TermWeightor, QueryTree> mapper : mappers) {
        QueryTree qt = mapper.apply(q, w);
        if (qt != null) {
          return qt;
        }
      }
      return null;
    };
  }

  /**
   * Build a new QueryAnalyzer using a TreeWeightor and a list of PresearcherComponents
   * <p>
   * A list of QueryTreeBuilders is extracted from each component, and combined to use
   * on the QueryAnalyzer
   *
   * @param components a list of PresearcherComponents
   * @return a QueryAnalyzer
   */
  public static QueryAnalyzer fromComponents(PresearcherComponent... components) {
    List<BiFunction<Query, TermWeightor, QueryTree>> builders = new ArrayList<>();
    for (PresearcherComponent component : components) {
      builders.addAll(component.getQueryTreeBuilders());
    }
    return new QueryAnalyzer(builders);
  }

  /**
   * Create a {@link QueryTree} from a passed in Query or Filter
   *
   * @param luceneQuery the query to analyze
   * @return a QueryTree describing the analyzed query
   */
  public QueryTree buildTree(Query luceneQuery, TermWeightor weightor) {
    QueryBuilder builder = new QueryBuilder();
    luceneQuery.visit(builder);
    return builder.apply(weightor);
  }

  /**
   * Collect terms from a QueryTree
   *
   * @param queryTree the analyzed QueryTree to collect terms from
   * @return a list of QueryTerms
   */
  public Set<QueryTerm> collectTerms(QueryTree queryTree) {
    Set<QueryTerm> terms = new HashSet<>();
    queryTree.collectTerms(terms);
    return terms;
  }

  /**
   * Collect terms from a lucene Query
   *
   * @param luceneQuery the query to analyze and collect terms from
   * @return a list of QueryTerms
   */
  public Set<QueryTerm> collectTerms(Query luceneQuery, TermWeightor weightor) {
    return collectTerms(buildTree(luceneQuery, weightor));
  }

  private class QueryBuilder extends QueryVisitor implements Function<TermWeightor, QueryTree> {

    final List<Function<TermWeightor, QueryTree>> children = new ArrayList<>();

    @Override
    public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
      if (occur == BooleanClause.Occur.MUST || occur == BooleanClause.Occur.FILTER) {
        QueryBuilder n = new QueryBuilder();
        children.add(n);
        return n;
      }
      if (occur == BooleanClause.Occur.MUST_NOT) {
        // Check if we're in a pure negative disjunction
        if (parent instanceof BooleanQuery) {
          BooleanQuery bq = (BooleanQuery) parent;
          long positiveCount = bq.clauses().stream()
              .filter(c -> c.getOccur() != BooleanClause.Occur.MUST_NOT)
              .count();
          if (positiveCount == 0) {
            children.add(w -> QueryTree.anyTerm("PURE NEGATIVE QUERY[" + parent + "]"));
          }
        }
        return QueryVisitor.EMPTY_VISITOR;
      }
      // It's a disjunction clause.  If the parent has MUST or FILTER clauses, we can
      // ignore it
      if (parent instanceof BooleanQuery) {
        BooleanQuery bq = (BooleanQuery) parent;
        long requiredCount = bq.clauses().stream()
            .filter(c -> c.getOccur() == BooleanClause.Occur.MUST || c.getOccur() == BooleanClause.Occur.FILTER)
            .count();
        if (requiredCount > 0) {
          return QueryVisitor.EMPTY_VISITOR;
        }
      }
      Disjunction n = new Disjunction();
      children.add(n);
      return n;
    }

    @Override
    public void consumeTerms(Query query, Term... terms) {
      for (Term term : terms) {
        children.add(w -> QueryTree.term(term, w));
      }
    }

    @Override
    public void visitLeaf(Query query) {
      children.add(w -> {
        QueryTree q = unknownQueryMapper.apply(query, w);
        if (q == null) {
          return QueryTree.anyTerm(query.toString());
        }
        return q;
      });
    }

    @Override
    public QueryTree apply(TermWeightor termWeightor) {
      return QueryTree.conjunction(children, termWeightor);
    }
  }

  private class Disjunction extends QueryBuilder {

    @Override
    public QueryTree apply(TermWeightor termWeightor) {
      return QueryTree.disjunction(children, termWeightor);
    }
  }

}
