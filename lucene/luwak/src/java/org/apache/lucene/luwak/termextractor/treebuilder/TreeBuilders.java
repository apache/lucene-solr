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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.lucene.luwak.termextractor.QueryAnalyzer;
import org.apache.lucene.luwak.termextractor.QueryTreeBuilder;
import org.apache.lucene.luwak.termextractor.querytree.AnyNode;
import org.apache.lucene.luwak.termextractor.querytree.ConjunctionNode;
import org.apache.lucene.luwak.termextractor.querytree.DisjunctionNode;
import org.apache.lucene.luwak.termextractor.querytree.QueryTree;
import org.apache.lucene.luwak.termextractor.querytree.TermNode;
import org.apache.lucene.luwak.termextractor.weights.TermWeightor;
import org.apache.lucene.luwak.util.CollectionUtils;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.FieldMaskingSpanQuery;
import org.apache.lucene.search.spans.SpanBoostQuery;
import org.apache.lucene.search.spans.SpanContainingQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanPositionCheckQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.SpanWithinQuery;

public class TreeBuilders {

  public static final QueryTreeBuilder<Query> ANY_NODE_BUILDER = new QueryTreeBuilder<Query>(Query.class) {
    @Override
    public QueryTree buildTree(QueryAnalyzer builder, TermWeightor weightor, Query query) {
      return new AnyNode("Cannot filter on query of type " + query.getClass().getName());
    }
  };

  private static final Function<FunctionScoreQuery, Query> functionScoreQueryExtractor = q -> {
    try {
      Field queryField = FunctionScoreQuery.class.getDeclaredField("in");
      queryField.setAccessible(true);
      return (Query) queryField.get(q);
    } catch (Exception e) {
      return new MatchAllDocsQuery();
    }
  };

  public static final List<QueryTreeBuilder<? extends Query>> DEFAULT_BUILDERS = CollectionUtils.makeUnmodifiableList(
      new BooleanQueryTreeBuilder(),
      newConjunctionBuilder(PhraseQuery.class,
          (b, w, q) -> Arrays.stream(q.getTerms()).map(qq -> new TermNode(qq, w)).collect(Collectors.toList())),
      newFilteringQueryBuilder(ConstantScoreQuery.class, ConstantScoreQuery::getQuery),
      newFilteringQueryBuilder(BoostQuery.class, BoostQuery::getQuery),
      newQueryBuilder(TermQuery.class, (q, w) -> new TermNode(q.getTerm(), w)),
      newQueryBuilder(SpanTermQuery.class, (q, w) -> new TermNode(q.getTerm(), w)),
      newConjunctionBuilder(SpanNearQuery.class,
          (b, w, q) -> Arrays.stream(q.getClauses()).map(qq -> b.buildTree(qq, w)).collect(Collectors.toList())),
      newDisjunctionBuilder(SpanOrQuery.class,
          (b, w, q) -> Arrays.stream(q.getClauses()).map(qq -> b.buildTree(qq, w)).collect(Collectors.toList())),
      newFilteringQueryBuilder(SpanMultiTermQueryWrapper.class, SpanMultiTermQueryWrapper::getWrappedQuery),
      newFilteringQueryBuilder(SpanNotQuery.class, SpanNotQuery::getInclude),
      newDisjunctionBuilder(DisjunctionMaxQuery.class,
          (b, w, q) -> q.getDisjuncts().stream().map(qq -> b.buildTree(qq, w)).collect(Collectors.toList())),
      TermInSetQueryTreeBuilder.INSTANCE,
      new QueryTreeBuilder<SpanWithinQuery>(SpanWithinQuery.class) {
        @Override
        public QueryTree buildTree(QueryAnalyzer builder, TermWeightor weightor, SpanWithinQuery query) {
          return ConjunctionNode.build(builder.buildTree(query.getBig(), weightor), builder.buildTree(query.getLittle(), weightor));
        }
      },
      new QueryTreeBuilder<SpanContainingQuery>(SpanContainingQuery.class) {
        @Override
        public QueryTree buildTree(QueryAnalyzer builder, TermWeightor weightor, SpanContainingQuery query) {
          return ConjunctionNode.build(builder.buildTree(query.getBig(), weightor), builder.buildTree(query.getLittle(), weightor));
        }
      },
      newFilteringQueryBuilder(SpanBoostQuery.class, SpanBoostQuery::getQuery),
      newFilteringQueryBuilder(FieldMaskingSpanQuery.class, FieldMaskingSpanQuery::getMaskedQuery),
      newFilteringQueryBuilder(SpanPositionCheckQuery.class, SpanPositionCheckQuery::getMatch),
      newFilteringQueryBuilder(FunctionScoreQuery.class, functionScoreQueryExtractor),
      PayloadScoreQueryTreeBuilder.INSTANCE,
      SpanPayloadCheckQueryTreeBuilder.INSTANCE,
      ANY_NODE_BUILDER
  );

  public static <T extends Query> QueryTreeBuilder<T> newFilteringQueryBuilder(Class<T> queryType, Function<T, Query> filter) {
    return new QueryTreeBuilder<T>(queryType) {
      @Override
      public QueryTree buildTree(QueryAnalyzer builder, TermWeightor weightor, T query) {
        return builder.buildTree(filter.apply(query), weightor);
      }
    };
  }

  public static <T extends Query> QueryTreeBuilder<T> newQueryBuilder(Class<T> queryType, NodeExtractor<T> nodeBuilder) {
    return new QueryTreeBuilder<T>(queryType) {
      @Override
      public QueryTree buildTree(QueryAnalyzer builder, TermWeightor weightor, T query) {
        return nodeBuilder.extract(query, weightor);
      }
    };
  }

  public static <T extends Query> QueryTreeBuilder<T>
  newConjunctionBuilder(Class<T> queryType, MultiExtractor<T> extractor) {
    return new QueryTreeBuilder<T>(queryType) {
      @Override
      public QueryTree buildTree(QueryAnalyzer builder, TermWeightor weightor, T query) {
        return ConjunctionNode.build(extractor.extract(builder, weightor, query));
      }
    };
  }

  public static <T extends Query> QueryTreeBuilder<T>
  newDisjunctionBuilder(Class<T> queryType, MultiExtractor<T> extractor) {
    return new QueryTreeBuilder<T>(queryType) {
      @Override
      public QueryTree buildTree(QueryAnalyzer builder, TermWeightor weightor, T query) {
        return DisjunctionNode.build(extractor.extract(builder, weightor, query));
      }
    };
  }

  public interface NodeExtractor<T extends Query> {
    QueryTree extract(T query, TermWeightor weightor);
  }

  public interface MultiExtractor<T extends Query> {
    List<QueryTree> extract(QueryAnalyzer builder, TermWeightor weightor, T query);
  }

}
