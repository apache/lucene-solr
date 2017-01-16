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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.queryparser.xml.builders.SpanQueryBuilder;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.solr.request.SolrQueryRequest;
import org.w3c.dom.Element;

// A simple test query builder to demonstrate use of
// SolrQueryBuilder's queryFactory constructor argument.
public class HandyQueryBuilder extends SolrQueryBuilder {

  public HandyQueryBuilder(String defaultField, Analyzer analyzer,
      SolrQueryRequest req, QueryBuilder queryFactory, SpanQueryBuilder spanFactory) {
    super(defaultField, analyzer, req, queryFactory, spanFactory);
  }

  @Override
  public Query getQuery(Element e) throws ParserException {
    final BooleanQuery.Builder bq = new BooleanQuery.Builder();
    final Query lhsQ = getSubQuery(e, "Left");
    final Query rhsQ = getSubQuery(e, "Right");
    bq.add(new BooleanClause(lhsQ, BooleanClause.Occur.SHOULD));
    bq.add(new BooleanClause(rhsQ, BooleanClause.Occur.SHOULD));
    return bq.build();
  }

  @Override
  public SpanQuery getSpanQuery(Element e) throws ParserException {
    SpanQuery subQueries[] = {
        getSubSpanQuery(e, "Left"),
        getSubSpanQuery(e, "Right"),
    };

    return new SpanOrQuery(subQueries);
  }

  private Query getSubQuery(Element e, String name) throws ParserException {
    Element subE = DOMUtils.getChildByTagOrFail(e, name);
    subE = DOMUtils.getFirstChildOrFail(subE);
    return queryFactory.getQuery(subE);
  }

  private SpanQuery getSubSpanQuery(Element e, String name) throws ParserException {
    Element subE = DOMUtils.getChildByTagOrFail(e, name);
    subE = DOMUtils.getFirstChildOrFail(subE);
    return spanFactory.getSpanQuery(subE);
  }
}
