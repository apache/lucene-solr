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
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.builders.SpanQueryBuilder;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.solr.request.SolrQueryRequest;
import org.w3c.dom.Element;

public class ApacheLuceneSolrNearQueryBuilder extends SolrSpanQueryBuilder {

  public ApacheLuceneSolrNearQueryBuilder(String defaultField, Analyzer analyzer,
      SolrQueryRequest req, SpanQueryBuilder spanFactory) {
    super(defaultField, analyzer, req, spanFactory);
  }

  public Query getQuery(Element e) throws ParserException {
    return getSpanQuery(e);
  }

  public SpanQuery getSpanQuery(Element e) throws ParserException {
    final String fieldName = DOMUtils.getAttributeWithInheritanceOrFail(e, "fieldName");
    final SpanQuery[] spanQueries = new SpanQuery[]{
        new SpanTermQuery(new Term(fieldName, "Apache")),
        new SpanTermQuery(new Term(fieldName, "Lucene")),
        new SpanTermQuery(new Term(fieldName, "Solr"))
    };
    final int slop = 42;
    final boolean inOrder = false;
    return new SpanNearQuery(spanQueries, slop, inOrder);
  }

}
