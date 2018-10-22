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
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.solr.request.SolrQueryRequest;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

public class ChooseOneWordQueryBuilder extends SolrSpanQueryBuilder {

  public ChooseOneWordQueryBuilder(String defaultField, Analyzer analyzer, SolrQueryRequest req,
      SpanQueryBuilder spanFactory) {
    super(defaultField, analyzer, req, spanFactory);
  }

  public Query getQuery(Element e) throws ParserException {
    return implGetQuery(e, false);
  }

  public SpanQuery getSpanQuery(Element e) throws ParserException {
    return (SpanQuery)implGetQuery(e, true);
  }

  public Query implGetQuery(Element e, boolean span) throws ParserException {
    Term term = null;
    final String fieldName = DOMUtils.getAttributeWithInheritanceOrFail(e, "fieldName");
    for (Node node = e.getFirstChild(); node != null; node = node.getNextSibling()) {
      if (node.getNodeType() == Node.ELEMENT_NODE &&
          node.getNodeName().equals("Word")) {
        final String word = DOMUtils.getNonBlankTextOrFail((Element) node);
        final Term t = new Term(fieldName, word);
        if (term == null || term.text().length() < t.text().length()) {
          term = t;
        }
      }
    }
    return (span ? new SpanTermQuery(term) : new TermQuery(term));
  }
}
