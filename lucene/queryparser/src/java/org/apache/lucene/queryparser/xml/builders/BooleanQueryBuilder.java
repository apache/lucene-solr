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
package org.apache.lucene.queryparser.xml.builders;

import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/** Builder for {@link BooleanQuery} */
public class BooleanQueryBuilder implements QueryBuilder {

  private final QueryBuilder factory;

  public BooleanQueryBuilder(QueryBuilder factory) {
    this.factory = factory;
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.xmlparser.QueryObjectBuilder#process(org.w3c.dom.Element)
   */

  @Override
  public Query getQuery(Element e) throws ParserException {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setMinimumNumberShouldMatch(DOMUtils.getAttribute(e, "minimumNumberShouldMatch", 0));

    NodeList nl = e.getChildNodes();
    final int nlLen = nl.getLength();
    for (int i = 0; i < nlLen; i++) {
      Node node = nl.item(i);
      if (node.getNodeName().equals("Clause")) {
        Element clauseElem = (Element) node;
        BooleanClause.Occur occurs = getOccursValue(clauseElem);

        Element clauseQuery = DOMUtils.getFirstChildOrFail(clauseElem);
        Query q = factory.getQuery(clauseQuery);
        bq.add(new BooleanClause(q, occurs));
      }
    }

    Query q = bq.build();
    float boost = DOMUtils.getAttribute(e, "boost", 1.0f);
    if (boost != 1f) {
      q = new BoostQuery(q, boost);
    }
    return q;
  }

  static BooleanClause.Occur getOccursValue(Element clauseElem) throws ParserException {
    String occs = clauseElem.getAttribute("occurs");
    if (occs == null || "should".equalsIgnoreCase(occs)) {
      return BooleanClause.Occur.SHOULD;
    } else if ("must".equalsIgnoreCase(occs)) {
      return BooleanClause.Occur.MUST;
    } else if ("mustNot".equalsIgnoreCase(occs)) {
      return BooleanClause.Occur.MUST_NOT;
    } else if ("filter".equals(occs)) {
      return BooleanClause.Occur.FILTER;
    }
    throw new ParserException("Invalid value for \"occurs\" attribute of clause:" + occs);
  }
}
