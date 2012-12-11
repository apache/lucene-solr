/*
 * Created on 25-Jan-2006
 */
package org.apache.lucene.queryparser.xml.builders;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.FilterBuilder;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.w3c.dom.Element;

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

/**
 * Builder for {@link FilteredQuery}
 */
public class FilteredQueryBuilder implements QueryBuilder {

  private final FilterBuilder filterFactory;
  private final QueryBuilder queryFactory;

  public FilteredQueryBuilder(FilterBuilder filterFactory, QueryBuilder queryFactory) {
    this.filterFactory = filterFactory;
    this.queryFactory = queryFactory;

  }

  /* (non-Javadoc)
    * @see org.apache.lucene.xmlparser.QueryObjectBuilder#process(org.w3c.dom.Element)
    */
  @Override
  public Query getQuery(Element e) throws ParserException {
    Element filterElement = DOMUtils.getChildByTagOrFail(e, "Filter");
    filterElement = DOMUtils.getFirstChildOrFail(filterElement);
    Filter f = filterFactory.getFilter(filterElement);

    Element queryElement = DOMUtils.getChildByTagOrFail(e, "Query");
    queryElement = DOMUtils.getFirstChildOrFail(queryElement);
    Query q = queryFactory.getQuery(queryElement);

    FilteredQuery fq = new FilteredQuery(q, f);
    fq.setBoost(DOMUtils.getAttribute(e, "boost", 1.0f));
    return fq;
  }

}
