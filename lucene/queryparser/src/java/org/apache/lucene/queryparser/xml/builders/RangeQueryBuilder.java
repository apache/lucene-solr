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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.w3c.dom.Element;

/** Builder for {@link TermRangeQuery} */
public class RangeQueryBuilder implements QueryBuilder {

  @Override
  public Query getQuery(Element e) throws ParserException {
    String fieldName = DOMUtils.getAttributeWithInheritance(e, "fieldName");

    String lowerTerm = e.getAttribute("lowerTerm");
    String upperTerm = e.getAttribute("upperTerm");
    boolean includeLower = DOMUtils.getAttribute(e, "includeLower", true);
    boolean includeUpper = DOMUtils.getAttribute(e, "includeUpper", true);
    return TermRangeQuery.newStringRange(
        fieldName, lowerTerm, upperTerm, includeLower, includeUpper);
  }
}
