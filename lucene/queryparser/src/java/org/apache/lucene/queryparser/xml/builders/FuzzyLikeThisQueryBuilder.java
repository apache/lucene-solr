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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.sandbox.queries.FuzzyLikeThisQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/** Builder for {@link FuzzyLikeThisQuery} */
public class FuzzyLikeThisQueryBuilder implements QueryBuilder {

  private static final int DEFAULT_MAX_NUM_TERMS = 50;
  private static final float DEFAULT_MIN_SIMILARITY = FuzzyQuery.defaultMaxEdits;
  private static final int DEFAULT_PREFIX_LENGTH = 1;
  private static final boolean DEFAULT_IGNORE_TF = false;

  private final Analyzer analyzer;

  public FuzzyLikeThisQueryBuilder(Analyzer analyzer) {
    this.analyzer = analyzer;
  }

  @Override
  public Query getQuery(Element e) throws ParserException {
    NodeList nl = e.getElementsByTagName("Field");
    int maxNumTerms = DOMUtils.getAttribute(e, "maxNumTerms", DEFAULT_MAX_NUM_TERMS);
    FuzzyLikeThisQuery fbq = new FuzzyLikeThisQuery(maxNumTerms, analyzer);
    fbq.setIgnoreTF(DOMUtils.getAttribute(e, "ignoreTF", DEFAULT_IGNORE_TF));

    final int nlLen = nl.getLength();
    for (int i = 0; i < nlLen; i++) {
      Element fieldElem = (Element) nl.item(i);
      float minSimilarity =
          DOMUtils.getAttribute(fieldElem, "minSimilarity", DEFAULT_MIN_SIMILARITY);
      int prefixLength = DOMUtils.getAttribute(fieldElem, "prefixLength", DEFAULT_PREFIX_LENGTH);
      String fieldName = DOMUtils.getAttributeWithInheritance(fieldElem, "fieldName");

      String value = DOMUtils.getText(fieldElem);
      fbq.addTerms(value, fieldName, minSimilarity, prefixLength);
    }

    Query q = fbq;
    float boost = DOMUtils.getAttribute(e, "boost", 1.0f);
    if (boost != 1f) {
      q = new BoostQuery(fbq, boost);
    }
    return q;
  }
}
