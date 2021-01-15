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

import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.w3c.dom.Element;

/**
 * Builds a BooleanQuery from all of the terms found in the XML element using the choice of analyzer
 */
public class TermsQueryBuilder implements QueryBuilder {

  private final Analyzer analyzer;

  public TermsQueryBuilder(Analyzer analyzer) {
    this.analyzer = analyzer;
  }

  @Override
  public Query getQuery(Element e) throws ParserException {
    String fieldName = DOMUtils.getAttributeWithInheritanceOrFail(e, "fieldName");
    String text = DOMUtils.getNonBlankTextOrFail(e);

    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setMinimumNumberShouldMatch(DOMUtils.getAttribute(e, "minimumNumberShouldMatch", 0));
    try (TokenStream ts = analyzer.tokenStream(fieldName, text)) {
      TermToBytesRefAttribute termAtt = ts.addAttribute(TermToBytesRefAttribute.class);
      Term term = null;
      ts.reset();
      while (ts.incrementToken()) {
        term = new Term(fieldName, BytesRef.deepCopyOf(termAtt.getBytesRef()));
        bq.add(new BooleanClause(new TermQuery(term), BooleanClause.Occur.SHOULD));
      }
      ts.end();
    } catch (IOException ioe) {
      throw new RuntimeException("Error constructing terms from index:" + ioe);
    }

    Query q = bq.build();
    float boost = DOMUtils.getAttribute(e, "boost", 1.0f);
    return new BoostQuery(q, boost);
  }
}
