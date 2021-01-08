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
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.search.spans.SpanBoostQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRef;
import org.w3c.dom.Element;

/** Builder that analyzes the text into a {@link SpanOrQuery} */
public class SpanOrTermsBuilder extends SpanBuilderBase {

  private final Analyzer analyzer;

  public SpanOrTermsBuilder(Analyzer analyzer) {
    this.analyzer = analyzer;
  }

  @Override
  public SpanQuery getSpanQuery(Element e) throws ParserException {
    String fieldName = DOMUtils.getAttributeWithInheritanceOrFail(e, "fieldName");
    String value = DOMUtils.getNonBlankTextOrFail(e);

    List<SpanQuery> clausesList = new ArrayList<>();

    try (TokenStream ts = analyzer.tokenStream(fieldName, value)) {
      TermToBytesRefAttribute termAtt = ts.addAttribute(TermToBytesRefAttribute.class);
      ts.reset();
      while (ts.incrementToken()) {
        SpanTermQuery stq =
            new SpanTermQuery(new Term(fieldName, BytesRef.deepCopyOf(termAtt.getBytesRef())));
        clausesList.add(stq);
      }
      ts.end();
      SpanOrQuery soq = new SpanOrQuery(clausesList.toArray(new SpanQuery[clausesList.size()]));
      float boost = DOMUtils.getAttribute(e, "boost", 1.0f);
      return new SpanBoostQuery(soq, boost);
    } catch (IOException ioe) {
      throw new ParserException("IOException parsing value:" + value);
    }
  }
}
