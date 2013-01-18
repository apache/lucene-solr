package org.apache.lucene.queryparser.xml.builders;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.FilterBuilder;
import org.apache.lucene.queryparser.xml.ParserException;
import org.w3c.dom.Element;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

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
 * Builder for {@link TermsFilter}
 */
public class TermsFilterBuilder implements FilterBuilder {

  private final Analyzer analyzer;

  public TermsFilterBuilder(Analyzer analyzer) {
    this.analyzer = analyzer;
  }

  /*
    * (non-Javadoc)
    *
    * @see org.apache.lucene.xmlparser.FilterBuilder#process(org.w3c.dom.Element)
    */
  @Override
  public Filter getFilter(Element e) throws ParserException {
    List<BytesRef> terms = new ArrayList<BytesRef>();
    String text = DOMUtils.getNonBlankTextOrFail(e);
    String fieldName = DOMUtils.getAttributeWithInheritanceOrFail(e, "fieldName");

    try {
      TokenStream ts = analyzer.tokenStream(fieldName, new StringReader(text));
      TermToBytesRefAttribute termAtt = ts.addAttribute(TermToBytesRefAttribute.class);
      Term term = null;
      BytesRef bytes = termAtt.getBytesRef();
      ts.reset();
      while (ts.incrementToken()) {
        termAtt.fillBytesRef();
        terms.add(BytesRef.deepCopyOf(bytes));
      }
      ts.end();
      ts.close();
    }
    catch (IOException ioe) {
      throw new RuntimeException("Error constructing terms from index:" + ioe);
    }
    return new TermsFilter(fieldName, terms);
  }
}
