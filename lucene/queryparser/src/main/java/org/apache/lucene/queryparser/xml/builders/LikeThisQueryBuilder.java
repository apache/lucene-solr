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
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.queries.mlt.MoreLikeThisQuery;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.w3c.dom.Element;

/**
 * Builder for {@link MoreLikeThisQuery}
 */
public class LikeThisQueryBuilder implements QueryBuilder {

  private static final int DEFAULT_MAX_QUERY_TERMS = 20;
  private static final int DEFAULT_MIN_TERM_FREQUENCY = 1;
  private static final float DEFAULT_PERCENT_TERMS_TO_MATCH = 30; //default is a 3rd of selected terms must match

  private final Analyzer analyzer;
  private final String defaultFieldNames[];

  public LikeThisQueryBuilder(Analyzer analyzer, String[] defaultFieldNames) {
    this.analyzer = analyzer;
    this.defaultFieldNames = defaultFieldNames;
  }

  /* (non-Javadoc)
    * @see org.apache.lucene.xmlparser.QueryObjectBuilder#process(org.w3c.dom.Element)
    */
  @Override
  public Query getQuery(Element e) throws ParserException {
    String fieldsList = e.getAttribute("fieldNames"); //a comma-delimited list of fields
    String fields[] = defaultFieldNames;
    if ((fieldsList != null) && (fieldsList.trim().length() > 0)) {
      fields = fieldsList.trim().split(",");
      //trim the fieldnames
      for (int i = 0; i < fields.length; i++) {
        fields[i] = fields[i].trim();
      }
    }

    //Parse any "stopWords" attribute
    //TODO MoreLikeThis needs to ideally have per-field stopWords lists - until then
    //I use all analyzers/fields to generate multi-field compatible stop list
    String stopWords = e.getAttribute("stopWords");
    Set<String> stopWordsSet = null;
    if ((stopWords != null) && (fields != null)) {
      stopWordsSet = new HashSet<>();
      for (String field : fields) {
        try (TokenStream ts = analyzer.tokenStream(field, stopWords)) {
          CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
          ts.reset();
          while (ts.incrementToken()) {
            stopWordsSet.add(termAtt.toString());
          }
          ts.end();
        } catch (IOException ioe) {
          throw new ParserException("IoException parsing stop words list in "
              + getClass().getName() + ":" + ioe.getLocalizedMessage());
        }
      }
    }


    MoreLikeThisQuery mlt = new MoreLikeThisQuery(DOMUtils.getText(e), fields, analyzer, fields[0]);
    mlt.setMaxQueryTerms(DOMUtils.getAttribute(e, "maxQueryTerms", DEFAULT_MAX_QUERY_TERMS));
    mlt.setMinTermFrequency(DOMUtils.getAttribute(e, "minTermFrequency", DEFAULT_MIN_TERM_FREQUENCY));
    mlt.setPercentTermsToMatch(DOMUtils.getAttribute(e, "percentTermsToMatch", DEFAULT_PERCENT_TERMS_TO_MATCH) / 100);
    mlt.setStopWords(stopWordsSet);
    int minDocFreq = DOMUtils.getAttribute(e, "minDocFreq", -1);
    if (minDocFreq >= 0) {
      mlt.setMinDocFreq(minDocFreq);
    }

    Query q = mlt;
    float boost = DOMUtils.getAttribute(e, "boost", 1.0f);
    if (boost != 1f) {
      q = new BoostQuery(mlt, boost);
    }
    return q;
  }

}
