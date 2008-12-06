package org.apache.solr.handler.component;
/**
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

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.TermsParams;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;


/**
 * Return TermEnum information, useful for things like auto suggest.
 *
 * @see org.apache.solr.common.params.TermsParams
 *      See Lucene's TermEnum class
 */
public class TermsComponent extends SearchComponent {
  public static final int UNLIMITED_MAX_COUNT = -1;


  public void process(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    if (params.getBool(TermsParams.TERMS, false)) {
      String lower = params.get(TermsParams.TERMS_LOWER, "");
      String[] fields = params.getParams(TermsParams.TERMS_FIELD);
      if (fields != null && fields.length > 0) {
        NamedList terms = new NamedList();
        rb.rsp.add("terms", terms);
        int rows = params.getInt(TermsParams.TERMS_ROWS, params.getInt(CommonParams.ROWS, 10));
        if (rows < 0) {
          rows = Integer.MAX_VALUE;
        }
        String upper = params.get(TermsParams.TERMS_UPPER);
        boolean upperIncl = params.getBool(TermsParams.TERMS_UPPER_INCLUSIVE, false);
        boolean lowerIncl = params.getBool(TermsParams.TERMS_LOWER_INCLUSIVE, true);
        int freqmin = params.getInt(TermsParams.TERMS_MINCOUNT, 1); // initialize freqmin
        int freqmax = params.getInt(TermsParams.TERMS_MAXCOUNT, UNLIMITED_MAX_COUNT); // initialize freqmax
        String prefix = params.get(TermsParams.TERMS_PREFIX_STR);
        for (int j = 0; j < fields.length; j++) {
          String field = fields[j];
          Term lowerTerm = new Term(field, lower);
          Term upperTerm = upper != null ? new Term(field, upper) : null;
          TermEnum termEnum = rb.req.getSearcher().getReader().terms(lowerTerm);//this will be positioned ready to go
          int i = 0;
          NamedList fieldTerms = new NamedList();
          terms.add(field, fieldTerms);
          boolean hasMore = true;
          Term lowerTestTerm = termEnum.term();
          //Only advance the enum if we are excluding the lower bound and the lower Term actually matches
          if (lowerIncl == false && lowerTestTerm.field().equals(field) == true && lowerTestTerm.text().equals(lower)) {
            hasMore = termEnum.next();
          }
          if (hasMore == true) {
            do {
              Term theTerm = termEnum.term();
              String theText = theTerm.text();
              int upperCmp = upperTerm != null ? theTerm.compareTo(upperTerm) : -1;
              if (theTerm != null && theTerm.field().equals(field)
                      && ((upperIncl == true && upperCmp <= 0) ||
                      (upperIncl == false && upperCmp < 0))
                      && (prefix == null || theText.startsWith(prefix))
                      ) {
                int docFreq = termEnum.docFreq();
                if (docFreq >= freqmin && (freqmax == UNLIMITED_MAX_COUNT || (docFreq <= freqmax))) {
                  fieldTerms.add(theText, docFreq);
                  i++;
                }
              } else {//we're done
                break;
              }
            }
            while (i < rows && termEnum.next());
          }
          termEnum.close();
        }
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No terms.fl parameter specified");
      }
    }
  }

  public void prepare(ResponseBuilder rb) throws IOException {
    //nothing to do
  }

  public String getVersion() {
    return "$Revision:$";
  }

  public String getSourceId() {
    return "$Id:$";
  }

  public String getSource() {
    return "$URL:$";
  }

  public String getDescription() {
    return "A Component for working with Term Enumerators";
  }
}
