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
 * See Lucene's TermEnum class
 */
public class TermsComponent extends SearchComponent {


  public void process(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    if (params.getBool(TermsParams.TERMS, false)) {
      String lower = params.get(TermsParams.TERMS_LOWER, "");
      String field = params.get(TermsParams.TERMS_FIELD);
      if (field != null) {
        Term lowerTerm = new Term(field, lower);
        TermEnum termEnum = rb.req.getSearcher().getReader().terms(lowerTerm);//this will be positioned ready to go
        int rows = params.getInt(TermsParams.TERMS_ROWS, params.getInt(CommonParams.ROWS, 10));
        int i = 0;
        NamedList terms = new NamedList();
        rb.rsp.add("terms", terms);
        String upper = params.get(TermsParams.TERMS_UPPER);
        Term upperTerm = upper != null ? new Term(field, upper) : null;
        boolean upperIncl = params.getBool(TermsParams.TERMS_UPPER_INCLUSIVE, false);
        boolean lowerIncl = params.getBool(TermsParams.TERMS_LOWER_INCLUSIVE, true);
        boolean hasMore = true;
        if (lowerIncl == false) {
          hasMore = termEnum.next();
        }
        if (hasMore == true) {
          do {
            Term theTerm = termEnum.term();
            String theText = theTerm.text();
            int upperCmp = upperTerm != null ? theTerm.compareTo(upperTerm) : -1;
            if (theTerm != null && theTerm.field().equals(field)
                    && ((upperIncl == true && upperCmp <= 0) ||
                    (upperIncl == false && upperCmp < 0))) {
              terms.add(theText, termEnum.docFreq());
            } else {//we're done
              break;
            }
            i++;
          }
          while (i < rows && termEnum.next());
        }
        termEnum.close();
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No terms.fl parameter specified");
      }
    }
  }

  public void prepare(ResponseBuilder rb) throws IOException {
    //nothing to do
  }

  public String getVersion() {
    return "$Revision$";
  }

  public String getSourceId() {
    return "$Id:$";
  }

  public String getSource() {
    return "$Revision:$";
  }

  public String getDescription() {
    return "A Component for working with Term Enumerators";
  }
}
