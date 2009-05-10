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
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.StrField;

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
      String lowerStr = params.get(TermsParams.TERMS_LOWER, null);
      String[] fields = params.getParams(TermsParams.TERMS_FIELD);
      if (fields != null && fields.length > 0) {
        NamedList terms = new NamedList();
        rb.rsp.add("terms", terms);
        int limit = params.getInt(TermsParams.TERMS_LIMIT, 10);
        if (limit < 0) {
          limit = Integer.MAX_VALUE;
        }
        String upperStr = params.get(TermsParams.TERMS_UPPER);
        boolean upperIncl = params.getBool(TermsParams.TERMS_UPPER_INCLUSIVE, false);
        boolean lowerIncl = params.getBool(TermsParams.TERMS_LOWER_INCLUSIVE, true);
        int freqmin = params.getInt(TermsParams.TERMS_MINCOUNT, 1); // initialize freqmin
        int freqmax = params.getInt(TermsParams.TERMS_MAXCOUNT, UNLIMITED_MAX_COUNT); // initialize freqmax
        if (freqmax<0) {
          freqmax = Integer.MAX_VALUE;
        }
        String prefix = params.get(TermsParams.TERMS_PREFIX_STR);
        boolean raw = params.getBool(TermsParams.TERMS_RAW, false);
        for (int j = 0; j < fields.length; j++) {
          String field = fields[j].intern();
          FieldType ft = raw ? null : rb.req.getSchema().getFieldTypeNoEx(field);
          if (ft==null) ft = new StrField();

          // If no lower bound was specified, use the prefix
          String lower = lowerStr==null ? prefix : (raw ? lowerStr : ft.toInternal(lowerStr));
          if (lower == null) lower="";
          String upper = upperStr==null ? null : (raw ? upperStr : ft.toInternal(upperStr));

          Term lowerTerm = new Term(field, lower);
          Term upperTerm = upper==null ? null : new Term(field, upper);
          
          TermEnum termEnum = rb.req.getSearcher().getReader().terms(lowerTerm); //this will be positioned ready to go
          int i = 0;
          NamedList fieldTerms = new NamedList();
          terms.add(field, fieldTerms);
          Term lowerTestTerm = termEnum.term();

          //Only advance the enum if we are excluding the lower bound and the lower Term actually matches
          if (lowerTestTerm!=null && lowerIncl == false && lowerTestTerm.field() == field  // intern'd comparison
                  && lowerTestTerm.text().equals(lower)) {
            termEnum.next();
          }

          while (i<limit) {

            Term theTerm = termEnum.term();

            // check for a different field, or the end of the index.
            if (theTerm==null || field != theTerm.field())  // intern'd comparison
              break;

            String indexedText = theTerm.text();

            // stop if the prefix doesn't match
            if (prefix != null && !indexedText.startsWith(prefix)) break;

            if (upperTerm != null) {
              int upperCmp = theTerm.compareTo(upperTerm);
              // if we are past the upper term, or equal to it (when don't include upper) then stop.
              if (upperCmp>0 || (upperCmp==0 && !upperIncl)) break;
            }

            // This is a good term in the range.  Check if mincount/maxcount conditions are satisfied.
            int docFreq = termEnum.docFreq();
            if (docFreq >= freqmin && docFreq <= freqmax) {
              // add the term to the list
              String label = raw ? indexedText : ft.indexedToReadable(indexedText);
              fieldTerms.add(label, docFreq);
              i++;
            }

            termEnum.next();
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
    return "$Revision$";
  }

  public String getSourceId() {
    return "$Id$";
  }

  public String getSource() {
    return "$URL$";
  }

  public String getDescription() {
    return "A Component for working with Term Enumerators";
  }
}
