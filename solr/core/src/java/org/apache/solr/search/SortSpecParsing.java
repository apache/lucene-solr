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
package org.apache.solr.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

public class SortSpecParsing {
  
  public static final String DOCID = "_docid_";
  public static final String SCORE = "score";

  /**
   * <p>
   * The form of the sort specification string currently parsed is:
   * </p>
   * <pre>
   * SortSpec ::= SingleSort [, SingleSort]*
   * SingleSort ::= &lt;fieldname|function&gt; SortDirection
   * SortDirection ::= top | desc | bottom | asc
   * </pre>
   * Examples:
   * <pre>
   *   score desc               #normal sort by score (will return null)
   *   weight bottom            #sort by weight ascending
   *   weight desc              #sort by weight descending
   *   height desc,weight desc  #sort by height descending, and use weight descending to break any ties
   *   height desc,weight asc   #sort by height descending, using weight ascending as a tiebreaker
   * </pre>
   * @return a SortSpec object populated with the appropriate Sort (which may be null if
   *         default score sort is used) and SchemaFields (where applicable) using
   *         hardcoded default count &amp; offset values.
   */
  public static SortSpec parseSortSpec(String sortSpec, SolrQueryRequest req) {
    return parseSortSpecImpl(sortSpec, req.getSchema(), req);
  }

  /**
   * <p>
   * The form of the (function free) sort specification string currently parsed is:
   * </p>
   * <pre>
   * SortSpec ::= SingleSort [, SingleSort]*
   * SingleSort ::= &lt;fieldname&gt; SortDirection
   * SortDirection ::= top | desc | bottom | asc
   * </pre>
   * Examples:
   * <pre>
   *   score desc               #normal sort by score (will return null)
   *   weight bottom            #sort by weight ascending
   *   weight desc              #sort by weight descending
   *   height desc,weight desc  #sort by height descending, and use weight descending to break any ties
   *   height desc,weight asc   #sort by height descending, using weight ascending as a tiebreaker
   * </pre>
   * @return a SortSpec object populated with the appropriate Sort (which may be null if 
   *         default score sort is used) and SchemaFields (where applicable) using 
   *         hardcoded default count &amp; offset values.
   */
  public static SortSpec parseSortSpec(String sortSpec, IndexSchema schema) {
    return parseSortSpecImpl(sortSpec, schema, null);
  }

  private static SortSpec parseSortSpecImpl(String sortSpec, IndexSchema schema,
      SolrQueryRequest optionalReq) {
    if (sortSpec == null || sortSpec.length() == 0) return newEmptySortSpec();

    List<SortField> sorts = new ArrayList<>(4);
    List<SchemaField> fields = new ArrayList<>(4);

    try {

      StrParser sp = new StrParser(sortSpec);
      while (sp.pos < sp.end) {
        sp.eatws();

        final int start = sp.pos;

        // short circuit test for a really simple field name
        String field = sp.getId(null);
        Exception qParserException = null;

        if ((field == null || !Character.isWhitespace(sp.peekChar())) && (optionalReq != null)) {
          // let's try it as a function instead
          field = null;
          String funcStr = sp.val.substring(start);

          QParser parser = QParser.getParser(funcStr, FunctionQParserPlugin.NAME, optionalReq);
          Query q = null;
          try {
            if (parser instanceof FunctionQParser) {
              FunctionQParser fparser = (FunctionQParser)parser;
              fparser.setParseMultipleSources(false);
              fparser.setParseToEnd(false);
              
              q = fparser.getQuery();
              
              if (fparser.localParams != null) {
                if (fparser.valFollowedParams) {
                  // need to find the end of the function query via the string parser
                  int leftOver = fparser.sp.end - fparser.sp.pos;
                  sp.pos = sp.end - leftOver;   // reset our parser to the same amount of leftover
                } else {
                  // the value was via the "v" param in localParams, so we need to find
                  // the end of the local params themselves to pick up where we left off
                  sp.pos = start + fparser.localParamsEnd;
                }
              } else {
                // need to find the end of the function query via the string parser
                int leftOver = fparser.sp.end - fparser.sp.pos;
                sp.pos = sp.end - leftOver;   // reset our parser to the same amount of leftover
              }
            } else {
              // A QParser that's not for function queries.
              // It must have been specified via local params.
              q = parser.getQuery();

              assert parser.getLocalParams() != null;
              sp.pos = start + parser.localParamsEnd;
            }

            Boolean top = sp.getSortDirection();
            if (null != top) {
              // we have a Query and a valid direction
              if (q instanceof FunctionQuery) {
                sorts.add(((FunctionQuery)q).getValueSource().getSortField(top));
              } else {
                sorts.add((new QueryValueSource(q, 0.0f)).getSortField(top));
              }
              fields.add(null);
              continue;
            }
          } catch (Exception e) {
            // hang onto this in case the string isn't a full field name either
            qParserException = e;
          }
        }

        // if we made it here, we either have a "simple" field name,
        // or there was a problem parsing the string as a complex func/quer

        if (field == null) {
          // try again, simple rules for a field name with no whitespace
          sp.pos = start;
          field = sp.getSimpleString();
        }
        Boolean top = sp.getSortDirection();
        if (null == top) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
                                    "Can't determine a Sort Order (asc or desc) in sort spec " + sp);
        }
        
        if (SCORE.equals(field)) {
          if (top) {
            sorts.add(SortField.FIELD_SCORE);
          } else {
            sorts.add(new SortField(null, SortField.Type.SCORE, true));
          }
          fields.add(null);
        } else if (DOCID.equals(field)) {
          sorts.add(new SortField(null, SortField.Type.DOC, top));
          fields.add(null);
        } else {
          // try to find the field
          SchemaField sf = schema.getFieldOrNull(field);
          if (null == sf) {
            if (null != qParserException) {
              throw new SolrException
                (SolrException.ErrorCode.BAD_REQUEST,
                 "sort param could not be parsed as a query, and is not a "+
                 "field that exists in the index: " + field,
                 qParserException);
            }
            throw new SolrException
              (SolrException.ErrorCode.BAD_REQUEST,
               "sort param field can't be found: " + field);
          }
          sorts.add(sf.getSortField(top));
          fields.add(sf);
        }
      }

    } catch (SyntaxError e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "error in sort: " + sortSpec, e);
    }


    // normalize a sort on score desc to null
    if (sorts.size()==1 && sorts.get(0) == SortField.FIELD_SCORE) {
      return newEmptySortSpec();
    }

    Sort s = new Sort(sorts.toArray(new SortField[sorts.size()]));
    return new SortSpec(s, fields);
  }

  private static SortSpec newEmptySortSpec() {
    return new SortSpec(null, Collections.<SchemaField>emptyList());
  }

}
