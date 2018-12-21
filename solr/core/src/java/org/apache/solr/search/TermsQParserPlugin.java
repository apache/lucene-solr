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

import java.util.Arrays;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocValuesTermsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.PointField;

/**
 * Finds documents whose specified field has any of the specified values. It's like
 * {@link TermQParserPlugin} but multi-valued, and supports a variety of internal algorithms.
 * <br>Parameters:
 * <br><code>f</code>: The field name (mandatory)
 * <br><code>separator</code>: the separator delimiting the values in the query string, defaulting to a comma.
 * If it's a " " then it splits on any consecutive whitespace.
 * <br><code>method</code>: Any of termsFilter (default), booleanQuery, automaton, docValuesTermsFilter.
 * <p>
 * Note that if no values are specified then the query matches no documents.
 */
public class TermsQParserPlugin extends QParserPlugin {
  public static final String NAME = "terms";

  /** The separator to use in the underlying suggester */
  public static final String SEPARATOR = "separator";

  /** Choose the internal algorithm */
  private static final String METHOD = "method";

  private static enum Method {
    termsFilter {
      @Override
      Query makeFilter(String fname, BytesRef[] bytesRefs) {
        return new TermInSetQuery(fname, bytesRefs);// constant scores
      }
    },
    booleanQuery {
      @Override
      Query makeFilter(String fname, BytesRef[] byteRefs) {
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        for (BytesRef byteRef : byteRefs) {
          bq.add(new TermQuery(new Term(fname, byteRef)), BooleanClause.Occur.SHOULD);
        }
        return new ConstantScoreQuery(bq.build());
      }
    },
    automaton {
      @Override
      Query makeFilter(String fname, BytesRef[] byteRefs) {
        ArrayUtil.timSort(byteRefs); // same sort algo as TermInSetQuery's choice
        Automaton union = Automata.makeStringUnion(Arrays.asList(byteRefs)); // input must be sorted
        return new AutomatonQuery(new Term(fname), union);//constant scores
      }
    },
    docValuesTermsFilter {//on 4x this is FieldCacheTermsFilter but we use the 5x name any way
      @Override
      Query makeFilter(String fname, BytesRef[] byteRefs) {
        return new DocValuesTermsQuery(fname, byteRefs);//constant scores
      }
    };

    abstract Query makeFilter(String fname, BytesRef[] byteRefs);
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        String fname = localParams.get(QueryParsing.F);
        FieldType ft = req.getSchema().getFieldTypeNoEx(fname);
        String separator = localParams.get(SEPARATOR, ",");
        String qstr = localParams.get(QueryParsing.V);//never null
        Method method = Method.valueOf(localParams.get(METHOD, Method.termsFilter.name()));
        //TODO pick the default method based on various heuristics from benchmarks
        //TODO pick the default using FieldType.getSetQuery

        //if space then split on all whitespace & trim, otherwise strictly interpret
        final boolean sepIsSpace = separator.equals(" ");
        if (sepIsSpace)
          qstr = qstr.trim();
        if (qstr.length() == 0)
          return new MatchNoDocsQuery();
        final String[] splitVals = sepIsSpace ? qstr.split("\\s+") : qstr.split(Pattern.quote(separator), -1);
        assert splitVals.length > 0;
        
        if (ft.isPointField()) {
          if (localParams.get(METHOD) != null) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Method '%s' not supported in TermsQParser when using PointFields", localParams.get(METHOD)));
          }
          return ((PointField)ft).getSetQuery(this, req.getSchema().getField(fname), Arrays.asList(splitVals));
        }

        BytesRef[] bytesRefs = new BytesRef[splitVals.length];
        BytesRefBuilder term = new BytesRefBuilder();
        for (int i = 0; i < splitVals.length; i++) {
          String stringVal = splitVals[i];
          //logic same as TermQParserPlugin
          if (ft != null) {
            ft.readableToIndexed(stringVal, term);
          } else {
            term.copyChars(stringVal);
          }
          bytesRefs[i] = term.toBytesRef();
        }

        return method.makeFilter(fname, bytesRefs);
      }
    };
  }
}
