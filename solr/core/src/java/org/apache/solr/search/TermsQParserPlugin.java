package org.apache.solr.search;

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

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldCacheTermsFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MultiTermQueryWrapperFilter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;

import java.util.Arrays;
import java.util.regex.Pattern;

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

  @Override
  public void init(NamedList args) {
  }

  private static enum Method {
    termsFilter {
      @Override
      Filter makeFilter(String fname, BytesRef[] bytesRefs) {
        return new TermsFilter(fname, bytesRefs);
      }
    },
    booleanQuery {
      @Override
      Filter makeFilter(String fname, BytesRef[] byteRefs) {
        BooleanQuery bq = new BooleanQuery(true);
        for (BytesRef byteRef : byteRefs) {
          bq.add(new TermQuery(new Term(fname, byteRef)), BooleanClause.Occur.SHOULD);
        }
        return new QueryWrapperFilter(bq);
      }
    },
    automaton {
      @Override
      Filter makeFilter(String fname, BytesRef[] byteRefs) {
        Automaton union = Automata.makeStringUnion(Arrays.asList(byteRefs));
        return new MultiTermQueryWrapperFilter<AutomatonQuery>(new AutomatonQuery(new Term(fname), union)) {
        };
      }
    },
    docValuesTermsFilter {//on 4x this is FieldCacheTermsFilter but we use the 5x name any way
      //note: limited to one val per doc
      @Override
      Filter makeFilter(String fname, BytesRef[] byteRefs) {
        return new FieldCacheTermsFilter(fname, byteRefs);
      }
    };

    abstract Filter makeFilter(String fname, BytesRef[] byteRefs);
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

        //if space then split on all whitespace & trim, otherwise strictly interpret
        final boolean sepIsSpace = separator.equals(" ");
        if (sepIsSpace)
          qstr = qstr.trim();
        if (qstr.length() == 0)
          return new BooleanQuery();//Matches nothing.
        final String[] splitVals = sepIsSpace ? qstr.split("\\s+") : qstr.split(Pattern.quote(separator), -1);
        assert splitVals.length > 0;

        BytesRef[] bytesRefs = new BytesRef[splitVals.length];
        BytesRef term = new BytesRef();
        for (int i = 0; i < splitVals.length; i++) {
          String stringVal = splitVals[i];
          //logic same as TermQParserPlugin
          if (ft != null) {
            ft.readableToIndexed(stringVal, term);
          } else {
            term.copyChars(stringVal);
          }
          bytesRefs[i] = BytesRef.deepCopyOf(term);
        }

        return new SolrConstantScoreQuery(method.makeFilter(fname, bytesRefs));
      }
    };
  }
}
