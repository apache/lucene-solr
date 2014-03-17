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

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.complexPhrase.ComplexPhraseQueryParser;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.parser.QueryParser;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Parse Solr's variant on the Lucene {@link org.apache.lucene.queryparser.complexPhrase.ComplexPhraseQueryParser} syntax.
 * <p/>
 * Modified from {@link org.apache.solr.search.LuceneQParserPlugin} and {@link org.apache.solr.search.SurroundQParserPlugin}
 */
public class ComplexPhraseQParserPlugin extends QParserPlugin {

  public static final String NAME = "complexphrase";

  private boolean inOrder = true;

  @Override
  public void init(NamedList args) {
    if (args != null) {
      Object val = args.get("inOrder");
      if (val != null) {
        inOrder = StrUtils.parseBool(val.toString());
      }
    }
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    ComplexPhraseQParser qParser = new ComplexPhraseQParser(qstr, localParams, params, req);
    qParser.setInOrder(inOrder);
    return qParser;
  }
}

/**
 * Modified from {@link org.apache.solr.search.LuceneQParser} and {@link org.apache.solr.search.SurroundQParser}
 */
class ComplexPhraseQParser extends QParser {

  ComplexPhraseQueryParser lparser;

  boolean inOrder = true;

  /**
   * When <code>inOrder</code> is true, the search terms must
   * exists in the documents as the same order as in query.
   *
   * @param inOrder parameter to choose between ordered or un-ordered proximity search
   */
  public void setInOrder(final boolean inOrder) {
    this.inOrder = inOrder;
  }

  public ComplexPhraseQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  @Override
  public Query parse() throws SyntaxError {
    String qstr = getString();

    String defaultField = getParam(CommonParams.DF);
    if (defaultField == null) {
      defaultField = getReq().getSchema().getDefaultSearchFieldName();
    }

    lparser = new ComplexPhraseQueryParser(getReq().getCore().getSolrConfig().luceneMatchVersion, defaultField, getReq().getSchema().getQueryAnalyzer());

    if (localParams != null)
      inOrder = localParams.getBool("inOrder", inOrder);

    lparser.setInOrder(inOrder);

    QueryParser.Operator defaultOperator = QueryParsing.getQueryParserDefaultOperator(getReq().getSchema(), getParam(QueryParsing.OP));

    if (QueryParser.Operator.AND.equals(defaultOperator))
      lparser.setDefaultOperator(org.apache.lucene.queryparser.classic.QueryParser.Operator.AND);
    else
      lparser.setDefaultOperator(org.apache.lucene.queryparser.classic.QueryParser.Operator.OR);

    try {
      return lparser.parse(qstr);
    } catch (ParseException pe) {
      throw new SyntaxError(pe);
    }
  }

  @Override
  public String[] getDefaultHighlightFields() {
    return lparser == null ? new String[]{} : new String[]{lparser.getField()};
  }
}

