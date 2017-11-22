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

import org.apache.lucene.search.Query;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;

/**
 * @see LuceneQParserPlugin
 */
public class LuceneQParser extends QParser {
  SolrQueryParser lparser;

  public LuceneQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }


  @Override
  public Query parse() throws SyntaxError {
    String qstr = getString();
    if (qstr == null || qstr.length()==0) return null;

    String defaultField = getParam(CommonParams.DF);
    lparser = new SolrQueryParser(this, defaultField);

    lparser.setDefaultOperator(QueryParsing.parseOP(getParam(QueryParsing.OP)));
    lparser.setSplitOnWhitespace(StrUtils.parseBool
      (getParam(QueryParsing.SPLIT_ON_WHITESPACE), SolrQueryParser.DEFAULT_SPLIT_ON_WHITESPACE));
    lparser.setAllowSubQueryParsing(true);

    return lparser.parse(qstr);
  }


  @Override
  public String[] getDefaultHighlightFields() {
    return lparser == null ? new String[]{} : new String[]{lparser.getDefaultField()};
  }

}
