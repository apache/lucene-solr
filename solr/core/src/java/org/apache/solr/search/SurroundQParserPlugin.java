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
import org.apache.solr.request.SolrQueryRequest;

import java.lang.invoke.MethodHandles;

import org.apache.lucene.queryparser.surround.parser.*;
import org.apache.lucene.queryparser.surround.query.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plugin for lucene/contrib Surround query parser, bringing SpanQuery support
 * to Solr.
 * <p>
 * &lt;queryParser name="surround"
 * class="org.apache.solr.search.SurroundQParserPlugin" /&gt;
 * <p>
 * Note that the query string is not analyzed in any way
 * 
 * @see QueryParser
 * @since 4.0
 */

public class SurroundQParserPlugin extends QParserPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String NAME = "surround";

  @Override
  public QParser createParser(String qstr, SolrParams localParams,
      SolrParams params, SolrQueryRequest req) {
    return new SurroundQParser(qstr, localParams, params, req);
  }

  static class SurroundQParser extends QParser {
    protected static final Logger LOG = SurroundQParserPlugin.log;
    static final int DEFMAXBASICQUERIES = 1000;
    static final String MBQParam = "maxBasicQueries";
    
    String sortStr;
    SolrQueryParser lparser;
    int maxBasicQueries;

    public SurroundQParser(String qstr, SolrParams localParams,
        SolrParams params, SolrQueryRequest req) {
      super(qstr, localParams, params, req);
    }

    @Override
    public Query parse()
        throws SyntaxError {
      SrndQuery sq;
      String qstr = getString();
      if (qstr == null)
        return null;
      String mbqparam = getParam(MBQParam);
      if (mbqparam == null) {
        this.maxBasicQueries = DEFMAXBASICQUERIES;
      } else {
        try {
          this.maxBasicQueries = Integer.parseInt(mbqparam);
        } catch (Exception e) {
          log.warn("Couldn't parse maxBasicQueries value {}, using default of 1000", mbqparam);
          this.maxBasicQueries = DEFMAXBASICQUERIES;
        }
      }
      // ugh .. colliding ParseExceptions
      try {
        sq = org.apache.lucene.queryparser.surround.parser.QueryParser
            .parse(qstr);
      } catch (org.apache.lucene.queryparser.surround.parser.ParseException pe) {
        throw new SyntaxError(pe);
      }
      
      // so what do we do with the SrndQuery ??
      // processing based on example in LIA Ch 9

      BasicQueryFactory bqFactory = new BasicQueryFactory(this.maxBasicQueries);
      String defaultField = getParam(CommonParams.DF);
      Query lquery = sq.makeLuceneQueryField(defaultField, bqFactory);
      return lquery;
    }
  }
}
