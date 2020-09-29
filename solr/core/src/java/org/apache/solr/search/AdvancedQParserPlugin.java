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

import java.lang.invoke.MethodHandles;

import org.apache.lucene.queryparser.surround.parser.QueryParser;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
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

public class AdvancedQParserPlugin extends QParserPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String NAME = "advanced";

  @Override
  public QParser createParser(String qstr, SolrParams localParams,
      SolrParams params, SolrQueryRequest req) {
    return new AdvancedQParser(qstr, localParams, params, req);
  }
}
