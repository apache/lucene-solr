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

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Parse Solr's variant on the Lucene QueryParser syntax.
 * <br>Other parameters:<ul>
 * <li>q.op - the default operator "OR" or "AND"</li>
 * <li>df - the default field name</li>
 * <li>sow - split on whitespace prior to analysis, boolean,
 *           default=<code>{@value org.apache.solr.search.SolrQueryParser#DEFAULT_SPLIT_ON_WHITESPACE}</code></li>
 * </ul>
 * <br>Example: <code>{!lucene q.op=AND df=text sort='price asc'}myfield:foo +bar -baz</code>
 */
public class LuceneQParserPlugin extends QParserPlugin {
  public static final String NAME = "lucene";

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new LuceneQParser(qstr, localParams, params, req);
  }
}

