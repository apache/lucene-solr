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
package org.apache.solr.search.join;

import org.apache.lucene.search.join.ScoreMode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;

/**
 * Usage: {!parent which="PARENT:true"}CHILD_PRICE:10
 * supports optional <code>score</code> parameter with one of {@link ScoreMode} values:
 *  None,Avg,Total,Min,Max. Lowercase is also accepted.
 **/
public class BlockJoinParentQParserPlugin extends QParserPlugin {
  public static final String NAME = "parent";

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    QParser parser = createBJQParser(qstr, localParams, params, req);
    return parser;
  }

  protected QParser createBJQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new BlockJoinParentQParser(qstr, localParams, params, req);
  }
}

