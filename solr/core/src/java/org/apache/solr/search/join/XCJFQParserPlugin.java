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

import java.util.HashSet;
import java.util.List;

import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;

/**
 * Cross-collection join filter.  Runs a query against a remote Solr collection to obtain a
 * set of join keys, then applies that set of join keys as a filter against the local collection.
 * <br>Example: {!xcjf collection="remoteCollection" from="fromField" to="toField" v="*:*"}
 */
public class XCJFQParserPlugin extends QParserPlugin {

  public static final String NAME = "xcjf";

  private String routerField;
  private HashSet<String> solrUrlWhitelist;

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new XCJFQParser(qstr, localParams, params, req, routerField, solrUrlWhitelist);
  }

  @Override
  public void init(NamedList args) {
    routerField = (String) args.get("routerField");
    solrUrlWhitelist = new HashSet<>();
    if (args.get("solrUrl") != null) {
      //noinspection unchecked
      for (String s : (List<String>) args.get("solrUrl")) {
        if (!StringUtils.isEmpty(s))
          solrUrlWhitelist.add(s);
      }
    } else {
      solrUrlWhitelist = null;
    }
  }

  @Override
  public String getName() {
    return NAME;
  }
}
